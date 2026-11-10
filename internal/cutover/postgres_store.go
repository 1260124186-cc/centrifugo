package cutover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	stateTableName     = "centrifugo_cutover_state"
	deliveredTableName = "centrifugo_cutover_delivered"
)

// cutoverSchema is idempotent: it is applied on every manager run so no
// separate migration step is required, matching the outbox consumer
// conventions of this repository.
const cutoverSchema = `
CREATE TABLE IF NOT EXISTS ` + stateTableName + ` (
    name        TEXT PRIMARY KEY,
    version     BIGINT NOT NULL,
    phase       TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    spec        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ` + deliveredTableName + ` (
    cutover_name  TEXT NOT NULL,
    identity      TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    status        TEXT NOT NULL,
    source_kind   TEXT NOT NULL,
    offset_str    TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cutover_name, identity)
);

CREATE INDEX IF NOT EXISTS centrifugo_cutover_delivered_status_idx
    ON ` + deliveredTableName + ` (cutover_name, status);
`

// PostgresStore persists cutover progress in the PostgreSQL database of the
// new source. Progress updates are optimistic transactions keyed on the
// state version: a failed or interrupted attempt never overwrites the last
// committed progress.
type PostgresStore struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewPostgresStore creates a store bound to pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool, now: time.Now}
}

// SetNow overrides the clock. Tests use it to get deterministic timestamps.
func (s *PostgresStore) SetNow(now func() time.Time) {
	s.now = now
}

// EnsureSchema implements Store.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, cutoverSchema); err != nil {
		return fmt.Errorf("ensure cutover schema: %w", err)
	}
	return nil
}

// GetState implements Store.
func (s *PostgresStore) GetState(ctx context.Context, name string) (*State, error) {
	var (
		spec    []byte
		version int64
		phase   string
		reason  string
	)
	err := s.pool.QueryRow(ctx,
		"SELECT spec, version, phase, reason FROM "+stateTableName+" WHERE name = $1", name,
	).Scan(&spec, &version, &phase, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get cutover state: %w", err)
	}
	state := &State{}
	if err := json.Unmarshal(spec, state); err != nil {
		return nil, fmt.Errorf("decode cutover state: %w", err)
	}
	// The columns are authoritative: FailState updates phase and reason
	// without rewriting the JSON spec so checkpoints stay byte-identical.
	state.Version = version
	state.Phase = phase
	state.Reason = reason
	return state, nil
}

// CreateState implements Store.
func (s *PostgresStore) CreateState(ctx context.Context, state *State) error {
	if state.Version == 0 {
		state.Version = 1
	}
	now := s.now()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
	spec, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode cutover state: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		"INSERT INTO "+stateTableName+" (name, version, phase, reason, spec, created_at, updated_at)"+
			" VALUES ($1, $2, $3, $4, $5, $6, $7)",
		state.Name, state.Version, state.Phase, state.Reason, spec, state.CreatedAt, state.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("create cutover state: %w", err)
	}
	return nil
}

// UpdateState implements Store.
func (s *PostgresStore) UpdateState(ctx context.Context, state *State) error {
	state.UpdatedAt = s.now()
	spec, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode cutover state: %w", err)
	}
	tag, err := s.pool.Exec(ctx,
		"UPDATE "+stateTableName+
			" SET version = version + 1, phase = $2, reason = $3, spec = $4, updated_at = $5"+
			" WHERE name = $1 AND version = $6",
		state.Name, state.Phase, state.Reason, spec, state.UpdatedAt, state.Version,
	)
	if err != nil {
		return fmt.Errorf("update cutover state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	state.Version++
	return nil
}

// FailState implements Store.
func (s *PostgresStore) FailState(ctx context.Context, name string, version int64, phase, reason string) error {
	tag, err := s.pool.Exec(ctx,
		"UPDATE "+stateTableName+
			" SET version = version + 1, phase = $2, reason = $3, updated_at = $4"+
			" WHERE name = $1 AND version = $5",
		name, phase, reason, s.now(), version,
	)
	if err != nil {
		return fmt.Errorf("fail cutover state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	return nil
}

// GetMarkers implements Store.
func (s *PostgresStore) GetMarkers(ctx context.Context, name string, identities []string) (map[string]Marker, error) {
	result := make(map[string]Marker, len(identities))
	if len(identities) == 0 {
		return result, nil
	}
	rows, err := s.pool.Query(ctx,
		"SELECT identity, content_hash, status, source_kind, offset_str FROM "+deliveredTableName+
			" WHERE cutover_name = $1 AND identity = ANY($2)",
		name, identities,
	)
	if err != nil {
		return nil, fmt.Errorf("get cutover markers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var marker Marker
		if err := rows.Scan(&marker.Identity, &marker.ContentHash, &marker.Status, &marker.SourceKind, &marker.Offset); err != nil {
			return nil, fmt.Errorf("scan cutover marker: %w", err)
		}
		result[marker.Identity] = marker
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cutover markers: %w", err)
	}
	return result, nil
}

// ReserveMarker implements Store.
func (s *PostgresStore) ReserveMarker(ctx context.Context, name string, marker Marker) (bool, Marker, error) {
	marker.Status = markerStatusReserved
	tag, err := s.pool.Exec(ctx,
		"INSERT INTO "+deliveredTableName+
			" (cutover_name, identity, content_hash, status, source_kind, offset_str, updated_at)"+
			" VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (cutover_name, identity) DO NOTHING",
		name, marker.Identity, marker.ContentHash, marker.Status, marker.SourceKind, marker.Offset, s.now(),
	)
	if err != nil {
		return false, Marker{}, fmt.Errorf("reserve cutover marker: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, Marker{}, nil
	}
	existing, err := s.getMarker(ctx, name, marker.Identity)
	if err != nil {
		return false, Marker{}, err
	}
	return false, existing, nil
}

func (s *PostgresStore) getMarker(ctx context.Context, name, identity string) (Marker, error) {
	var marker Marker
	err := s.pool.QueryRow(ctx,
		"SELECT identity, content_hash, status, source_kind, offset_str FROM "+deliveredTableName+
			" WHERE cutover_name = $1 AND identity = $2",
		name, identity,
	).Scan(&marker.Identity, &marker.ContentHash, &marker.Status, &marker.SourceKind, &marker.Offset)
	if err != nil {
		return Marker{}, fmt.Errorf("load cutover marker: %w", err)
	}
	return marker, nil
}

// ConfirmMarker implements Store.
func (s *PostgresStore) ConfirmMarker(ctx context.Context, name, identity string) error {
	tag, err := s.pool.Exec(ctx,
		"UPDATE "+deliveredTableName+
			" SET status = $3, updated_at = $4 WHERE cutover_name = $1 AND identity = $2",
		name, identity, markerStatusDelivered, s.now(),
	)
	if err != nil {
		return fmt.Errorf("confirm cutover marker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ReleaseMarker implements Store.
func (s *PostgresStore) ReleaseMarker(ctx context.Context, name, identity string) error {
	_, err := s.pool.Exec(ctx,
		"DELETE FROM "+deliveredTableName+
			" WHERE cutover_name = $1 AND identity = $2 AND status = $3",
		name, identity, markerStatusReserved,
	)
	if err != nil {
		return fmt.Errorf("release cutover marker: %w", err)
	}
	return nil
}

// ReleaseReservations implements Store.
func (s *PostgresStore) ReleaseReservations(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx,
		"DELETE FROM "+deliveredTableName+" WHERE cutover_name = $1 AND status = $2",
		name, markerStatusReserved,
	)
	if err != nil {
		return fmt.Errorf("release cutover reservations: %w", err)
	}
	return nil
}

// CountReservations implements Store.
func (s *PostgresStore) CountReservations(ctx context.Context, name string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM "+deliveredTableName+" WHERE cutover_name = $1 AND status = $2",
		name, markerStatusReserved,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count cutover reservations: %w", err)
	}
	return count, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgerrcode.UniqueViolation
	}
	return false
}
