package cutover

import (
	"context"
	"fmt"
	"strconv"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSource reads the new PostgreSQL outbox source without deleting
// rows: the live outbox consumer keeps owning delivery while a cutover
// converges. Rows still present in the table are precisely the events the
// outbox consumer has not delivered yet.
type PostgresSource struct {
	config     configtypes.PostgresConsumerConfig
	identity   Options
	pool       *pgxpool.Pool
	ownsPool   bool
	descriptor SourceDescriptor
}

// NewPostgresSource builds a PostgresSource from the named consumer
// configuration. It owns the returned pool and closes it on Close.
func NewPostgresSource(cfg configtypes.PostgresConsumerConfig, identity Options) (*PostgresSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadOptions, err)
	}
	if !validSQLIdentifier(cfg.OutboxTableName) {
		return nil, fmt.Errorf("%w: invalid outbox table name %q", ErrBadOptions, cfg.OutboxTableName)
	}
	if identity.PostgresIdentityColumn != "" && !validSQLIdentifier(identity.PostgresIdentityColumn) {
		return nil, fmt.Errorf("%w: invalid identity column %q", ErrBadOptions, identity.PostgresIdentityColumn)
	}
	conf, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parse postgresql DSN: %v", ErrBadOptions, err)
	}
	if cfg.TLS.Enabled {
		tlsConfig, err := cfg.TLS.ToGoTLSConfig("cutover:" + identity.PostgresConsumerName)
		if err != nil {
			return nil, fmt.Errorf("%w: postgresql TLS config: %v", ErrBadOptions, err)
		}
		conf.ConnConfig.TLSConfig = tlsConfig
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("create postgresql pool: %w", err)
	}
	source, err := newPostgresSourceWithPool(cfg, identity, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	source.ownsPool = true
	return source, nil
}

// newPostgresSourceWithPool wraps an existing pool. The caller keeps pool
// ownership: Close is then a no-op, which lets the manager share one pool
// between the source and the progress store.
func newPostgresSourceWithPool(cfg configtypes.PostgresConsumerConfig, identity Options, pool *pgxpool.Pool) (*PostgresSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadOptions, err)
	}
	if !validSQLIdentifier(cfg.OutboxTableName) {
		return nil, fmt.Errorf("%w: invalid outbox table name %q", ErrBadOptions, cfg.OutboxTableName)
	}
	if identity.PostgresIdentityColumn != "" && !validSQLIdentifier(identity.PostgresIdentityColumn) {
		return nil, fmt.Errorf("%w: invalid identity column %q", ErrBadOptions, identity.PostgresIdentityColumn)
	}
	numPartitions := cfg.NumPartitions
	if numPartitions <= 0 {
		numPartitions = 1
	}
	descriptor := SourceDescriptor{
		Kind:         SourceKindPostgresOutbox,
		ConsumerName: identity.PostgresConsumerName,
		Locator:      cfg.OutboxTableName,
		Details: map[string]string{
			"dsn":              pool.Config().ConnConfig.Host + ":" + strconv.FormatUint(uint64(pool.Config().ConnConfig.Port), 10) + "/" + pool.Config().ConnConfig.Database,
			"outbox_table":     cfg.OutboxTableName,
			"num_partitions":   strconv.Itoa(numPartitions),
			"identity_column":  identity.PostgresIdentityColumn,
			"identity_payload": identity.IdentityPayloadField,
		},
	}
	return &PostgresSource{
		config:     cfg,
		identity:   identity,
		pool:       pool,
		descriptor: descriptor,
	}, nil
}

// Descriptor implements Source.
func (s *PostgresSource) Descriptor() SourceDescriptor { return s.descriptor }

// Ping implements Source.
func (s *PostgresSource) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("%w: postgresql ping: %v", ErrReadFailed, err)
	}
	return nil
}

// Frontier implements Source: the start point of every partition is the
// maximum outbox ID visible when the cutover is created.
func (s *PostgresSource) Frontier(ctx context.Context) ([]FrontierPoint, error) {
	numPartitions := s.config.NumPartitions
	if numPartitions <= 0 {
		numPartitions = 1
	}
	points := make([]FrontierPoint, 0, numPartitions)
	for partition := 0; partition < numPartitions; partition++ {
		var maxID int64
		err := s.pool.QueryRow(ctx,
			fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s WHERE partition = $1", s.config.OutboxTableName),
			partition,
		).Scan(&maxID)
		if err != nil {
			return nil, fmt.Errorf("%w: read outbox frontier partition %d: %v", ErrReadFailed, partition, err)
		}
		points = append(points, FrontierPoint{
			Partition: int64(partition),
			Offset:    strconv.FormatInt(maxID, 10),
		})
	}
	return points, nil
}

// Read implements Source. It returns the outbox rows still present between
// the cursor and the recorded start point, in ID order.
func (s *PostgresSource) Read(ctx context.Context, cursor Cursor, limit int) ([]Event, Cursor, error) {
	if limit <= 0 {
		limit = defaultBatchLimit
	}
	lastID, err := strconv.ParseInt(cursor.Offset, 10, 64)
	if err != nil {
		return nil, cursor, fmt.Errorf("%w: invalid postgres cursor %q: %v", ErrReadFailed, cursor.Offset, err)
	}
	boundID, err := strconv.ParseInt(cursor.Bound, 10, 64)
	if err != nil {
		return nil, cursor, fmt.Errorf("%w: invalid postgres bound %q: %v", ErrReadFailed, cursor.Bound, err)
	}

	sql := "SELECT id, method, payload FROM " + s.config.OutboxTableName +
		" WHERE partition = $1 AND id > $2 AND id <= $3 ORDER BY id ASC LIMIT $4"
	args := []any{cursor.Partition, lastID, boundID, limit}
	if s.identity.PostgresIdentityColumn != "" {
		sql = "SELECT id, method, payload, " + s.identity.PostgresIdentityColumn +
			" FROM " + s.config.OutboxTableName +
			" WHERE partition = $1 AND id > $2 AND id <= $3 ORDER BY id ASC LIMIT $4"
	}

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, cursor, fmt.Errorf("%w: select outbox partition %d: %v", ErrReadFailed, cursor.Partition, err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var (
			id      int64
			method  string
			payload []byte
		)
		if s.identity.PostgresIdentityColumn != "" {
			var nativeIdentity *string
			if err := rows.Scan(&id, &method, &payload, &nativeIdentity); err != nil {
				return nil, cursor, fmt.Errorf("%w: scan outbox event: %v", ErrReadFailed, err)
			}
			events = append(events, Event{
				SourceKind: SourceKindPostgresOutbox,
				Partition:  cursor.Partition,
				Offset:     strconv.FormatInt(id, 10),
				Method:     method,
				Payload:    payload,
				Fields:     map[string]string{s.identity.PostgresIdentityColumn: derefString(nativeIdentity)},
			})
			continue
		}
		if err := rows.Scan(&id, &method, &payload); err != nil {
			return nil, cursor, fmt.Errorf("%w: scan outbox event: %v", ErrReadFailed, err)
		}
		events = append(events, Event{
			SourceKind: SourceKindPostgresOutbox,
			Partition:  cursor.Partition,
			Offset:     strconv.FormatInt(id, 10),
			Method:     method,
			Payload:    payload,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, fmt.Errorf("%w: iterate outbox events: %v", ErrReadFailed, err)
	}
	next := cursor
	if len(events) > 0 {
		next.Offset = events[len(events)-1].Offset
	}
	return events, next, nil
}

// Identity implements Source.
func (s *PostgresSource) Identity(ev Event) (string, error) {
	if s.identity.PostgresIdentityColumn != "" {
		if value := ev.Fields[s.identity.PostgresIdentityColumn]; value != "" {
			return value, nil
		}
	}
	return payloadIdentity(ev.Payload, s.identity.IdentityPayloadField)
}

// Close implements Source. When the pool was injected by the manager it is
// shared with the progress store and closed by the manager instead.
func (s *PostgresSource) Close() error {
	if s.ownsPool {
		s.pool.Close()
	}
	return nil
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
