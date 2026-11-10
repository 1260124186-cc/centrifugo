package pgstreambroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/centrifugal/centrifugo/v6/internal/historyrestore"
	"github.com/centrifugal/centrifugo/v6/internal/pgshard"

	"github.com/centrifugal/centrifuge"
	"github.com/jackc/pgx/v5"
)

// Compile-time assertion: the PostgreSQL stream broker supports the history
// restore capability consumed by the history_snapshot/history_restore API.
var _ historyrestore.Broker = (*PostgresStreamBroker)(nil)

// SnapshotHistory builds a recovery package for channel. It reads the channel
// meta (epoch and top offset) and every publication row still retained for the
// current epoch, including rows outside the configured history_size window but
// not yet dropped by partition retention: a recovery package should capture
// everything physically present. A channel without a meta row yields an empty
// package, which restores as a no-op.
func (e *PostgresStreamBroker) SnapshotHistory(ctx context.Context, channel string) (*historyrestore.Package, error) {
	pool := e.getReadPool(channel, true)

	var topOffset int64
	var epoch string
	err := pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT top_offset, epoch FROM %s WHERE channel = $1`,
		e.names.meta,
	), channel).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return historyrestore.NewPackage(channel, "", 0, nil), nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres stream broker: snapshot meta: %w", err)
	}

	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT channel_offset, epoch, data, tags, client_id, user_id, conn_info, chan_info
		  FROM %s
		 WHERE channel = $1
		   AND kind = 0
		   AND epoch = $2
		 ORDER BY channel_offset ASC
	`, e.names.stream), channel, epoch)
	if err != nil {
		return nil, fmt.Errorf("postgres stream broker: snapshot query: %w", err)
	}
	defer rows.Close()

	records := make([]historyrestore.Record, 0)
	for rows.Next() {
		var (
			offset                          int64
			rowEpoch                        string
			data                            []byte
			tags                            map[string]string
			clientID, userID                *string
			connInfo, chanInfo              []byte
		)
		if err := rows.Scan(&offset, &rowEpoch, &data, &tags, &clientID, &userID, &connInfo, &chanInfo); err != nil {
			return nil, fmt.Errorf("postgres stream broker: snapshot scan: %w", err)
		}
		rec := historyrestore.Record{
			Offset: uint64(offset),
			Epoch:  rowEpoch,
			Data:   data,
			Tags:   tags,
		}
		if clientID != nil || userID != nil || len(connInfo) > 0 || len(chanInfo) > 0 {
			info := &centrifuge.ClientInfo{
				ConnInfo: connInfo,
				ChanInfo: chanInfo,
			}
			if clientID != nil {
				info.ClientID = *clientID
			}
			if userID != nil {
				info.UserID = *userID
			}
			rec.Info = info
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres stream broker: snapshot rows: %w", err)
	}

	return historyrestore.NewPackage(channel, epoch, uint64(topOffset), records), nil
}

// RestoreHistory re-inserts the package publications missing from channel at
// their original offsets. The whole operation runs in one transaction:
//
//   - the shard lock serializes restores against regular publishes on the
//     channel;
//   - an existing publication at a package offset with different content is a
//     conflict and aborts the transaction, so no partial state is visible;
//   - identical publications are counted as already present and left alone,
//     publications outside the package are never touched;
//   - the meta top offset only moves forward, to the maximum of its current
//     value and the package top.
//
// Because inserts are keyed by (channel, epoch, offset), re-applying the same
// package (or continuing after a crashed attempt) only fills what is still
// missing.
func (e *PostgresStreamBroker) RestoreHistory(ctx context.Context, channel string, pkg *historyrestore.Package, opts historyrestore.Options) (*historyrestore.Result, error) {
	if err := historyrestore.Validate(pkg); err != nil {
		return nil, err
	}
	if pkg.Channel != channel {
		return nil, fmt.Errorf("%w: package channel %q does not match target channel %q", historyrestore.ErrInvalid, pkg.Channel, channel)
	}

	result := &historyrestore.Result{Total: uint64(len(pkg.Publications))}
	if len(pkg.Publications) == 0 {
		// Nothing to write; report the live position when the channel exists.
		result.Complete = true
		if pos, err := e.streamTop(ctx, channel); err == nil {
			result.Position = pos
		}
		return result, nil
	}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres stream broker: restore begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	shardID := pgshard.Of(channel, e.conf.NumShards)
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`SELECT 1 FROM %s WHERE shard_id = $1 FOR UPDATE`,
		e.names.shardLock,
	), shardID); err != nil {
		return nil, fmt.Errorf("postgres stream broker: restore shard lock: %w", err)
	}

	var currentTop int64
	var currentEpoch string
	err = tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT top_offset, epoch FROM %s WHERE channel = $1 FOR UPDATE`,
		e.names.meta,
	), channel).Scan(&currentTop, &currentEpoch)
	switch {
	case err == nil:
		if currentEpoch != pkg.Epoch {
			return nil, fmt.Errorf(
				"%w: channel %q stream epoch %q differs from package epoch %q",
				historyrestore.ErrConflict, channel, currentEpoch, pkg.Epoch,
			)
		}
	case errors.Is(err, pgx.ErrNoRows):
		// The channel was fully forgotten: recreate its meta row under the
		// package epoch. Column defaults fill history TTL/size/expiration.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (channel, top_offset, epoch) VALUES ($1, $2, $3)`,
			e.names.meta,
		), channel, int64(pkg.Offset), pkg.Epoch); err != nil {
			return nil, fmt.Errorf("postgres stream broker: restore meta insert: %w", err)
		}
		currentTop = int64(pkg.Offset)
		currentEpoch = pkg.Epoch
	default:
		return nil, fmt.Errorf("postgres stream broker: restore meta: %w", err)
	}

	for _, rec := range pkg.Publications {
		inserted, err := e.restoreRecord(ctx, tx, channel, rec)
		if err != nil {
			return nil, err
		}
		if inserted {
			result.Inserted++
		} else {
			result.AlreadyPresent++
		}
	}

	targetTop := int64(pkg.Offset)
	if currentTop > targetTop {
		targetTop = currentTop
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET top_offset = $2, updated_at = NOW() WHERE channel = $1`,
		e.names.meta,
	), channel, targetTop); err != nil {
		return nil, fmt.Errorf("postgres stream broker: restore meta update: %w", err)
	}

	result.Position = centrifuge.StreamPosition{Offset: uint64(targetTop), Epoch: currentEpoch}
	result.Complete = result.Inserted+result.AlreadyPresent == result.Total

	if opts.DryRun {
		// Explicit rollback: checks ran under the same locks as a real
		// restore, but nothing must be persisted.
		return result, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres stream broker: restore commit: %w", err)
	}
	committed = true
	return result, nil
}

// restoreRecord inserts one package record or verifies the stored one. It
// returns true when a row was inserted, false when an identical row already
// existed. Any content mismatch is reported as a conflict, which aborts the
// surrounding transaction.
func (e *PostgresStreamBroker) restoreRecord(ctx context.Context, tx pgx.Tx, channel string, rec historyrestore.Record) (bool, error) {
	var (
		data                         []byte
		tags                         map[string]string
		clientID, userID             *string
		connInfo, chanInfo           []byte
	)
	err := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT data, tags, client_id, user_id, conn_info, chan_info
		  FROM %s
		 WHERE channel = $1
		   AND kind = 0
		   AND epoch = $2
		   AND channel_offset = $3
	`, e.names.stream), channel, rec.Epoch, int64(rec.Offset)).
		Scan(&data, &tags, &clientID, &userID, &connInfo, &chanInfo)
	if errors.Is(err, pgx.ErrNoRows) {
		var insertClientID, insertUserID *string
		var insertConnInfo, insertChanInfo any
		if rec.Info != nil {
			if rec.Info.ClientID != "" {
				id := rec.Info.ClientID
				insertClientID = &id
			}
			if rec.Info.UserID != "" {
				uid := rec.Info.UserID
				insertUserID = &uid
			}
			insertConnInfo = e.dataParam(rec.Info.ConnInfo)
			insertChanInfo = e.dataParam(rec.Info.ChanInfo)
		}
		var tagsArg any
		if len(rec.Tags) > 0 {
			b, marshalErr := json.Marshal(rec.Tags)
			if marshalErr != nil {
				return false, fmt.Errorf("postgres stream broker: restore marshal tags: %w", marshalErr)
			}
			tagsArg = json.RawMessage(b)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				channel, channel_offset, epoch, kind, data, tags,
				client_id, user_id, conn_info, chan_info, shard_id
			) VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9, $10)
		`, e.names.stream),
			channel, int64(rec.Offset), rec.Epoch, e.dataParam(rec.Data), tagsArg,
			insertClientID, insertUserID, insertConnInfo, insertChanInfo,
			pgshard.Of(channel, e.conf.NumShards),
		); err != nil {
			return false, fmt.Errorf("postgres stream broker: restore insert at offset %d: %w", rec.Offset, err)
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("postgres stream broker: restore select at offset %d: %w", rec.Offset, err)
	}

	// The offset is occupied: identical content is an idempotent no-op, any
	// difference is a conflict that fails the whole restore.
	if !e.dataEqual(data, rec.Data) ||
		!historyrestore.EqualTags(tags, rec.Tags) ||
		!clientInfoEqual(clientID, userID, connInfo, chanInfo, rec.Info) {
		return false, fmt.Errorf(
			"%w: channel %q already stores a different publication at offset %d",
			historyrestore.ErrConflict, channel, rec.Offset,
		)
	}
	return false, nil
}

// dataEqual compares stored and package payloads. BYTEA columns are compared
// byte-for-byte; JSONB columns normalize whitespace and key order on write, so
// valid JSON payloads are compared semantically there to avoid false
// conflicts on formatting differences.
func (e *PostgresStreamBroker) dataEqual(stored, incoming []byte) bool {
	if equalBytes(stored, incoming) {
		return true
	}
	if e.conf.BinaryData {
		return false
	}
	var a, b any
	if err := json.Unmarshal(stored, &a); err != nil {
		return false
	}
	if err := json.Unmarshal(incoming, &b); err != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// clientInfoEqual compares a scanned client info row with a package record's
// client info. A NULL info row matches a nil record info.
func clientInfoEqual(clientID, userID *string, connInfo, chanInfo []byte, want *centrifuge.ClientInfo) bool {
	if want == nil {
		return clientID == nil && userID == nil && len(connInfo) == 0 && len(chanInfo) == 0
	}
	if (clientID == nil && want.ClientID != "") || (clientID != nil && *clientID != want.ClientID) {
		return false
	}
	if (userID == nil && want.UserID != "") || (userID != nil && *userID != want.UserID) {
		return false
	}
	return equalBytes(connInfo, want.ConnInfo) && equalBytes(chanInfo, want.ChanInfo)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// streamTop returns the current stream position of channel, or a zero
// position when it has no meta row.
func (e *PostgresStreamBroker) streamTop(ctx context.Context, channel string) (centrifuge.StreamPosition, error) {
	var topOffset int64
	var epoch string
	err := e.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT top_offset, epoch FROM %s WHERE channel = $1`,
		e.names.meta,
	), channel).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return centrifuge.StreamPosition{}, nil
	}
	if err != nil {
		return centrifuge.StreamPosition{}, err
	}
	return centrifuge.StreamPosition{Offset: uint64(topOffset), Epoch: epoch}, nil
}
