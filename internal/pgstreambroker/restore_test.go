//go:build integration

package pgstreambroker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/historyrestore"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

func publishTestMessages(t *testing.T, e *PostgresStreamBroker, channel string, n int) (string, []*centrifuge.Publication) {
	t.Helper()
	published := make([]*centrifuge.Publication, 0, n)
	var epoch string
	for i := 0; i < n; i++ {
		opts := centrifuge.PublishOptions{
			HistoryTTL:  10 * time.Minute,
			HistorySize: 100,
			Tags:        map[string]string{"idx": fmt.Sprintf("%d", i)},
		}
		if i == 0 {
			opts.ClientInfo = &centrifuge.ClientInfo{
				ClientID: "client-1",
				UserID:   "user-1",
				ConnInfo: []byte(`{"role":"admin"}`),
			}
		}
		res, err := e.Publish(channel, []byte(fmt.Sprintf(`{"i":%d}`, i)), opts)
		require.NoError(t, err)
		epoch = res.Epoch
		published = append(published, &centrifuge.Publication{
			Offset: res.Offset,
			Epoch:  res.Epoch,
			Data:   []byte(fmt.Sprintf(`{"i":%d}`, i)),
			Tags:   opts.Tags,
			Info:   opts.ClientInfo,
		})
	}
	return epoch, published
}

func readAllHistory(t *testing.T, e *PostgresStreamBroker, channel string) ([]*centrifuge.Publication, centrifuge.StreamPosition) {
	t.Helper()
	pubs, sp, err := e.History(channel, centrifuge.HistoryOptions{
		Filter:  centrifuge.HistoryFilter{Limit: -1},
		MetaTTL: time.Hour,
	})
	require.NoError(t, err)
	return pubs, sp
}

func requireSamePublications(t *testing.T, want []*centrifuge.Publication, got []*centrifuge.Publication) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Offset, got[i].Offset, "offset %d", i)
		require.Equal(t, want[i].Epoch, got[i].Epoch, "epoch %d", i)
		require.Equal(t, want[i].Data, got[i].Data, "data %d", i)
		require.Equal(t, want[i].Tags, got[i].Tags, "tags %d", i)
		if want[i].Info != nil {
			require.NotNil(t, got[i].Info, "info %d", i)
			require.Equal(t, want[i].Info.ClientID, got[i].Info.ClientID)
			require.Equal(t, want[i].Info.UserID, got[i].Info.UserID)
			require.Equal(t, want[i].Info.ConnInfo, got[i].Info.ConnInfo)
		}
	}
}

func TestPostgresStreamBroker_SnapshotRestoreFullCycle(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_full_cycle"

	epoch, published := publishTestMessages(t, e, channel, 5)

	// Snapshot captures every publication with position and client info.
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, channel, pkg.Channel)
	require.Equal(t, epoch, pkg.Epoch)
	require.EqualValues(t, 5, pkg.Offset)
	require.Len(t, pkg.Publications, 5)
	require.NoError(t, historyrestore.Validate(pkg))
	require.NotNil(t, pkg.Publications[0].Info)
	require.Equal(t, "client-1", pkg.Publications[0].Info.ClientID)
	require.Equal(t, map[string]string{"idx": "0"}, pkg.Publications[0].Tags)

	// Wipe the publications; meta (epoch, top) is kept by RemoveHistory.
	require.NoError(t, e.RemoveHistory(channel))
	pubs, _ := readAllHistory(t, e, channel)
	require.Empty(t, pubs)

	// Restore fills every missing publication at its original offset.
	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 5, res.Total)
	require.EqualValues(t, 5, res.Inserted)
	require.EqualValues(t, 0, res.AlreadyPresent)
	require.True(t, res.Complete)
	require.Equal(t, epoch, res.Position.Epoch)
	require.EqualValues(t, 5, res.Position.Offset)

	// The timeline is readable again by the original offsets.
	got, sp := readAllHistory(t, e, channel)
	requireSamePublications(t, published, got)
	require.Equal(t, epoch, sp.Epoch)
	require.EqualValues(t, 5, sp.Offset)

	// Re-applying the same package creates no duplicates.
	res, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Inserted)
	require.EqualValues(t, 5, res.AlreadyPresent)
	require.True(t, res.Complete)
	got, _ = readAllHistory(t, e, channel)
	require.Len(t, got, 5)
}

func TestPostgresStreamBroker_RestoreFillsOnlyMissing(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_partial"

	_, published := publishTestMessages(t, e, channel, 5)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	// First restore brings everything back.
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)

	// Simulate a partial loss: only offset 3 is gone.
	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE channel = $1 AND kind = 0 AND channel_offset = 3`,
		e.names.stream,
	), channel)
	require.NoError(t, err)
	pubs, _ := readAllHistory(t, e, channel)
	require.Len(t, pubs, 4)

	// Re-run fills the single gap and leaves the rest untouched.
	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Inserted)
	require.EqualValues(t, 4, res.AlreadyPresent)
	got, _ := readAllHistory(t, e, channel)
	requireSamePublications(t, published, got)
}

func TestPostgresStreamBroker_RestoreConflictRollsBack(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_conflict"

	_, _ = publishTestMessages(t, e, channel, 3)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)

	// Diverge the stream: offset 2 gets different content, offset 3 is removed.
	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET data = $1 WHERE channel = $2 AND kind = 0 AND channel_offset = 2`,
		e.names.stream,
	), e.dataParam([]byte(`{"i":999}`)), channel)
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE channel = $1 AND kind = 0 AND channel_offset = 3`,
		e.names.stream,
	), channel)
	require.NoError(t, err)

	// Restore must fail on the conflict and roll back: offset 3 stays missing.
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.ErrorIs(t, err, historyrestore.ErrConflict)

	pubs, _ := readAllHistory(t, e, channel)
	require.Len(t, pubs, 2)
	require.EqualValues(t, 2, pubs[1].Offset)
	require.Equal(t, []byte(`{"i":999}`), pubs[1].Data)
}

func TestPostgresStreamBroker_RestoreCorruptedPackage(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_corrupted"

	_, _ = publishTestMessages(t, e, channel, 3)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	// Damage a payload without recomputing the checksum.
	pkg.Publications[1].Data = []byte(`{"i":42}`)
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.ErrorIs(t, err, historyrestore.ErrCorrupted)

	pubs, _ := readAllHistory(t, e, channel)
	require.Empty(t, pubs)
}

func TestPostgresStreamBroker_RestoreDryRun(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_dry_run"

	_, _ = publishTestMessages(t, e, channel, 3)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{DryRun: true})
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Inserted)
	require.True(t, res.Complete)

	// Nothing was committed.
	pubs, _ := readAllHistory(t, e, channel)
	require.Empty(t, pubs)

	// A dry run also detects conflicts without writing.
	require.NoError(t, e.RemoveHistory(channel))
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET data = $1 WHERE channel = $2 AND kind = 0 AND channel_offset = 1`,
		e.names.stream,
	), e.dataParam([]byte(`{"i":999}`)), channel)
	require.NoError(t, err)
	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{DryRun: true})
	require.ErrorIs(t, err, historyrestore.ErrConflict)
}

func TestPostgresStreamBroker_RestoreEpochMismatch(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_epoch"

	epoch, _ := publishTestMessages(t, e, channel, 2)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, epoch, pkg.Epoch)

	// A different live epoch (e.g. meta recreated after expiry) is a conflict.
	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET epoch = 'another-epoch' WHERE channel = $1`,
		e.names.meta,
	), channel)
	require.NoError(t, err)

	_, err = e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.ErrorIs(t, err, historyrestore.ErrConflict)
}

func TestPostgresStreamBroker_RestoreResumesAcrossCalls(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_resume"

	_, published := publishTestMessages(t, e, channel, 5)
	full, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	// A first attempt only carries the first two records.
	partial := historyrestore.NewPackage(channel, full.Epoch, full.Offset, full.Publications[:2])
	res, err := e.RestoreHistory(ctx, channel, partial, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Inserted)
	pubs, _ := readAllHistory(t, e, channel)
	require.Len(t, pubs, 2)

	// Re-running with the full package only fills the remaining three.
	res, err = e.RestoreHistory(ctx, channel, full, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Inserted)
	require.EqualValues(t, 2, res.AlreadyPresent)
	require.True(t, res.Complete)
	got, sp := readAllHistory(t, e, channel)
	requireSamePublications(t, published, got)
	require.EqualValues(t, 5, sp.Offset)
}

func TestPostgresStreamBroker_RestorePreservesOnlineMessages(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_keeps_online"

	_, _ = publishTestMessages(t, e, channel, 5)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	// New messages published after the wipe keep growing the stream at offset 6.
	res6, err := e.Publish(channel, []byte(`{"i":6}`), centrifuge.PublishOptions{
		HistoryTTL: 10 * time.Minute, HistorySize: 100,
	})
	require.NoError(t, err)
	require.EqualValues(t, 6, res6.Offset)
	require.Equal(t, pkg.Epoch, res6.Epoch)

	// Restore fills 1..5 and leaves the online offset 6 untouched.
	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 5, res.Inserted)
	pubs, sp := readAllHistory(t, e, channel)
	require.Len(t, pubs, 6)
	require.EqualValues(t, 1, pubs[0].Offset)
	require.EqualValues(t, 6, pubs[5].Offset)
	require.EqualValues(t, 6, sp.Offset)
}

func TestPostgresStreamBroker_SnapshotEmptyChannel(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_empty_channel"

	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, channel, pkg.Channel)
	require.Empty(t, pkg.Publications)
	require.NoError(t, historyrestore.Validate(pkg))

	// Restoring an empty package is a no-op and never creates meta rows.
	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.True(t, res.Complete)
	require.EqualValues(t, 0, res.Total)

	var metaCount int
	require.NoError(t, e.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE channel = $1`, e.names.meta,
	), channel).Scan(&metaCount))
	require.Zero(t, metaCount)
}

func TestPostgresStreamBroker_RestoreWrongChannel(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()

	_, _ = publishTestMessages(t, e, "test_restore_src", 2)
	pkg, err := e.SnapshotHistory(ctx, "test_restore_src")
	require.NoError(t, err)

	_, err = e.RestoreHistory(ctx, "test_restore_dst", pkg, historyrestore.Options{})
	require.ErrorIs(t, err, historyrestore.ErrInvalid)
}

func TestPostgresStreamBroker_RestoreWriteFailureRollback(t *testing.T) {
	t.Parallel()
	e, _, _ := newTestPostgresStreamBroker(t)
	ctx := context.Background()
	channel := "test_restore_write_failure"

	_, _ = publishTestMessages(t, e, channel, 3)
	pkg, err := e.SnapshotHistory(ctx, channel)
	require.NoError(t, err)
	require.NoError(t, e.RemoveHistory(channel))

	// A canceled context fails the transaction; nothing may be partially
	// committed, so a later retry with a live context restores everything.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = e.RestoreHistory(canceledCtx, channel, pkg, historyrestore.Options{})
	require.Error(t, err)

	pubs, _ := readAllHistory(t, e, channel)
	require.Empty(t, pubs)

	res, err := e.RestoreHistory(ctx, channel, pkg, historyrestore.Options{})
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Inserted)
	pubs, _ = readAllHistory(t, e, channel)
	require.Len(t, pubs, 3)
}
