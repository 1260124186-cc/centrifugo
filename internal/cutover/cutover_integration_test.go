//go:build integration

package cutover

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/pgtest"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type recordingDispatcher struct {
	mu      sync.Mutex
	methods []string
}

func (d *recordingDispatcher) DispatchCommand(_ context.Context, method string, _ []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.methods = append(d.methods, method)
	return nil
}

func (d *recordingDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.methods)
}

// TestCutoverEndToEnd drives a real Redis Stream and a real PostgreSQL
// outbox through the full cutover: stable Redis entries are recorded without
// dispatch, pending and outbox-only events are dispatched once, and a repeat
// run converges without duplicate work.
func TestCutoverEndToEnd(t *testing.T) {
	ctx := context.Background()
	stream := "cutover_stream_" + uuid.NewString()
	group := "cutover_group_" + uuid.NewString()
	consumer := "cutover_consumer_" + uuid.NewString()
	table := "cutover_outbox_" + uuid.NewString()[:8]

	// --- Redis setup: three entries, two acknowledged and one pending. ---
	redisCfg := configtypes.Redis{Address: []string{"localhost:6379"}}
	shards, err := redisshard.BuildRedisShards(redisCfg)
	require.NoError(t, err)
	require.Len(t, shards, 1)
	t.Cleanup(func() {
		_ = shards[0].RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(context.Background(), client.B().Del().Key(stream).Build())
		}).Error()
		shards[0].Close()
	})
	shard := shards[0]

	payload3 := `{"idempotency_key":"evt-3"}`
	for idx, identity := range []string{"evt-1", "evt-2", "evt-3"} {
		payload := fmt.Sprintf(`{"idempotency_key":%q}`, identity)
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(ctx, client.B().Arbitrary("XADD").Keys(stream).
				Args("*", "payload", payload, "method", "publish", "identity", identity).Build())
		})
		require.NoError(t, res.Error(), "XADD %d", idx)
	}
	require.NoError(t, shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().XgroupCreate().Key(stream).Group(group).Id("0").Mkstream().Build())
	}).Error())

	// Deliver every entry to the group, then acknowledge the first two so
	// only evt-3 remains in the pending entries list.
	read := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().Xreadgroup().Group(group, consumer).Count(10).
			Streams().Key(stream).Id(">").Build())
	})
	require.NoError(t, read.Error())
	xread, err := read.AsXRead()
	require.NoError(t, err)
	entries := xread[stream]
	require.Len(t, entries, 3)
	for _, entry := range entries[:2] {
		require.NoError(t, shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(ctx, client.B().Xack().Key(stream).Group(group).Id(entry.ID).Build())
		}).Error())
	}

	// --- PostgreSQL setup: an overlapping row and an outbox-only row. ---
	dsn := pgtest.SchemaDSN(t)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE %s (
    id BIGSERIAL PRIMARY KEY,
    method TEXT NOT NULL,
    payload BYTEA NOT NULL,
    partition INTEGER NOT NULL,
    identity TEXT NOT NULL
)`, table))
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (method, payload, partition, identity) VALUES ($1,$2,$3,$4),($5,$6,$7,$8)", table),
		"publish", []byte(payload3), 0, "evt-3",
		"publish", []byte(`{"idempotency_key":"evt-4"}`), 0, "evt-4",
	)
	require.NoError(t, err)

	consumerConfigs := []configtypes.Consumer{
		{Name: "redis1", Type: configtypes.ConsumerTypeRedisStream, Enabled: true, RedisStream: configtypes.RedisStreamConsumerConfig{
			Redis:         redisCfg,
			Streams:       []string{stream},
			ConsumerGroup: group,
			PayloadValue:  "payload",
			MethodValue:   "method",
		}},
		{Name: "pg1", Type: configtypes.ConsumerTypePostgres, Enabled: true, Postgres: configtypes.PostgresConsumerConfig{
			DSN:             dsn,
			OutboxTableName: table,
			NumPartitions:   1,
		}},
	}

	dispatcher := &recordingDispatcher{}
	newManager := func() *Manager {
		return NewManager(ManagerConfig{
			Consumers:  consumerConfigs,
			Dispatcher: dispatcher,
			NewStore: func(_ configtypes.PostgresConsumerConfig) (Store, error) {
				return NewPostgresStore(pool), nil
			},
		})
	}

	opts := Options{
		Name:                   "e2e-" + uuid.NewString(),
		RedisConsumerName:      "redis1",
		PostgresConsumerName:   "pg1",
		RedisIdentityValue:     "identity",
		PostgresIdentityColumn: "identity",
		BatchLimit:             1, // force many small, resumable batches
	}

	mgr := newManager()
	state, err := mgr.Run(ctx, opts)
	require.NoError(t, err)
	require.True(t, state.Complete(), "cutover should converge: %+v", state)
	require.Equal(t, PhaseCommitted, state.Phase)

	// evt-1/evt-2 were stably delivered by Redis; evt-3 and evt-4 are the
	// only dispatches, each executed exactly once.
	require.Equal(t, 2, dispatcher.count())
	require.Equal(t, int64(2), state.Delivered)
	require.Equal(t, int64(3), state.Skipped, "evt-1/evt-2 stable, evt-3 overlap dedup")

	// The recorded stop and start points match the live source frontiers.
	require.Equal(t, entries[2].ID, state.RedisPoints[0].StopID)
	require.Equal(t, entries[2].ID, state.RedisPoints[0].LastScannedID)
	require.Equal(t, int64(2), state.PostgresPoints[0].StartID)
	require.Equal(t, int64(2), state.PostgresPoints[0].LastScannedID)

	// Repeating the committed cutover is a no-op even from a fresh manager.
	mgr2 := newManager()
	state2, err := mgr2.Run(ctx, opts)
	require.NoError(t, err)
	require.True(t, state2.Complete())
	require.Equal(t, state.Version, state2.Version)
	require.Equal(t, 2, dispatcher.count(), "no duplicate dispatch after repeat")
}

// TestCutoverRestartResumesOnRealSources verifies that a second manager
// instance picks up an interrupted cutover from the persisted progress.
func TestCutoverRestartResumesOnRealSources(t *testing.T) {
	ctx := context.Background()
	stream := "cutover_stream_" + uuid.NewString()
	group := "cutover_group_" + uuid.NewString()
	consumer := "cutover_consumer_" + uuid.NewString()
	table := "cutover_outbox_" + uuid.NewString()[:8]

	redisCfg := configtypes.Redis{Address: []string{"localhost:6379"}}
	shards, err := redisshard.BuildRedisShards(redisCfg)
	require.NoError(t, err)
	require.Len(t, shards, 1)
	t.Cleanup(func() {
		_ = shards[0].RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(context.Background(), client.B().Del().Key(stream).Build())
		}).Error()
		shards[0].Close()
	})
	shard := shards[0]
	for _, identity := range []string{"evt-1", "evt-2"} {
		res := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(ctx, client.B().Arbitrary("XADD").Keys(stream).
				Args("*", "payload", fmt.Sprintf(`{"idempotency_key":%q}`, identity),
					"method", "publish", "identity", identity).Build())
		})
		require.NoError(t, res.Error())
	}
	require.NoError(t, shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().XgroupCreate().Key(stream).Group(group).Id("0").Mkstream().Build())
	}).Error())
	read := shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().Xreadgroup().Group(group, consumer).Count(10).
			Streams().Key(stream).Id(">").Build())
	})
	require.NoError(t, read.Error())
	xread, err := read.AsXRead()
	require.NoError(t, err)
	readEntries := xread[stream]
	require.Len(t, readEntries, 2)
	// Both stay pending: the cutover must reconcile and dispatch them.

	dsn := pgtest.SchemaDSN(t)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id BIGSERIAL PRIMARY KEY, method TEXT NOT NULL, payload BYTEA NOT NULL,
		partition INTEGER NOT NULL, identity TEXT NOT NULL)`, table))
	require.NoError(t, err)

	consumerConfigs := []configtypes.Consumer{
		{Name: "redis1", Type: configtypes.ConsumerTypeRedisStream, Enabled: true, RedisStream: configtypes.RedisStreamConsumerConfig{
			Redis: redisCfg, Streams: []string{stream}, ConsumerGroup: group,
			PayloadValue: "payload", MethodValue: "method",
		}},
		{Name: "pg1", Type: configtypes.ConsumerTypePostgres, Enabled: true, Postgres: configtypes.PostgresConsumerConfig{
			DSN: dsn, OutboxTableName: table, NumPartitions: 1,
		}},
	}
	dispatcher := &recordingDispatcher{}
	newManager := func() *Manager {
		return NewManager(ManagerConfig{
			Consumers:  consumerConfigs,
			Dispatcher: dispatcher,
			NewStore: func(_ configtypes.PostgresConsumerConfig) (Store, error) {
				return NewPostgresStore(pool), nil
			},
		})
	}
	opts := Options{
		Name:                 "restart-" + uuid.NewString(),
		RedisConsumerName:    "redis1",
		PostgresConsumerName: "pg1",
		RedisIdentityValue:   "identity",
		BatchLimit:           1,
		MaxEvents:            1,
	}

	mgr1 := newManager()
	var state *State
	for i := 0; i < 10; i++ {
		state, err = mgr1.Run(ctx, opts)
		require.NoError(t, err)
		if state.Complete() {
			break
		}
	}
	require.True(t, state.Complete())
	require.Equal(t, 2, dispatcher.count())

	// A simulated restart must not re-deliver anything.
	mgr2 := newManager()
	state2, err := mgr2.Run(ctx, opts)
	require.NoError(t, err)
	require.True(t, state2.Complete())
	require.Equal(t, 2, dispatcher.count())
}
