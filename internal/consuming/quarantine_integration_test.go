//go:build integration

package consuming

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/api"
	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/quarantine"

	"github.com/centrifugal/centrifuge"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// quarantineTestDSN allows pointing the integration test at a local PostgreSQL
// instance without the docker-compose setup. It falls back to the shared
// default used by the rest of the package.
func quarantineTestDSN() string {
	if dsn := os.Getenv("CENTRIFUGO_TEST_PG_DSN"); dsn != "" {
		return dsn
	}
	return testPGDSN
}

// TestPostgresConsumer_QuarantineAndReplay drives a real Postgres consumer
// through the real API dispatcher with the quarantine area enabled: a
// non-retryable failure is stored with its source and original payload, then
// inspected and replayed through the quarantine service.
func TestPostgresConsumer_QuarantineAndReplay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	dsn := quarantineTestDSN()
	tableName := "centrifugo_quarantine_test_" + strings.Replace(uuid.New().String(), "-", "_", -1)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(ctx, "CREATE TABLE "+tableName+` (
		id BIGSERIAL PRIMARY KEY,
		method text NOT NULL,
		payload BYTEA NOT NULL,
		partition INTEGER NOT NULL
	)`)
	require.NoError(t, err)

	// Real node with a memory engine and history enabled, so replay side
	// effects can be observed through the history API.
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	cfg = cfgContainer.Config()
	cfg.Channel.WithoutNamespace.HistorySize = 100
	cfg.Channel.WithoutNamespace.HistoryTTL = configtypes.Duration(time.Minute)
	require.NoError(t, cfgContainer.Reload(cfg))

	executor := api.NewExecutor(node, cfgContainer, nil, api.ExecutorConfig{Protocol: "consuming"})
	consumingHandler := api.NewConsumingHandler(node, executor, api.ConsumingHandlerConfig{})
	dispatcher := api.NewDispatcher(consumingHandler)
	store := quarantine.NewMemoryStore()
	dispatcher.SetQuarantineStore(store)
	service := quarantine.NewService(store, dispatcher)

	consumerName := "quarantine-" + strings.Replace(uuid.New().String(), "-", "_", -1)
	// Go through the production entry point so the source-context wrapper
	// created in New is exercised exactly as in the running server.
	consumers, err := New(node.ID(), dispatcher, []ConsumerConfig{{
		Name:    consumerName,
		Enabled: true,
		Type:    configtypes.ConsumerTypePostgres,
		Postgres: PostgresConfig{
			DSN:                   dsn,
			OutboxTableName:       tableName,
			PartitionSelectLimit:  10,
			NumPartitions:         1,
			PartitionPollInterval: configtypes.Duration(100 * time.Millisecond),
		},
	}})
	require.NoError(t, err)
	require.Len(t, consumers, 1)
	consumer := consumers[0]
	defer func() { _ = consumer.Close(context.Background()) }()

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- consumer.Run(ctx) }()

	// 1. A non-retryable failure lands in the quarantine area instead of
	// being only logged. The outbox row is acknowledged (deleted) as before.
	badPayload := []byte(`{"channel":"unknown:quarantine","data":{"n":1}}`)
	require.NoError(t, insertEvent(ctx, pool, tableName, "publish", badPayload, 0))

	var quarantined quarantine.Record
	require.Eventually(t, func() bool {
		records, _, listErr := store.List(ctx, quarantine.ListFilter{})
		if listErr != nil || len(records) != 1 {
			return false
		}
		quarantined = records[0]
		return true
	}, 15*time.Second, 100*time.Millisecond, "failed event must be quarantined")

	require.Equal(t, consumerName, quarantined.Consumer)
	require.Equal(t, configtypes.ConsumerTypePostgres, quarantined.ConsumerType)
	require.Equal(t, "publish", quarantined.Method)
	require.Equal(t, badPayload, quarantined.Payload)
	require.NotEmpty(t, quarantined.Error)
	require.Equal(t, quarantine.StatusQuarantined, quarantined.Status)

	// 2. A valid event keeps being processed normally and leaves the outbox.
	goodPayload := []byte(`{"channel":"test","data":{"n":2}}`)
	require.NoError(t, insertEvent(ctx, pool, tableName, "publish", goodPayload, 0))
	require.Eventually(t, func() bool {
		var count int
		if rowErr := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count); rowErr != nil {
			return false
		}
		return count == 0
	}, 15*time.Second, 100*time.Millisecond, "good event must be consumed and removed from outbox")

	history := executor.History(ctx, &apiproto.HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1)

	// 3. Replaying the still-broken event fails and keeps the record with its
	// real state; no new quarantine entry is created by the replay itself.
	report, err := service.Replay(ctx, []string{quarantined.ID}, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Failed)
	require.Zero(t, report.Replayed)
	kept, err := store.Get(ctx, quarantined.ID)
	require.NoError(t, err)
	require.Equal(t, quarantine.StatusQuarantined, kept.Status)
	require.Equal(t, 1, kept.Attempts)

	// 4. After the message is fixed (a corrected payload for the same source)
	// replay succeeds and removes only that record.
	fixed, err := store.Add(ctx, quarantine.Record{
		Consumer:     consumerName,
		ConsumerType: configtypes.ConsumerTypePostgres,
		Method:       "publish",
		Payload:      []byte(`{"channel":"test","data":{"n":3}}`),
	})
	require.NoError(t, err)
	report, err = service.Replay(ctx, []string{fixed.ID}, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Replayed)
	_, err = store.Get(ctx, fixed.ID)
	require.ErrorIs(t, err, quarantine.ErrNotFound)
	// The unselected, still-failing record is untouched.
	kept, err = store.Get(ctx, quarantined.ID)
	require.NoError(t, err)
	require.Equal(t, quarantine.StatusQuarantined, kept.Status)

	history = executor.History(ctx, &apiproto.HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 2)

	// 5. Replaying the same batch again produces no duplicate side effects:
	// the removed record is reported not_found and nothing is dispatched.
	report, err = service.Replay(ctx, []string{fixed.ID}, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.NotFound)
	require.Zero(t, report.Replayed)
	history = executor.History(ctx, &apiproto.HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 2)

	cancel()
	require.ErrorIs(t, <-consumerErr, context.Canceled)
}
