package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/quarantine"

	"github.com/stretchr/testify/require"
)

type quarantineFixture struct {
	executor   *Executor
	dispatcher *Dispatcher
	store      *quarantine.MemoryStore
	service    *quarantine.Service
}

func newQuarantineFixture(t *testing.T, enableHistory bool) (*quarantineFixture, func()) {
	t.Helper()
	n := nodeWithMemoryEngine()

	cfg := config.DefaultConfig()
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	if enableHistory {
		cfg = cfgContainer.Config()
		cfg.Channel.WithoutNamespace.HistorySize = 100
		cfg.Channel.WithoutNamespace.HistoryTTL = configtypes.Duration(time.Minute)
		require.NoError(t, cfgContainer.Reload(cfg))
	}

	executor := NewExecutor(n, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test"})
	consumingHandler := NewConsumingHandler(n, executor, ConsumingHandlerConfig{})
	dispatcher := NewDispatcher(consumingHandler)
	store := quarantine.NewMemoryStore()
	dispatcher.SetQuarantineStore(store)
	service := quarantine.NewService(store, dispatcher)
	executor.SetQuarantineService(service)

	cleanup := func() {
		_ = n.Shutdown(context.Background())
	}
	return &quarantineFixture{
		executor:   executor,
		dispatcher: dispatcher,
		store:      store,
		service:    service,
	}, cleanup
}

func sourceCtx() context.Context {
	return ContextWithConsumerSource(context.Background(), ConsumerSource{Name: "outbox", Type: "postgresql"})
}

func TestDispatcherQuarantinesNonRetryableError(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()

	payload := []byte(`{"channel":"unknown:news","data":{"hello":1}}`)
	err := fixture.dispatcher.DispatchCommand(sourceCtx(), "publish", payload)
	require.NoError(t, err, "non-retryable errors stay swallowed for consumers")

	records, _, err := fixture.store.List(context.Background(), quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, "outbox", record.Consumer)
	require.Equal(t, "postgresql", record.ConsumerType)
	require.Equal(t, "publish", record.Method)
	require.Equal(t, payload, record.Payload)
	require.NotEmpty(t, record.Error)
	require.Equal(t, quarantine.StatusQuarantined, record.Status)
	require.NotZero(t, record.CreatedAt)
}

func TestDispatcherWithoutStoreKeepsLegacyBehavior(t *testing.T) {
	n := nodeWithMemoryEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	dispatcher := NewDispatcher(NewConsumingHandler(n, NewExecutor(n, cfgContainer, nil, ExecutorConfig{
		Protocol: "consumer",
	}), ConsumingHandlerConfig{}))

	// No store configured: the permanent error is logged and nil is returned,
	// exactly as before the quarantine feature existed.
	err = dispatcher.DispatchCommand(sourceCtx(), "publish", []byte(`{"channel":"unknown:news","data":{}}`))
	require.NoError(t, err)
}

func TestDispatcherQuarantinesPublicationMode(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()

	err := fixture.dispatcher.DispatchPublication(sourceCtx(), []string{"unknown:a", "unknown:b"}, ConsumedPublication{
		Data: []byte(`{"x":1}`),
	})
	require.NoError(t, err)

	records, _, err := fixture.store.List(context.Background(), quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "broadcast", records[0].Method)
	require.Equal(t, []string{"unknown:a", "unknown:b"}, records[0].Channels)
	// The stored payload is a fully formed API request carrying the original data.
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(records[0].Payload, &fields))
	require.JSONEq(t, `{"x":1}`, string(fields["data"]))
}

func TestQuarantineReplayEndToEnd(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, true)
	defer cleanup()
	ctx := context.Background()

	// One permanently failing event (unknown channel).
	badPayload := []byte(`{"channel":"unknown:news","data":{"n":1}}`)
	require.NoError(t, fixture.dispatcher.DispatchCommand(sourceCtx(), "publish", badPayload))
	// One event which can be replayed successfully now (the failure reason,
	// e.g. a missing namespace, has been fixed).
	goodPayload := []byte(`{"channel":"test","data":{"n":2}}`)
	goodRecord, err := fixture.store.Add(ctx, quarantine.Record{
		Consumer: "outbox", ConsumerType: "postgresql", Method: "publish", Payload: goodPayload,
	})
	require.NoError(t, err)

	records, _, err := fixture.store.List(ctx, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, records, 2)

	// First replay: one success (record removed), one failure (record kept).
	resp := fixture.executor.ConsumerQuarantineReplay(ctx, &ConsumerQuarantineReplayRequest{
		Ids: []string{records[0].ID, records[1].ID},
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(1), resp.Result.Replayed)
	require.Equal(t, uint32(1), resp.Result.Failed)
	require.Equal(t, uint32(0), resp.Result.NotFound)
	statuses := map[string]string{}
	for _, item := range resp.Result.Items {
		statuses[item.Id] = item.Status
	}
	require.Equal(t, "replayed", statuses[goodRecord.ID])

	remaining, _, err := fixture.store.List(ctx, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	require.Equal(t, quarantine.StatusQuarantined, remaining[0].Status)
	require.Equal(t, 1, remaining[0].Attempts)
	require.NotEmpty(t, remaining[0].Error)

	// Exactly one publication reached the channel, from the successful replay.
	history := fixture.executor.History(ctx, &HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1)

	// Replaying the same batch again must not dispatch anything: the removed
	// record is reported not_found and the still-failing one fails again
	// without creating a duplicate quarantine entry.
	resp2 := fixture.executor.ConsumerQuarantineReplay(ctx, &ConsumerQuarantineReplayRequest{
		Ids: []string{records[0].ID, goodRecord.ID},
	})
	require.Nil(t, resp2.Error)
	require.Equal(t, uint32(1), resp2.Result.NotFound)
	require.Equal(t, uint32(1), resp2.Result.Failed)
	require.Equal(t, uint32(0), resp2.Result.Replayed)

	remaining2, _, err := fixture.store.List(ctx, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, remaining2, 1, "failed replay must not create another quarantine record")
	history = fixture.executor.History(ctx, &HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Len(t, history.Result.Publications, 1, "repeated replay produced a duplicate side effect")
}

func TestQuarantineListAPI(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, fixture.dispatcher.DispatchCommand(sourceCtx(), "publish",
		[]byte(`{"channel":"unknown:1","data":{}}`)))
	require.NoError(t, fixture.dispatcher.DispatchCommand(sourceCtx(), "broadcast",
		[]byte(`{"channels":["unknown:2"],"data":{}}`)))

	resp := fixture.executor.ConsumerQuarantineList(ctx, &ConsumerQuarantineListRequest{})
	require.Nil(t, resp.Error)
	require.Len(t, resp.Result.Records, 2)
	require.False(t, resp.Result.HasMore)
	require.Empty(t, resp.Result.NextCursor)
	require.Equal(t, "outbox", resp.Result.Records[0].Consumer)
	require.Equal(t, "postgresql", resp.Result.Records[0].ConsumerType)
	require.Greater(t, resp.Result.Records[0].CreatedAt, int64(0))

	// Method filter.
	resp = fixture.executor.ConsumerQuarantineList(ctx, &ConsumerQuarantineListRequest{Method: "broadcast"})
	require.Nil(t, resp.Error)
	require.Len(t, resp.Result.Records, 1)
	require.Equal(t, "broadcast", resp.Result.Records[0].Method)

	// Consumer filter.
	resp = fixture.executor.ConsumerQuarantineList(ctx, &ConsumerQuarantineListRequest{Consumer: "other"})
	require.Nil(t, resp.Error)
	require.Empty(t, resp.Result.Records)
}

func TestQuarantineReplayCorruptionFailsWholeBatch(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, true)
	defer cleanup()
	ctx := context.Background()

	corruptRecord, err := fixture.store.Add(ctx, quarantine.Record{
		Consumer: "outbox", ConsumerType: "postgresql", Method: "publish",
		Payload: []byte("{not json"),
	})
	require.NoError(t, err)
	goodRecord, err := fixture.store.Add(ctx, quarantine.Record{
		Consumer: "outbox", ConsumerType: "postgresql", Method: "publish",
		Payload: []byte(`{"channel":"test","data":{}}`),
	})
	require.NoError(t, err)

	records, _, err := fixture.store.List(ctx, quarantine.ListFilter{})
	require.NoError(t, err)
	require.Len(t, records, 2)

	resp := fixture.executor.ConsumerQuarantineReplay(ctx, &ConsumerQuarantineReplayRequest{
		Ids: []string{corruptRecord.ID, goodRecord.ID},
	})
	require.NotNil(t, resp.Error)
	require.Equal(t, ErrorBadRequest.Code, resp.Error.Code)

	// Nothing was dispatched or modified: both records stay quarantined and
	// no publication happened.
	for _, rec := range records {
		got, getErr := fixture.store.Get(ctx, rec.ID)
		require.NoError(t, getErr)
		require.Equal(t, quarantine.StatusQuarantined, got.Status)
	}
	history := fixture.executor.History(ctx, &HistoryRequest{Channel: "test", Limit: 100})
	require.Nil(t, history.Error)
	require.Empty(t, history.Result.Publications)
}

func TestQuarantineReplayUnknownSourceFailsBatch(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()
	ctx := context.Background()

	record, err := fixture.store.Add(ctx, quarantine.Record{
		// Consumer intentionally empty: the origin cannot be determined.
		Method: "publish", Payload: []byte(`{"channel":"test","data":{}}`),
	})
	require.NoError(t, err)

	resp := fixture.executor.ConsumerQuarantineReplay(ctx, &ConsumerQuarantineReplayRequest{
		Ids: []string{record.ID},
	})
	require.NotNil(t, resp.Error)
	require.Equal(t, ErrorBadRequest.Code, resp.Error.Code)

	got, err := fixture.store.Get(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, quarantine.StatusQuarantined, got.Status)
}

func TestQuarantineHTTPRoutes(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()

	handler := NewHandler(nil, fixture.executor, Config{})
	mux := http.NewServeMux()
	for path, h := range handler.Routes() {
		mux.Handle("/api"+path, h)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	require.NoError(t, fixture.dispatcher.DispatchCommand(sourceCtx(), "publish",
		[]byte(`{"channel":"unknown:1","data":{}}`)))

	listResp, err := http.Post(server.URL+"/api/consumer_quarantine_list", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var listBody struct {
		Result struct {
			Records []struct {
				ID       string          `json:"id"`
				Consumer string          `json:"consumer"`
				Method   string          `json:"method"`
				Payload  json.RawMessage `json:"payload"`
			} `json:"records"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listBody))
	_ = listResp.Body.Close()
	require.Len(t, listBody.Result.Records, 1)
	require.Equal(t, "outbox", listBody.Result.Records[0].Consumer)

	replayResp, err := http.Post(
		server.URL+"/api/consumer_quarantine_replay",
		"application/json",
		bytes.NewBufferString(`{"ids":["`+listBody.Result.Records[0].ID+`"]}`),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, replayResp.StatusCode)
	var replayBody struct {
		Result struct {
			Failed uint32 `json:"failed"`
		} `json:"result"`
		Error *struct {
			Code uint32 `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(replayResp.Body).Decode(&replayBody))
	_ = replayResp.Body.Close()
	require.Nil(t, replayBody.Error)
	require.Equal(t, uint32(1), replayBody.Result.Failed, "unknown channel keeps failing, record stays")
}

func TestQuarantineGRPCService(t *testing.T) {
	fixture, cleanup := newQuarantineFixture(t, false)
	defer cleanup()
	ctx := context.Background()

	service := newGRPCAPIService(nil, fixture.executor, GRPCAPIServiceConfig{})

	require.NoError(t, fixture.dispatcher.DispatchCommand(sourceCtx(), "publish",
		[]byte(`{"channel":"unknown:1","data":{}}`)))

	listResp, err := service.ConsumerQuarantineList(ctx, &ConsumerQuarantineListRequest{})
	require.NoError(t, err)
	require.Nil(t, listResp.Error)
	require.Len(t, listResp.Result.Records, 1)

	replayResp, err := service.ConsumerQuarantineReplay(ctx, &ConsumerQuarantineReplayRequest{
		Ids: []string{listResp.Result.Records[0].Id},
	})
	require.NoError(t, err)
	require.Nil(t, replayResp.Error)
	require.Equal(t, uint32(1), replayResp.Result.Failed)
}

func TestQuarantineCommandsUnavailableWithoutService(t *testing.T) {
	n := nodeWithMemoryEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	executor := NewExecutor(n, cfgContainer, nil, ExecutorConfig{Protocol: "test"})

	listResp := executor.ConsumerQuarantineList(context.Background(), &ConsumerQuarantineListRequest{})
	require.Equal(t, ErrorNotAvailable, listResp.Error)
	replayResp := executor.ConsumerQuarantineReplay(context.Background(), &ConsumerQuarantineReplayRequest{})
	require.Equal(t, ErrorNotAvailable, replayResp.Error)
}

func TestWithReplayIdempotencyKey(t *testing.T) {
	// A key is injected when the payload carries none.
	payload := withReplayIdempotencyKey([]byte(`{"channel":"test","data":{}}`), "rec-1")
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.JSONEq(t, `"quarantine:rec-1"`, string(fields["idempotency_key"]))
	// The original data and channel fields are preserved.
	require.Contains(t, string(payload), `"channel":"test"`)
	require.Contains(t, string(payload), `"data":{}`)

	// An existing key is never overridden — retries keep the same key.
	payload = withReplayIdempotencyKey([]byte(`{"channel":"test","idempotency_key":"own-key"}`), "rec-1")
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.JSONEq(t, `"own-key"`, string(fields["idempotency_key"]))

	// Unparsable payloads are returned untouched; the real dispatch error
	// surfaces during replay.
	original := []byte("{not json")
	require.Equal(t, original, withReplayIdempotencyKey(original, "rec-1"))
}
