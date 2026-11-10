package cutover

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// fakeSource is an in-memory Source. It implements StableDeliverySource so
// the manager can drive the same reconciliation logic as against Redis.
type fakeSource struct {
	kind       string
	descriptor SourceDescriptor
	frontier   []FrontierPoint

	mu       sync.Mutex
	redis    map[string][]Event // stream -> ordered events
	pending  map[string]map[string]bool
	pg       map[int64][]Event // partition -> ordered events
	readErr  error
	pingErr  error
	frontErr error
	reads    int
}

func (f *fakeSource) Descriptor() SourceDescriptor { return f.descriptor }

func (f *fakeSource) Ping(_ context.Context) error { return f.pingErr }

func (f *fakeSource) Frontier(_ context.Context) ([]FrontierPoint, error) {
	if f.frontErr != nil {
		return nil, f.frontErr
	}
	return append([]FrontierPoint(nil), f.frontier...), nil
}

func (f *fakeSource) Read(_ context.Context, cursor Cursor, limit int) ([]Event, Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.readErr != nil {
		return nil, cursor, f.readErr
	}
	if limit <= 0 {
		limit = defaultBatchLimit
	}
	var out []Event
	next := cursor
	if f.kind == SourceKindRedisStream {
		for _, ev := range f.redis[cursor.Stream] {
			if ev.Offset > cursor.Offset && ev.Offset <= cursor.Bound {
				out = append(out, ev)
				next.Offset = ev.Offset
			}
			if len(out) == limit {
				break
			}
		}
		return out, next, nil
	}
	last := int64(0)
	if cursor.Offset != "" {
		last = mustParseInt64(cursor.Offset)
	}
	bound := mustParseInt64(cursor.Bound)
	for _, ev := range f.pg[cursor.Partition] {
		id := mustParseInt64(ev.Offset)
		if id > last && id <= bound {
			out = append(out, ev)
			next.Offset = ev.Offset
		}
		if len(out) == limit {
			break
		}
	}
	return out, next, nil
}

func (f *fakeSource) Identity(ev Event) (string, error) {
	if value := ev.Fields["identity"]; value != "" {
		return value, nil
	}
	return payloadIdentity(ev.Payload, "idempotency_key")
}

func (f *fakeSource) PendingIDs(_ context.Context, stream string) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	out := make(map[string]bool, len(f.pending[stream]))
	for id := range f.pending[stream] {
		out[id] = true
	}
	return out, nil
}

func (f *fakeSource) Close() error { return nil }

func (f *fakeSource) setReadErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readErr = err
}

// fakeDispatcher records dispatched identities and can inject failures.
type fakeDispatcher struct {
	mu         sync.Mutex
	dispatched []string
	fail       map[string]int // identity -> remaining failures
}

func (d *fakeDispatcher) DispatchCommand(_ context.Context, _ string, payload []byte) error {
	identity, err := payloadIdentity(payload, "idempotency_key")
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if remaining := d.fail[identity]; remaining > 0 {
		d.fail[identity] = remaining - 1
		return errors.New("boom")
	}
	d.dispatched = append(d.dispatched, identity)
	return nil
}

func (d *fakeDispatcher) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.dispatched...)
}

func testConsumers() []configtypes.Consumer {
	return []configtypes.Consumer{
		{Name: "redis1", Type: configtypes.ConsumerTypeRedisStream, Enabled: true},
		{Name: "pg1", Type: configtypes.ConsumerTypePostgres, Enabled: true},
	}
}

func testOptions() Options {
	return Options{
		Name:                 "cut-1",
		RedisConsumerName:    "redis1",
		PostgresConsumerName: "pg1",
		BatchLimit:           2,
		IdentityPayloadField: "idempotency_key",
	}
}

func redisEvent(offset, identity string) Event {
	return Event{
		SourceKind: SourceKindRedisStream,
		Stream:     "s1",
		Offset:     offset,
		Method:     "publish",
		Payload:    []byte(fmt.Sprintf(`{"idempotency_key":%q}`, identity)),
		Fields:     map[string]string{"identity": identity},
	}
}

func pgEvent(id int64, identity, payloadIdentity string) Event {
	payloadIdentityField := identity
	if payloadIdentity != "" {
		payloadIdentityField = payloadIdentity
	}
	return Event{
		SourceKind: SourceKindPostgresOutbox,
		Partition:  0,
		Offset:     fmt.Sprintf("%d", id),
		Method:     "publish",
		Payload:    []byte(fmt.Sprintf(`{"idempotency_key":%q}`, payloadIdentityField)),
		Fields:     map[string]string{"identity": identity},
	}
}

// newTestManager wires a manager with in-memory sources and the given store.
// The sources are returned so tests can mutate their failure knobs.
func newTestManager(t *testing.T, store Store, dispatcher *fakeDispatcher, redis, pg *fakeSource) *Manager {
	t.Helper()
	mgr := NewManager(ManagerConfig{
		Consumers:  testConsumers(),
		Dispatcher: dispatcher,
		NewStore: func(_ configtypes.PostgresConsumerConfig) (Store, error) {
			return store, nil
		},
		BuildRedis: func(_ configtypes.RedisStreamConsumerConfig, _ Options) (Source, error) {
			return redis, nil
		},
		BuildPostgres: func(_ configtypes.PostgresConsumerConfig, _ Options) (Source, error) {
			return pg, nil
		},
	})
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

func newFakeSources() (*fakeSource, *fakeSource) {
	redis := &fakeSource{
		kind: SourceKindRedisStream,
		descriptor: SourceDescriptor{
			Kind: SourceKindRedisStream, ConsumerName: "redis1", Locator: "s1",
			Details: map[string]string{"group": "g1"},
		},
		frontier: []FrontierPoint{{Stream: "s1", Offset: "5-0"}},
		redis:    map[string][]Event{},
		pending:  map[string]map[string]bool{"s1": {}},
	}
	pg := &fakeSource{
		kind: SourceKindPostgresOutbox,
		descriptor: SourceDescriptor{
			Kind: SourceKindPostgresOutbox, ConsumerName: "pg1", Locator: "outbox",
			Details: map[string]string{"table": "outbox"},
		},
		frontier: []FrontierPoint{{Partition: 0, Offset: "100"}},
		pg:       map[int64][]Event{},
	}
	return redis, pg
}

func TestCutoverConvergesAndDeduplicates(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()

	// r1,r2,r4 are stably delivered; r3,r5 are still pending.
	redisEvents := []Event{}
	pending := map[string]bool{}
	for _, spec := range []struct {
		offset, id string
		pending    bool
	}{
		{"1-0", "r1", false},
		{"2-0", "r2", false},
		{"3-0", "r3", true},
		{"4-0", "r4", false},
		{"5-0", "r5", true},
	} {
		ev := redisEvent(spec.offset, spec.id)
		redisEvents = append(redisEvents, ev)
		if spec.pending {
			pending[spec.offset] = true
		}
	}
	redis.redis["s1"] = redisEvents
	redis.pending["s1"] = pending

	// r3 and r5 also exist in the outbox (overlap); p1 is new. The overlap
	// rows must never be dispatched a second time.
	pg.pg[0] = []Event{
		pgEvent(91, "r3", "r3"),
		pgEvent(92, "p1", "p1"),
		pgEvent(93, "r5", "r5"),
	}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	state, err := mgr.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state.Complete())
	require.Equal(t, PhaseCommitted, state.Phase)

	dispatched := dispatcher.snapshot()
	require.ElementsMatch(t, []string{"r3", "r5", "p1"}, dispatched)
	require.Len(t, dispatched, 3, "each event must be dispatched at most once")

	// Stable identities are recorded as delivered without dispatch.
	markers := store.SnapshotMarkers(state.Name)
	byIdentity := map[string]Marker{}
	for _, marker := range markers {
		require.Equal(t, markerStatusDelivered, marker.Status)
		byIdentity[marker.Identity] = marker
	}
	for _, identity := range []string{"r1", "r2", "r3", "r4", "r5", "p1"} {
		_, ok := byIdentity[identity]
		require.True(t, ok, "missing delivered marker for %s", identity)
	}
	require.Equal(t, int64(3), state.Delivered)
	require.Equal(t, int64(5), state.Skipped) // r1,r2,r4 stable + r3,r5 overlap

	// Re-running a committed cutover is a no-op.
	state2, err := mgr.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state2.Complete())
	require.Equal(t, state.Version, state2.Version)
	require.Len(t, dispatcher.snapshot(), 3)
}

func TestCutoverResumesAcrossRestartAndInterruption(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()

	redis.redis["s1"] = []Event{
		redisEvent("1-0", "r1"),
		redisEvent("2-0", "r2"),
	}
	redis.pending["s1"] = map[string]bool{"2-0": true}
	pg.pg[0] = []Event{pgEvent(10, "p1", "p1"), pgEvent(20, "p2", "p2")}

	opts := testOptions()
	opts.MaxEvents = 1

	// First node: one event per call, simulating bounded convergence calls.
	mgr1 := newTestManager(t, store, dispatcher, redis, pg)
	for i := 0; i < 10; i++ {
		state, err := mgr1.Run(context.Background(), opts)
		require.NoError(t, err)
		if state.Complete() {
			break
		}
		require.Equal(t, PhaseRunning, state.Phase)
	}
	require.ElementsMatch(t, []string{"r2", "p1", "p2"}, dispatcher.snapshot())

	// A brand new manager models a node restart: it must resume from the
	// persisted state instead of re-executing anything.
	dispatcher2 := &fakeDispatcher{}
	mgr2 := newTestManager(t, store, dispatcher2, redis, pg)
	state, err := mgr2.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state.Complete())
	require.Empty(t, dispatcher2.snapshot(), "restart must not re-deliver events")

	// Context cancellation is an interruption, not a failure.
	store2 := NewMemoryStore()
	dispatcher3 := &fakeDispatcher{}
	redis3, pg3 := newFakeSources()
	redis3.redis["s1"] = []Event{redisEvent("1-0", "r1")}
	redis3.pending["s1"] = map[string]bool{"1-0": true}
	pg3.pg[0] = []Event{pgEvent(10, "p1", "p1")}
	mgr3 := newTestManager(t, store2, dispatcher3, redis3, pg3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	state3, err := mgr3.Run(ctx, testOptions())
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, state3.Complete())
	require.Equal(t, PhaseRunning, state3.Phase)

	state4, err := mgr3.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state4.Complete())
	// Each identity still dispatched exactly once across the interruption.
	counts := map[string]int{}
	for _, identity := range dispatcher3.snapshot() {
		counts[identity]++
	}
	require.Equal(t, map[string]int{"r1": 1, "p1": 1}, counts)
}

func TestCutoverIdentityConflictPreservesProgress(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()

	redis.redis["s1"] = []Event{redisEvent("1-0", "shared")}
	redis.pending["s1"] = map[string]bool{}
	// Same identity, different payload on the new source.
	conflict := pgEvent(10, "shared", "shared")
	conflict.Payload = []byte(`{"idempotency_key":"shared","extra":"different"}`)
	pg.pg[0] = []Event{conflict}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	state, err := mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrIdentityConflict)
	require.Equal(t, PhaseFailed, state.Phase)
	require.Contains(t, state.Reason, "identity conflict")

	// Redis progress is kept; Postgres never advanced past the conflict.
	require.Equal(t, "1-0", state.RedisPoints[0].LastScannedID)
	require.Equal(t, int64(0), state.PostgresPoints[0].LastScannedID)

	// The conflicting event was never dispatched and retrying fails again
	// deterministically, leaving the previous progress in place.
	require.Empty(t, dispatcher.snapshot())
	state2, err := mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrIdentityConflict)
	require.Greater(t, state2.Version, state.Version)
	require.Equal(t, int64(0), state2.PostgresPoints[0].LastScannedID,
		"postgres checkpoint must not advance past the conflicting event")
}

func TestCutoverReadFailurePreservesProgress(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()
	redis.redis["s1"] = []Event{redisEvent("1-0", "r1")}
	redis.pending["s1"] = map[string]bool{"1-0": true}
	pg.pg[0] = []Event{pgEvent(10, "p1", "p1")}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	pg.setReadErr(ErrReadFailed)
	state, err := mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrReadFailed)
	require.Equal(t, PhaseFailed, state.Phase)

	// Redis-side progress from the batch before the failure is preserved.
	require.Equal(t, "1-0", state.RedisPoints[0].LastScannedID)
	require.Equal(t, int64(0), state.PostgresPoints[0].LastScannedID)
	redisVersion := state.Version

	pg.setReadErr(nil)
	state2, err := mgr.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state2.Complete())
	require.Greater(t, state2.Version, redisVersion)
	require.ElementsMatch(t, []string{"r1", "p1"}, dispatcher.snapshot())
}

func TestCutoverValidationMismatch(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()
	redis.redis["s1"] = []Event{redisEvent("1-0", "r1")}
	redis.pending["s1"] = map[string]bool{}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	state, err := mgr.Run(context.Background(), testOptions())
	require.NoError(t, err)
	checkpoints := append([]RedisPoint(nil), state.RedisPoints...)

	// Another call resolves a different source descriptor.
	pg.descriptor.Details["table"] = "another_outbox"
	state2, err := mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrValidationMismatch)
	require.Equal(t, PhaseFailed, state2.Phase)
	require.Equal(t, checkpoints, state2.RedisPoints)
	require.NotEmpty(t, state2.Reason)
}

func TestCutoverCancel(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()
	redis.redis["s1"] = []Event{redisEvent("1-0", "r1")}
	redis.pending["s1"] = map[string]bool{}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	opts := testOptions()
	opts.MaxEvents = 1
	state, err := mgr.Run(context.Background(), opts)
	require.NoError(t, err)
	require.Equal(t, PhaseRunning, state.Phase)

	cancelOpts := testOptions()
	cancelOpts.Cancel = true
	state2, err := mgr.Run(context.Background(), cancelOpts)
	require.NoError(t, err)
	require.Equal(t, PhaseCancelled, state2.Phase)
	// Progress survives the cancellation.
	require.Equal(t, state.RedisPoints, state2.RedisPoints)

	_, err = mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrBadOptions)
}

func TestCutoverDispatchFailureReleasesMarker(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{fail: map[string]int{"r1": 1}}
	redis, pg := newFakeSources()
	redis.redis["s1"] = []Event{redisEvent("1-0", "r1")}
	redis.pending["s1"] = map[string]bool{"1-0": true}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	_, err := mgr.Run(context.Background(), testOptions())
	require.Error(t, err)
	require.Equal(t, PhaseFailed, mgr.mustState(t, store).Phase)

	reservations, err := store.CountReservations(context.Background(), "cut-1")
	require.NoError(t, err)
	require.Zero(t, reservations, "failed dispatch must release its reservation")

	state, err := mgr.Run(context.Background(), testOptions())
	require.NoError(t, err)
	require.True(t, state.Complete())
	require.Equal(t, []string{"r1"}, dispatcher.snapshot())
}

func TestCutoverEventWithoutIdentityFails(t *testing.T) {
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{}
	redis, pg := newFakeSources()
	ev := redisEvent("1-0", "r1")
	ev.Fields = nil
	ev.Payload = []byte(`{"unrelated":true}`)
	redis.redis["s1"] = []Event{ev}
	redis.pending["s1"] = map[string]bool{"1-0": true}

	mgr := newTestManager(t, store, dispatcher, redis, pg)
	state, err := mgr.Run(context.Background(), testOptions())
	require.ErrorIs(t, err, ErrNoIdentity)
	require.Equal(t, PhaseFailed, state.Phase)
	require.Empty(t, dispatcher.snapshot())
}

func TestMemoryStoreVersionCAS(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	state := &State{Name: "c", Version: 1, Phase: PhaseRunning}
	require.NoError(t, store.CreateState(ctx, state))
	require.ErrorIs(t, store.CreateState(ctx, state), ErrAlreadyExists)

	loaded, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, int64(1), loaded.Version)
	require.NoError(t, store.UpdateState(ctx, loaded))
	require.ErrorIs(t, store.UpdateState(ctx, state), ErrVersionConflict)

	// FailState keeps checkpoints and counters intact.
	loaded2, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	loaded2.RedisPoints = []RedisPoint{{Stream: "s", StopID: "9-0", LastScannedID: "9-0"}}
	require.NoError(t, store.UpdateState(ctx, loaded2))
	current, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, store.FailState(ctx, "c", current.Version, PhaseFailed, "boom"))
	failed, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, PhaseFailed, failed.Phase)
	require.Equal(t, []RedisPoint{{Stream: "s", StopID: "9-0", LastScannedID: "9-0"}}, failed.RedisPoints)
}

func (m *Manager) mustState(t *testing.T, store Store) *State {
	t.Helper()
	state, err := store.GetState(context.Background(), "cut-1")
	require.NoError(t, err)
	return state
}
