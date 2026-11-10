package quarantine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testRecord(consumer, method, payload string) Record {
	return Record{
		Consumer:     consumer,
		ConsumerType: "postgresql",
		Method:       method,
		Payload:      []byte(payload),
	}
}

func TestMemoryStoreAddGetList(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	r1, err := store.Add(ctx, testRecord("c1", "publish", `{"channel":"a"}`))
	require.NoError(t, err)
	require.NotEmpty(t, r1.ID)
	require.Equal(t, StatusQuarantined, r1.Status)
	require.NotZero(t, r1.CreatedAt)

	r2, err := store.Add(ctx, testRecord("c1", "broadcast", `{"channels":["a"]}`))
	require.NoError(t, err)
	require.NotEmpty(t, r2.ID)
	r3, err := store.Add(ctx, testRecord("c2", "publish", `{"channel":"b"}`))
	require.NoError(t, err)

	got, err := store.Get(ctx, r1.ID)
	require.NoError(t, err)
	require.Equal(t, r1.ID, got.ID)
	require.Equal(t, `{"channel":"a"}`, string(got.Payload))

	// Mutating the returned record must not affect store state.
	got.Payload[0] = 'X'
	gotAgain, err := store.Get(ctx, r1.ID)
	require.NoError(t, err)
	require.Equal(t, `{"channel":"a"}`, string(gotAgain.Payload))

	_, err = store.Get(ctx, "missing")
	require.ErrorIs(t, err, ErrNotFound)

	// List ordering follows insertion time; filters apply.
	list, cursor, err := store.List(ctx, ListFilter{})
	require.NoError(t, err)
	require.Len(t, list, 3)
	require.Equal(t, r1.ID, list[0].ID)
	require.Empty(t, cursor)

	list, _, err = store.List(ctx, ListFilter{Consumer: "c1"})
	require.NoError(t, err)
	require.Len(t, list, 2)

	list, _, err = store.List(ctx, ListFilter{Method: "publish"})
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, r3.ID, list[1].ID)
}

func TestMemoryStoreListPagination(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	ids := make([]string, 5)
	for i := 0; i < 5; i++ {
		rec, err := store.Add(ctx, testRecord("c", "publish", fmt.Sprintf(`{"n":%d}`, i)))
		require.NoError(t, err)
		ids[i] = rec.ID
	}

	page1, cursor, err := store.List(ctx, ListFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, cursor)
	require.Equal(t, ids[0], page1[0].ID)
	require.Equal(t, ids[1], page1[1].ID)

	page2, cursor2, err := store.List(ctx, ListFilter{Limit: 2, Cursor: cursor})
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.Equal(t, ids[2], page2[0].ID)
	require.NotEmpty(t, cursor2)

	page3, cursor3, err := store.List(ctx, ListFilter{Limit: 2, Cursor: cursor2})
	require.NoError(t, err)
	require.Len(t, page3, 1)
	require.Equal(t, ids[4], page3[0].ID)
	require.Empty(t, cursor3)
}

func TestMemoryStoreEvictionKeepsReplayingRecords(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStoreWithLimit(2)

	r1, err := store.Add(ctx, testRecord("c", "publish", `{}`))
	require.NoError(t, err)
	r2, err := store.Add(ctx, testRecord("c", "publish", `{}`))
	require.NoError(t, err)

	claimed, err := store.Claim(ctx, []string{r1.ID}, "claim-1")
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// Store is at capacity and the only evictable record is r2.
	r3, err := store.Add(ctx, testRecord("c", "publish", `{}`))
	require.NoError(t, err)

	_, err = store.Get(ctx, r1.ID)
	require.NoError(t, err, "replaying record must not be evicted")
	_, err = store.Get(ctx, r2.ID)
	require.ErrorIs(t, err, ErrNotFound, "oldest quarantined record must be evicted")
	_, err = store.Get(ctx, r3.ID)
	require.NoError(t, err)
}

func TestMemoryStoreClaimAtomicAndReleaseRecover(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	r1, err := store.Add(ctx, testRecord("c", "publish", `{}`))
	require.NoError(t, err)
	r2, err := store.Add(ctx, testRecord("c", "publish", `{}`))
	require.NoError(t, err)

	claimed, err := store.Claim(ctx, []string{r1.ID, r2.ID}, "claim-1")
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	for _, rec := range claimed {
		require.Equal(t, StatusReplaying, rec.Status)
		require.Equal(t, "claim-1", rec.ClaimID)
	}

	// A second claim must fail without changing anything: r1 is already
	// claimed and r2 must stay under the original claim.
	_, err = store.Claim(ctx, []string{r1.ID, r2.ID}, "claim-2")
	require.ErrorIs(t, err, ErrNotClaimable)
	got, err := store.Get(ctx, r2.ID)
	require.NoError(t, err)
	require.Equal(t, "claim-1", got.ClaimID)

	// Unknown IDs fail the whole claim without side effects.
	_, err = store.Claim(ctx, []string{"missing"}, "claim-3")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, store.Release(ctx, r1.ID, "still broken"))
	got, err = store.Get(ctx, r1.ID)
	require.NoError(t, err)
	require.Equal(t, StatusQuarantined, got.Status)
	require.Equal(t, "still broken", got.Error)
	require.Equal(t, 1, got.Attempts)
	require.Empty(t, got.ClaimID)

	// Recover sweeps the record still stuck in replaying status (simulating a
	// process interruption between claim and finalization).
	swept, err := store.Recover(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, swept)
	got, err = store.Get(ctx, r2.ID)
	require.NoError(t, err)
	require.Equal(t, StatusQuarantined, got.Status)
	require.Empty(t, got.ClaimID)

	swept, err = store.Recover(ctx)
	require.NoError(t, err)
	require.Zero(t, swept)

	// Delete is idempotent: an already removed record does not error.
	require.NoError(t, store.Delete(ctx, r1.ID))
	require.NoError(t, store.Delete(ctx, r1.ID))
	_, err = store.Get(ctx, r1.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestValidate(t *testing.T) {
	valid := Record{
		ID:       "id",
		Consumer: "c",
		Method:   "publish",
		Payload:  []byte(`{}`),
		Status:   StatusQuarantined,
	}
	require.NoError(t, Validate(valid))

	cases := []struct {
		name   string
		mutate func(*Record)
		target error
	}{
		{"missing id", func(r *Record) { r.ID = "" }, ErrCorrupted},
		{"missing source", func(r *Record) { r.Consumer = "" }, ErrUnknownSource},
		{"missing method", func(r *Record) { r.Method = "" }, ErrUnknownSource},
		{"unsupported method", func(r *Record) { r.Method = "drop_table" }, ErrUnknownSource},
		{"empty payload", func(r *Record) { r.Payload = nil }, ErrCorrupted},
		{"invalid json payload", func(r *Record) { r.Payload = []byte("{not json") }, ErrCorrupted},
		{"unexpected status", func(r *Record) { r.Status = StatusReplaying }, ErrCorrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := valid
			tc.mutate(&rec)
			err := Validate(rec)
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.target), "want %v, got %v", tc.target, err)
		})
	}
}

// fakeDispatcher counts dispatches per record ID and fails the configured IDs.
type fakeDispatcher struct {
	mu    sync.Mutex
	calls []string
	fail  map[string]error
}

func (f *fakeDispatcher) DispatchQuarantineRecord(_ context.Context, record Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, record.ID)
	if err, ok := f.fail[record.ID]; ok {
		return err
	}
	return nil
}

func (f *fakeDispatcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newServiceWithRecords(t *testing.T, n int) (*Service, *MemoryStore, *fakeDispatcher, []Record) {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{fail: map[string]error{}}
	svc := NewService(store, dispatcher)
	records := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		rec, err := store.Add(ctx, testRecord("c", "publish", fmt.Sprintf(`{"channel":"ch%d"}`, i)))
		require.NoError(t, err)
		records = append(records, rec)
	}
	return svc, store, dispatcher, records
}

func TestReplaySuccessRemovesOnlyReplayedRecords(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 3)

	report, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, report.Replayed)
	require.Zero(t, report.Failed)
	require.Len(t, report.Items, 2)
	for _, item := range report.Items {
		require.Equal(t, ItemReplayed, item.Status)
	}
	require.Equal(t, 2, dispatcher.callCount())

	// Replayed records are gone; the unselected one keeps its real state.
	_, err = store.Get(ctx, records[0].ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.Get(ctx, records[1].ID)
	require.ErrorIs(t, err, ErrNotFound)
	kept, err := store.Get(ctx, records[2].ID)
	require.NoError(t, err)
	require.Equal(t, StatusQuarantined, kept.Status)
}

func TestReplayFailureKeepsRecordWithError(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 2)
	dispatcher.fail[records[0].ID] = errors.New("still unknown channel")

	report, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Replayed)
	require.Equal(t, 1, report.Failed)

	failedItem := report.Items[0]
	require.Equal(t, ItemFailed, failedItem.Status)
	require.Equal(t, "still unknown channel", failedItem.Error)

	kept, err := store.Get(ctx, records[0].ID)
	require.NoError(t, err)
	require.Equal(t, StatusQuarantined, kept.Status)
	require.Equal(t, "still unknown channel", kept.Error)
	require.Equal(t, 1, kept.Attempts)

	_, err = store.Get(ctx, records[1].ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestReplaySameBatchTwiceHasNoDuplicateSideEffects(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 2)

	report, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, report.Replayed)
	require.Equal(t, 2, dispatcher.callCount())

	// Replaying the same batch again must not dispatch anything: the records
	// were removed on success and show up as not_found.
	report2, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.NoError(t, err)
	require.Zero(t, report2.Replayed)
	require.Equal(t, 2, report2.NotFound)
	require.Equal(t, 2, dispatcher.callCount())
	for _, item := range report2.Items {
		require.Equal(t, ItemNotFound, item.Status)
	}

	list, _, err := store.List(ctx, ListFilter{})
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestReplayFailedThenSuccess(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 1)
	dispatcher.fail[records[0].ID] = errors.New("boom")

	report, err := svc.Replay(ctx, []string{records[0].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Failed)
	require.Equal(t, 1, dispatcher.callCount())

	// Message got fixed upstream: replay now succeeds and removes the record.
	delete(dispatcher.fail, records[0].ID)
	report, err = svc.Replay(ctx, []string{records[0].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Replayed)
	require.Equal(t, 2, dispatcher.callCount())

	_, err = store.Get(ctx, records[0].ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestReplayCorruptedRecordFailsBatchAndChangesNothing(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 2)

	// Tamper with a stored record directly to simulate corruption.
	store.mu.Lock()
	store.records[records[1].ID].Payload = []byte("{not valid json")
	store.mu.Unlock()

	_, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCorrupted)

	// Nothing was claimed or dispatched and every record keeps its state.
	require.Zero(t, dispatcher.callCount())
	for _, rec := range records {
		got, getErr := store.Get(ctx, rec.ID)
		require.NoError(t, getErr)
		require.Equal(t, StatusQuarantined, got.Status)
	}
}

func TestReplayUnknownSourceFailsBatchAndChangesNothing(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 2)

	store.mu.Lock()
	store.records[records[0].ID].Consumer = ""
	store.mu.Unlock()

	_, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID}, ListFilter{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnknownSource)
	require.Zero(t, dispatcher.callCount())

	list, _, err := store.List(ctx, ListFilter{})
	require.NoError(t, err)
	require.Len(t, list, 2)
	for _, rec := range list {
		require.Equal(t, StatusQuarantined, rec.Status)
	}
}

func TestReplayInterruptionReleasesClaims(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{fail: map[string]error{}}
	svc := NewService(store, dispatcher)

	records := make([]Record, 0, 3)
	for i := 0; i < 3; i++ {
		rec, err := store.Add(ctx, testRecord("c", "publish", `{}`))
		require.NoError(t, err)
		records = append(records, rec)
	}

	// Cancel after the first dispatch: remaining records must be released
	// rather than stuck in replaying status.
	var once sync.Once
	wrapped := &cancelDispatcher{
		inner: dispatcher,
		onDispatch: func(id string) {
			if id == records[1].ID {
				once.Do(cancel)
				time.Sleep(10 * time.Millisecond)
			}
		},
	}
	svc.dispatcher = wrapped

	_, err := svc.Replay(ctx, []string{records[0].ID, records[1].ID, records[2].ID}, ListFilter{})
	require.ErrorIs(t, err, context.Canceled)

	// First record was finalized (deleted), the other two are quarantined
	// again and can be replayed later.
	_, err = store.Get(ctx, records[0].ID)
	require.ErrorIs(t, err, ErrNotFound)
	for _, id := range []string{records[1].ID, records[2].ID} {
		rec, getErr := store.Get(ctx, id)
		require.NoError(t, getErr)
		require.Equal(t, StatusQuarantined, rec.Status, id)
		require.Empty(t, rec.ClaimID, id)
	}

	// A new replay after recovery processes the remaining records exactly
	// once each — no duplicate side effects.
	report, err := svc.Replay(context.Background(), []string{records[1].ID, records[2].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, report.Replayed)
	require.Equal(t, 1, dispatcher.callCountFor(records[1].ID))
}

type cancelDispatcher struct {
	inner      *fakeDispatcher
	onDispatch func(id string)
}

func (d *cancelDispatcher) DispatchQuarantineRecord(ctx context.Context, record Record) error {
	d.onDispatch(record.ID)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return d.inner.DispatchQuarantineRecord(ctx, record)
}

func (f *fakeDispatcher) callCountFor(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, called := range f.calls {
		if called == id {
			count++
		}
	}
	return count
}

func TestReplayEmptyIDsReplaysAllQuarantined(t *testing.T) {
	ctx := context.Background()
	svc, store, dispatcher, records := newServiceWithRecords(t, 3)

	// Claim one record to simulate an in-flight run: Recover sweeps it first.
	_, err := store.Claim(ctx, []string{records[0].ID}, "stale")
	require.NoError(t, err)

	report, err := svc.Replay(ctx, nil, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 3, report.Replayed)
	require.Equal(t, 3, dispatcher.callCount())

	list, _, err := store.List(ctx, ListFilter{})
	require.NoError(t, err)
	require.Empty(t, list)
}

func TestReplayConsumerFilter(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	dispatcher := &fakeDispatcher{fail: map[string]error{}}
	svc := NewService(store, dispatcher)

	recA, err := store.Add(ctx, testRecord("consumer-a", "publish", `{}`))
	require.NoError(t, err)
	recB, err := store.Add(ctx, testRecord("consumer-b", "publish", `{}`))
	require.NoError(t, err)

	report, err := svc.Replay(ctx, nil, ListFilter{Consumer: "consumer-a"})
	require.NoError(t, err)
	require.Equal(t, 1, report.Replayed)
	require.Equal(t, recA.ID, report.Items[0].ID)

	_, err = store.Get(ctx, recA.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.Get(ctx, recB.ID)
	require.NoError(t, err)
}

func TestReplayDuplicateIDsAreDeduped(t *testing.T) {
	ctx := context.Background()
	svc, _, dispatcher, records := newServiceWithRecords(t, 1)

	report, err := svc.Replay(ctx, []string{records[0].ID, records[0].ID}, ListFilter{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Replayed)
	require.Len(t, report.Items, 1)
	require.Equal(t, 1, dispatcher.callCount())
}
