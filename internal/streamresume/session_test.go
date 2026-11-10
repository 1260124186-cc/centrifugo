package streamresume

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionFreshEstablishesCursor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	_, _, ok := s.ResumeCursor()
	require.False(t, ok)

	s.OnSubscribeResponse("stream-1")

	require.Equal(t, DecisionDeliver, s.Observe(10))
	require.NoError(t, s.Confirmed(ctx, 10))

	cursor, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Cursor{StreamID: "stream-1", Seq: 10, Version: 1, UpdatedAt: cursor.UpdatedAt}, cursor)

	// Once established, the session tracks consecutive sequences.
	require.Equal(t, DecisionDeliver, s.Observe(11))
	require.NoError(t, s.Confirmed(ctx, 11))
}

func TestSessionResumeSkipsConfirmedAndDeliversAfterCursor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")
	_, err := store.Save(ctx, key, Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	streamID, seq, ok := s.ResumeCursor()
	require.True(t, ok)
	require.Equal(t, "stream-1", streamID)
	require.Equal(t, uint64(5), seq)

	s.OnSubscribeResponse("stream-1")

	// An at-least-once replay of the confirmed publication (and anything
	// before it) is never delivered again.
	require.Equal(t, DecisionSkip, s.Observe(5))
	require.Equal(t, DecisionSkip, s.Observe(4))
	// Only events after the cursor are delivered.
	require.Equal(t, DecisionDeliver, s.Observe(6))
	require.NoError(t, s.Confirmed(ctx, 6))

	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(6), got.Seq)
}

func TestSessionRepeatedReconnectDoesNotRedeliverConfirmed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")

	// First connection confirms seq 1..3.
	first, err := Start(ctx, store, key)
	require.NoError(t, err)
	first.OnSubscribeResponse("stream-1")
	for seq := uint64(1); seq <= 3; seq++ {
		require.Equal(t, DecisionDeliver, first.Observe(seq))
		require.NoError(t, first.Confirmed(ctx, seq))
	}

	// Reconnect: the backend replays from the cursor, including a duplicate
	// of the last confirmed event. Nothing confirmed is delivered twice.
	second, err := Start(ctx, store, key)
	require.NoError(t, err)
	streamID, seq, ok := second.ResumeCursor()
	require.True(t, ok)
	require.Equal(t, "stream-1", streamID)
	require.Equal(t, uint64(3), seq)
	second.OnSubscribeResponse("stream-1")
	require.Equal(t, DecisionSkip, second.Observe(3))
	require.Equal(t, DecisionDeliver, second.Observe(4))
	require.NoError(t, second.Confirmed(ctx, 4))
}

func TestSessionSequenceJumpStopsResumeAndKeepsCursor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")
	previous, err := store.Save(ctx, key, Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	s.OnSubscribeResponse("stream-1")

	// Expected next is 6 but the proxy jumps to 9: recovery stops.
	require.Equal(t, DecisionFallback, s.Observe(9))
	require.True(t, s.FallenBack())
	// Later publications are delivered plain and never advance the cursor.
	require.Equal(t, DecisionFallback, s.Observe(10))
	require.Error(t, s.Confirmed(ctx, 10))

	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, got)

	// A new subscription still resumes from the retained cursor.
	next, err := Start(ctx, store, key)
	require.NoError(t, err)
	_, seq, ok := next.ResumeCursor()
	require.True(t, ok)
	require.Equal(t, uint64(5), seq)
}

func TestSessionStreamResetFallsBackAndRetainsPreviousCursor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")
	previous, err := store.Save(ctx, key, Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	// The backend lost its stream and opens a new one: unrecoverable, so the
	// session restarts in fresh mode rather than fallback.
	s.OnSubscribeResponse("stream-2")
	require.False(t, s.FallenBack())

	// Fresh tracking of the new lineage starts from its first event.
	require.Equal(t, DecisionDeliver, s.Observe(1))
	require.NoError(t, s.Confirmed(ctx, 1))

	// The cursor now describes the new lineage; the previous position stayed
	// stored untouched until this confirmation replaced it.
	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "stream-2", got.StreamID)
	require.Equal(t, uint64(1), got.Seq)
	require.Equal(t, previous.Version+1, got.Version)

	// Monotonic protection is scoped to a lineage: the same new stream still
	// cannot move backwards.
	_, err = store.Save(ctx, key, Cursor{StreamID: "stream-2", Seq: 1})
	require.ErrorIs(t, err, ErrStaleCursor)
}

func TestSessionEmptyStreamIDDisablesTracking(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")
	previous, err := store.Save(ctx, key, Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	s.OnSubscribeResponse("")
	require.True(t, s.FallenBack())
	require.Equal(t, DecisionFallback, s.Observe(6))
	require.Error(t, s.Confirmed(ctx, 6))

	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, got)
}

func TestSessionUnsequencedPublicationDisablesTracking(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")

	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	s.OnSubscribeResponse("stream-1")
	require.Equal(t, DecisionFallback, s.Observe(0))
	require.True(t, s.FallenBack())

	_, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.False(t, ok, "no cursor is saved for an unsequenced stream")
}

// failingStore fails every Save while Get works, simulating a cursor storage
// outage.
type failingStore struct {
	Store
}

func (f failingStore) Save(_ context.Context, _ Key, _ Cursor) (Cursor, error) {
	return Cursor{}, errors.New("storage unavailable")
}

func TestSessionSaveFailureKeepsPreviousCursorAndRedelivers(t *testing.T) {
	ctx := context.Background()
	backing := NewMemoryStore()
	key := testKey("")
	previous, err := backing.Save(ctx, key, Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	store := failingStore{Store: backing}
	s, err := Start(ctx, store, key)
	require.NoError(t, err)
	s.OnSubscribeResponse("stream-1")

	// seq 6 is delivered, but its cursor cannot be saved: it stays
	// unconfirmed.
	require.Equal(t, DecisionDeliver, s.Observe(6))
	require.Error(t, s.Confirmed(ctx, 6))
	require.True(t, s.FallenBack())

	// The previous cursor is retained unchanged.
	got, ok, err := backing.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, got)

	// Next subscription resumes from the old cursor: seq 6 is redelivered.
	next, err := Start(ctx, backing, key)
	require.NoError(t, err)
	next.OnSubscribeResponse("stream-1")
	require.Equal(t, DecisionDeliver, next.Observe(6))
}

func TestSessionRestartRedeliversOnlyUnconfirmed(t *testing.T) {
	ctx := context.Background()
	store := newFileStore(t)
	key := testKey("")

	// First process: seq 1 and 2 are confirmed and persisted; the process
	// dies while seq 3 is in flight (never confirmed).
	first, err := Start(ctx, store, key)
	require.NoError(t, err)
	first.OnSubscribeResponse("stream-1")
	for _, seq := range []uint64{1, 2} {
		require.Equal(t, DecisionDeliver, first.Observe(seq))
		require.NoError(t, first.Confirmed(ctx, seq))
	}

	// A new Session over the same store models the restart.
	second, err := Start(ctx, store, key)
	require.NoError(t, err)
	streamID, seq, ok := second.ResumeCursor()
	require.True(t, ok)
	require.Equal(t, "stream-1", streamID)
	require.Equal(t, uint64(2), seq)
	second.OnSubscribeResponse("stream-1")

	// Confirmed events are skipped, the unconfirmed one is redelivered.
	require.Equal(t, DecisionSkip, second.Observe(2))
	require.Equal(t, DecisionDeliver, second.Observe(3))
}
