package streamresume

import (
	"context"
	"errors"
	"sync"
)

// Decision tells the subscription how to handle one incoming stream
// publication.
type Decision int

const (
	// DecisionDeliver: the publication is new — write it to the client and,
	// once the write succeeds, call Session.Confirmed to persist the cursor.
	DecisionDeliver Decision = iota
	// DecisionSkip: the publication was already confirmed delivered (an
	// at-least-once replay) — do not write it again.
	DecisionSkip
	// DecisionFallback: resume can no longer track the stream — deliver the
	// publication without touching any cursor, as a plain stream subscription
	// would.
	DecisionFallback
)

// sessionMode is the resume state of one subscription attempt.
type sessionMode int

const (
	// modeFresh: no usable cursor yet (first subscription or a reset stream).
	// The first sequenced delivery establishes a cursor and upgrades to
	// modeResume.
	modeFresh sessionMode = iota
	// modeResume: a cursor exists and the backend stream matches it; incoming
	// sequences are checked against the expected next one.
	modeResume
	// modeFallback: tracking was abandoned for this subscription; cursors are
	// neither read nor written until the client subscribes again.
	modeFallback
)

// Session drives resume for a single subscription. It is used from the
// stream reader goroutine and guards its state with a mutex. A Session never
// deletes cursors: falling back keeps the last confirmed cursor for the next
// subscription attempt.
type Session struct {
	store Store
	key   Key

	mu       sync.Mutex
	mode     sessionMode
	streamID string
	// nextSeq is the sequence expected from the next publication while in
	// modeResume.
	nextSeq uint64
	// cursor is the last cursor the store accepted for key.
	cursor Cursor
}

// Start loads any stored cursor for key and prepares the subscription. A
// store error is returned to the caller, which should then subscribe without
// resume rather than fail the subscription.
func Start(ctx context.Context, store Store, key Key) (*Session, error) {
	s := &Session{
		store: store,
		key:   key,
		mode:  modeFresh,
	}
	cursor, ok, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if ok {
		s.cursor = cursor
		s.mode = modeResume
		s.streamID = cursor.StreamID
		s.nextSeq = cursor.Seq + 1
	}
	return s, nil
}

// ResumeCursor returns the position to send in the backend SubscribeRequest.
// ok is false on a first subscription without a stored cursor.
func (s *Session) ResumeCursor() (streamID string, seq uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode == modeFallback || s.cursor.StreamID == "" {
		return "", 0, false
	}
	return s.cursor.StreamID, s.cursor.Seq, true
}

// OnSubscribeResponse records the stream identity the backend returned with
// its SubscribeResponse. An identity differing from the stored cursor means
// the backend reset its stream: recovery is impossible, so the session
// restarts in fresh mode while the previous cursor stays stored. An empty
// identity disables tracking entirely.
func (s *Session) OnSubscribeResponse(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode == modeFallback {
		return
	}
	s.streamID = streamID
	if streamID == "" {
		// The backend cannot identify its streams, so no cursor can be
		// verified: deliver without resume and keep any stored cursor.
		s.mode = modeFallback
		return
	}
	if s.mode == modeResume && streamID != s.cursor.StreamID {
		s.mode = modeFresh
		s.nextSeq = 0
	}
}

// Observe classifies an incoming publication by its sequence.
func (s *Session) Observe(seq uint64) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mode == modeFallback {
		return DecisionFallback
	}
	if seq == 0 {
		// The backend does not number publications: resume is impossible.
		s.mode = modeFallback
		return DecisionFallback
	}
	switch s.mode {
	case modeResume:
		switch {
		case seq < s.nextSeq:
			// seq <= cursor.Seq: a confirmed publication replayed by the
			// backend. Never deliver it twice.
			return DecisionSkip
		case seq > s.nextSeq:
			// A gap in the sequence: advancing the cursor would confirm
			// publications the client never saw. Stop recovery and keep the
			// previous cursor.
			s.mode = modeFallback
			return DecisionFallback
		default:
			return DecisionDeliver
		}
	case modeFresh:
		if s.streamID == "" {
			s.mode = modeFallback
			return DecisionFallback
		}
		// First event of a new stream: it becomes the baseline cursor.
		return DecisionDeliver
	default:
		return DecisionFallback
	}
}

// Confirmed persists the cursor after a publication was successfully
// delivered. On a store error the session falls back: the store keeps the
// previous cursor, so the unconfirmed publication is redelivered on the next
// subscription.
func (s *Session) Confirmed(ctx context.Context, seq uint64) error {
	s.mu.Lock()
	if s.mode == modeFallback {
		s.mu.Unlock()
		return errors.New("stream resume session is not tracking the stream")
	}
	streamID := s.streamID
	s.mu.Unlock()

	if streamID == "" || seq == 0 {
		return errors.New("stream resume cursor requires a stream identity and a sequence")
	}

	accepted, err := s.store.Save(ctx, s.key, Cursor{StreamID: streamID, Seq: seq})
	if err != nil {
		s.mu.Lock()
		s.mode = modeFallback
		s.mu.Unlock()
		return err
	}

	s.mu.Lock()
	s.cursor = accepted
	s.mode = modeResume
	s.nextSeq = seq + 1
	s.mu.Unlock()
	return nil
}

// FallenBack reports whether tracking was abandoned for this subscription.
func (s *Session) FallenBack() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode == modeFallback
}
