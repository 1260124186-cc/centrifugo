package streamresume

import (
	"context"
	"sync"
	"time"
)

// MemoryStore keeps cursors in process memory. It is safe for concurrent use
// and useful in tests and single-node deployments, but cursors do not
// survive a process restart: use FileStore when resume has to outlive the
// Centrifugo process.
type MemoryStore struct {
	mu      sync.RWMutex
	cursors map[Key]Cursor
	now     func() time.Time
}

// NewMemoryStore creates an empty in-memory cursor store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		cursors: map[Key]Cursor{},
		now:     time.Now,
	}
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key Key) (Cursor, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cursor, ok := s.cursors[key]
	return cursor, ok, nil
}

// Save implements Store.
func (s *MemoryStore) Save(_ context.Context, key Key, cursor Cursor) (Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, hasPrevious := s.cursors[key]
	accepted, err := acceptCursor(previous, hasPrevious, cursor, s.now())
	if err != nil {
		return previous, err
	}
	s.cursors[key] = accepted
	return accepted, nil
}
