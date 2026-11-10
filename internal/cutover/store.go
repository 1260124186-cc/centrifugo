package cutover

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Store persists cutover progress and delivered event identities. All
// implementations must be safe for concurrent use: more than one node may
// attempt to drive the same cutover, and optimistic version checks decide
// who wins.
type Store interface {
	// EnsureSchema prepares any backing schema. It must be idempotent.
	EnsureSchema(ctx context.Context) error

	// GetState returns the current state or ErrNotFound.
	GetState(ctx context.Context, name string) (*State, error)
	// CreateState inserts a new state. It returns ErrAlreadyExists when the
	// name is taken.
	CreateState(ctx context.Context, state *State) error
	// UpdateState replaces the state when its Version still matches the
	// stored one and increments the version. It returns ErrVersionConflict
	// otherwise. The previous progress stays untouched on conflict.
	UpdateState(ctx context.Context, state *State) error
	// FailState records phase and reason without changing checkpoints or
	// counters. It uses the same optimistic version check as UpdateState.
	FailState(ctx context.Context, name string, version int64, phase, reason string) error

	// GetMarkers returns the markers for the given identities.
	GetMarkers(ctx context.Context, name string, identities []string) (map[string]Marker, error)
	// ReserveMarker inserts a reserved marker when no marker exists yet. It
	// reports whether the marker was acquired. An existing marker is returned
	// so callers can distinguish delivered, reserved and conflicting entries.
	ReserveMarker(ctx context.Context, name string, marker Marker) (acquired bool, existing Marker, err error)
	// ConfirmMarker turns a reserved marker into a delivered one.
	ConfirmMarker(ctx context.Context, name, identity string) error
	// ReleaseMarker removes a reserved marker. Delivered markers are never
	// removed.
	ReleaseMarker(ctx context.Context, name, identity string) error
	// ReleaseReservations removes all reserved markers of a cutover. It runs
	// when a cutover resumes, because the dispatches they guarded never
	// reached a confirmed outcome.
	ReleaseReservations(ctx context.Context, name string) error
	// CountReservations reports how many reserved markers remain. The
	// cutover only commits when the number is zero.
	CountReservations(ctx context.Context, name string) (int, error)
}

// MemoryStore is an in-memory Store. It is the default store for tests and
// keeps the same concurrency and versioning semantics as the PostgreSQL
// implementation.
type MemoryStore struct {
	mu      sync.Mutex
	states  map[string]*State
	markers map[string]map[string]Marker
	now     func() time.Time
}

// NewMemoryStore creates an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		states:  make(map[string]*State),
		markers: make(map[string]map[string]Marker),
		now:     time.Now,
	}
}

// SetNow overrides the clock. Tests use it to get deterministic timestamps.
func (s *MemoryStore) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func cloneState(in *State) *State {
	out := *in
	out.RedisPoints = append([]RedisPoint(nil), in.RedisPoints...)
	out.PostgresPoints = append([]PostgresPoint(nil), in.PostgresPoints...)
	return &out
}

// EnsureSchema implements Store.
func (s *MemoryStore) EnsureSchema(_ context.Context) error { return nil }

// GetState implements Store.
func (s *MemoryStore) GetState(_ context.Context, name string) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.states[name]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneState(state), nil
}

// CreateState implements Store.
func (s *MemoryStore) CreateState(_ context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[state.Name]; ok {
		return ErrAlreadyExists
	}
	cp := cloneState(state)
	if cp.Version == 0 {
		cp.Version = 1
	}
	now := s.now()
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = now
	}
	cp.UpdatedAt = now
	s.states[state.Name] = cp
	state.Version = cp.Version
	return nil
}

// UpdateState implements Store.
func (s *MemoryStore) UpdateState(_ context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[state.Name]
	if !ok {
		return ErrNotFound
	}
	if current.Version != state.Version {
		return ErrVersionConflict
	}
	cp := cloneState(state)
	cp.Version = current.Version + 1
	cp.UpdatedAt = s.now()
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = current.CreatedAt
	}
	s.states[state.Name] = cp
	// Mirror the stored version on the caller's copy, matching the
	// PostgreSQL store, so subsequent CAS calls use the current version.
	state.Version = cp.Version
	state.UpdatedAt = cp.UpdatedAt
	return nil
}

// FailState implements Store.
func (s *MemoryStore) FailState(_ context.Context, name string, version int64, phase, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[name]
	if !ok {
		return ErrNotFound
	}
	if current.Version != version {
		return ErrVersionConflict
	}
	// Checkpoints, counters and fingerprint are intentionally untouched: the
	// previous progress must survive the failure.
	current.Phase = phase
	current.Reason = reason
	current.Version++
	current.UpdatedAt = s.now()
	return nil
}

// GetMarkers implements Store.
func (s *MemoryStore) GetMarkers(_ context.Context, name string, identities []string) (map[string]Marker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]Marker, len(identities))
	byName := s.markers[name]
	for _, identity := range identities {
		if marker, ok := byName[identity]; ok {
			result[identity] = marker
		}
	}
	return result, nil
}

// ReserveMarker implements Store.
func (s *MemoryStore) ReserveMarker(_ context.Context, name string, marker Marker) (bool, Marker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byName := s.markers[name]
	if byName == nil {
		byName = make(map[string]Marker)
		s.markers[name] = byName
	}
	if existing, ok := byName[marker.Identity]; ok {
		return false, existing, nil
	}
	marker.Status = markerStatusReserved
	byName[marker.Identity] = marker
	return true, Marker{}, nil
}

// ConfirmMarker implements Store.
func (s *MemoryStore) ConfirmMarker(_ context.Context, name, identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker, ok := s.markers[name][identity]
	if !ok {
		return ErrNotFound
	}
	marker.Status = markerStatusDelivered
	s.markers[name][identity] = marker
	return nil
}

// ReleaseMarker implements Store.
func (s *MemoryStore) ReleaseMarker(_ context.Context, name, identity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker, ok := s.markers[name][identity]
	if !ok {
		return nil
	}
	if marker.Status == markerStatusReserved {
		delete(s.markers[name], identity)
	}
	return nil
}

// ReleaseReservations implements Store.
func (s *MemoryStore) ReleaseReservations(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for identity, marker := range s.markers[name] {
		if marker.Status == markerStatusReserved {
			delete(s.markers[name], identity)
		}
	}
	return nil
}

// CountReservations implements Store.
func (s *MemoryStore) CountReservations(_ context.Context, name string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, marker := range s.markers[name] {
		if marker.Status == markerStatusReserved {
			n++
		}
	}
	return n, nil
}

// SnapshotMarkers returns a sorted copy of all markers of a cutover. It is a
// test helper, not part of the Store interface.
func (s *MemoryStore) SnapshotMarkers(name string) []Marker {
	s.mu.Lock()
	defer s.mu.Unlock()
	markers := make([]Marker, 0, len(s.markers[name]))
	for _, marker := range s.markers[name] {
		markers = append(markers, marker)
	}
	sort.Slice(markers, func(i, j int) bool { return markers[i].Identity < markers[j].Identity })
	return markers
}
