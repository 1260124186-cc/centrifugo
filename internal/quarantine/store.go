package quarantine

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors shared by Store implementations and callers.
var (
	// ErrNotFound is returned when no record exists for the given ID.
	ErrNotFound = errors.New("quarantine record not found")
	// ErrNotClaimable is returned when a record cannot be claimed for a
	// replay, for example because it is already claimed or no longer in the
	// quarantined status.
	ErrNotClaimable = errors.New("quarantine record is not claimable")
)

// Store persists quarantine records. All methods are safe for concurrent use.
type Store interface {
	// Add stores a new record. The store assigns ID, CreatedAt, UpdatedAt and
	// Status when they are zero values and returns the stored record.
	Add(ctx context.Context, record Record) (Record, error)
	// Get returns the record for the given ID.
	Get(ctx context.Context, id string) (Record, error)
	// List returns records matching the filter, ordered by creation time and
	// then ID, together with the cursor of the next page (empty when there is
	// nothing more).
	List(ctx context.Context, filter ListFilter) (records []Record, nextCursor string, err error)
	// Claim atomically moves the records of the given IDs from the
	// quarantined to the replaying status under claimID. It fails without
	// claiming anything if an ID is missing or not claimable, and returns the
	// claimed records ordered by creation time and then ID.
	Claim(ctx context.Context, ids []string, claimID string) ([]Record, error)
	// Release returns a claimed record to the quarantined status, records
	// lastError as its last error and increments its attempt counter.
	Release(ctx context.Context, id string, lastError string) error
	// Delete removes the record of the given ID. Deleting a missing record is
	// not an error: a successful replay removes its record, and a repeated
	// replay must not fail because of that.
	Delete(ctx context.Context, id string) error
	// Recover sweeps records left in the replaying status by an interrupted
	// run (including a previous process) back to the quarantined status and
	// returns the number of swept records.
	Recover(ctx context.Context) (int, error)
}

// defaultMaxRecords bounds the memory used by the in-memory store. When the
// bound is reached the oldest quarantined records are evicted first; records
// currently being replayed are never evicted.
const defaultMaxRecords = 10000

// MemoryStore is a process-local Store implementation. It is safe for
// concurrent use and needs no external infrastructure.
type MemoryStore struct {
	mu         sync.RWMutex
	records    map[string]*Record
	order      []string
	maxRecords int
	now        func() time.Time
}

// NewMemoryStore creates a MemoryStore with the default capacity.
func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithLimit(defaultMaxRecords)
}

// NewMemoryStoreWithLimit creates a MemoryStore holding at most maxRecords
// records. Non-positive values fall back to the default capacity.
func NewMemoryStoreWithLimit(maxRecords int) *MemoryStore {
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	return &MemoryStore{
		records:    make(map[string]*Record),
		maxRecords: maxRecords,
		now:        time.Now,
	}
}

// Add implements Store.
func (s *MemoryStore) Add(_ context.Context, record Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if record.Status == "" {
		record.Status = StatusQuarantined
	}
	record.ClaimID = ""
	record.ClaimedAt = time.Time{}

	stored := cloneRecord(record)
	s.records[stored.ID] = &stored
	s.order = append(s.order, stored.ID)

	s.evictLocked()
	return cloneRecord(stored), nil
}

// evictLocked drops the oldest quarantined records while the store is over
// capacity. Caller must hold s.mu.
func (s *MemoryStore) evictLocked() {
	for len(s.order) > s.maxRecords {
		evicted := -1
		for i, id := range s.order {
			if s.records[id].Status == StatusQuarantined {
				evicted = i
				break
			}
		}
		if evicted == -1 {
			// Only replaying records remain: never evict those.
			return
		}
		id := s.order[evicted]
		delete(s.records, id)
		s.order = append(s.order[:evicted], s.order[evicted+1:]...)
	}
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, id string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[id]
	if !ok {
		return Record{}, ErrNotFound
	}
	return cloneRecord(*record), nil
}

// List implements Store.
func (s *MemoryStore) List(_ context.Context, filter ListFilter) ([]Record, string, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]*Record, 0, len(s.records))
	for _, record := range s.records {
		if filter.Consumer != "" && record.Consumer != filter.Consumer {
			continue
		}
		if filter.Method != "" && record.Method != filter.Method {
			continue
		}
		if filter.Status != "" && record.Status != filter.Status {
			continue
		}
		candidates = append(candidates, record)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].ID < candidates[j].ID
	})

	start := 0
	if filter.Cursor != "" {
		start = -1
		for i, record := range candidates {
			if record.ID == filter.Cursor {
				start = i + 1
				break
			}
		}
		if start == -1 {
			// Unknown cursor: start from the beginning rather than failing,
			// the caller simply gets the first page again.
			start = 0
		}
	}
	if start >= len(candidates) {
		return []Record{}, "", nil
	}

	end := start + limit
	var nextCursor string
	if end < len(candidates) {
		nextCursor = candidates[end-1].ID
	} else {
		end = len(candidates)
	}

	records := make([]Record, 0, end-start)
	for _, record := range candidates[start:end] {
		records = append(records, cloneRecord(*record))
	}
	return records, nextCursor, nil
}

// Claim implements Store.
func (s *MemoryStore) Claim(_ context.Context, ids []string, claimID string) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uniqueIDs := uniqueOrdered(ids)
	for _, id := range uniqueIDs {
		record, ok := s.records[id]
		if !ok {
			return nil, ErrNotFound
		}
		if record.Status != StatusQuarantined {
			return nil, ErrNotClaimable
		}
	}

	now := s.now()
	claimed := make([]*Record, 0, len(uniqueIDs))
	for _, id := range uniqueIDs {
		record := s.records[id]
		record.Status = StatusReplaying
		record.ClaimID = claimID
		record.ClaimedAt = now
		record.UpdatedAt = now
		claimed = append(claimed, record)
	}
	sort.Slice(claimed, func(i, j int) bool {
		if !claimed[i].CreatedAt.Equal(claimed[j].CreatedAt) {
			return claimed[i].CreatedAt.Before(claimed[j].CreatedAt)
		}
		return claimed[i].ID < claimed[j].ID
	})

	records := make([]Record, 0, len(claimed))
	for _, record := range claimed {
		records = append(records, cloneRecord(*record))
	}
	return records, nil
}

// Release implements Store.
func (s *MemoryStore) Release(_ context.Context, id string, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok {
		return ErrNotFound
	}
	record.Status = StatusQuarantined
	record.ClaimID = ""
	record.ClaimedAt = time.Time{}
	record.Error = lastError
	record.Attempts++
	record.UpdatedAt = s.now()
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[id]; !ok {
		return nil
	}
	delete(s.records, id)
	for i, existing := range s.order {
		if existing == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return nil
}

// Recover implements Store.
func (s *MemoryStore) Recover(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	swept := 0
	for _, record := range s.records {
		if record.Status == StatusReplaying {
			record.Status = StatusQuarantined
			record.ClaimID = ""
			record.ClaimedAt = time.Time{}
			record.UpdatedAt = now
			swept++
		}
	}
	return swept, nil
}

// uniqueOrdered deduplicates ids while preserving the first occurrence.
func uniqueOrdered(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

// cloneRecord returns a copy of record with independent slice fields, so
// callers can never mutate store state through returned values.
func cloneRecord(record Record) Record {
	clone := record
	if record.Payload != nil {
		clone.Payload = append([]byte(nil), record.Payload...)
	}
	if record.Channels != nil {
		clone.Channels = append([]string(nil), record.Channels...)
	}
	return clone
}
