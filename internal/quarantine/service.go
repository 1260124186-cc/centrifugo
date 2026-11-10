package quarantine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// KnownMethods are the consumer API methods a quarantine record can be
// replayed through. A record with any other method cannot be dispatched and
// fails the replay batch before any side effect happens.
var KnownMethods = map[string]struct{}{
	"publish":        {},
	"broadcast":      {},
	"subscribe":      {},
	"unsubscribe":    {},
	"disconnect":     {},
	"history_remove": {},
	"refresh":        {},
}

// Sentinel errors returned by Validate and the replay flow. Callers can use
// errors.Is to distinguish a corrupt record or an undeterminable source from
// ordinary dispatch failures.
var (
	// ErrCorrupted means a quarantine record is not in a state in which it can
	// be safely replayed (bad payload, broken status, ...).
	ErrCorrupted = errors.New("corrupted quarantine record")
	// ErrUnknownSource means the record does not identify a consumer source or
	// a supported method, so the origin of the event cannot be determined.
	ErrUnknownSource = errors.New("quarantine record source cannot be determined")
)

// Validate checks that record can be replayed safely. It does not touch the
// store. Both a corrupted record and an undeterminable source make the whole
// replay batch fail explicitly.
func Validate(record Record) error {
	if record.ID == "" {
		return fmt.Errorf("%w: missing record ID", ErrCorrupted)
	}
	if record.Consumer == "" {
		return fmt.Errorf("%w: record %s has no source consumer", ErrUnknownSource, record.ID)
	}
	if record.Method == "" {
		return fmt.Errorf("%w: record %s has no method", ErrUnknownSource, record.ID)
	}
	if _, ok := KnownMethods[record.Method]; !ok {
		return fmt.Errorf("%w: record %s uses unsupported method %q", ErrUnknownSource, record.ID, record.Method)
	}
	if record.Status != StatusQuarantined {
		return fmt.Errorf("%w: record %s is in unexpected status %q", ErrCorrupted, record.ID, record.Status)
	}
	if len(record.Payload) == 0 {
		return fmt.Errorf("%w: record %s has an empty payload", ErrCorrupted, record.ID)
	}
	if !json.Valid(record.Payload) {
		return fmt.Errorf("%w: record %s payload is not valid JSON", ErrCorrupted, record.ID)
	}
	return nil
}

// RecordDispatcher re-dispatches a quarantined event. Implementations must
// route the event through the same pipeline the original consumer used, with
// the record's source attached to the context.
type RecordDispatcher interface {
	DispatchQuarantineRecord(ctx context.Context, record Record) error
}

// ItemStatus is the per-record outcome of a replay run.
type ItemStatus string

const (
	// ItemReplayed means the event was dispatched successfully and its
	// quarantine record was removed.
	ItemReplayed ItemStatus = "replayed"
	// ItemFailed means the dispatch failed again; the record is kept with its
	// last error.
	ItemFailed ItemStatus = "failed"
	// ItemNotFound means no record exists for the ID anymore, typically because
	// an earlier replay already succeeded and removed it. Nothing is
	// dispatched, so replaying the same batch again stays side-effect free.
	ItemNotFound ItemStatus = "not_found"
)

// ReplayItem is the outcome of replaying a single record ID.
type ReplayItem struct {
	ID       string     `json:"id"`
	Consumer string     `json:"consumer,omitempty"`
	Status   ItemStatus `json:"status"`
	// Error is the dispatch error when Status is ItemFailed.
	Error string `json:"error,omitempty"`
}

// ReplayReport summarizes a replay run.
type ReplayReport struct {
	Items    []ReplayItem `json:"items"`
	Replayed int          `json:"replayed"`
	Failed   int          `json:"failed"`
	NotFound int          `json:"not_found"`
}

// Service runs queries and replays against a quarantine Store.
type Service struct {
	store      Store
	dispatcher RecordDispatcher
	now        func() time.Time
	log        zerolog.Logger

	// replayMu serializes replay runs within the process. Together with the
	// claim status on each record this guarantees that no record is
	// dispatched twice concurrently, and that an interrupted run cannot
	// produce duplicate side effects when it is retried.
	replayMu sync.Mutex
}

// NewService creates a Service over store. dispatcher performs the actual
// re-dispatch of quarantined events.
func NewService(store Store, dispatcher RecordDispatcher) *Service {
	return &Service{
		store:      store,
		dispatcher: dispatcher,
		now:        time.Now,
		log:        log.With().Str("subsystem", "consumer_quarantine").Logger(),
	}
}

// List returns the quarantine records matching the filter and the cursor of
// the next page (empty on the last page).
func (s *Service) List(ctx context.Context, filter ListFilter) ([]Record, string, error) {
	return s.store.List(ctx, filter)
}

// Replay re-dispatches the records of the given IDs. When ids is empty every
// currently quarantined record (optionally narrowed by filter.Consumer) is
// replayed.
//
// The whole batch fails without any state change when a selected record is
// corrupted or its source cannot be determined. Each record is claimed before
// dispatch and finalized independently afterwards: a successful dispatch
// removes only that record while a failed one is kept with its last error.
// Records not selected by the call are never touched.
func (s *Service) Replay(ctx context.Context, ids []string, filter ListFilter) (*ReplayReport, error) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	// Sweep claims left by an interrupted run (or a previous process) before
	// doing anything else, so they can be retried safely.
	if _, err := s.store.Recover(ctx); err != nil {
		return nil, fmt.Errorf("error recovering interrupted quarantine replays: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	requestedIDs := uniqueOrdered(ids)
	if len(requestedIDs) == 0 {
		records, _, err := s.store.List(ctx, ListFilter{
			Consumer: filter.Consumer,
			Status:   StatusQuarantined,
			Limit:    MaxListLimit,
		})
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			requestedIDs = append(requestedIDs, record.ID)
		}
	}

	report := &ReplayReport{Items: make([]ReplayItem, 0, len(requestedIDs))}
	if len(requestedIDs) == 0 {
		return report, nil
	}

	// Phase 1: load and validate every selected record before any side
	// effect. A single corrupt or undeterminable record fails the whole
	// batch and leaves all records untouched.
	eligible := make([]Record, 0, len(requestedIDs))
	for _, id := range requestedIDs {
		record, err := s.store.Get(ctx, id)
		if errors.Is(err, ErrNotFound) {
			// The record was already replayed successfully (or never
			// existed). Report it but never dispatch anything for it.
			report.NotFound++
			report.Items = append(report.Items, ReplayItem{ID: id, Status: ItemNotFound})
			continue
		}
		if err != nil {
			return nil, err
		}
		if filter.Consumer != "" && record.Consumer != filter.Consumer {
			report.NotFound++
			report.Items = append(report.Items, ReplayItem{ID: id, Consumer: record.Consumer, Status: ItemNotFound})
			continue
		}
		if err := Validate(record); err != nil {
			return nil, err
		}
		eligible = append(eligible, record)
	}

	if len(eligible) == 0 {
		return report, nil
	}

	// Phase 2: claim all eligible records atomically. Claim is all-or-
	// nothing, so a failure here leaves the quarantine area unchanged.
	claimID := uuid.NewString()
	eligibleIDs := make([]string, 0, len(eligible))
	for _, record := range eligible {
		eligibleIDs = append(eligibleIDs, record.ID)
	}
	claimed, err := s.store.Claim(ctx, eligibleIDs, claimID)
	if err != nil {
		return nil, fmt.Errorf("error claiming quarantine records for replay: %w", err)
	}

	// Phase 3: dispatch and finalize each claimed record independently. If
	// the context ends or the process panics, a deferred sweep releases the
	// records this run still owns back to the quarantined status instead of
	// leaving them stuck.
	finalized := make(map[string]struct{}, len(claimed))
	defer s.releaseOwnedClaims(claimID, claimed, finalized)

	for _, record := range claimed {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		err := s.dispatcher.DispatchQuarantineRecord(ctx, record)
		if err == nil {
			if deleteErr := s.store.Delete(ctx, record.ID); deleteErr != nil {
				// The event was dispatched but the record could not be
				// removed. Release it so it stays replayable; the
				// deterministic idempotency key applied on replay prevents a
				// duplicate publication.
				_ = s.store.Release(ctx, record.ID, deleteErr.Error())
				report.Failed++
				report.Items = append(report.Items, ReplayItem{ID: record.ID, Consumer: record.Consumer, Status: ItemFailed, Error: deleteErr.Error()})
				finalized[record.ID] = struct{}{}
				continue
			}
			report.Replayed++
			report.Items = append(report.Items, ReplayItem{ID: record.ID, Consumer: record.Consumer, Status: ItemReplayed})
			s.log.Info().
				Str("consumer", record.Consumer).
				Str("method", record.Method).
				Str("id", record.ID).
				Msg("quarantined consumer event replayed successfully")
		} else {
			errMsg := err.Error()
			_ = s.store.Release(ctx, record.ID, errMsg)
			report.Failed++
			report.Items = append(report.Items, ReplayItem{ID: record.ID, Consumer: record.Consumer, Status: ItemFailed, Error: errMsg})
			s.log.Error().Err(err).
				Str("consumer", record.Consumer).
				Str("method", record.Method).
				Str("id", record.ID).
				Msg("replaying quarantined consumer event failed")
		}
		finalized[record.ID] = struct{}{}
	}

	return report, nil
}

// releaseOwnedClaims releases records still owned by claimID when the replay
// run ends before finalizing them (context cancellation, panic). It never
// touches records owned by another claim or already finalized.
func (s *Service) releaseOwnedClaims(claimID string, claimed []Record, finalized map[string]struct{}) {
	for _, record := range claimed {
		if _, ok := finalized[record.ID]; ok {
			continue
		}
		current, err := s.store.Get(context.Background(), record.ID)
		if err != nil {
			continue
		}
		if current.Status == StatusReplaying && current.ClaimID == claimID {
			_ = s.store.Release(context.Background(), record.ID, "replay interrupted")
		}
	}
}
