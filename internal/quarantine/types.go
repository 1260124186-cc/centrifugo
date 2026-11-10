// Package quarantine implements a queryable quarantine area for asynchronous
// consumer events which failed with a non-retryable error. Such events are
// stored together with their source (consumer name and type), the API method
// and the original raw payload, so they can be inspected and replayed later
// through the consumer_quarantine_list and consumer_quarantine_replay API
// commands.
package quarantine

import "time"

// Status is the lifecycle status of a quarantine Record.
type Status string

const (
	// StatusQuarantined means the record is stored and waits for a replay.
	StatusQuarantined Status = "quarantined"
	// StatusReplaying means the record was claimed by a replay run and is
	// being dispatched. Records stuck in this status (for example after a
	// process interruption) are swept back to StatusQuarantined by
	// Store.Recover before the next replay.
	StatusReplaying Status = "replaying"
)

// Record is a failed consumer event kept in the quarantine area.
type Record struct {
	// ID is the unique identifier of the record. It is stable across replays
	// and used to address records from the replay API.
	ID string
	// Consumer is the name of the consumer the event came from.
	Consumer string
	// ConsumerType is the type of the consumer the event came from.
	ConsumerType string
	// Method is the API method the event carries (publish, broadcast, ...).
	Method string
	// Payload is the original raw consumed payload, untouched.
	Payload []byte
	// Channels carries the target channels for events consumed in the
	// publication data mode; empty for command mode events.
	Channels []string
	// Error is the last error observed for the event.
	Error string
	// Status is the current lifecycle status of the record.
	Status Status
	// Attempts is the number of replay attempts made for the record.
	Attempts int
	// CreatedAt is when the record entered the quarantine area.
	CreatedAt time.Time
	// UpdatedAt is when the record was last modified.
	UpdatedAt time.Time

	// ClaimID identifies the replay run currently owning the record while it
	// is in StatusReplaying. It is empty while the record is quarantined.
	ClaimID string
	// ClaimedAt is when the current claim was taken.
	ClaimedAt time.Time
}

// ListFilter selects and pages records in Store.List. Zero values do not
// filter.
type ListFilter struct {
	// Consumer filters records by source consumer name.
	Consumer string
	// Method filters records by API method.
	Method string
	// Status filters records by lifecycle status.
	Status Status
	// Limit caps the number of returned records. Zero means DefaultListLimit.
	Limit int
	// Cursor is the ID of the record to start after (exclusive). Records are
	// ordered by creation time and then ID.
	Cursor string
}

// DefaultListLimit is applied when ListFilter.Limit is zero.
const DefaultListLimit = 100

// MaxListLimit caps the page size a caller can request.
const MaxListLimit = 1000
