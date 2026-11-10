// Package cutover implements an interruptible, resumable cutover of command
// consumption from a Redis Stream source to a PostgreSQL outbox source.
//
// The cutover progress (old source stop points, new source start points and
// the identities of stably delivered events) is persisted in a versioned
// store before any source switch is committed. That makes every cutover
// reentrant: repeating the same call or restarting the node resumes
// convergence from the last committed progress instead of starting over.
package cutover

import (
	"errors"
	"time"
)

// Source kinds participating in a cutover.
const (
	SourceKindRedisStream    = "redis_stream"
	SourceKindPostgresOutbox = "postgresql_outbox"
	markerStatusReserved     = "reserved"
	markerStatusDelivered    = "delivered"
	defaultBatchLimit        = 100
)

// Phases of a cutover state machine.
const (
	// PhaseRunning means the cutover was created and is converging, possibly
	// across several calls or node restarts.
	PhaseRunning = "running"
	// PhaseCommitted means both sources reached their recorded frontiers and
	// the new progress was committed. Only after this phase the operator is
	// expected to retire the old source configuration.
	PhaseCommitted = "committed"
	// PhaseFailed means the last run stopped on a validation mismatch, an
	// identity conflict or a read/dispatch failure. The previous progress is
	// preserved and a repeated call resumes from it.
	PhaseFailed = "failed"
	// PhaseCancelled means the cutover was explicitly cancelled. Its progress
	// is kept for inspection but no longer converges.
	PhaseCancelled = "cancelled"
)

var (
	// ErrNotFound is returned by Store implementations when a state does not exist.
	ErrNotFound = errors.New("cutover state not found")
	// ErrAlreadyExists is returned when a cutover with the same name exists.
	ErrAlreadyExists = errors.New("cutover already exists")
	// ErrVersionConflict is returned on a lost optimistic concurrency update.
	ErrVersionConflict = errors.New("cutover state version conflict")
	// ErrValidationMismatch means the sources resolved for a repeated call do
	// not match the ones recorded when the cutover was created.
	ErrValidationMismatch = errors.New("cutover source validation mismatch")
	// ErrIdentityConflict means the same stable identity is associated with
	// different event content on the two sources.
	ErrIdentityConflict = errors.New("cutover event identity conflict")
	// ErrReadFailed wraps a failure while reading either source. The cutover
	// stops and the previous progress remains in place.
	ErrReadFailed = errors.New("cutover source read failed")
	// ErrNoIdentity means an event carries no extractable stable identity.
	ErrNoIdentity = errors.New("cutover event has no stable identity")
	// ErrBadOptions means the request references unknown or misconfigured consumers.
	ErrBadOptions = errors.New("invalid cutover options")
)

// Options configures one consumer_cutover call. Zero-value optional fields
// fall back to the defaults documented on the API request.
type Options struct {
	// Name identifies the cutover instance. It is the resume key across calls
	// and node restarts. Required.
	Name string
	// RedisConsumerName is the name of the configured redis_stream consumer.
	RedisConsumerName string
	// PostgresConsumerName is the name of the configured postgresql consumer.
	PostgresConsumerName string
	// RedisIdentityValue is the Redis Stream message value field holding the
	// stable event identity.
	RedisIdentityValue string
	// PostgresIdentityColumn is the outbox column holding the stable event
	// identity. When empty the identity is extracted from IdentityPayloadField.
	PostgresIdentityColumn string
	// IdentityPayloadField is the JSON field inside the event payload used as
	// a fallback identity source on both sides.
	IdentityPayloadField string
	// BatchLimit bounds the number of events scanned per source per pass.
	BatchLimit int
	// MaxEvents bounds the number of events handled by one call. Zero means
	// the call runs until convergence or context cancellation.
	MaxEvents int64
	// Cancel requests cancellation of an in-progress cutover.
	Cancel bool
}

// Event is a source-neutral view of a consumed command.
type Event struct {
	// SourceKind is SourceKindRedisStream or SourceKindPostgresOutbox.
	SourceKind string
	// Stream is the Redis Stream key (Redis events only).
	Stream string
	// Partition is the outbox partition (Postgres events only).
	Partition int64
	// Offset is the source-native position: a Redis entry ID or an outbox row ID.
	Offset string
	// Method is the API command method.
	Method string
	// Payload is the serialized API command request.
	Payload []byte
	// Fields carries the native source entry values (Redis Stream message
	// values). It is nil for Postgres events and lets identity extraction
	// use a configured native field without an extra source round trip.
	Fields map[string]string
}

// RedisPoint is a recorded position of one Redis Stream.
type RedisPoint struct {
	// Stream is the Redis Stream key.
	Stream string `json:"stream"`
	// StopID is the old source stop point: the group's last delivered entry
	// ID captured when the cutover was created.
	StopID string `json:"stop_id"`
	// LastScannedID is the convergence scan position. Entries up to this ID
	// have already been reconciled.
	LastScannedID string `json:"last_scanned_id"`
}

// PostgresPoint is a recorded position of one outbox partition.
type PostgresPoint struct {
	// Partition is the outbox partition number.
	Partition int64 `json:"partition"`
	// StartID is the new source start point: the maximum outbox ID captured
	// when the cutover was created.
	StartID int64 `json:"start_id"`
	// LastScannedID is the convergence scan position. Rows up to this ID have
	// already been reconciled.
	LastScannedID int64 `json:"last_scanned_id"`
}

// State is the versioned, persistable cutover progress.
type State struct {
	// Name is the cutover instance name.
	Name string `json:"name"`
	// Version is monotonically increased on every committed progress update.
	Version int64 `json:"version"`
	// Phase is one of the Phase* constants.
	Phase string `json:"phase"`
	// Reason describes why the last run failed or was cancelled.
	Reason string `json:"reason"`

	// Fingerprint pins the validated source descriptors and identity
	// extraction rules. A repeated call resolving different sources fails
	// validation instead of touching this progress.
	Fingerprint string `json:"fingerprint"`

	// RedisPoints records the old source stop points and scan positions.
	RedisPoints []RedisPoint `json:"redis_points"`
	// PostgresPoints records the new source start points and scan positions.
	PostgresPoints []PostgresPoint `json:"postgres_points"`

	// Delivered is the number of events confirmed delivered by the cutover.
	Delivered int64 `json:"delivered"`
	// Skipped is the number of events already delivered by either source and
	// therefore not executed again.
	Skipped int64 `json:"skipped"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Complete reports whether the cutover reached the committed phase.
func (s *State) Complete() bool {
	return s.Phase == PhaseCommitted
}

// Marker is the persisted bookkeeping for one stable event identity.
type Marker struct {
	// Identity is the stable cross-source event identity.
	Identity string
	// ContentHash is the hash of the event content used to detect identity
	// conflicts between the two sources.
	ContentHash string
	// Status is markerStatusReserved or markerStatusDelivered.
	Status string
	// SourceKind records which side produced the marker.
	SourceKind string
	// Offset is the source-native position the marker was created from.
	Offset string
}
