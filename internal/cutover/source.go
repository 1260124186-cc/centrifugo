package cutover

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SourceDescriptor identifies and validates one cutover source. Its
// canonical rendering feeds the cutover fingerprint, so a repeated call that
// resolves different sources is rejected before any progress changes.
type SourceDescriptor struct {
	// Kind is one of the SourceKind* constants.
	Kind string
	// ConsumerName is the name of the consumer in the Centrifugo configuration.
	ConsumerName string
	// Locator is the human-readable source location (stream keys or table).
	Locator string
	// Details carries the validated connection and field mapping.
	Details map[string]string
}

// Fingerprint renders the descriptors and the identity rules into a stable
// hash. Any change invalidates a resumed cutover.
func Fingerprint(redis, postgres SourceDescriptor, opts Options) string {
	h := sha256.New()
	write := func(s string) { _, _ = h.Write([]byte(s)); _, _ = h.Write([]byte{0}) }
	write(redis.Kind)
	write(redis.ConsumerName)
	write(redis.Locator)
	for _, k := range sortedKeys(redis.Details) {
		write(k)
		write(redis.Details[k])
	}
	write(postgres.Kind)
	write(postgres.ConsumerName)
	write(postgres.Locator)
	for _, k := range sortedKeys(postgres.Details) {
		write(k)
		write(postgres.Details[k])
	}
	write(opts.RedisIdentityValue)
	write(opts.PostgresIdentityColumn)
	write(opts.IdentityPayloadField)
	return hex.EncodeToString(h.Sum(nil))
}

// Source is the read-only view the Manager uses over one side of the
// cutover. Implementations must not acknowledge or delete anything: the
// live consumers keep owning their source while a cutover converges.
type Source interface {
	// Descriptor returns the validated source descriptor.
	Descriptor() SourceDescriptor
	// Ping verifies connectivity.
	Ping(ctx context.Context) error
	// Frontier returns the positions captured when a cutover is created:
	// the old source stop points or the new source start points.
	Frontier(ctx context.Context) ([]FrontierPoint, error)
	// Read returns events strictly after the given cursor, bounded by limit
	// and by the frontier. Events come back in source order.
	Read(ctx context.Context, cursor Cursor, limit int) ([]Event, Cursor, error)
	// Identity extracts the stable event identity. It returns ErrNoIdentity
	// when neither the native field nor the payload field is present.
	Identity(ev Event) (string, error)
	// Close releases connections held by the source.
	Close() error
}

// FrontierPoint is one element of a source frontier.
type FrontierPoint struct {
	// Stream is the Redis Stream key (Redis sources only).
	Stream string
	// Partition is the outbox partition (Postgres sources only).
	Partition int64
	// Offset is the frontier position, entry ID for Redis and row ID for Postgres.
	Offset string
}

// Cursor resumes a Read at a concrete source position.
type Cursor struct {
	// Stream is the Redis Stream key (Redis sources only).
	Stream string
	// Partition is the outbox partition (Postgres sources only).
	Partition int64
	// Offset is the last scanned position, exclusive.
	Offset string
	// Bound is the frontier offset; reads never pass it. Empty means no bound.
	Bound string
}

// ContentHash hashes the content of an event so the same identity arriving
// with different payloads can be detected as a conflict.
func ContentHash(ev Event) string {
	h := sha256.New()
	_, _ = h.Write([]byte(ev.Method))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(ev.Payload)
	return hex.EncodeToString(h.Sum(nil))
}

// payloadIdentity extracts a top-level JSON string field from the payload.
func payloadIdentity(payload []byte, field string) (string, error) {
	if field == "" || len(payload) == 0 {
		return "", ErrNoIdentity
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return "", fmt.Errorf("%w: payload is not valid JSON: %v", ErrNoIdentity, err)
	}
	raw, ok := doc[field]
	if !ok {
		return "", ErrNoIdentity
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%w: identity field %q is not a string", ErrNoIdentity, field)
	}
	if value == "" {
		return "", ErrNoIdentity
	}
	return value, nil
}
