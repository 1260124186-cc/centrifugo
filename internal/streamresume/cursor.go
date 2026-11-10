// Package streamresume keeps the last publication sequence a subscribe
// stream proxy confirmed delivered to a client, keyed independently of any
// single connection: by user, channel and proxy stream identity. On
// reconnect, possibly on another node, the cursor lets Centrifugo ask the
// backend stream to continue after it instead of restarting from the
// beginning.
package streamresume

import (
	"context"
	"errors"
	"time"
)

// Key identifies a subscription cursor. It deliberately contains no
// connection ID: a reconnecting client (or the same user reconnecting
// through another node) must find the cursor the previous connection left.
type Key struct {
	// UserID is the connection user the subscription belongs to.
	UserID string
	// Channel is the subscribed channel.
	Channel string
	// ProxyName is the configured subscribe stream proxy serving the
	// channel, so cursors of different backends never mix.
	ProxyName string
}

// Cursor is the last delivery position confirmed for a Key. A cursor only
// advances after the publication it points at was written to the client and
// the cursor itself was durably saved.
type Cursor struct {
	// StreamID identifies the backend stream the sequence belongs to, as
	// returned in SubscribeResult.stream_id. A different StreamID means the
	// backend reset its stream and the previous cursor is unrecoverable.
	StreamID string `json:"stream_id"`
	// Seq is the last sequence confirmed delivered.
	Seq uint64 `json:"seq"`
	// Version increments on every accepted save. It makes every persisted
	// cursor version distinguishable and lets callers detect stale writes.
	Version uint64 `json:"version"`
	// UpdatedAt is when the cursor version was saved.
	UpdatedAt time.Time `json:"updated_at"`
}

// ErrStaleCursor is returned by Store.Save when a cursor for the same
// StreamID already exists at an equal or greater Seq. The stored cursor is
// left untouched. A new StreamID always starts a new cursor lineage.
var ErrStaleCursor = errors.New("stream resume cursor is stale")

// Store persists delivery cursors. All implementations must make Save
// atomic: when Save returns an error, including ErrStaleCursor, the
// previously stored cursor has to remain readable unchanged.
type Store interface {
	// Get returns the cursor stored for key. ok is false when none exists.
	Get(ctx context.Context, key Key) (cursor Cursor, ok bool, err error)
	// Save stores cursor for key. The StreamID and Seq fields of cursor
	// carry the new position; implementations own Version and UpdatedAt.
	// It returns the cursor as persisted. Save rejects a regression of Seq
	// within the same StreamID with ErrStaleCursor.
	Save(ctx context.Context, key Key, cursor Cursor) (Cursor, error)
}

// acceptCursor applies the common save rules: a cursor never moves
// backwards within a stream lineage, and every accepted save gets a new
// version and timestamp. It is shared by Store implementations.
func acceptCursor(previous Cursor, hasPrevious bool, next Cursor, now time.Time) (Cursor, error) {
	if hasPrevious && previous.StreamID == next.StreamID && previous.Seq >= next.Seq {
		return previous, ErrStaleCursor
	}
	if hasPrevious {
		next.Version = previous.Version + 1
	} else {
		next.Version = 1
	}
	next.UpdatedAt = now
	return next, nil
}
