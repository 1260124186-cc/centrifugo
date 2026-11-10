// Package historyrestore defines the broker-agnostic history snapshot and
// restore contract used by the history_snapshot and history_restore API
// methods. A recovery Package captures every publication of a channel with
// its original stream position (offset and epoch), payload, tags and client
// information, and carries a checksum so corruption is detected before any
// write. Brokers that can re-insert publications at their original offsets
// implement Broker; the API layer rejects restore calls against brokers that
// do not.
package historyrestore

import (
	"context"
	"errors"

	"github.com/centrifugal/centrifuge"
)

// PackageVersion is the version of the recovery package format produced and
// accepted by this build. Bump when the on-the-wire shape of Package changes
// in an incompatible way.
const PackageVersion = 1

var (
	// ErrCorrupted means the package bytes do not match their checksum: the
	// recovery package was damaged after creation. Restore must not write
	// anything in this case.
	ErrCorrupted = errors.New("history restore package corrupted")
	// ErrInvalid means the package is well-encoded but structurally invalid
	// (bad channel, unsupported version, duplicate offsets, wrong epoch, ...).
	ErrInvalid = errors.New("history restore package invalid")
	// ErrConflict means a publication already exists at one of the package's
	// offsets but carries different data, tags or client info. The caller's
	// stream and the package disagree about history, so restore fails and
	// leaves the stream untouched.
	ErrConflict = errors.New("history restore conflict")
)

// Record is one publication captured in a recovery Package. It keeps the
// original stream position so the publication can be re-inserted exactly
// where clients expect it.
type Record struct {
	// Offset is the original channel_offset of the publication. It is unique
	// and strictly increasing inside a Package.
	Offset uint64
	// Epoch is the stream epoch the publication belonged to. Every record of
	// a package shares the package-level Epoch.
	Epoch string
	// Data is the publication payload.
	Data []byte
	// Tags are the publication tags, nil when the publication had none.
	Tags map[string]string
	// Info is the optional information about the client that published.
	Info *centrifuge.ClientInfo
}

// Package is a self-describing recovery package for one channel.
type Package struct {
	// Version is the package format version, currently PackageVersion.
	Version int
	// Channel is the channel the publications belong to. Restore requests
	// must target the same channel.
	Channel string
	// Epoch is the stream epoch captured at snapshot time.
	Epoch string
	// Offset is the stream top offset captured at snapshot time. Every
	// publication offset is within [1, Offset].
	Offset uint64
	// CreatedAtMs is the snapshot creation time as Unix milliseconds.
	CreatedAtMs int64
	// Checksum is the lowercase hex SHA-256 of the canonical encoding of all
	// other fields, see Sign and Validate.
	Checksum string
	// Publications are the captured publications ordered by ascending offset.
	Publications []Record
}

// Options tunes a RestoreHistory call.
type Options struct {
	// DryRun runs every validation and conflict check without writing. The
	// returned Result describes what a real restore would do; no row is
	// committed.
	DryRun bool
}

// Result reports what a RestoreHistory call did.
type Result struct {
	// Total is the number of records carried by the package.
	Total uint64
	// Inserted is the number of publications written by this call.
	Inserted uint64
	// AlreadyPresent is the number of package records whose publication was
	// already stored with identical content. Repeated restores therefore
	// report zero Inserted once the stream is complete.
	AlreadyPresent uint64
	// Position is the stream top position after the restore (or, for a dry
	// run, the position that would be in effect afterwards).
	Position centrifuge.StreamPosition
	// Complete is true when every package record is present after the call,
	// i.e. Inserted + AlreadyPresent == Total.
	Complete bool
}

// Broker is the optional capability a centrifuge.Broker implementation can
// provide to support history snapshots and offset-preserving restores.
type Broker interface {
	// SnapshotHistory builds a recovery Package for channel. It must include
	// every publication still retained for the channel's current epoch with
	// its original offset, epoch, payload, tags and client info.
	SnapshotHistory(ctx context.Context, channel string) (*Package, error)
	// RestoreHistory validates pkg and fills the missing publications of
	// channel at their original offsets. Existing identical publications are
	// kept, publications outside the package are never touched, and any
	// corruption, conflict or write failure fails the whole call without
	// changing the stream. The operation must be idempotent: re-applying the
	// same package never creates duplicates and only fills what is still
	// missing.
	RestoreHistory(ctx context.Context, channel string, pkg *Package, opts Options) (*Result, error)
}
