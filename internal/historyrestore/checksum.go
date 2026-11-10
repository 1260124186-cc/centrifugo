package historyrestore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/centrifugal/centrifuge"
)

// clientInfoWire is the canonical representation of centrifuge.ClientInfo
// used for checksum computation. Field names follow the API proto conventions.
type clientInfoWire struct {
	Client   string `json:"client"`
	User     string `json:"user"`
	ConnInfo []byte `json:"conn_info"`
	ChanInfo []byte `json:"chan_info"`
}

// recordWire is the canonical representation of a Record. encoding/json emits
// map keys sorted and []byte as deterministic base64, so the same package
// content always produces the same bytes.
type recordWire struct {
	Offset uint64            `json:"offset"`
	Epoch  string            `json:"epoch"`
	Data   []byte            `json:"data"`
	Tags   map[string]string `json:"tags,omitempty"`
	Info   *clientInfoWire   `json:"info,omitempty"`
}

// packageWire mirrors Package without Checksum: the checksum is computed over
// exactly this representation, so changing Checksum can never affect it.
type packageWire struct {
	Version      int          `json:"version"`
	Channel      string       `json:"channel"`
	Epoch        string       `json:"epoch"`
	Offset       uint64       `json:"offset"`
	CreatedAtMs  int64        `json:"created_at_ms"`
	Publications []recordWire `json:"publications"`
}

func toWire(pkg *Package) packageWire {
	wire := packageWire{
		Version:      pkg.Version,
		Channel:      pkg.Channel,
		Epoch:        pkg.Epoch,
		Offset:       pkg.Offset,
		CreatedAtMs:  pkg.CreatedAtMs,
		Publications: make([]recordWire, 0, len(pkg.Publications)),
	}
	for _, rec := range pkg.Publications {
		rw := recordWire{
			Offset: rec.Offset,
			Epoch:  rec.Epoch,
			Data:   rec.Data,
			Tags:   rec.Tags,
		}
		if rec.Info != nil {
			rw.Info = &clientInfoWire{
				Client:   rec.Info.ClientID,
				User:     rec.Info.UserID,
				ConnInfo: rec.Info.ConnInfo,
				ChanInfo: rec.Info.ChanInfo,
			}
		}
		wire.Publications = append(wire.Publications, rw)
	}
	return wire
}

// ComputeChecksum returns the lowercase hex SHA-256 of the canonical encoding
// of the package content. The Checksum field itself is not part of the input.
func ComputeChecksum(pkg *Package) string {
	data, err := json.Marshal(toWire(pkg))
	if err != nil {
		// All involved types are JSON-encodable, so this is unreachable in
		// practice. Keep a deterministic fallback instead of panicking.
		return fmt.Sprintf("marshal-error:%x", sha256.Sum256(nil))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Sign fills Version (when unset), CreatedAtMs (when zero) and Checksum.
// Brokers call it once after assembling a snapshot; API clients never need to
// sign packages themselves.
func Sign(pkg *Package) {
	if pkg.Version == 0 {
		pkg.Version = PackageVersion
	}
	if pkg.CreatedAtMs == 0 {
		pkg.CreatedAtMs = time.Now().UnixMilli()
	}
	pkg.Checksum = ComputeChecksum(pkg)
}

// NewPackage assembles a signed recovery package for channel.
func NewPackage(channel, epoch string, topOffset uint64, records []Record) *Package {
	pkg := &Package{
		Version:      PackageVersion,
		Channel:      channel,
		Epoch:        epoch,
		Offset:       topOffset,
		CreatedAtMs:  time.Now().UnixMilli(),
		Publications: records,
	}
	Sign(pkg)
	return pkg
}

// Validate checks the checksum and the structural integrity of pkg. It returns
// ErrCorrupted when the checksum is missing or does not match, and ErrInvalid
// for any structural problem (unsupported version, empty channel, offsets out
// of range or not strictly increasing, epoch mismatches). Callers must run it
// before touching storage.
func Validate(pkg *Package) error {
	if pkg == nil {
		return fmt.Errorf("%w: empty package", ErrInvalid)
	}
	if pkg.Version != PackageVersion {
		return fmt.Errorf("%w: unsupported package version %d", ErrInvalid, pkg.Version)
	}
	if pkg.Channel == "" {
		return fmt.Errorf("%w: empty channel", ErrInvalid)
	}
	if pkg.Checksum == "" {
		return fmt.Errorf("%w: missing checksum", ErrCorrupted)
	}
	if ComputeChecksum(pkg) != pkg.Checksum {
		return fmt.Errorf("%w: checksum mismatch for channel %q", ErrCorrupted, pkg.Channel)
	}

	var lastOffset uint64
	for i, rec := range pkg.Publications {
		if rec.Offset == 0 {
			return fmt.Errorf("%w: publication %d has zero offset", ErrInvalid, i)
		}
		if rec.Offset > pkg.Offset {
			return fmt.Errorf("%w: publication offset %d exceeds package top offset %d", ErrInvalid, rec.Offset, pkg.Offset)
		}
		if i > 0 && rec.Offset <= lastOffset {
			return fmt.Errorf("%w: offsets must be strictly increasing, got %d after %d", ErrInvalid, rec.Offset, lastOffset)
		}
		if rec.Epoch != pkg.Epoch {
			return fmt.Errorf("%w: publication at offset %d has epoch %q, package epoch is %q", ErrInvalid, rec.Offset, rec.Epoch, pkg.Epoch)
		}
		lastOffset = rec.Offset
	}
	return nil
}

// EqualInfo reports whether two client info values carry the same content.
// Both nil means equal; one nil means different.
func EqualInfo(a, b *centrifuge.ClientInfo) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ClientID == b.ClientID &&
		a.UserID == b.UserID &&
		equalBytes(a.ConnInfo, b.ConnInfo) &&
		equalBytes(a.ChanInfo, b.ChanInfo)
}

// EqualTags reports whether two tag maps carry the same key/value pairs.
// nil and empty maps are treated as equal.
func EqualTags(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
