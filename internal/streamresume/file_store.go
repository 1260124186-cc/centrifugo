package streamresume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileStore persists every cursor as its own JSON file, so cursors survive
// process restarts and can be shared by nodes mounting the same directory.
// Writes are atomic (temp file, fsync, rename): a failed or interrupted save
// always leaves the previous cursor file intact.
type FileStore struct {
	dir string

	mu     sync.Mutex
	cached map[Key]Cursor
	loaded map[Key]bool
	now    func() time.Time
}

// NewFileStore opens (creating if needed) the cursor directory at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("stream resume file store requires a directory")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create stream resume cursor directory: %w", err)
	}
	return &FileStore{
		dir:    dir,
		cached: map[Key]Cursor{},
		loaded: map[Key]bool{},
		now:    time.Now,
	}, nil
}

// keyPart separates the components of a key while hashing: none of them can
// contain a NUL byte.
const keyPart = "\x00"

func fileName(key Key) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{key.UserID, key.Channel, key.ProxyName}, keyPart)))
	return hex.EncodeToString(sum[:]) + ".json"
}

func (s *FileStore) path(key Key) string {
	return filepath.Join(s.dir, fileName(key))
}

// Get implements Store. A corrupt cursor file is reported as an error and
// never deleted, so an operator can inspect it.
func (s *FileStore) Get(_ context.Context, key Key) (Cursor, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded[key] {
		cursor, ok := s.cached[key]
		return cursor, ok, nil
	}
	data, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.loaded[key] = true
			return Cursor{}, false, nil
		}
		return Cursor{}, false, fmt.Errorf("read stream resume cursor: %w", err)
	}
	var cursor Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return Cursor{}, false, fmt.Errorf("decode stream resume cursor: %w", err)
	}
	s.cached[key] = cursor
	s.loaded[key] = true
	return cursor, true, nil
}

// Save implements Store with an atomic file replacement.
func (s *FileStore) Save(_ context.Context, key Key, cursor Cursor) (Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous, hasPrevious := s.cached[key], s.loaded[key]
	if !hasPrevious {
		// The cache may be cold: consult the file before deciding staleness.
		data, err := os.ReadFile(s.path(key))
		switch {
		case err == nil:
			if err := json.Unmarshal(data, &previous); err != nil {
				return Cursor{}, fmt.Errorf("decode stream resume cursor: %w", err)
			}
			hasPrevious = true
		case errors.Is(err, os.ErrNotExist):
		default:
			return Cursor{}, fmt.Errorf("read stream resume cursor: %w", err)
		}
	}

	accepted, err := acceptCursor(previous, hasPrevious, cursor, s.now())
	if err != nil {
		return previous, err
	}

	data, err := json.MarshalIndent(&accepted, "", "  ")
	if err != nil {
		return previous, fmt.Errorf("encode stream resume cursor: %w", err)
	}

	if err := s.writeAtomic(s.path(key), data); err != nil {
		// The previous file is untouched: keep serving the previous cursor.
		return previous, err
	}

	s.cached[key] = accepted
	s.loaded[key] = true
	return accepted, nil
}

// writeAtomic writes data to path via a temp file in the same directory and
// renames it into place, fsync-ing both the file and its directory so the
// cursor is durable once the call returns nil.
func (s *FileStore) writeAtomic(path string, data []byte) (retErr error) {
	tmp, err := os.CreateTemp(s.dir, ".cursor-*.tmp")
	if err != nil {
		return fmt.Errorf("create stream resume cursor temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if retErr != nil {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write stream resume cursor: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync stream resume cursor: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close stream resume cursor: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace stream resume cursor: %w", err)
	}
	syncDir(s.dir)
	return nil
}
