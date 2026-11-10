package streamresume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testKey(suffix string) Key {
	return Key{UserID: "user", Channel: "channel" + suffix, ProxyName: "default"}
}

func TestMemoryStoreSaveGet(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	key := testKey("")

	_, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.False(t, ok)

	saved, err := store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), saved.Version)
	require.False(t, saved.UpdatedAt.IsZero())

	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, saved, got)

	// Keys are independent: another channel/proxy/user has no cursor.
	other := testKey("-other")
	_, ok, err = store.Get(ctx, other)
	require.NoError(t, err)
	require.False(t, ok)

	advanced, err := store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 4})
	require.NoError(t, err)
	require.Equal(t, uint64(2), advanced.Version)
	require.Equal(t, uint64(4), advanced.Seq)
}

func TestStoreRejectsRegressionWithinStream(t *testing.T) {
	ctx := context.Background()
	for name, store := range map[string]Store{
		"memory": NewMemoryStore(),
		"file":   newFileStore(t),
	} {
		t.Run(name, func(t *testing.T) {
			key := testKey("")
			first, err := store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 10})
			require.NoError(t, err)

			// A lower sequence in the same stream is stale.
			_, err = store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 9})
			require.ErrorIs(t, err, ErrStaleCursor)
			// An equal sequence is stale too.
			_, err = store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 10})
			require.ErrorIs(t, err, ErrStaleCursor)

			// The previous cursor is still the one readable.
			got, ok, err := store.Get(ctx, key)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, first, got)

			// A new stream lineage starts over even with a small sequence.
			reset, err := store.Save(ctx, key, Cursor{StreamID: "s2", Seq: 1})
			require.NoError(t, err)
			require.Equal(t, "s2", reset.StreamID)
			require.Equal(t, uint64(1), reset.Seq)
			require.Equal(t, first.Version+1, reset.Version)
		})
	}
}

func newFileStore(t *testing.T) *FileStore {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)
	return store
}

func TestFileStoreSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := testKey("")

	store, err := NewFileStore(dir)
	require.NoError(t, err)
	saved, err := store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 7})
	require.NoError(t, err)

	// A brand-new instance (simulating a process restart) reads the cursor.
	reopened, err := NewFileStore(dir)
	require.NoError(t, err)
	got, ok, err := reopened.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, saved.StreamID, got.StreamID)
	require.Equal(t, saved.Seq, got.Seq)
	require.Equal(t, saved.Version, got.Version)
	require.True(t, saved.UpdatedAt.Equal(got.UpdatedAt))

	// The cursor is a single JSON file named after the hashed key.
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestFileStoreFailedSaveKeepsPreviousCursor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	require.NoError(t, err)
	key := testKey("")

	previous, err := store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 5})
	require.NoError(t, err)

	// Make the directory read-only: the temp file can no longer be created,
	// so the atomic replacement fails and the old file must survive.
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	_, err = store.Save(ctx, key, Cursor{StreamID: "s1", Seq: 6})
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrStaleCursor))

	// No temp files are left behind.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".tmp")
	}

	// The previous cursor is intact and still readable.
	got, ok, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, got)
}

func TestFileStoreCorruptCursorReported(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	require.NoError(t, err)
	key := testKey("")

	require.NoError(t, os.WriteFile(filepath.Join(dir, fileName(key)), []byte("{not json"), 0o644))
	_, _, err = store.Get(ctx, key)
	require.Error(t, err)
}

func TestNewFileStoreRequiresDir(t *testing.T) {
	_, err := NewFileStore("")
	require.Error(t, err)
}
