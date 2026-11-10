//go:build integration

package cutover

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPostgresStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, pgtest.SchemaDSN(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	store := NewPostgresStore(pool)
	require.NoError(t, store.EnsureSchema(ctx))
	// Idempotent: a second call must not fail.
	require.NoError(t, store.EnsureSchema(ctx))

	state := &State{
		Name:        "c",
		Version:     1,
		Phase:       PhaseRunning,
		Fingerprint: "fp",
		RedisPoints: []RedisPoint{{Stream: "s", StopID: "9-0"}},
	}
	require.NoError(t, store.CreateState(ctx, state))
	require.ErrorIs(t, store.CreateState(ctx, state), ErrAlreadyExists)

	loaded, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, int64(1), loaded.Version)
	require.Equal(t, "fp", loaded.Fingerprint)
	require.Equal(t, "9-0", loaded.RedisPoints[0].StopID)

	require.NoError(t, store.UpdateState(ctx, loaded))
	require.Equal(t, int64(2), loaded.Version)
	// Stale version is rejected, the previous progress stays untouched.
	require.ErrorIs(t, store.UpdateState(ctx, &State{Name: "c", Version: 1}), ErrVersionConflict)

	// Markers: reserve, confirm, release and conflict detection.
	acquired, _, err := store.ReserveMarker(ctx, "c", Marker{Identity: "e1", ContentHash: "h1", SourceKind: SourceKindRedisStream, Offset: "1-0"})
	require.NoError(t, err)
	require.True(t, acquired)
	acquired, existing, err := store.ReserveMarker(ctx, "c", Marker{Identity: "e1", ContentHash: "h1"})
	require.NoError(t, err)
	require.False(t, acquired)
	require.Equal(t, markerStatusReserved, existing.Status)

	require.NoError(t, store.ConfirmMarker(ctx, "c", "e1"))
	markers, err := store.GetMarkers(ctx, "c", []string{"e1"})
	require.NoError(t, err)
	require.Equal(t, markerStatusDelivered, markers["e1"].Status)

	count, err := store.CountReservations(ctx, "c")
	require.NoError(t, err)
	require.Zero(t, count)

	acquired, _, err = store.ReserveMarker(ctx, "c", Marker{Identity: "e2", ContentHash: "h2"})
	require.NoError(t, err)
	require.True(t, acquired)
	count, err = store.CountReservations(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, store.ReleaseReservations(ctx, "c"))
	count, err = store.CountReservations(ctx, "c")
	require.NoError(t, err)
	require.Zero(t, count)

	// FailState keeps checkpoints intact.
	current, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, store.FailState(ctx, "c", current.Version, PhaseFailed, "boom"))
	failed, err := store.GetState(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, PhaseFailed, failed.Phase)
	require.Equal(t, "9-0", failed.RedisPoints[0].StopID)
}
