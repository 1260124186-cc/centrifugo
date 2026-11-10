package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

const reconcileTestChannel = "ns:board"

// reconcileTestAPI builds an Executor backed by a memory map broker for the
// persistent map namespace "ns".
func reconcileTestAPI(t *testing.T) *Executor {
	t.Helper()
	cfg := configWithNamespace("ns", "map")
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	node := nodeWithMapSupport(cfgContainer)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test"})
}

func reconcileEntries(pairs ...string) []*MapReconcileEntry {
	entries := make([]*MapReconcileEntry, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		entries = append(entries, &MapReconcileEntry{
			Key:  pairs[i],
			Data: Raw(pairs[i+1]),
		})
	}
	return entries
}

// readAllMapEntries returns the current state of a map channel keyed by key.
func readAllMapEntries(t *testing.T, api *Executor, ch string) map[string]*MapEntry {
	t.Helper()
	resp := api.MapReadState(context.Background(), &MapReadStateRequest{Channel: ch, Limit: -1})
	require.Nil(t, resp.Error)
	out := make(map[string]*MapEntry, len(resp.Result.Entries))
	for _, entry := range resp.Result.Entries {
		out[entry.Key] = entry
	}
	return out
}

func TestMapReconcileAPI_Validation(t *testing.T) {
	api := reconcileTestAPI(t)

	ctx := context.Background()

	// Missing channel.
	resp := api.MapReconcile(ctx, &MapReconcileRequest{IdempotencyKey: "m", Entries: reconcileEntries("a", `1`)})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Missing idempotency key — without it ownership cannot be tracked.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{Channel: reconcileTestChannel, Entries: reconcileEntries("a", `1`)})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Empty entry key.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m",
		Entries: []*MapReconcileEntry{{Key: "", Data: Raw(`1`)}},
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Nil entry.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m",
		Entries: []*MapReconcileEntry{nil},
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Duplicate keys.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m",
		Entries: reconcileEntries("a", `1`, "a", `2`),
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Invalid base64.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m",
		Entries: []*MapReconcileEntry{{Key: "a", B64Data: "not-base64!"}},
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Namespace data format policy is enforced for every entry.
	cfg := configWithNamespace("ns", "map")
	cfg.Channel.Namespaces[0].PublicationDataFormat = configtypes.PublicationDataFormatJSONObject
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	node := nodeWithMapSupport(cfgContainer)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	strictAPI := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test"})
	resp = strictAPI.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m",
		Entries: reconcileEntries("a", `[1,2]`), // array is not a JSON object
	})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Unknown channel.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: "missing:test", IdempotencyKey: "m",
		Entries: reconcileEntries("a", `1`),
	})
	require.Equal(t, ErrorUnknownChannel, resp.Error)

	// None of the invalid calls wrote anything.
	require.Empty(t, readAllMapEntries(t, api, reconcileTestChannel))
}

func TestMapReconcileAPI_RejectsNonMapNamespaces(t *testing.T) {
	node := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := configWithNamespace("ns", "") // default = stream
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)
	api := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test"})

	resp := api.MapReconcile(context.Background(), &MapReconcileRequest{
		Channel: "ns:test", IdempotencyKey: "m",
		Entries: reconcileEntries("a", `1`),
	})
	require.Equal(t, ErrorBadRequest, resp.Error)
}

func TestMapReconcileAPI_Lifecycle(t *testing.T) {
	api := reconcileTestAPI(t)
	ctx := context.Background()

	// Initial manifest: two cards are created.
	resp := api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `{"v":1}`, "b", `{"v":2}`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Created)
	require.Equal(t, uint32(0), resp.Result.Updated)
	require.Equal(t, uint32(0), resp.Result.Deleted)
	require.Equal(t, uint32(0), resp.Result.Unchanged)
	require.NotZero(t, resp.Result.Offset)
	require.NotEmpty(t, resp.Result.Epoch)

	entries := readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 2)
	require.Equal(t, `{"v":1}`, string(entries["a"].Data))
	require.Equal(t, "board", entries["a"].Tags[mapReconcileOwnerTag])
	require.Equal(t, "board", entries["b"].Tags[mapReconcileOwnerTag])

	// Identical resubmission changes nothing.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `{"v":1}`, "b", `{"v":2}`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(0), resp.Result.Created)
	require.Equal(t, uint32(0), resp.Result.Updated)
	require.Equal(t, uint32(0), resp.Result.Deleted)
	require.Equal(t, uint32(2), resp.Result.Unchanged)
	require.Zero(t, resp.Result.Offset)

	// Update a, keep b, add c.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `{"v":10}`, "b", `{"v":2}`, "c", `{"v":3}`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(1), resp.Result.Created)
	require.Equal(t, uint32(1), resp.Result.Updated)
	require.Equal(t, uint32(0), resp.Result.Deleted)
	require.Equal(t, uint32(1), resp.Result.Unchanged)

	entries = readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 3)
	require.Equal(t, `{"v":10}`, string(entries["a"].Data))
	require.Equal(t, "board", entries["c"].Tags[mapReconcileOwnerTag])

	// b disappears from the manifest: only that owned key is deleted.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `{"v":10}`, "c", `{"v":3}`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(0), resp.Result.Created)
	require.Equal(t, uint32(0), resp.Result.Updated)
	require.Equal(t, uint32(1), resp.Result.Deleted)
	require.Equal(t, uint32(2), resp.Result.Unchanged)

	entries = readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 2)
	require.NotContains(t, entries, "b")
	require.Contains(t, entries, "a")
	require.Contains(t, entries, "c")

	// Empty manifest removes every owned key and touches nothing else.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Deleted)
	require.Empty(t, readAllMapEntries(t, api, reconcileTestChannel))
}

func TestMapReconcileAPI_ForeignKeysProtected(t *testing.T) {
	api := reconcileTestAPI(t)
	ctx := context.Background()

	// A key written through the regular map API has no reconcile owner tag.
	pubResp := api.MapPublish(ctx, &MapPublishRequest{
		Channel: reconcileTestChannel, Key: "manual", Data: Raw(`{"who":"human"}`),
	})
	require.Nil(t, pubResp.Error)

	// Reconcile creates its own cards and leaves the manual key untouched.
	resp := api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `1`, "b", `2`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Created)

	entries := readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 3)
	require.Equal(t, `{"who":"human"}`, string(entries["manual"].Data))
	require.Empty(t, entries["manual"].Tags[mapReconcileOwnerTag])

	// An empty manifest only removes owned keys; the manual key survives.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Deleted)
	entries = readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 1)
	require.Contains(t, entries, "manual")

	// Trying to take over a foreign key is a conflict, and the whole call
	// fails before any write: the sibling new key must not appear.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("manual", `1`, "new", `2`),
	})
	require.NotNil(t, resp.Error)
	require.Equal(t, ErrorConflict.Code, resp.Error.Code)
	entries = readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 1)
	require.NotContains(t, entries, "new")
	require.Equal(t, `{"who":"human"}`, string(entries["manual"].Data))

	// A key owned by another manifest is foreign as well.
	pubResp = api.MapPublish(ctx, &MapPublishRequest{
		Channel: reconcileTestChannel, Key: "other",
		Data: Raw(`3`),
		Tags: map[string]string{mapReconcileOwnerTag: "other-board"},
	})
	require.Nil(t, pubResp.Error)
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("other", `3`),
	})
	require.Equal(t, ErrorConflict.Code, resp.Error.Code)
}

func TestMapReconcileAPI_MultipleManifests(t *testing.T) {
	api := reconcileTestAPI(t)
	ctx := context.Background()

	resp := api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m1",
		Entries: reconcileEntries("a", `1`, "b", `2`),
	})
	require.Nil(t, resp.Error)
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m2",
		Entries: reconcileEntries("x", `10`, "y", `20`),
	})
	require.Nil(t, resp.Error)
	require.Len(t, readAllMapEntries(t, api, reconcileTestChannel), 4)

	// m1 drops b: only b disappears, m2's keys and m1's a remain.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m1",
		Entries: reconcileEntries("a", `1`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(1), resp.Result.Deleted)
	entries := readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 3)
	require.Contains(t, entries, "a")
	require.NotContains(t, entries, "b")
	require.Contains(t, entries, "x")
	require.Contains(t, entries, "y")

	// m2 cannot take over m1's key.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m2",
		Entries: reconcileEntries("a", `1`, "x", `10`, "y", `20`),
	})
	require.Equal(t, ErrorConflict.Code, resp.Error.Code)

	// m2 clearing its manifest leaves m1's key alone.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "m2",
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Deleted)
	entries = readAllMapEntries(t, api, reconcileTestChannel)
	require.Len(t, entries, 1)
	require.Contains(t, entries, "a")
}

func TestMapReconcileAPI_Resumable(t *testing.T) {
	api := reconcileTestAPI(t)
	ctx := context.Background()

	// First application with a partial manifest, as if an earlier run was
	// interrupted before creating the remaining cards.
	resp := api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `1`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(1), resp.Result.Created)

	// Resubmitting the full manifest only applies the missing part.
	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `1`, "b", `2`, "c", `3`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(2), resp.Result.Created)
	require.Equal(t, uint32(1), resp.Result.Unchanged)
	require.Len(t, readAllMapEntries(t, api, reconcileTestChannel), 3)

	// Repeating the full manifest is a no-op and must not grow the stream.
	position := api.MapReadStream(ctx, &MapReadStreamRequest{Channel: reconcileTestChannel, Limit: 0})
	require.Nil(t, position.Error)
	topBefore := position.Result.Offset

	resp = api.MapReconcile(ctx, &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: reconcileEntries("a", `1`, "b", `2`, "c", `3`),
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(3), resp.Result.Unchanged)

	position = api.MapReadStream(ctx, &MapReadStreamRequest{Channel: reconcileTestChannel, Limit: 0})
	require.Nil(t, position.Error)
	require.Equal(t, topBefore, position.Result.Offset)
}

func TestMapReconcileAPI_B64Data(t *testing.T) {
	api := reconcileTestAPI(t)

	payload := []byte(`{"v":1}`)
	resp := api.MapReconcile(context.Background(), &MapReconcileRequest{
		Channel: reconcileTestChannel, IdempotencyKey: "board",
		Entries: []*MapReconcileEntry{{
			Key: "a", B64Data: base64.StdEncoding.EncodeToString(payload),
		}},
	})
	require.Nil(t, resp.Error)
	require.Equal(t, uint32(1), resp.Result.Created)

	entries := readAllMapEntries(t, api, reconcileTestChannel)
	require.True(t, bytes.Equal(payload, entries["a"].Data))
}

func TestMapReconcileAPI_HTTPRoute(t *testing.T) {
	api := reconcileTestAPI(t)

	mux := http.NewServeMux()
	handler := NewHandler(nil, api, Config{})
	mux.Handle("/api/map_reconcile", http.HandlerFunc(handler.handleMapReconcile))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	body := `{"channel":"` + reconcileTestChannel + `","idempotency_key":"board","entries":[{"key":"a","data":{"v":1}}]}`
	httpResp, err := http.Post(server.URL+"/api/map_reconcile", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var decoded MapReconcileResponse
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&decoded))
	_ = httpResp.Body.Close()
	require.Nil(t, decoded.Error)
	require.Equal(t, uint32(1), decoded.Result.Created)

	// Invalid request surfaces the API error in the body.
	httpResp, err = http.Post(server.URL+"/api/map_reconcile", "application/json", bytes.NewBufferString(`{"channel":"`+reconcileTestChannel+`"}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&decoded))
	_ = httpResp.Body.Close()
	require.Equal(t, ErrorBadRequest.Code, decoded.Error.Code)
}

func TestMapReconcileAPI_Batch(t *testing.T) {
	api := reconcileTestAPI(t)

	resp := api.Batch(context.Background(), &BatchRequest{
		Commands: []*Command{{
			MapReconcile: &MapReconcileRequest{
				Channel: reconcileTestChannel, IdempotencyKey: "board",
				Entries: reconcileEntries("a", `1`),
			},
		}},
	})
	require.Nil(t, resp.Replies[0].Error)
	require.NotNil(t, resp.Replies[0].MapReconcile)
	require.Equal(t, uint32(1), resp.Replies[0].MapReconcile.Created)
}

func TestMapReconcileAPI_LegacyRoute(t *testing.T) {
	api := reconcileTestAPI(t)
	node := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	mux := http.NewServeMux()
	handler := NewHandler(node, api, Config{})
	mux.Handle("/api", handler.OldRoute())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	body := `{"method":40,"params":{"channel":"` + reconcileTestChannel + `","idempotency_key":"board","entries":[{"key":"a","data":1}]}}`
	httpResp, err := http.Post(server.URL+"/api", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var reply LegacyReply
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&reply))
	_ = httpResp.Body.Close()
	require.Nil(t, reply.Error)

	var result MapReconcileResult
	require.NoError(t, json.Unmarshal(reply.Result, &result))
	require.Equal(t, uint32(1), result.Created)
}
