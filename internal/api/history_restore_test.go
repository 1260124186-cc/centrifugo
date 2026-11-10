package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/historyrestore"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// fakeRestorer is an in-memory historyrestore.Broker that mirrors the PG
// broker's restore semantics: identical offsets are no-ops, different content
// at an occupied offset is a conflict, and dry runs never mutate state.
type fakeRestorer struct {
	mu        sync.Mutex
	channel   string
	epoch     string
	top       uint64
	stored    map[uint64]historyrestore.Record
	failNext  error
	optsSeen  []historyrestore.Options
	callCount int
}

func newFakeRestorer(channel, epoch string) *fakeRestorer {
	return &fakeRestorer{channel: channel, epoch: epoch, stored: map[uint64]historyrestore.Record{}}
}

func (f *fakeRestorer) SnapshotHistory(_ context.Context, channel string) (*historyrestore.Package, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offsets := make([]uint64, 0, len(f.stored))
	for off := range f.stored {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	records := make([]historyrestore.Record, 0, len(offsets))
	for _, off := range offsets {
		records = append(records, f.stored[off])
	}
	return historyrestore.NewPackage(channel, f.epoch, f.top, records), nil
}

func (f *fakeRestorer) RestoreHistory(_ context.Context, channel string, pkg *historyrestore.Package, opts historyrestore.Options) (*historyrestore.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	f.optsSeen = append(f.optsSeen, opts)
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return nil, err
	}
	if err := historyrestore.Validate(pkg); err != nil {
		return nil, err
	}
	result := &historyrestore.Result{Total: uint64(len(pkg.Publications))}
	for _, rec := range pkg.Publications {
		if existing, ok := f.stored[rec.Offset]; ok {
			if !bytes.Equal(existing.Data, rec.Data) ||
				!historyrestore.EqualTags(existing.Tags, rec.Tags) ||
				!historyrestore.EqualInfo(existing.Info, rec.Info) {
				return nil, fmt.Errorf("%w: offset %d differs", historyrestore.ErrConflict, rec.Offset)
			}
			result.AlreadyPresent++
			continue
		}
		if !opts.DryRun {
			f.stored[rec.Offset] = rec
			if rec.Offset > f.top {
				f.top = rec.Offset
			}
		}
		result.Inserted++
	}
	result.Complete = result.Inserted+result.AlreadyPresent == result.Total
	result.Position = centrifuge.StreamPosition{Offset: f.top, Epoch: f.epoch}
	return result, nil
}

func newRestoreTestExecutor(t *testing.T, restorer historyrestore.Broker) (*Executor, *config.Container) {
	t.Helper()
	node := nodeWithMemoryEngine()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	cfg := config.DefaultConfig()
	cfg.Channel.WithoutNamespace.HistorySize = 100
	cfg.Channel.WithoutNamespace.HistoryTTL = configtypes.Duration(time.Hour)
	cfgContainer, err := config.NewContainer(cfg)
	require.NoError(t, err)

	executor := NewExecutor(node, cfgContainer, &testSurveyCaller{}, ExecutorConfig{Protocol: "test"})
	if restorer != nil {
		executor.SetHistoryRestorer(restorer)
	}
	return executor, cfgContainer
}

func samplePackageProto(channel string) *HistorySnapshotResult {
	pkg := historyrestore.NewPackage(channel, "ep-1", 2, []historyrestore.Record{
		{
			Offset: 1,
			Epoch:  "ep-1",
			Data:   []byte(`{"msg":"one"}`),
			Tags:   map[string]string{"room": "1"},
			Info:   &centrifuge.ClientInfo{ClientID: "c1", UserID: "u1", ConnInfo: []byte(`{"role":"admin"}`)},
		},
		{Offset: 2, Epoch: "ep-1", Data: []byte(`{"msg":"two"}`)},
	})
	return snapshotResultFromPackage(pkg)
}

func TestHistorySnapshotAPI(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	restorer.stored[1] = historyrestore.Record{Offset: 1, Epoch: "ep-1", Data: []byte(`{"msg":"one"}`), Tags: map[string]string{"k": "v"}}
	restorer.top = 1
	executor, _ := newRestoreTestExecutor(t, restorer)

	// Bad request: empty channel.
	resp := executor.HistorySnapshot(context.Background(), &HistorySnapshotRequest{})
	require.Equal(t, ErrorBadRequest, resp.Error)

	// Unknown channel.
	resp = executor.HistorySnapshot(context.Background(), &HistorySnapshotRequest{Channel: "test:unknown"})
	require.Equal(t, ErrorUnknownChannel, resp.Error)

	// Happy path: full proto mapping.
	resp = executor.HistorySnapshot(context.Background(), &HistorySnapshotRequest{Channel: "test"})
	require.Nil(t, resp.Error)
	require.Equal(t, "test", resp.Result.Channel)
	require.Equal(t, "ep-1", resp.Result.Epoch)
	require.EqualValues(t, 1, resp.Result.Offset)
	require.Len(t, resp.Result.Publications, 1)
	pub := resp.Result.Publications[0]
	require.EqualValues(t, 1, pub.Offset)
	require.Equal(t, "ep-1", pub.Epoch)
	require.Equal(t, `{"msg":"one"}`, string(pub.Data))
	require.Equal(t, "v", pub.Tags["k"])
	require.NotEmpty(t, resp.Result.Checksum)
	require.EqualValues(t, historyrestore.PackageVersion, resp.Result.Version)
}

func TestHistorySnapshotAPINotAvailable(t *testing.T) {
	executor, _ := newRestoreTestExecutor(t, nil)
	resp := executor.HistorySnapshot(context.Background(), &HistorySnapshotRequest{Channel: "test"})
	require.Equal(t, ErrorNotAvailable, resp.Error)

	restoreResp := executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{
		Channel: "test",
		Snapshot: samplePackageProto("test"),
	})
	require.Equal(t, ErrorNotAvailable, restoreResp.Error)
}

func TestHistoryRestoreAPI(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	executor, _ := newRestoreTestExecutor(t, restorer)

	// Bad request: empty channel / missing snapshot.
	require.Equal(t, ErrorBadRequest, executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{}).Error)
	require.Equal(t, ErrorBadRequest, executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test"}).Error)

	snap := samplePackageProto("test")

	// First restore inserts everything.
	resp := executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.Nil(t, resp.Error)
	require.EqualValues(t, 2, resp.Result.Total)
	require.EqualValues(t, 2, resp.Result.Inserted)
	require.EqualValues(t, 0, resp.Result.AlreadyPresent)
	require.True(t, resp.Result.Complete)
	require.Equal(t, "ep-1", resp.Result.Epoch)

	// Second restore is idempotent: nothing inserted.
	resp = executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.Nil(t, resp.Error)
	require.EqualValues(t, 0, resp.Result.Inserted)
	require.EqualValues(t, 2, resp.Result.AlreadyPresent)
	require.True(t, resp.Result.Complete)

	// Dry run must not mutate anything and reports would-be inserts.
	restorer.stored = map[uint64]historyrestore.Record{}
	restorer.top = 0
	resp = executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap, DryRun: true})
	require.Nil(t, resp.Error)
	require.EqualValues(t, 2, resp.Result.Inserted)
	require.Empty(t, restorer.stored)
	require.True(t, restorer.optsSeen[len(restorer.optsSeen)-1].DryRun)
}

func TestHistoryRestoreAPIb64Data(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	executor, _ := newRestoreTestExecutor(t, restorer)

	snap := samplePackageProto("test")
	// Replace the first payload with a base64 binary payload.
	snap.Publications[0].Data = nil
	snap.Publications[0].B64Data = "AQID" // 0x01 0x02 0x03
	// Re-sign as the checksum now covers different content.
	pkg, err := restorePackageFromProto("test", snap)
	require.NoError(t, err)
	historyrestore.Sign(pkg)
	snap.Checksum = pkg.Checksum

	resp := executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.Nil(t, resp.Error)
	require.EqualValues(t, 2, resp.Result.Inserted)
	require.Equal(t, []byte{1, 2, 3}, restorer.stored[1].Data)

	// Invalid base64 is a bad request.
	snap.Publications[0].B64Data = "not-base64!!"
	resp = executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.Equal(t, ErrorBadRequest, resp.Error)
}

func TestHistoryRestoreAPIValidationErrors(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	executor, _ := newRestoreTestExecutor(t, restorer)

	// Corrupted checksum.
	snap := samplePackageProto("test")
	snap.Checksum = "deadbeef"
	resp := executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.Equal(t, ErrorBadRequest, resp.Error)
	require.Equal(t, ErrorBadRequest.Code, resp.Error.Code)
	require.Empty(t, restorer.stored) // nothing was written

	// Broker-side conflict maps to 113.
	goodSnap := samplePackageProto("test")
	restorer.failNext = fmt.Errorf("%w: offset 1", historyrestore.ErrConflict)
	resp = executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: goodSnap})
	require.Equal(t, ErrorConflict, resp.Error)
	require.Equal(t, ErrorConflict.Code, resp.Error.Code)

	// Broker internal failure maps to internal error.
	restorer.failNext = fmt.Errorf("storage down")
	resp = executor.HistoryRestore(context.Background(), &HistoryRestoreRequest{Channel: "test", Snapshot: goodSnap})
	require.Equal(t, ErrorInternal, resp.Error)
}

func TestHistorySnapshotRestoreHTTP(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	executor, _ := newRestoreTestExecutor(t, restorer)

	mux := http.NewServeMux()
	apiHandler := NewHandler(nil, executor, Config{})
	for path, handler := range apiHandler.Routes() {
		mux.Handle("/api"+path, handler)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	snap := samplePackageProto("test")

	// Restore over the dedicated HTTP route.
	body, err := json.Marshal(&HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.NoError(t, err)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/history_restore", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	httpResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var restoreResp HistoryRestoreResponse
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&restoreResp))
	_ = httpResp.Body.Close()
	require.Nil(t, restoreResp.Error)
	require.EqualValues(t, 2, restoreResp.Result.Inserted)

	// Snapshot over the dedicated HTTP route.
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/history_snapshot", bytes.NewReader([]byte(`{"channel":"test"}`)))
	httpResp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var snapshotResp HistorySnapshotResponse
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&snapshotResp))
	_ = httpResp.Body.Close()
	require.Nil(t, snapshotResp.Error)
	require.Len(t, snapshotResp.Result.Publications, 2)

	// Batch command carrying both methods.
	batch := &BatchRequest{Commands: []*Command{
		{HistorySnapshot: &HistorySnapshotRequest{Channel: "test"}},
		{HistoryRestore: &HistoryRestoreRequest{Channel: "test", Snapshot: snap}},
	}}
	body, err = json.Marshal(batch)
	require.NoError(t, err)
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/api/batch", bytes.NewReader(body))
	httpResp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var batchResp BatchResponse
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&batchResp))
	_ = httpResp.Body.Close()
	require.Len(t, batchResp.Replies, 2)
	require.Nil(t, batchResp.Replies[0].Error)
	require.NotNil(t, batchResp.Replies[0].HistorySnapshot)
	require.Nil(t, batchResp.Replies[1].Error)
	require.NotNil(t, batchResp.Replies[1].HistoryRestore)
}

func TestHistorySnapshotRestoreLegacyRoute(t *testing.T) {
	restorer := newFakeRestorer("test", "ep-1")
	executor, _ := newRestoreTestExecutor(t, restorer)

	mux := http.NewServeMux()
	apiHandler := NewHandler(executor.node, executor, Config{})
	mux.Handle("/api", apiHandler.OldRoute())
	server := httptest.NewServer(mux)
	defer server.Close()

	snap := samplePackageProto("test")
	params, err := json.Marshal(&HistoryRestoreRequest{Channel: "test", Snapshot: snap})
	require.NoError(t, err)
	cmd := LegacyCommand{Method: Command_HISTORY_RESTORE, Params: Raw(params)}
	body, err := json.Marshal(cmd)
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api", bytes.NewReader(body))
	httpResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	var reply LegacyReply
	require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&reply))
	_ = httpResp.Body.Close()
	require.Nil(t, reply.Error)
	require.NotEmpty(t, reply.Result)
	require.Equal(t, Command_HISTORY_RESTORE, CommandMethodType(41))
}
