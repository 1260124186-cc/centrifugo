package proxy

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/streamresume"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// resumeClient answers SubscribeUnidirectional with a resumeStream and
// records the SubscribeRequest Centrifugo sent, including its resume cursor.
type resumeClient struct {
	proxyproto.CentrifugoProxyClient
	stream *resumeStream
}

func (c resumeClient) SubscribeUnidirectional(_ context.Context, req *proxyproto.SubscribeRequest, _ ...grpc.CallOption) (proxyproto.CentrifugoProxy_SubscribeUnidirectionalClient, error) {
	c.stream.req = req
	return c.stream, nil
}

type resumeStream struct {
	grpc.ClientStream
	mu    sync.Mutex
	sent  bool
	first *proxyproto.StreamSubscribeResponse
	feed  chan *proxyproto.StreamSubscribeResponse
	req   *proxyproto.SubscribeRequest
}

func (s *resumeStream) Recv() (*proxyproto.StreamSubscribeResponse, error) {
	s.mu.Lock()
	if !s.sent {
		s.sent = true
		s.mu.Unlock()
		return s.first, nil
	}
	s.mu.Unlock()
	resp, ok := <-s.feed
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

// recordingClient records every publication written to the client, keyed by
// the stream sequence it arrived with.
type recordingClient struct {
	*tools.TestClientMock
	mu       sync.Mutex
	written  []uint64
	attempts int
	writeErr error
}

func (c *recordingClient) WritePublication(_ string, pub *centrifuge.Publication, _ centrifuge.StreamPosition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	if c.writeErr != nil {
		return c.writeErr
	}
	seq, _ := strconv.ParseUint(pub.Tags["seq"], 10, 64)
	c.written = append(c.written, seq)
	return nil
}

func (c *recordingClient) Unsubscribe(string, ...centrifuge.Unsubscribe) {}

func (c *recordingClient) sequences() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uint64, len(c.written))
	copy(out, c.written)
	return out
}

func newResumeHandler(t *testing.T, store streamresume.Store, stream *resumeStream) *SubscribeStreamHandler {
	t.Helper()
	return NewSubscribeStreamHandler(SubscribeStreamHandlerConfig{
		Proxies: map[string]*SubscribeStreamProxy{
			"default": {
				config: Config{Timeout: configtypes.Duration(10 * time.Second)},
				client: resumeClient{stream: stream},
			},
		},
		ResumeStore: store,
	})
}

func newResumeStream(streamID string) *resumeStream {
	return &resumeStream{
		first: &proxyproto.StreamSubscribeResponse{
			SubscribeResponse: &proxyproto.SubscribeResponse{
				Result: &proxyproto.SubscribeResult{StreamId: streamID},
			},
		},
		feed: make(chan *proxyproto.StreamSubscribeResponse, 8),
	}
}

func newRecordingClient() *recordingClient {
	return &recordingClient{
		TestClientMock: &tools.TestClientMock{
			IDFunc:        func() string { return "client" },
			UserIDFunc:    func() string { return "user" },
			ContextFunc:   context.Background,
			TransportFunc: func() centrifuge.TransportInfo { return tools.NewTestTransport() },
		},
	}
}

func pubWithSeq(seq uint64) *proxyproto.StreamSubscribeResponse {
	return &proxyproto.StreamSubscribeResponse{
		Publication: &proxyproto.Publication{
			Data: []byte("d"),
			// The client protocol does not carry the proxy sequence, so it is
			// mirrored into a tag for the recording mock.
			Tags: map[string]string{"seq": strconv.FormatUint(seq, 10)},
			Seq:  seq,
		},
	}
}

func resumeChannelOptions() configtypes.ChannelOptions {
	return configtypes.ChannelOptions{
		SubscribeStreamProxyEnabled: true,
		SubscribeStreamProxyName:    "default",
		StreamResume:                true,
	}
}

// A first subscription sends no resume cursor and persists the sequence of
// every publication confirmed delivered.
func TestSubscribeStreamResumeFirstSubscription(t *testing.T) {
	store := streamresume.NewMemoryStore()
	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	require.Nil(t, stream.req.Resume)

	for _, seq := range []uint64{1, 2, 3} {
		stream.feed <- pubWithSeq(seq)
	}
	require.Eventually(t, func() bool { return len(client.sequences()) == 3 }, time.Second, 10*time.Millisecond)

	cursor, ok, err := store.Get(context.Background(), streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "stream-1", cursor.StreamID)
	require.Equal(t, uint64(3), cursor.Seq)
}

// On reconnect the saved cursor goes into the SubscribeRequest, confirmed
// events are never redelivered and only events after the cursor reach the
// client, even after several reconnect attempts.
func TestSubscribeStreamResumeReconnectSkipsConfirmed(t *testing.T) {
	store := streamresume.NewMemoryStore()
	key := streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"}
	_, err := store.Save(context.Background(), key, streamresume.Cursor{StreamID: "stream-1", Seq: 3})
	require.NoError(t, err)

	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	require.NotNil(t, stream.req.Resume)
	require.Equal(t, "stream-1", stream.req.Resume.StreamId)
	require.Equal(t, uint64(3), stream.req.Resume.Seq)

	// The backend replays at-least-once (including seq 3) then continues.
	stream.feed <- pubWithSeq(3)
	stream.feed <- pubWithSeq(4)
	stream.feed <- pubWithSeq(5)
	require.Eventually(t, func() bool { return len(client.sequences()) == 2 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{4, 5}, client.sequences())

	cursor, ok, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(5), cursor.Seq)

	// A second reconnect again must not redeliver anything confirmed.
	stream2 := newResumeStream("stream-1")
	handler2 := newResumeHandler(t, store, stream2)
	client2 := newRecordingClient()
	reply2, _, cancel2, err := handler2.Handle()(client2, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply2.SubscriptionReady)
	defer cancel2()
	require.Equal(t, uint64(5), stream2.req.Resume.Seq)

	stream2.feed <- pubWithSeq(5)
	stream2.feed <- pubWithSeq(6)
	require.Eventually(t, func() bool { return len(client2.sequences()) == 1 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{6}, client2.sequences())
}

// A backend stream whose identity changed is unrecoverable: the
// subscription follows the new stream with plain behavior and the previous
// cursor stays stored until the new stream confirms its first event.
func TestSubscribeStreamResumeResetFallsBack(t *testing.T) {
	store := streamresume.NewMemoryStore()
	key := streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"}
	previous, err := store.Save(context.Background(), key, streamresume.Cursor{StreamID: "stream-1", Seq: 3})
	require.NoError(t, err)

	stream := newResumeStream("stream-2")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	// Centrifugo still asks for resume; the backend answers with a new id.
	require.NotNil(t, stream.req.Resume)

	// Before the new stream delivers anything, the old cursor is retained.
	cursor, ok, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, cursor)

	stream.feed <- pubWithSeq(1)
	stream.feed <- pubWithSeq(2)
	require.Eventually(t, func() bool {
		c, _, _ := store.Get(context.Background(), key)
		return c.StreamID == "stream-2" && c.Seq == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{1, 2}, client.sequences())
}

// A gap in the proxy sequence stops recovery: publications keep flowing
// with plain behavior, but the cursor never advances past the gap.
func TestSubscribeStreamResumeSequenceJumpKeepsCursor(t *testing.T) {
	store := streamresume.NewMemoryStore()
	key := streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"}
	previous, err := store.Save(context.Background(), key, streamresume.Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	stream.feed <- pubWithSeq(7) // expected 6: jump.
	stream.feed <- pubWithSeq(8)
	require.Eventually(t, func() bool { return len(client.sequences()) == 2 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{7, 8}, client.sequences())

	cursor, ok, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, cursor)
}

// When the cursor cannot be saved, the delivered event stays unconfirmed:
// recovery stops for the subscription and the previous cursor is retained.
func TestSubscribeStreamResumeSaveFailureRetainsCursor(t *testing.T) {
	backing := streamresume.NewMemoryStore()
	key := streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"}
	previous, err := backing.Save(context.Background(), key, streamresume.Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)
	store := failingResumeStore{Store: backing}

	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	stream.feed <- pubWithSeq(6)
	stream.feed <- pubWithSeq(7)
	require.Eventually(t, func() bool { return len(client.sequences()) == 2 }, time.Second, 10*time.Millisecond)

	cursor, ok, err := backing.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, cursor)

	// A new subscription resumes from the retained cursor: seq 6 is
	// redelivered.
	stream2 := newResumeStream("stream-1")
	handler2 := newResumeHandler(t, backing, stream2)
	client2 := newRecordingClient()
	reply2, _, cancel2, err := handler2.Handle()(client2, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply2.SubscriptionReady)
	defer cancel2()
	require.Equal(t, uint64(5), stream2.req.Resume.Seq)
	stream2.feed <- pubWithSeq(6)
	require.Eventually(t, func() bool { return len(client2.sequences()) == 1 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{6}, client2.sequences())
}

// A publication which cannot be written to the client stays unconfirmed: no
// cursor is advanced, so the next subscription redelivers it.
func TestSubscribeStreamResumeWriteFailureLeavesUnconfirmed(t *testing.T) {
	store := streamresume.NewMemoryStore()
	key := streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"}
	previous, err := store.Save(context.Background(), key, streamresume.Cursor{StreamID: "stream-1", Seq: 5})
	require.NoError(t, err)

	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()
	client.writeErr = errors.New("connection closed")

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	stream.feed <- pubWithSeq(6)
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.attempts == 1
	}, time.Second, 10*time.Millisecond)
	require.Empty(t, client.sequences())

	cursor, ok, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, previous, cursor, "the failed write is not confirmed")

	// Reconnect with a healthy client: seq 6 is redelivered after the
	// retained cursor.
	stream2 := newResumeStream("stream-1")
	handler2 := newResumeHandler(t, store, stream2)
	client2 := newRecordingClient()
	reply2, _, cancel2, err := handler2.Handle()(client2, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply2.SubscriptionReady)
	defer cancel2()
	stream2.feed <- pubWithSeq(6)
	require.Eventually(t, func() bool { return len(client2.sequences()) == 1 }, time.Second, 10*time.Millisecond)
	require.Equal(t, []uint64{6}, client2.sequences())
}

// Without the channel option everything behaves as before: no resume
// request, no cursor, all publications delivered regardless of sequence.
func TestSubscribeStreamResumeDisabledKeepsOriginalBehavior(t *testing.T) {
	store := streamresume.NewMemoryStore()
	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	chOpts := resumeChannelOptions()
	chOpts.StreamResume = false
	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, chOpts, PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	require.Nil(t, stream.req.Resume)
	stream.feed <- pubWithSeq(1)
	stream.feed <- pubWithSeq(1) // duplicates are delivered without a session.
	require.Eventually(t, func() bool { return len(client.sequences()) == 2 }, time.Second, 10*time.Millisecond)

	_, ok, err := store.Get(context.Background(), streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"})
	require.NoError(t, err)
	require.False(t, ok)
}

// A stream which does not number publications cannot be resumed: events are
// delivered and no cursor is written.
func TestSubscribeStreamResumeUnsequencedPublications(t *testing.T) {
	store := streamresume.NewMemoryStore()
	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()

	stream.feed <- &proxyproto.StreamSubscribeResponse{Publication: &proxyproto.Publication{Data: []byte("d")}}
	require.Eventually(t, func() bool { return len(client.sequences()) == 1 }, time.Second, 10*time.Millisecond)

	_, ok, err := store.Get(context.Background(), streamresume.Key{UserID: "user", Channel: "ch", ProxyName: "default"})
	require.NoError(t, err)
	require.False(t, ok)
}

// Cursors of different channels never interfere with each other.
func TestSubscribeStreamResumeCursorPerChannel(t *testing.T) {
	store := streamresume.NewMemoryStore()
	stream := newResumeStream("stream-1")
	handler := newResumeHandler(t, store, stream)
	client := newRecordingClient()

	reply, _, cancel, err := handler.Handle()(client, false,
		centrifuge.SubscribeEvent{Channel: "ch-a"}, resumeChannelOptions(), PerCallData{})
	require.NoError(t, err)
	close(reply.SubscriptionReady)
	defer cancel()
	stream.feed <- pubWithSeq(1)
	require.Eventually(t, func() bool { return len(client.sequences()) == 1 }, time.Second, 10*time.Millisecond)

	cursorA, ok, err := store.Get(context.Background(), streamresume.Key{UserID: "user", Channel: "ch-a", ProxyName: "default"})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), cursorA.Seq)
	_, ok, err = store.Get(context.Background(), streamresume.Key{UserID: "user", Channel: "ch-b", ProxyName: "default"})
	require.NoError(t, err)
	require.False(t, ok)
}

// failingResumeStore fails saves while reads keep working.
type failingResumeStore struct {
	streamresume.Store
}

func (f failingResumeStore) Save(_ context.Context, _ streamresume.Key, _ streamresume.Cursor) (streamresume.Cursor, error) {
	return streamresume.Cursor{}, errors.New("cursor storage unavailable")
}
