package consuming

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/api"

	"github.com/stretchr/testify/require"
)

// recordingDispatcher captures the context and arguments of dispatch calls so
// tests can assert the consumer source attached by sourceDispatcher.
type recordingDispatcher struct {
	gotSource   api.ConsumerSource
	hasSource   bool
	method      string
	payload     []byte
	channels    []string
	publication api.ConsumedPublication
}

func (d *recordingDispatcher) DispatchCommand(ctx context.Context, method string, data []byte) error {
	d.gotSource, d.hasSource = api.ConsumerSourceFromContext(ctx)
	d.method = method
	d.payload = data
	return nil
}

func (d *recordingDispatcher) DispatchPublication(ctx context.Context, channels []string, pub api.ConsumedPublication) error {
	d.gotSource, d.hasSource = api.ConsumerSourceFromContext(ctx)
	d.channels = channels
	d.publication = pub
	return nil
}

func TestSourceDispatcherAttachesConsumerSource(t *testing.T) {
	inner := &recordingDispatcher{}
	wrapped := &sourceDispatcher{
		inner: inner,
		source: api.ConsumerSource{
			Name: "outbox",
			Type: "postgresql",
		},
	}

	ctx := context.Background()
	require.NoError(t, wrapped.DispatchCommand(ctx, "publish", []byte(`{}`)))
	require.True(t, inner.hasSource)
	require.Equal(t, "outbox", inner.gotSource.Name)
	require.Equal(t, "postgresql", inner.gotSource.Type)
	require.Equal(t, "publish", inner.method)
	require.Equal(t, []byte(`{}`), inner.payload)

	require.NoError(t, wrapped.DispatchPublication(ctx, []string{"ch1", "ch2"}, api.ConsumedPublication{
		Data: []byte(`{"x":1}`),
	}))
	require.True(t, inner.hasSource)
	require.Equal(t, "outbox", inner.gotSource.Name)
	require.Equal(t, []string{"ch1", "ch2"}, inner.channels)
	require.Equal(t, []byte(`{"x":1}`), inner.publication.Data)
}

func TestNewSkipsDisabledConsumers(t *testing.T) {
	// Disabled consumers are skipped before any external connection is made,
	// so New needs no broker and returns no consumer while still wiring the
	// provided dispatcher.
	consumers, err := New("node-1", &recordingDispatcher{}, []ConsumerConfig{
		{Name: "off", Enabled: false, Type: "postgresql"},
	})
	require.NoError(t, err)
	require.Empty(t, consumers)
}
