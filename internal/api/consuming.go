package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/quarantine"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ConsumingHandlerConfig configures ConsumingHandler.
type ConsumingHandlerConfig struct {
	UseOpenTelemetry bool
}

// ConsumingHandler is responsible for processing API commands from asynchronous consumers.
type ConsumingHandler struct {
	node   *centrifuge.Node
	config ConsumingHandlerConfig
	api    *Executor
}

// NewConsumingHandler creates new ConsumingHandler.
func NewConsumingHandler(n *centrifuge.Node, apiExecutor *Executor, c ConsumingHandlerConfig) *ConsumingHandler {
	h := &ConsumingHandler{
		node:   n,
		config: c,
		api:    apiExecutor,
	}
	return h
}

func logNonRetryableConsumingError(err error, method string) {
	log.Error().Err(err).Str("method", method).Msg("non retryable error during consuming, skip message")
}

// handleNonRetryableError deals with a permanent consuming error. During a
// quarantine replay the error is returned so the replay flow can mark the
// record as failed without creating a new quarantine entry. Otherwise the
// failed event is moved to the quarantine area (when configured) together
// with its source, method and original payload, and nil is returned so the
// consumer acknowledges the message.
func (d *Dispatcher) handleNonRetryableError(ctx context.Context, method string, payload []byte, channels []string, err error) error {
	if _, ok := quarantineReplayFromContext(ctx); ok {
		return err
	}
	logNonRetryableConsumingError(err, method)
	d.quarantineEvent(ctx, method, payload, channels, err)
	return nil
}

// quarantineEvent stores a permanently failed consumed event in the
// quarantine area. Failures of the store itself are logged, never propagated
// to the consumer: they must not change the existing acknowledge behavior.
func (d *Dispatcher) quarantineEvent(ctx context.Context, method string, payload []byte, channels []string, cause error) {
	if d.quarantine == nil {
		return
	}
	source, _ := ConsumerSourceFromContext(ctx)
	if source.Name == "" {
		log.Warn().Str("method", method).Msg("quarantine event without consumer source, record may not be replayable")
	}
	record := quarantine.Record{
		Consumer:     source.Name,
		ConsumerType: source.Type,
		Method:       method,
		Payload:      payload,
		Channels:     channels,
		Error:        cause.Error(),
	}
	stored, err := d.quarantine.Add(ctx, record)
	if err != nil {
		log.Error().Err(err).Str("method", method).Msg("failed to quarantine consumed event")
		return
	}
	metrics.IncConsumerQuarantine(source.Name, "quarantined")
	log.Warn().
		Str("consumer", source.Name).
		Str("method", method).
		Str("quarantine_id", stored.ID).
		Msg("consumed event moved to quarantine")
}

// ErrShutdown is returned to consumers for messages received after Node shutdown
// started. Such messages must not be acknowledged: this node is no longer able to
// deliver them, and depending on the broker a publish may still return success (a
// memory broker never fails, so without this check a message would be committed
// while going nowhere). Returning an error makes consumers leave the message
// unacknowledged, so it's redelivered to another node or after restart.
//
// It wraps context.Canceled since consumers treat cancellation as a stop signal
// and give up immediately instead of retrying with backoff until the shutdown
// timeout is reached.
var ErrShutdown = fmt.Errorf("node is shutting down: %w", context.Canceled)

// ConsumerSource identifies the consumer a dispatched event came from. It is
// attached to the context by the consuming package and stored with
// quarantined events so their origin stays known when they are replayed.
type ConsumerSource struct {
	// Name is the configured name of the consumer.
	Name string
	// Type is the configured type of the consumer.
	Type string
}

type consumerSourceCtxKey struct{}

// ContextWithConsumerSource returns a context carrying the given consumer
// source.
func ContextWithConsumerSource(ctx context.Context, source ConsumerSource) context.Context {
	return context.WithValue(ctx, consumerSourceCtxKey{}, source)
}

// ConsumerSourceFromContext returns the consumer source attached to ctx, if
// any.
func ConsumerSourceFromContext(ctx context.Context) (ConsumerSource, bool) {
	source, ok := ctx.Value(consumerSourceCtxKey{}).(ConsumerSource)
	return source, ok
}

type quarantineReplayCtxKey struct{}

// contextWithQuarantineReplay marks ctx as a quarantine replay of the record
// with the given ID. While marked, non-retryable errors are returned instead
// of being swallowed and re-quarantined, so the replay flow can finalize the
// record.
func contextWithQuarantineReplay(ctx context.Context, recordID string) context.Context {
	return context.WithValue(ctx, quarantineReplayCtxKey{}, recordID)
}

func quarantineReplayFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(quarantineReplayCtxKey{}).(string)
	return id, ok
}

type Dispatcher struct {
	handler    *ConsumingHandler
	quarantine quarantine.Store
}

func NewDispatcher(handler *ConsumingHandler) *Dispatcher {
	return &Dispatcher{
		handler: handler,
	}
}

// SetQuarantineStore enables quarantine for non-retryable consuming errors.
// Events failing permanently are stored together with their source, method
// and original raw payload and become available through the
// consumer_quarantine_list and consumer_quarantine_replay API commands.
func (d *Dispatcher) SetQuarantineStore(store quarantine.Store) {
	d.quarantine = store
}

// shuttingDown returns true once Node shutdown started.
func (d *Dispatcher) shuttingDown() bool {
	select {
	case <-d.handler.node.NotifyShutdown():
		return true
	default:
		return false
	}
}

func (d *Dispatcher) Publish(ctx context.Context, req *apiproto.PublishRequest) error {
	if d.shuttingDown() {
		return ErrShutdown
	}
	resp := d.handler.api.Publish(ctx, req)
	if d.handler.config.UseOpenTelemetry && resp.Error != nil {
		span := trace.SpanFromContext(ctx)
		span.SetStatus(codes.Error, resp.Error.Error())
	}
	if resp.Error != nil && resp.Error.Temporary {
		return resp.Error
	}
	if resp.Error != nil {
		payload, _ := json.Marshal(req)
		return d.handleNonRetryableError(ctx, "publish", payload, []string{req.Channel}, resp.Error)
	}
	return nil
}

func (d *Dispatcher) Broadcast(ctx context.Context, req *apiproto.BroadcastRequest) error {
	if d.shuttingDown() {
		return ErrShutdown
	}
	resp := d.handler.api.Broadcast(ctx, req)
	if d.handler.config.UseOpenTelemetry && resp.Error != nil {
		span := trace.SpanFromContext(ctx)
		span.SetStatus(codes.Error, resp.Error.Error())
	}
	if resp.Error != nil && resp.Error.Temporary {
		return resp.Error
	}
	if resp.Error != nil {
		payload, _ := json.Marshal(req)
		return d.handleNonRetryableError(ctx, "broadcast", payload, req.Channels, resp.Error)
	}
	for _, response := range resp.Result.Responses {
		if response.Error == nil {
			continue
		}
		if response.Error.Temporary {
			// Any temporary error in any channel response will result into a retry by a consumer.
			// To prevent duplicate messages publishers may use idempotency keys.
			return response.Error
		}
		// A permanent per-channel error used to be swallowed: the consumer
		// acknowledged the message and it was lost. Quarantine it instead so
		// it can be inspected and replayed after the problem is fixed.
		payload, _ := json.Marshal(req)
		return d.handleNonRetryableError(ctx, "broadcast", payload, req.Channels, response.Error)
	}
	return nil
}

type ConsumedPublication struct {
	Data []byte
	// IdempotencyKey is used to prevent duplicate messages.
	IdempotencyKey string
	// Delta is used to indicate that the message is a delta update.
	Delta bool
	// Tags are used to attach metadata to the message.
	Tags map[string]string
	// Version of publication.
	Version uint64
	// VersionEpoch of publication.
	VersionEpoch string
}

func (d *Dispatcher) DispatchPublication(
	ctx context.Context, channels []string, pub ConsumedPublication,
) error {
	if d.shuttingDown() {
		return ErrShutdown
	}
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		req := &apiproto.PublishRequest{
			Data:           pub.Data,
			Channel:        channels[0],
			IdempotencyKey: pub.IdempotencyKey,
			Delta:          pub.Delta,
			Tags:           pub.Tags,
			Version:        pub.Version,
			VersionEpoch:   pub.VersionEpoch,
		}
		return d.Publish(ctx, req)
	}
	req := &apiproto.BroadcastRequest{
		Data:           pub.Data,
		Channels:       channels,
		IdempotencyKey: pub.IdempotencyKey,
		Delta:          pub.Delta,
		Tags:           pub.Tags,
		Version:        pub.Version,
		VersionEpoch:   pub.VersionEpoch,
	}
	return d.Broadcast(ctx, req)
}

// Dispatch processes commands received from asynchronous consumers.
func (d *Dispatcher) dispatchMethodPayload(ctx context.Context, method string, payload []byte) error {
	switch method {
	case "publish":
		_, err := d.handler.handlePublish(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	case "broadcast":
		// This one is special as we need to iterate over responses. Ideally we want to use
		// code gen for Dispatch but need to make sure that broadcast logic is preserved.
		res, err := d.handler.handleBroadcast(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		for _, resp := range res.Responses {
			if resp.Error == nil {
				continue
			}
			if resp.Error.Temporary {
				// Any temporary error in any channel response will result into a retry by a consumer.
				// To prevent duplicate messages publishers may use idempotency keys.
				return resp.Error
			}
			// A permanent per-channel error used to be swallowed: the consumer
			// acknowledged the message and it was lost. Quarantine it instead.
			return d.handleNonRetryableError(ctx, method, payload, nil, resp.Error)
		}
		return nil
	case "subscribe":
		_, err := d.handler.handleSubscribe(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	case "unsubscribe":
		_, err := d.handler.handleUnsubscribe(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	case "disconnect":
		_, err := d.handler.handleDisconnect(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	case "history_remove":
		_, err := d.handler.handleHistoryRemove(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	case "refresh":
		_, err := d.handler.handleRefresh(ctx, payload)
		if err != nil {
			var apiError *apiproto.Error
			if errors.As(err, &apiError) && apiError.Temporary {
				return err
			}
			return d.handleNonRetryableError(ctx, method, payload, nil, err)
		}
		return nil
	default:
		// Skip unsupported.
		log.Info().Msg("skip unsupported API command method")
		return nil
	}
}

var ErrInvalidData = errors.New("invalid data")

// JSONRawOrString can decode payload from bytes and from JSON string. This gives
// us better interoperability. For example, JSONB field is encoded as JSON string in
// Debezium PostgreSQL connector.
type JSONRawOrString json.RawMessage

func (j *JSONRawOrString) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		// Unmarshal as a string, then convert the string to json.RawMessage.
		var str string
		if err := json.Unmarshal(data, &str); err != nil {
			return err
		}
		*j = JSONRawOrString(str)
	} else {
		// Unmarshal directly as json.RawMessage
		*j = data
	}
	return nil
}

// MarshalJSON returns m as the JSON encoding of m.
func (j JSONRawOrString) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	return j, nil
}

// replayIdempotencyKeyPrefix namespaces the deterministic idempotency keys
// attached to publish/broadcast replays which did not carry their own key.
const replayIdempotencyKeyPrefix = "quarantine:"

// DispatchQuarantineRecord re-dispatches a quarantined event through the same
// pipeline the original consumer used. The record's source is attached to the
// context and the dispatch is marked as a replay, so a still-failing event
// returns its error instead of being quarantined again.
//
// publish and broadcast replays without an idempotency key get a deterministic
// one derived from the record ID. If the process is interrupted after the
// side effect happened but before the quarantine record is removed, retrying
// the replay then carries the same key and cannot publish the event twice.
func (d *Dispatcher) DispatchQuarantineRecord(ctx context.Context, record quarantine.Record) error {
	ctx = ContextWithConsumerSource(ctx, ConsumerSource{
		Name: record.Consumer,
		Type: record.ConsumerType,
	})
	ctx = contextWithQuarantineReplay(ctx, record.ID)

	payload := record.Payload
	if record.Method == "publish" || record.Method == "broadcast" {
		payload = withReplayIdempotencyKey(payload, record.ID)
	}
	return d.DispatchCommand(ctx, record.Method, payload)
}

// withReplayIdempotencyKey returns payload with a deterministic idempotency
// key set when it did not carry one. The original payload is returned
// unchanged on any decoding problem so the regular dispatch surfaces the
// real failure.
func withReplayIdempotencyKey(payload []byte, recordID string) []byte {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return payload
	}
	var key string
	if raw, ok := fields["idempotency_key"]; ok {
		_ = json.Unmarshal(raw, &key)
	}
	if key != "" {
		return payload
	}
	fields["idempotency_key"] = json.RawMessage(`"` + replayIdempotencyKeyPrefix + recordID + `"`)
	encoded, err := json.Marshal(fields)
	if err != nil {
		return payload
	}
	return encoded
}

type MethodWithRequestPayload struct {
	Method  string          `json:"method"`
	Payload JSONRawOrString `json:"payload"`
}

// DispatchCommand processes commands received from asynchronous consumers.
func (d *Dispatcher) DispatchCommand(ctx context.Context, method string, payload []byte) error {
	if d.shuttingDown() {
		return ErrShutdown
	}
	if method != "" {
		// If method already known – we can skip decoding into MethodWithRequestPayload.
		return d.dispatchMethodPayload(ctx, method, payload)
	}
	// Otherwise we expect payload to be MethodWithRequestPayload.
	var e MethodWithRequestPayload
	err := json.Unmarshal(payload, &e)
	if err != nil {
		log.Error().Err(err).Msg("skip malformed consumed message")
		return nil
	}
	return d.dispatchMethodPayload(ctx, e.Method, e.Payload)
}
