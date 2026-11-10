package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	. "github.com/centrifugal/centrifugo/v6/internal/apiproto"
	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	// mapReconcileOwnerTag marks state entries managed by map_reconcile.
	// The tag value is the manifest's idempotency_key. Entries without the
	// tag, or with another manifest's value, are foreign entries: reconcile
	// never updates or deletes them, and refuses to take them over.
	mapReconcileOwnerTag = "__centrifugo_map_reconcile_owner"

	// mapReconcileStatePageSize bounds every ReadState page while collecting
	// the channel's current state.
	mapReconcileStatePageSize = 1000
)

// MapReconcile applies the complete desired entry set of a map channel for
// one manifest. See MapReconcileRequest in api.proto for the full contract.
//
// The flow is: validate the request, read the current state, verify there is
// no conflict with foreign entries, compute the diff against the entries
// owned by this manifest, then apply creates/updates followed by deletes.
// All validation happens before the first write, so an invalid request keeps
// the pre-application state. Every write uses a deterministic per-entry
// idempotency key, which makes retries safe and lets an interrupted
// application converge by simply re-submitting the same manifest.
func (h *Executor) MapReconcile(ctx context.Context, cmd *MapReconcileRequest) *MapReconcileResponse {
	defer metrics.ObserveAPICommand(time.Now(), h.config.Protocol, "map_reconcile")

	ch := cmd.Channel
	if h.config.UseOpenTelemetry {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(attribute.String("centrifugo.channel", ch))
	}

	resp := &MapReconcileResponse{}

	// Phase 1: validate the request. Nothing is written in this phase.
	if ch == "" || cmd.IdempotencyKey == "" {
		resp.Error = ErrorBadRequest
		return resp
	}

	_, _, chOpts, found, err := h.cfgContainer.ChannelOptions(ch)
	if err != nil {
		resp.Error = ErrorInternal
		return resp
	}
	if !found {
		resp.Error = ErrorUnknownChannel
		return resp
	}
	if chOpts.SubscriptionType != "map" && chOpts.SubscriptionType != "map_clients" && chOpts.SubscriptionType != "map_users" {
		log.Warn().Str("channel", ch).Str("subscription_type", chOpts.SubscriptionType).Msg("map_reconcile called on non-map namespace")
		resp.Error = ErrorBadRequest
		return resp
	}

	// desired keeps the decoded entry data, desiredOrder preserves the
	// submission order so application order is deterministic.
	desired := make(map[string][]byte, len(cmd.Entries))
	desiredOrder := make([]string, 0, len(cmd.Entries))
	for _, entry := range cmd.Entries {
		if entry == nil || entry.Key == "" {
			resp.Error = ErrorBadRequest
			return resp
		}
		if _, duplicate := desired[entry.Key]; duplicate {
			log.Warn().Str("channel", ch).Str("key", entry.Key).Msg("map_reconcile: duplicate key in manifest")
			resp.Error = ErrorBadRequest
			return resp
		}

		var data []byte
		if entry.B64Data != "" {
			data, err = base64.StdEncoding.DecodeString(entry.B64Data)
			if err != nil {
				resp.Error = ErrorBadRequest
				return resp
			}
		} else {
			data = entry.Data
		}
		if err = config.ValidatePublicationData(data, chOpts.PublicationDataFormat); err != nil {
			log.Error().Err(err).Str("channel", ch).Str("key", entry.Key).Msg("map_reconcile: invalid entry data")
			resp.Error = ErrorBadRequest
			return resp
		}

		desired[entry.Key] = data
		desiredOrder = append(desiredOrder, entry.Key)
	}

	// Phase 2: collect the current channel state, following pagination.
	current := make(map[string]*centrifuge.Publication)
	cursor := ""
	for {
		var result centrifuge.MapStateResult
		result, err = h.node.MapStateRead(ctx, ch, centrifuge.MapReadStateOptions{
			Cursor: cursor,
			Limit:  mapReconcileStatePageSize,
		})
		if err != nil {
			log.Error().Err(err).Str("channel", ch).Msg("error in map reconcile state read")
			resp.Error = toAPIErr(err)
			return resp
		}
		for _, pub := range result.Publications {
			current[pub.Key] = pub
		}
		cursor = result.Cursor
		if cursor == "" {
			break
		}
	}

	// Phase 3: refuse to take over entries this manifest does not own. The
	// check runs before any write, so a conflict leaves the state untouched.
	for _, key := range desiredOrder {
		pub, exists := current[key]
		if !exists {
			continue
		}
		if owner := pub.Tags[mapReconcileOwnerTag]; owner != cmd.IdempotencyKey {
			log.Warn().Str("channel", ch).Str("key", key).Str("owner", owner).
				Msg("map_reconcile: key collides with a foreign entry")
			resp.Error = &Error{
				Code:    ErrorConflict.Code,
				Message: fmt.Sprintf("conflict: key %q exists but is not managed by this manifest", key),
			}
			return resp
		}
	}

	// Phase 4: compute the diff.
	var toCreate, toUpdate []string
	unchanged := 0
	for _, key := range desiredOrder {
		pub, exists := current[key]
		switch {
		case !exists:
			toCreate = append(toCreate, key)
		case bytes.Equal(pub.Data, desired[key]):
			unchanged++
		default:
			toUpdate = append(toUpdate, key)
		}
	}
	toDelete := make([]string, 0)
	for key, pub := range current {
		if pub.Tags[mapReconcileOwnerTag] != cmd.IdempotencyKey {
			continue
		}
		if _, inDesired := desired[key]; !inDesired {
			toDelete = append(toDelete, key)
		}
	}
	// Map iteration order is randomized; sort deletions for deterministic
	// application and reproducible stream ordering.
	sort.Strings(toDelete)

	// Phase 5: apply. Creates/updates go first, then deletes. A broker error
	// leaves a partial application; resubmitting the same manifest re-runs
	// this diff and only applies the still-missing part.
	result := &MapReconcileResult{Unchanged: uint32(unchanged)}
	var lastPosition centrifuge.StreamPosition

	applyPublish := func(keys []string, counter *uint32) *Error {
		for _, key := range keys {
			pubResult, pubErr := h.node.MapPublish(ctx, ch, key, centrifuge.MapPublishOptions{
				Data: desired[key],
				Tags: map[string]string{
					mapReconcileOwnerTag: cmd.IdempotencyKey,
				},
				IdempotencyKey: reconcilePublishIdempotencyKey(cmd.IdempotencyKey, key, desired[key]),
				// Honor the namespace-wide delta policy, same as map_publish.
				// State always stores full data; this only affects delivery to
				// subscribers that negotiated delta compression.
				UseDelta: chOpts.DeltaPublish,
			})
			if pubErr != nil {
				log.Error().Err(pubErr).Str("channel", ch).Str("key", key).Msg("error in map reconcile publish")
				return toAPIErr(pubErr)
			}
			lastPosition = pubResult.Position
			*counter++
		}
		return nil
	}

	if apiErr := applyPublish(toCreate, &result.Created); apiErr != nil {
		resp.Error = apiErr
		return resp
	}
	if apiErr := applyPublish(toUpdate, &result.Updated); apiErr != nil {
		resp.Error = apiErr
		return resp
	}

	for _, key := range toDelete {
		rmResult, rmErr := h.node.MapRemove(ctx, ch, key, centrifuge.MapRemoveOptions{
			IdempotencyKey: reconcileRemoveIdempotencyKey(cmd.IdempotencyKey, key),
		})
		if rmErr != nil {
			log.Error().Err(rmErr).Str("channel", ch).Str("key", key).Msg("error in map reconcile remove")
			resp.Error = toAPIErr(rmErr)
			return resp
		}
		lastPosition = rmResult.Position
		result.Deleted++
	}

	result.Offset = lastPosition.Offset
	result.Epoch = lastPosition.Epoch
	resp.Result = result
	return resp
}

// reconcilePublishIdempotencyKey builds the deterministic broker idempotency
// key of one manifest entry publication. The data hash is part of the key,
// so retrying identical data is deduplicated while changed data still gets
// applied. The broker keeps idempotency results only for a limited TTL, but
// the state-based diff makes reconciliation correct beyond that window.
func reconcilePublishIdempotencyKey(manifestKey, entryKey string, data []byte) string {
	sum := sha256.Sum256(data)
	return "mr:pub:" + manifestKey + ":" + entryKey + ":" + hex.EncodeToString(sum[:8])
}

// reconcileRemoveIdempotencyKey builds the deterministic broker idempotency
// key of one manifest entry removal.
func reconcileRemoveIdempotencyKey(manifestKey, entryKey string) string {
	return "mr:rm:" + manifestKey + ":" + entryKey
}
