package cutover

import (
	"context"
	"fmt"
	"strings"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/redisshard"

	"github.com/redis/rueidis"
)

// redisPendingLimit caps the number of pending entries fetched in one
// XPENDING call. Pending sets beyond it are paged by the caller through
// repeated scans.
const redisPendingLimit = 1000

// RedisSource reads the old Redis Stream source without acknowledging
// anything: the live consumer group keeps owning delivery while a cutover
// converges.
type RedisSource struct {
	config     configtypes.RedisStreamConsumerConfig
	identity   Options
	shard      *redisshard.RedisShard
	descriptor SourceDescriptor
}

// NewRedisSource builds a RedisSource from the named consumer configuration.
// It owns the returned shard and closes it on Close.
func NewRedisSource(cfg configtypes.RedisStreamConsumerConfig, identity Options) (*RedisSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadOptions, err)
	}
	shards, err := redisshard.BuildRedisShards(cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("build redis shards: %w", err)
	}
	if len(shards) != 1 {
		for _, shard := range shards {
			shard.Close()
		}
		return nil, fmt.Errorf("%w: expected a single Redis shard, got %d", ErrBadOptions, len(shards))
	}
	shard := shards[0]
	descriptor := SourceDescriptor{
		Kind:         SourceKindRedisStream,
		ConsumerName: identity.RedisConsumerName,
		Locator:      strings.Join(cfg.Streams, ","),
		Details: map[string]string{
			"addresses":        strings.Join(cfg.Redis.Address, ","),
			"streams":          strings.Join(cfg.Streams, ","),
			"consumer_group":   cfg.ConsumerGroup,
			"payload_value":    cfg.PayloadValue,
			"method_value":     cfg.MethodValue,
			"identity_value":   identity.RedisIdentityValue,
			"identity_payload": identity.IdentityPayloadField,
		},
	}
	return &RedisSource{
		config:     cfg,
		identity:   identity,
		shard:      shard,
		descriptor: descriptor,
	}, nil
}

// Descriptor implements Source.
func (s *RedisSource) Descriptor() SourceDescriptor { return s.descriptor }

// Ping implements Source.
func (s *RedisSource) Ping(ctx context.Context) error {
	if err := s.shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().Ping().Build())
	}).Error(); err != nil {
		return fmt.Errorf("%w: redis ping: %v", ErrReadFailed, err)
	}
	return nil
}

// Frontier implements Source: the stop point of every stream is the
// consumer group's last delivered entry ID.
func (s *RedisSource) Frontier(ctx context.Context) ([]FrontierPoint, error) {
	points := make([]FrontierPoint, 0, len(s.config.Streams))
	for _, stream := range s.config.Streams {
		res := s.shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(ctx, client.B().XinfoGroups().Key(stream).Build())
		})
		if res.Error() != nil {
			return nil, fmt.Errorf("%w: XINFO GROUPS %q: %v", ErrReadFailed, stream, res.Error())
		}
		groups, err := res.ToArray()
		if err != nil {
			return nil, fmt.Errorf("%w: parse XINFO GROUPS %q: %v", ErrReadFailed, stream, err)
		}
		var lastDelivered string
		for _, groupMsg := range groups {
			group, err := groupMsg.AsMap()
			if err != nil {
				return nil, fmt.Errorf("%w: parse XINFO group %q: %v", ErrReadFailed, stream, err)
			}
			nameMsg, ok := group["name"]
			if !ok {
				continue
			}
			name, err := nameMsg.ToString()
			if err != nil {
				return nil, fmt.Errorf("%w: parse group name %q: %v", ErrReadFailed, stream, err)
			}
			if name != s.config.ConsumerGroup {
				continue
			}
			lastMsg, ok := group["last-delivered-id"]
			if !ok {
				return nil, fmt.Errorf("%w: XINFO group %q has no last-delivered-id", ErrReadFailed, stream)
			}
			lastDelivered, err = lastMsg.ToString()
			if err != nil {
				return nil, fmt.Errorf("%w: parse last-delivered-id %q: %v", ErrReadFailed, stream, err)
			}
			break
		}
		if lastDelivered == "" {
			return nil, fmt.Errorf("%w: consumer group %q does not exist on stream %q",
				ErrReadFailed, s.config.ConsumerGroup, stream)
		}
		points = append(points, FrontierPoint{Stream: stream, Offset: lastDelivered})
	}
	return points, nil
}

// PendingIDs returns the entry IDs delivered to but not yet acknowledged by
// the consumer group. Entries outside this set at or below the stop point are
// stably delivered.
func (s *RedisSource) PendingIDs(ctx context.Context, stream string) (map[string]bool, error) {
	pending := make(map[string]bool)
	start := "-"
	for {
		res := s.shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
			return client.Do(ctx, client.B().Xpending().
				Key(stream).Group(s.config.ConsumerGroup).
				Start(start).End("+").Count(redisPendingLimit).Build())
		})
		if res.Error() != nil {
			return nil, fmt.Errorf("%w: XPENDING %q: %v", ErrReadFailed, stream, res.Error())
		}
		rows, err := res.ToArray()
		if err != nil {
			return nil, fmt.Errorf("%w: parse XPENDING %q: %v", ErrReadFailed, stream, err)
		}
		var lastID string
		for _, row := range rows {
			fields, err := row.ToArray()
			if err != nil || len(fields) == 0 {
				return nil, fmt.Errorf("%w: malformed XPENDING row on %q", ErrReadFailed, stream)
			}
			id, err := fields[0].ToString()
			if err != nil {
				return nil, fmt.Errorf("%w: parse pending entry id on %q: %v", ErrReadFailed, stream, err)
			}
			pending[id] = true
			lastID = id
		}
		if len(rows) < redisPendingLimit {
			return pending, nil
		}
		// Page with the exclusive "(" form so the last ID is not repeated.
		start = "(" + lastID
	}
}

// Read implements Source. It scans entries strictly after cursor.Offset up to
// the stop point recorded for cursor.Stream.
func (s *RedisSource) Read(ctx context.Context, cursor Cursor, limit int) ([]Event, Cursor, error) {
	if limit <= 0 {
		limit = defaultBatchLimit
	}
	start := cursor.Offset
	if start == "" {
		start = "-"
	} else {
		start = "(" + start
	}
	res := s.shard.RunOp(func(client rueidis.Client) rueidis.RedisResult {
		return client.Do(ctx, client.B().Xrange().
			Key(cursor.Stream).Start(start).End(cursor.Bound).Count(int64(limit)).Build())
	})
	if res.Error() != nil {
		return nil, cursor, fmt.Errorf("%w: XRANGE %q: %v", ErrReadFailed, cursor.Stream, res.Error())
	}
	entries, err := res.AsXRange()
	if err != nil {
		return nil, cursor, fmt.Errorf("%w: parse XRANGE %q: %v", ErrReadFailed, cursor.Stream, err)
	}
	events := make([]Event, 0, len(entries))
	for _, entry := range entries {
		fields := entry.FieldValues
		payload := fields[s.config.PayloadValue]
		events = append(events, Event{
			SourceKind: SourceKindRedisStream,
			Stream:     cursor.Stream,
			Offset:     entry.ID,
			Method:     fields[s.config.MethodValue],
			Payload:    []byte(payload),
			Fields:     fields,
		})
	}
	next := cursor
	if len(entries) > 0 {
		next.Offset = entries[len(entries)-1].ID
	}
	return events, next, nil
}

// Identity implements Source.
func (s *RedisSource) Identity(ev Event) (string, error) {
	if s.identity.RedisIdentityValue != "" {
		if value := ev.Fields[s.identity.RedisIdentityValue]; value != "" {
			return value, nil
		}
	}
	return payloadIdentity(ev.Payload, s.identity.IdentityPayloadField)
}

// Close implements Source.
func (s *RedisSource) Close() error {
	s.shard.Close()
	return nil
}
