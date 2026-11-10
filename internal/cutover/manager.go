package cutover

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// Dispatcher executes the API command carried by an event. *api.Dispatcher
// satisfies this interface, so the cutover dispatches through the same path
// as the live consumers.
type Dispatcher interface {
	DispatchCommand(ctx context.Context, method string, payload []byte) error
}

// StableDeliverySource is implemented by sources that can distinguish
// stably delivered entries from in-flight ones. The Redis Stream source
// uses the consumer group pending entries list for this.
type StableDeliverySource interface {
	Source
	// PendingIDs reports the entries delivered to but not acknowledged by
	// the group. Entries outside this set below the stop point count as
	// stably delivered and must never be executed again.
	PendingIDs(ctx context.Context, stream string) (map[string]bool, error)
}

// ManagerConfig configures a Manager. The builder functions are overridable
// in tests to inject in-memory sources and stores.
type ManagerConfig struct {
	Consumers  []configtypes.Consumer
	Dispatcher Dispatcher
	// NewStore builds the progress store for a PostgreSQL consumer config.
	// Default: a PostgresStore backed by the consumer's connection pool.
	NewStore func(configtypes.PostgresConsumerConfig) (Store, error)
	// BuildRedis builds the old Redis Stream source.
	BuildRedis func(configtypes.RedisStreamConsumerConfig, Options) (Source, error)
	// BuildPostgres builds the new PostgreSQL outbox source.
	BuildPostgres func(configtypes.PostgresConsumerConfig, Options) (Source, error)
	// Now overrides the clock.
	Now func() time.Time
}

// Manager runs consumer cutovers. It serializes cutovers with the same name
// inside the node and relies on the store's version checks to coordinate
// across nodes.
type Manager struct {
	cfg ManagerConfig

	mu       sync.Mutex
	runLocks map[string]*sync.Mutex
	redisSrc map[string]Source
	pgSrc    map[string]Source
	stores   map[string]Store
}

// NewManager creates a Manager with production source and store builders.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewStore == nil {
		cfg.NewStore = newDefaultPostgresStore
	}
	if cfg.BuildRedis == nil {
		cfg.BuildRedis = func(c configtypes.RedisStreamConsumerConfig, o Options) (Source, error) {
			return NewRedisSource(c, o)
		}
	}
	if cfg.BuildPostgres == nil {
		cfg.BuildPostgres = func(c configtypes.PostgresConsumerConfig, o Options) (Source, error) {
			return NewPostgresSource(c, o)
		}
	}
	return &Manager{
		cfg:      cfg,
		runLocks: make(map[string]*sync.Mutex),
		redisSrc: make(map[string]Source),
		pgSrc:    make(map[string]Source),
		stores:   make(map[string]Store),
	}
}

// Close releases sources, stores and connection pools cached by the manager.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for _, source := range m.redisSrc {
		if err := source.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, source := range m.pgSrc {
		if err := source.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	m.redisSrc = map[string]Source{}
	m.pgSrc = map[string]Source{}
	m.stores = map[string]Store{}
	return errors.Join(errs...)
}

func (m *Manager) lockFor(name string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.runLocks[name]
	if !ok {
		lock = &sync.Mutex{}
		m.runLocks[name] = lock
	}
	return lock
}

// resolveConsumer finds a consumer config by name and checks its type.
func (m *Manager) resolveConsumer(name, expectedType string) (configtypes.Consumer, error) {
	for _, consumer := range m.cfg.Consumers {
		if consumer.Name != name {
			continue
		}
		if consumer.Type != expectedType {
			return configtypes.Consumer{}, fmt.Errorf("%w: consumer %q is %s, expected %s",
				ErrBadOptions, name, consumer.Type, expectedType)
		}
		return consumer, nil
	}
	return configtypes.Consumer{}, fmt.Errorf("%w: consumer %q is not configured", ErrBadOptions, name)
}

func (m *Manager) redisSource(cfg configtypes.Consumer, opts Options) (Source, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if source, ok := m.redisSrc[cfg.Name]; ok {
		return source, nil
	}
	source, err := m.cfg.BuildRedis(cfg.RedisStream, opts)
	if err != nil {
		return nil, err
	}
	m.redisSrc[cfg.Name] = source
	return source, nil
}

func (m *Manager) postgresSource(cfg configtypes.Consumer, opts Options) (Source, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if source, ok := m.pgSrc[cfg.Name]; ok {
		return source, nil
	}
	source, err := m.cfg.BuildPostgres(cfg.Postgres, opts)
	if err != nil {
		return nil, err
	}
	m.pgSrc[cfg.Name] = source
	return source, nil
}

func (m *Manager) storeFor(cfg configtypes.PostgresConsumerConfig) (Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := cfg.DSN + "|" + cfg.OutboxTableName
	if store, ok := m.stores[key]; ok {
		return store, nil
	}
	store, err := m.cfg.NewStore(cfg)
	if err != nil {
		return nil, err
	}
	m.stores[key] = store
	return store, nil
}

// Run drives one cutover call to completion or until the context is
// cancelled. Progress is persisted after every batch, so a repeated call or
// a node restart resumes from the last committed state.
func (m *Manager) Run(ctx context.Context, opts Options) (*State, error) {
	if opts.Name == "" || opts.RedisConsumerName == "" || opts.PostgresConsumerName == "" {
		return nil, fmt.Errorf("%w: name, redis_consumer and postgres_consumer are required", ErrBadOptions)
	}
	if opts.BatchLimit < 0 {
		return nil, fmt.Errorf("%w: batch_limit must be non-negative", ErrBadOptions)
	}
	if opts.BatchLimit == 0 {
		opts.BatchLimit = defaultBatchLimit
	}

	lock := m.lockFor(opts.Name)
	lock.Lock()
	defer lock.Unlock()

	redisCfg, err := m.resolveConsumer(opts.RedisConsumerName, configtypes.ConsumerTypeRedisStream)
	if err != nil {
		return nil, err
	}
	pgCfg, err := m.resolveConsumer(opts.PostgresConsumerName, configtypes.ConsumerTypePostgres)
	if err != nil {
		return nil, err
	}

	redisSource, err := m.redisSource(redisCfg, opts)
	if err != nil {
		return nil, err
	}
	pgSource, err := m.postgresSource(pgCfg, opts)
	if err != nil {
		return nil, err
	}
	store, err := m.storeFor(pgCfg.Postgres)
	if err != nil {
		return nil, err
	}
	if err := store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	if err := redisSource.Ping(ctx); err != nil {
		return nil, err
	}
	if err := pgSource.Ping(ctx); err != nil {
		return nil, err
	}

	state, err := store.GetState(ctx, opts.Name)
	switch {
	case err == nil:
		state, err = m.resume(ctx, store, state, opts, redisSource, pgSource)
		if err != nil {
			return state, err
		}
		if state.Phase == PhaseCommitted && !opts.Cancel {
			// A committed cutover is a no-op on repeat.
			return state, nil
		}
	case errors.Is(err, ErrNotFound):
		if opts.Cancel {
			return nil, fmt.Errorf("%w: cutover %q does not exist", ErrBadOptions, opts.Name)
		}
		state, err = m.createState(ctx, store, opts, redisSource, pgSource)
		if err != nil {
			return nil, err
		}
	default:
		return nil, err
	}

	if opts.Cancel {
		if state.Phase == PhaseCommitted {
			return nil, fmt.Errorf("%w: committed cutover %q cannot be cancelled", ErrBadOptions, opts.Name)
		}
		if err := store.FailState(ctx, state.Name, state.Version, PhaseCancelled, "cancelled by operator"); err != nil {
			return state, err
		}
		return store.GetState(ctx, state.Name)
	}

	// Stale reservations never reached a confirmed outcome (process crash,
	// node restart). Drop them so the guarded events are reconciled again.
	if err := store.ReleaseReservations(ctx, opts.Name); err != nil {
		return nil, err
	}

	return m.converge(ctx, store, state, opts, redisSource, pgSource)
}

func (m *Manager) createState(
	ctx context.Context, store Store, opts Options, redisSource, pgSource Source,
) (*State, error) {
	redisFrontier, err := redisSource.Frontier(ctx)
	if err != nil {
		return nil, err
	}
	pgFrontier, err := pgSource.Frontier(ctx)
	if err != nil {
		return nil, err
	}
	fingerprint := Fingerprint(redisSource.Descriptor(), pgSource.Descriptor(), opts)

	state := &State{
		Name:        opts.Name,
		Version:     1,
		Phase:       PhaseRunning,
		Fingerprint: fingerprint,
		RedisPoints: make([]RedisPoint, 0, len(redisFrontier)),
	}
	for _, point := range redisFrontier {
		state.RedisPoints = append(state.RedisPoints, RedisPoint{
			Stream: point.Stream,
			StopID: point.Offset,
		})
	}
	for _, point := range pgFrontier {
		state.PostgresPoints = append(state.PostgresPoints, PostgresPoint{
			Partition: point.Partition,
			StartID:   mustParseInt64(point.Offset),
		})
	}
	if err := store.CreateState(ctx, state); err != nil {
		return nil, err
	}
	return store.GetState(ctx, opts.Name)
}

func (m *Manager) resume(
	ctx context.Context, store Store, state *State, opts Options, redisSource, pgSource Source,
) (*State, error) {
	fingerprint := Fingerprint(redisSource.Descriptor(), pgSource.Descriptor(), opts)
	if fingerprint != state.Fingerprint {
		reason := fmt.Sprintf("%v: stored fingerprint %s, current %s",
			ErrValidationMismatch, state.Fingerprint, fingerprint)
		// Record the mismatch without touching checkpoints, then stop.
		if failErr := store.FailState(ctx, state.Name, state.Version, PhaseFailed, reason); failErr != nil {
			return state, failErr
		}
		failed, getErr := store.GetState(ctx, state.Name)
		if getErr != nil {
			return state, getErr
		}
		return failed, ErrValidationMismatch
	}
	switch state.Phase {
	case PhaseCommitted:
		return state, nil
	case PhaseCancelled:
		return state, fmt.Errorf("%w: cutover %q was cancelled", ErrBadOptions, state.Name)
	case PhaseRunning, PhaseFailed:
		return state, nil
	default:
		return state, fmt.Errorf("%w: unknown cutover phase %q", ErrBadOptions, state.Phase)
	}
}

// converge runs reconciliation passes until it commits, exhausts MaxEvents
// or the context is cancelled.
func (m *Manager) converge(
	ctx context.Context, store Store, state *State, opts Options, redisSource, pgSource Source,
) (*State, error) {
	var handled int64
	batchLimit := opts.BatchLimit

	for {
		if err := ctx.Err(); err != nil {
			// Interruption: the last batch already persisted its progress.
			return state, err
		}

		advanced, err := m.processRedis(ctx, store, state, opts, redisSource, batchLimit, &handled)
		if err != nil {
			return m.fail(ctx, store, state, err)
		}
		if opts.MaxEvents > 0 && handled >= opts.MaxEvents {
			return state, nil
		}
		if err := ctx.Err(); err != nil {
			return state, err
		}

		advancedPG, err := m.processPostgres(ctx, store, state, opts, pgSource, batchLimit, &handled)
		if err != nil {
			return m.fail(ctx, store, state, err)
		}
		if opts.MaxEvents > 0 && handled >= opts.MaxEvents {
			return state, nil
		}

		if !advanced && !advancedPG && m.reachedFrontiers(state) {
			reservations, err := store.CountReservations(ctx, opts.Name)
			if err != nil {
				return m.fail(ctx, store, state, err)
			}
			if reservations == 0 {
				state.Phase = PhaseCommitted
				state.Reason = ""
				if err := store.UpdateState(ctx, state); err != nil {
					return nil, err
				}
				return store.GetState(ctx, opts.Name)
			}
		}
	}
}

// processRedis reconciles one batch per stream. It returns whether any scan
// position moved.
func (m *Manager) processRedis(
	ctx context.Context, store Store, state *State, opts Options, source Source,
	batchLimit int, handled *int64,
) (bool, error) {
	stable, ok := source.(StableDeliverySource)
	if !ok {
		return false, fmt.Errorf("%w: redis source does not expose stable delivery info", ErrBadOptions)
	}
	advanced := false
	for i := range state.RedisPoints {
		point := &state.RedisPoints[i]
		if point.LastScannedID == point.StopID {
			continue
		}
		pending, err := stable.PendingIDs(ctx, point.Stream)
		if err != nil {
			return false, err
		}
		cursor := Cursor{Stream: point.Stream, Offset: point.LastScannedID, Bound: point.StopID}
		events, _, err := source.Read(ctx, cursor, batchLimit)
		if err != nil {
			return false, err
		}
		if len(events) == 0 {
			// Nothing between the scan position and the stop point: the scan
			// reached the recorded frontier.
			point.LastScannedID = point.StopID
			advanced = true
			if err := store.UpdateState(ctx, state); err != nil {
				return false, err
			}
			continue
		}
		for _, ev := range events {
			if err := ctx.Err(); err != nil {
				return advanced, err
			}
			identity, err := source.Identity(ev)
			if err != nil {
				return false, err
			}
			hash := ContentHash(ev)
			if pending[ev.Offset] {
				// In flight on the old source: not stably delivered, reconcile.
				if err := m.reconcileEvent(ctx, store, state, opts, source, ev, identity, hash); err != nil {
					return false, err
				}
			} else {
				// Acknowledged below the stop point: record the stable
				// identity without executing the event again.
				if err := m.markStableDelivered(ctx, store, state, ev, identity, hash); err != nil {
					return false, err
				}
			}
			*handled++
			point.LastScannedID = ev.Offset
			if opts.MaxEvents > 0 && *handled >= opts.MaxEvents {
				break
			}
		}
		advanced = true
		if err := store.UpdateState(ctx, state); err != nil {
			return false, err
		}
	}
	return advanced, nil
}

// processPostgres reconciles one batch per outbox partition. Rows still
// present in the outbox were not delivered by the new source yet.
func (m *Manager) processPostgres(
	ctx context.Context, store Store, state *State, opts Options, source Source,
	batchLimit int, handled *int64,
) (bool, error) {
	advanced := false
	for i := range state.PostgresPoints {
		point := &state.PostgresPoints[i]
		if point.LastScannedID == point.StartID {
			continue
		}
		cursor := Cursor{
			Partition: point.Partition,
			Offset:    strconv.FormatInt(point.LastScannedID, 10),
			Bound:     strconv.FormatInt(point.StartID, 10),
		}
		events, _, err := source.Read(ctx, cursor, batchLimit)
		if err != nil {
			return false, err
		}
		if len(events) == 0 {
			point.LastScannedID = point.StartID
			advanced = true
			if err := store.UpdateState(ctx, state); err != nil {
				return false, err
			}
			continue
		}
		for _, ev := range events {
			if err := ctx.Err(); err != nil {
				return advanced, err
			}
			identity, err := source.Identity(ev)
			if err != nil {
				return false, err
			}
			hash := ContentHash(ev)
			if err := m.reconcileEvent(ctx, store, state, opts, source, ev, identity, hash); err != nil {
				return false, err
			}
			*handled++
			point.LastScannedID = mustParseInt64(ev.Offset)
			if opts.MaxEvents > 0 && *handled >= opts.MaxEvents {
				break
			}
		}
		advanced = true
		if err := store.UpdateState(ctx, state); err != nil {
			return false, err
		}
	}
	return advanced, nil
}

// reconcileEvent dispatches an event only when its identity has no delivered
// marker. A content mismatch on an existing marker is an identity conflict.
func (m *Manager) reconcileEvent(
	ctx context.Context, store Store, state *State, opts Options, source Source,
	ev Event, identity, hash string,
) error {
	marker := Marker{
		Identity:    identity,
		ContentHash: hash,
		SourceKind:  ev.SourceKind,
		Offset:      ev.Offset,
	}
	acquired, existing, err := store.ReserveMarker(ctx, opts.Name, marker)
	if err != nil {
		return err
	}
	if !acquired {
		if existing.ContentHash != hash {
			return fmt.Errorf("%w: identity %q seen with different content (%s vs %s)",
				ErrIdentityConflict, identity, existing.ContentHash, hash)
		}
		if existing.Status == markerStatusDelivered {
			// Already delivered by one of the sources: never execute again.
			state.Skipped++
			return nil
		}
		// A reservation held by another worker: treat it as an in-flight
		// event and dispatch through the same reservation path.
	}

	if err := m.cfg.Dispatcher.DispatchCommand(ctx, ev.Method, ev.Payload); err != nil {
		if releaseErr := store.ReleaseMarker(ctx, opts.Name, identity); releaseErr != nil {
			log.Error().Err(releaseErr).Msg("failed to release cutover marker after dispatch error")
		}
		return fmt.Errorf("dispatch event %q: %w", identity, err)
	}
	if err := store.ConfirmMarker(ctx, opts.Name, identity); err != nil {
		return err
	}
	state.Delivered++
	return nil
}

// markStableDelivered records an identity the old source already delivered.
// No dispatch happens, and a conflicting content hash aborts the cutover.
func (m *Manager) markStableDelivered(
	ctx context.Context, store Store, state *State, ev Event, identity, hash string,
) error {
	marker := Marker{
		Identity:    identity,
		ContentHash: hash,
		SourceKind:  ev.SourceKind,
		Offset:      ev.Offset,
	}
	acquired, existing, err := store.ReserveMarker(ctx, state.Name, marker)
	if err != nil {
		return err
	}
	if !acquired {
		if existing.ContentHash != hash {
			return fmt.Errorf("%w: identity %q seen with different content (%s vs %s)",
				ErrIdentityConflict, identity, existing.ContentHash, hash)
		}
		state.Skipped++
		return nil
	}
	if err := store.ConfirmMarker(ctx, state.Name, identity); err != nil {
		return err
	}
	state.Skipped++
	return nil
}

func (m *Manager) fail(ctx context.Context, store Store, state *State, cause error) (*State, error) {
	reason := cause.Error()
	if err := store.FailState(ctx, state.Name, state.Version, PhaseFailed, reason); err != nil {
		log.Error().Err(err).Msg("failed to persist cutover failure")
		return state, cause
	}
	failed, err := store.GetState(ctx, state.Name)
	if err != nil {
		return state, cause
	}
	return failed, cause
}

func (m *Manager) reachedFrontiers(state *State) bool {
	for _, point := range state.RedisPoints {
		if point.LastScannedID != point.StopID {
			return false
		}
	}
	for _, point := range state.PostgresPoints {
		if point.LastScannedID != point.StartID {
			return false
		}
	}
	return true
}

func mustParseInt64(value string) int64 {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("cutover: invalid int64 offset %q: %v", value, err))
	}
	return id
}

// newDefaultPostgresStore opens a pool for the PostgreSQL consumer config
// and wraps it in a PostgresStore.
func newDefaultPostgresStore(cfg configtypes.PostgresConsumerConfig) (Store, error) {
	conf, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("%w: parse postgresql DSN: %v", ErrBadOptions, err)
	}
	if cfg.TLS.Enabled {
		tlsConfig, err := cfg.TLS.ToGoTLSConfig("cutover-store")
		if err != nil {
			return nil, fmt.Errorf("%w: postgresql TLS config: %v", ErrBadOptions, err)
		}
		conf.ConnConfig.TLSConfig = tlsConfig
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("create postgresql pool: %w", err)
	}
	return NewPostgresStore(pool), nil
}
