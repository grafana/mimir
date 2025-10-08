// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	pkg_errors "github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// loadBalancer defines the interface for different load balancing strategies
// to determine which instance should process a given block
type loadBalancer interface {
	services.Service

	// shouldEnqueue returns true if the current instance should enqueue
	// the block for processing. Before attempting to process a block,
	// the instance should still call lock to acquire the right to process it.
	shouldEnqueue(ctx context.Context, blockID string) (bool, error)

	// lock attempts to acquire the right to process the block.
	// If false is returned, the instance should not process the block.
	lock(ctx context.Context, blockID string) (bool, error)

	// unlock must be called after processing a block to release the lock.
	unlock(ctx context.Context, blockID string) error
}

const (
	lockTTL    = 4 * time.Hour
	lockPrefix = "parquet-converter-lock:"
)

// cacheLockLoadBalancer implements loadBalancer with a best-effort distributed locking
// using a cache backend.
type cacheLockLoadBalancer struct {
	services.Service
	cache cache.Cache
}

func newCacheLockLoadBalancer(cache cache.Cache) *cacheLockLoadBalancer {
	l := &cacheLockLoadBalancer{
		cache: cache,
	}
	l.Service = services.NewIdleService(l.starting, l.stopping).WithName("cache-lock-load-balancer")
	return l
}

func (l *cacheLockLoadBalancer) starting(ctx context.Context) error {
	return nil
}

func (l *cacheLockLoadBalancer) stopping(error) error {
	l.cache.Stop()
	return nil
}

func (l *cacheLockLoadBalancer) shouldEnqueue(ctx context.Context, blockID string) (bool, error) {
	return true, nil
}

func (l *cacheLockLoadBalancer) lock(ctx context.Context, blockID string) (bool, error) {
	lockKey := lockPrefix + blockID
	err := l.cache.Add(ctx, lockKey, []byte("locked"), lockTTL)
	if errors.Is(err, cache.ErrNotStored) {
		return false, nil
	}
	return err == nil, err
}

func (l *cacheLockLoadBalancer) unlock(ctx context.Context, blockID string) error {
	lockKey := lockPrefix + blockID
	return l.cache.Delete(ctx, lockKey)
}

// ringLoadBalancer implements loadBalancer using a ring for consistent hashing
type ringLoadBalancer struct {
	services.Service
	ring           *ring.Ring
	ringLifecycler *ring.BasicLifecycler
	manager        *services.Manager
	watcher        *services.FailureWatcher
}

const (
	ringKey = "parquet-converter"
	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 10
)

var (
	RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

func newRingLoadBalancer(cfg RingConfig, logger log.Logger, registerer prometheus.Registerer) (*ringLoadBalancer, error) {
	ring, ringLifecycler, err := newRingAndLifecycler(cfg, logger, registerer)
	if err != nil {
		return nil, err
	}

	ringServices, err := services.NewManager(ringLifecycler, ring)
	if err != nil {
		return nil, pkg_errors.Wrap(err, "unable to create parquet-converter ring dependencies")
	}

	watcher := services.NewFailureWatcher()

	r := &ringLoadBalancer{
		ring:           ring,
		ringLifecycler: ringLifecycler,
		manager:        ringServices,
		watcher:        watcher,
	}
	r.Service = services.NewBasicService(r.starting, r.running, r.stopping).WithName("ring-load-balancer")
	return r, nil
}

func (r *ringLoadBalancer) starting(ctx context.Context) error {
	r.watcher.WatchManager(r.manager)
	return services.StartManagerAndAwaitHealthy(ctx, r.manager)
}

func (r *ringLoadBalancer) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.watcher.Chan():
		return err
	}
}

func (r *ringLoadBalancer) stopping(error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), r.manager)
}

func newRingAndLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "parquet-converter-lifecycler"), logger)
	if err != nil {
		return nil, nil, pkg_errors.Wrap(err, "failed to initialize parquet-converters' KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, pkg_errors.Wrap(err, "failed to build parquet-converters' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, logger)

	parquetConvertersLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "parquet-converter", ringKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, pkg_errors.Wrap(err, "failed to initialize parquet-converter' lifecycler")
	}

	parquetConvertersRing, err := ring.New(cfg.toRingConfig(), "parquet-converter", ringKey, logger, reg)
	if err != nil {
		return nil, nil, pkg_errors.Wrap(err, "failed to initialize parquet-converter' parquetConvertersRing client")
	}

	return parquetConvertersRing, parquetConvertersLifecycler, nil
}

func (r *ringLoadBalancer) shouldEnqueue(ctx context.Context, blockID string) (bool, error) {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(blockID))
	rs, err := r.ring.Get(hasher.Sum32(), RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of parquet-converter in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == r.ringLifecycler.GetInstanceAddr(), nil
}

func (r *ringLoadBalancer) lock(ctx context.Context, blockID string) (bool, error) {
	return true, nil
}

func (r *ringLoadBalancer) unlock(ctx context.Context, blockID string) error {
	return nil
}
