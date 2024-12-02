// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"math"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/atomicswissmap"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const shards = 128

type Config struct {
	InstanceRing  InstanceRingConfig  `yaml:"ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
}

type UsageTracker struct {
	services.Service

	overrides *validation.Overrides
	logger    log.Logger

	partitionID          int32
	partitionLifecycler  *ring.PartitionInstanceLifecycler
	partitionWatcher     *ring.PartitionRingWatcher
	partitionPageHandler *ring.PartitionRingPageHandler

	instanceLifecycler *ring.BasicLifecycler

	// Dependencies.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// lock[i] locks data[i] map.
	lock [shards]shardLock
	// data[i] map is protected by lock[i].
	data [shards]map[string]*atomicswissmap.Map

	tenantsMtx sync.RWMutex
	tenants    map[string]*tenantInfo
}

// shardLock is borrowed from https://github.com/prometheus/prometheus/blob/cd1f8ac129a289be8e7d98b6de57a9ba5814c406/tsdb/agent/series.go#L132-L136
type shardLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

type tenantInfo struct {
	series     atomic.Uint64
	lastUpdate atomic.Int64
}

func NewUsageTracker(cfg Config, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		overrides: overrides,
		logger:    logger,
		tenants:   map[string]*tenantInfo{},
	}
	for i := range t.data {
		t.data[i] = make(map[string]*atomicswissmap.Map)
	}

	// Get the partition ID.
	var err error
	t.partitionID, err = partitionIDFromInstanceID(cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating usage-tracker partition ID")
	}

	// Init instance ring.
	t.instanceLifecycler, err = NewInstanceRingLifecycler(cfg.InstanceRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Init the partition ring.
	partitionKVClient, err := NewPartitionRingKVClient(cfg.PartitionRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, t.partitionID, cfg.InstanceRing.InstanceID, partitionKVClient, logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionWatcher = ring.NewPartitionRingWatcher(partitionRingName, partitionRingKey, partitionKVClient, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	t.partitionPageHandler = ring.NewPartitionRingPageHandler(t.partitionWatcher, ring.NewPartitionRingEditor(partitionRingKey, partitionKVClient))

	t.Service = services.NewBasicService(t.start, t.run, t.stop)

	return t, nil
}

// start implements services.StartingFn.
func (t *UsageTracker) start(ctx context.Context) error {
	var err error

	// Start dependencies.
	if t.subservices, err = services.NewManager(t.instanceLifecycler, t.partitionLifecycler, t.partitionWatcher); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker dependencies")
	}

	t.subservicesWatcher = services.NewFailureWatcher()
	t.subservicesWatcher.WatchManager(t.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, t.subservices); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker subservices")
	}

	return nil
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	// Stop dependencies.
	if t.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), t.subservices)
	}

	return nil
}

// run implements services.RunningFn.
func (t *UsageTracker) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	return t.trackSeries(context.Background(), req, time.Now())
}

// trackSeries is used in tests so we can provide custom time.Now() value.
func (t *UsageTracker) trackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest, now time.Time) (*usagetrackerpb.TrackSeriesResponse, error) {
	info := t.getOrCreateUpdatedTenantInfo(req.UserID)
	limit := t.localSeriesLimit(req.UserID)
	if limit == 0 {
		limit = math.MaxUint64
	}

	// Sort series by shard.
	// Start each tenant on its own shard to avoid hotspots.
	tenantStartingShard := xxhash.Sum64String(req.UserID) % shards
	series := req.SeriesHashes
	slices.SortFunc(series, func(a, b uint64) int {
		return int((a%shards+tenantStartingShard)%shards) - int((b%shards+tenantStartingShard)%shards)
	})

	nowMinutes := int32(now.Sub(now.Truncate(4 * time.Hour)).Minutes())

	created := make([]uint64, 0, len(series))
	rejected := series[:0]
	i0 := 0
	for i := 1; i <= len(series); i++ {
		if i == len(series) || (series[i]%shards) != (series[i0]%shards) {
			created, rejected = t.trackShardSeries(req.UserID, info, series[i0:i], limit, created, rejected, nowMinutes)
			i0 = i
		}
	}

	// TODO: send the created series to Kafka events topic.
	level.Info(t.logger).Log("msg", "tracked series", "userID", req.UserID, "received", len(series), "created", len(created), "rejected", len(rejected))

	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

func (t *UsageTracker) trackShardSeries(userID string, info *tenantInfo, series []uint64, limit uint64, created, rejected []uint64, now int32) (_created, _rejected []uint64) {
	shard := series[0] % shards
	m := t.getOrCreateTenantShardMap(userID, shard, limit)
	newSeries := 0
	m.LockKeys()
	for i := 0; i < len(series); i++ {
		ts, ok := m.Get(series[i])
		if !ok {
			series[newSeries] = series[i]
			newSeries++
			continue
		}

		// CAS it if it's higher.
		for val := ts.Load(); minutesGreater(now, val) && !ts.CompareAndSwap(val, now); val = ts.Load() {
		}

	}
	m.UnlockKeys()

	// Check if there's any need to create series first, maybe we don't need to take the mutex at all.
	if info.series.Load() >= limit {
		rejected = append(rejected, series[:newSeries]...)
		return created, rejected
	}

	m.Lock()
	for i := 0; i < newSeries && limit <= info.series.Load(); i++ {
		ts, found := m.GetOrCreate(series[i], now)
		if found {
			// CAS it if it's higher.
			for val := ts.Load(); minutesGreater(now, val) && !ts.CompareAndSwap(val, now); val = ts.Load() {
			}
		}
	}
	m.Unlock()

	rejected = append(rejected, series[:newSeries]...)
	return created, rejected
}

// minutesGreater returns true if a is greater than b as minutes values on a four-hour clock face assuming that none of the values is ever older than 1h.
func minutesGreater(a, b int32) bool {
	if a > b {
		// true: 40-20 = 20
		// true: 130-100 = 30
		// false: 190-10 = 180 (that 190 is from the previous hour)
		return a-b < 60
	}
	// true: 10, 190: 250-195 = 55
	// false: 10, 120: 250-120 = 130
	return a+(4*60)-b < 60
}

func (t *UsageTracker) getOrCreateTenantShardMap(id string, shard uint64, limit uint64) *atomicswissmap.Map {
	t.lock[shard].RLock()
	m := t.data[shard][id]
	t.lock[shard].RUnlock()
	if m != nil {
		return m
	}

	t.lock[shard].Lock()
	defer t.lock[shard].Unlock()
	if m = t.data[shard][id]; m != nil {
		return m
	}

	m = atomicswissmap.New(uint32(limit / shards))
	t.data[shard][id] = m
	return m
}

func (t *UsageTracker) getOrCreateUpdatedTenantInfo(tenantID string) *tenantInfo {
	now := time.Now().Unix()

	t.tenantsMtx.RLock()
	if info, ok := t.tenants[tenantID]; ok {
		// blind update here, we don't care if we overwrite the lastUpdate value with one second earlier.
		info.lastUpdate.Store(now)
		// don't unlock until we've updated the lastUpdate value.
		t.tenantsMtx.RUnlock()
		return info
	}
	t.tenantsMtx.RUnlock()

	t.tenantsMtx.Lock()
	defer t.tenantsMtx.Unlock()
	if info, ok := t.tenants[tenantID]; ok {
		return info
	}

	info := &tenantInfo{}
	info.lastUpdate.Store(now)

	t.tenants[tenantID] = info
	return info
}

func (t *UsageTracker) InstanceRingHandler(w http.ResponseWriter, req *http.Request) {
	t.instanceLifecycler.ServeHTTP(w, req)
}

func (t *UsageTracker) PartitionRingHandler(w http.ResponseWriter, req *http.Request) {
	t.partitionPageHandler.ServeHTTP(w, req)
}

func (t *UsageTracker) localSeriesLimit(userID string) uint64 {
	globalLimit := t.overrides.MaxGlobalSeriesPerUser(userID) // TODO: use a new active series limit.
	if globalLimit <= 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return uint64(float64(globalLimit) / float64(t.partitionWatcher.PartitionRing().ActivePartitionsCount()))
}
