// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_replicated_set.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
)

type loadBalancingStrategy int

const (
	noLoadBalancing = loadBalancingStrategy(iota)
	randomLoadBalancing
	// loadAwareLoadBalancing biases the per-block replica pick by a per-instance
	// EWMA-decayed pick counter, using power-of-two-choices over two random eligible
	// candidates. Dampens fan-out amplification on hot store-gateway pods during
	// multi-replica fan-out events. Counters are local to this querier instance and
	// do not require any cooperation from store-gateways.
	loadAwareLoadBalancing
)

// defaultLoadAwarePickHalfLife is the EWMA half-life applied to the pick counters
// when no operator override is supplied. A pick "ages out" of the load signal over
// this duration, so counters reflect roughly the last ~half-life of activity rather
// than lifetime totals. The 30-second value is long enough to smooth across
// single-query fan-outs (which complete in seconds) but short enough that a hot pod
// that calms down stops being avoided. Operators can override via
// -querier.store-gateway-load-aware-pick-half-life.
const defaultLoadAwarePickHalfLife = 30 * time.Second

// loadCounterPruneInterval is the cadence at which the load-aware path walks
// loadCounters and drops entries whose address is no longer in the storesRing.
// Five minutes is long enough to be cheap (one ring read + one map walk) and short
// enough that a multi-month-uptime querier doesn't accumulate dead state across
// store-gateway rolling deploys.
const loadCounterPruneInterval = 5 * time.Minute

// loadCounterPruneRingOp is a ring operation that accepts every instance state
// (ACTIVE, LEAVING, PENDING, JOINING, LEFT). Used by pruneStaleLoadCounters so a
// store-gateway transitioning between healthy/unhealthy retains its counter — only
// instances genuinely removed from the ring (or with a stale heartbeat that the
// ring's IsHealthy check rejects) get their counter dropped. Using
// storegateway.BlocksRead here would prune counters for instances that are merely
// JOINING or LEAVING, giving them a brief surge of picks on recovery as their
// fresh-zero counter wins the power-of-two comparison.
var loadCounterPruneRingOp = ring.NewOp([]ring.InstanceState{
	ring.ACTIVE, ring.LEAVING, ring.PENDING, ring.JOINING, ring.LEFT,
}, nil)

// BlocksStoreSet implementation used when the blocks are sharded and replicated across
// a set of store-gateway instances.
type blocksStoreReplicationSet struct {
	services.Service

	storesRing         *ring.Ring
	clientsPool        *client.Pool
	balancingStrategy  loadBalancingStrategy
	dynamicReplication storegateway.DynamicReplication
	limits             BlocksStoreLimits

	// When non empty, the querier prioritises querying blocks from store-gateways in these zones with equal priority.
	preferredZones []string

	// loadCounters holds per-instance pick counters used by loadAwareLoadBalancing.
	// The map is keyed by instance address; each counter has its own mutex so
	// updates don't contend across instances.
	//
	// Under pod-IP churn (rolling store-gateway deploys), dead addresses would
	// otherwise linger forever. The running() loop periodically calls
	// pruneStaleLoadCounters to drop entries whose address is no longer in the
	// storesRing, so steady-state cardinality is bounded by the ring size.
	loadCounters sync.Map // map[string]*loadCounter

	// loadAwarePickHalfLife is the EWMA half-life applied to entries in loadCounters.
	loadAwarePickHalfLife time.Duration

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

// loadCounter is a per-instance EWMA-decayed pick counter. Reads and writes apply
// the decay lazily based on time since the last update, so we don't need a separate
// goroutine to age values out.
type loadCounter struct {
	mu        sync.Mutex
	value     float64
	lastTouch time.Time
}

func (c *loadCounter) bump(halfLife time.Duration) {
	c.mu.Lock()
	c.applyDecayLocked(time.Now(), halfLife)
	c.value += 1.0
	c.mu.Unlock()
}

func (c *loadCounter) load(halfLife time.Duration) float64 {
	c.mu.Lock()
	c.applyDecayLocked(time.Now(), halfLife)
	v := c.value
	c.mu.Unlock()
	return v
}

func (c *loadCounter) applyDecayLocked(now time.Time, halfLife time.Duration) {
	if c.lastTouch.IsZero() {
		c.lastTouch = now
		return
	}
	elapsed := now.Sub(c.lastTouch)
	if elapsed <= 0 {
		return
	}
	if halfLife <= 0 {
		// Defensive: a zero-value blocksStoreReplicationSet (e.g. constructed via
		// struct literal in a test) would otherwise divide by zero here and zero
		// out the counter. Constructed instances always go through
		// newBlocksStoreReplicationSet which substitutes the default; this guard
		// catches any other caller path.
		halfLife = defaultLoadAwarePickHalfLife
	}
	// Exponential decay with the configured half-life: value *= 2^(-elapsed/halfLife).
	c.value *= math.Exp2(-elapsed.Seconds() / halfLife.Seconds())
	c.lastTouch = now
}

func newBlocksStoreReplicationSet(
	storesRing *ring.Ring,
	balancingStrategy loadBalancingStrategy,
	loadAwarePickHalfLife time.Duration,
	dynamicReplication storegateway.DynamicReplication,
	preferredZones []string,
	limits BlocksStoreLimits,
	clientConfig StoreGatewayClientConfig,
	logger log.Logger,
	reg prometheus.Registerer,
) (*blocksStoreReplicationSet, error) {
	if loadAwarePickHalfLife <= 0 {
		loadAwarePickHalfLife = defaultLoadAwarePickHalfLife
	}
	s := &blocksStoreReplicationSet{
		storesRing:            storesRing,
		clientsPool:           newStoreGatewayClientPool(client.NewRingServiceDiscovery(storesRing), clientConfig, logger, reg),
		dynamicReplication:    dynamicReplication,
		balancingStrategy:     balancingStrategy,
		preferredZones:        preferredZones,
		limits:                limits,
		loadAwarePickHalfLife: loadAwarePickHalfLife,
		subservicesWatcher:    services.NewFailureWatcher(),
	}

	var err error
	s.subservices, err = services.NewManager(s.storesRing, s.clientsPool)
	if err != nil {
		return nil, err
	}

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)

	return s, nil
}

func (s *blocksStoreReplicationSet) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks store set subservices")
	}

	return nil
}

func (s *blocksStoreReplicationSet) running(ctx context.Context) error {
	// Only the load-aware path needs the periodic counter prune; for other strategies
	// loadCounters is never written to, so a nil channel disables the case entirely.
	var pruneCh <-chan time.Time
	if s.balancingStrategy == loadAwareLoadBalancing {
		ticker := time.NewTicker(loadCounterPruneInterval)
		defer ticker.Stop()
		pruneCh = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pruneCh:
			s.pruneStaleLoadCounters()
		case err := <-s.subservicesWatcher.Chan():
			return errors.Wrap(err, "blocks store set subservice failed")
		}
	}
}

func (s *blocksStoreReplicationSet) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
}

func (s *blocksStoreReplicationSet) GetClientsFor(userID string, blocks bucketindex.Blocks, exclude map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error) {
	blocksByAddr := make(map[string][]ulid.ULID)
	instances := make(map[string]ring.InstanceDesc)
	userRing := storegateway.GetShuffleShardingSubring(s.storesRing, userID, s.limits)

	// It's safe to reuse buffers across iterations, as far as we don't retain the ReplicationSet.Instances slice
	// (retaining individual instances from the slice is fine).
	ringBuffersOpt := ring.WithBuffers(ring.MakeBuffersForGet())

	// Find the replication set of each block we need to query.
	for _, block := range blocks {
		ringOpts := []ring.Option{ringBuffersOpt}
		if eligible, replicationFactor := s.dynamicReplication.EligibleForQuerying(block); eligible {
			ringOpts = append(ringOpts, ring.WithReplicationFactor(replicationFactor))
		}

		set, err := userRing.GetWithOptions(mimir_tsdb.HashBlockID(block.ID), storegateway.BlocksRead, ringOpts...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get store-gateway replication set owning the block %s", block.ID)
		}

		// Pick a non excluded store-gateway instance.
		// We copy the instance descriptor, and we don't use the replication set's slice buffers any more beyond this point,
		// so that it's safe to reuse ring buffers across iterations.
		inst := s.pickInstance(set, exclude[block.ID])
		if inst == nil {
			return nil, fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block.ID)
		}

		instances[inst.Addr] = *inst
		blocksByAddr[inst.Addr] = append(blocksByAddr[inst.Addr], block.ID)
	}

	clients := map[BlocksStoreClient][]ulid.ULID{}

	// Get the client for each store-gateway.
	for addr, instance := range instances {
		c, err := s.clientsPool.GetClientForInstance(instance)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get store-gateway client for %s %s", instance.Id, addr)
		}

		clients[c.(BlocksStoreClient)] = blocksByAddr[addr]
	}

	return clients, nil
}

// pickInstance selects one non-excluded store-gateway from set per the configured
// balancing strategy and preferred zones. For loadAwareLoadBalancing the pick is
// power-of-two-choices over the per-instance EWMA pick counters; the counter for
// the chosen instance is incremented on the way out so subsequent picks see the
// updated load.
func (s *blocksStoreReplicationSet) pickInstance(set ring.ReplicationSet, exclude []string) *ring.InstanceDesc {
	switch s.balancingStrategy {
	case randomLoadBalancing, loadAwareLoadBalancing:
		// Random shuffle so within-tier sampling is uniform. The load-aware path then
		// applies power-of-two-choices on top of this; the random path just picks the
		// first non-excluded instance after shuffle.
		rand.Shuffle(len(set.Instances), func(i, j int) {
			set.Instances[i], set.Instances[j] = set.Instances[j], set.Instances[i]
		})
	}

	// Move preferred-zone instances to the front and remember the boundary. nextPos
	// is the count of preferred-zone instances at the head of set.Instances; the
	// remaining instances [nextPos:] live in non-preferred zones (or there are no
	// preferred zones configured, in which case nextPos == len(set.Instances) and
	// the whole set is treated as one tier).
	nextPos := len(set.Instances)
	if len(s.preferredZones) > 0 {
		nextPos = 0
		for idx, instance := range set.Instances {
			if slices.Contains(s.preferredZones, instance.Zone) {
				set.Instances[nextPos], set.Instances[idx] = set.Instances[idx], set.Instances[nextPos]
				nextPos++
			}
		}
	}

	if s.balancingStrategy != loadAwareLoadBalancing {
		for _, instance := range set.Instances {
			if !slices.Contains(exclude, instance.Addr) {
				return &instance
			}
		}
		return nil
	}

	// Load-aware path: power-of-two-choices, but constrained to preserve preferred-
	// zone semantics. If first lands in the preferred-zone tier, second must come
	// from the same tier; otherwise the load comparison could downgrade us into a
	// non-preferred zone whenever exactly one preferred replica is eligible — the
	// most likely production configuration.
	firstIdx := -1
	for i := range set.Instances {
		if !slices.Contains(exclude, set.Instances[i].Addr) {
			firstIdx = i
			break
		}
	}
	if firstIdx == -1 {
		return nil
	}
	first := &set.Instances[firstIdx]

	// Constrain second-sample search to the same tier as first.
	secondSearchEnd := len(set.Instances)
	if firstIdx < nextPos {
		secondSearchEnd = nextPos
	}
	var second *ring.InstanceDesc
	for i := firstIdx + 1; i < secondSearchEnd; i++ {
		if !slices.Contains(exclude, set.Instances[i].Addr) {
			second = &set.Instances[i]
			break
		}
	}

	// Tied counters (e.g. both 0.0 at warmup) go to first by virtue of the strict
	// less-than. The upstream rand.Shuffle gives warmup-window randomness across
	// callers, so the bias is inert in aggregate; no fairer tiebreak is needed.
	chosen := first
	if second != nil && s.instanceLoad(second.Addr) < s.instanceLoad(first.Addr) {
		chosen = second
	}
	s.bumpInstance(chosen.Addr)
	return chosen
}

// instanceLoad returns the current EWMA-decayed pick count for addr.
func (s *blocksStoreReplicationSet) instanceLoad(addr string) float64 {
	v, ok := s.loadCounters.Load(addr)
	if !ok {
		return 0
	}
	return v.(*loadCounter).load(s.loadAwarePickHalfLife)
}

// bumpInstance increments the pick counter for addr, allocating a new counter on
// first use.
func (s *blocksStoreReplicationSet) bumpInstance(addr string) {
	c, ok := s.loadCounters.Load(addr)
	if !ok {
		c, _ = s.loadCounters.LoadOrStore(addr, &loadCounter{})
	}
	c.(*loadCounter).bump(s.loadAwarePickHalfLife)
}

// pruneStaleLoadCounters reads the current set of ring instances (in any state)
// and drops loadCounters entries whose address is no longer present. Called from
// running() on loadCounterPruneInterval. A ring lookup error is swallowed (logging
// would be noisy and the next tick will retry); the worst case is one extra prune
// cycle of dead state. The all-states op (loadCounterPruneRingOp) is deliberate:
// see its docstring for why we don't use storegateway.BlocksRead here.
func (s *blocksStoreReplicationSet) pruneStaleLoadCounters() {
	all, err := s.storesRing.GetAllHealthy(loadCounterPruneRingOp)
	if err != nil {
		return
	}
	live := make(map[string]struct{}, len(all.Instances))
	for _, inst := range all.Instances {
		live[inst.Addr] = struct{}{}
	}
	s.pruneLoadCountersNotIn(live)
}

// pruneLoadCountersNotIn deletes every loadCounters entry whose address is not in
// live. Split out for unit tests so the prune logic can be exercised without a
// real ring.
func (s *blocksStoreReplicationSet) pruneLoadCountersNotIn(live map[string]struct{}) {
	s.loadCounters.Range(func(key, _ any) bool {
		addr := key.(string)
		if _, ok := live[addr]; !ok {
			s.loadCounters.Delete(addr)
		}
		return true
	})
}
