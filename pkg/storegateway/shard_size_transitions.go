// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ShardSizeTransitions tracks, in memory, each tenant's store-gateway shard size. After a change, the new owners of
// a tenant's blocks load them only at their next sync, so the previous shard size is reported for the transition
// period, for store-gateways to keep their blocks and queriers to fall back to them.
//
// A restarted process has no history and reports no transition. A nil *ShardSizeTransitions never reports one.
type ShardSizeTransitions struct {
	period time.Duration
	now    func() time.Time
	logger log.Logger

	transitionsStarted prometheus.Counter
	transitionsEnded   prometheus.Counter

	mtx     sync.RWMutex
	tenants map[string]*tenantShardSize
}

// TenantShardSize is a tenant's shard size as configured, not as resolved to a total, so that adding or removing a
// zone isn't a change when the shard size is per zone: the tenant's shard in the other zones doesn't change.
type TenantShardSize struct {
	// PerZone takes precedence over Total if greater than 0.
	PerZone int

	// Total of 0 means shuffle sharding is disabled.
	Total int
}

func (s TenantShardSize) String() string {
	if s.PerZone > 0 {
		return fmt.Sprintf("%d per zone", s.PerZone)
	}
	return fmt.Sprintf("%d total", s.Total)
}

// normalized sets only the field in use, so that equivalent sizes are equal.
func (s TenantShardSize) normalized() TenantShardSize {
	if s.PerZone > 0 {
		return TenantShardSize{PerZone: s.PerZone}
	}
	return TenantShardSize{Total: max(s.Total, 0)}
}

// total returns the number of store-gateways in the tenant shard of r. ZonesCount() is 0 only for an empty ring, and 1 in
// non-zone-aware setups, where the empty zone counts as a zone.
func (s TenantShardSize) total(r *ring.Ring) int {
	if s.PerZone > 0 {
		return s.PerZone * max(r.ZonesCount(), 1)
	}
	return s.Total
}

type tenantShardSize struct {
	current TenantShardSize

	// previous is the last settled shard size before the current one. It's only meaningful during a transition.
	previous TenantShardSize

	// since is the time the current shard size was first observed. It's zero when the tenant is settled.
	since time.Time
}

func (s *tenantShardSize) inTransition(now time.Time, period time.Duration) bool {
	return !s.since.IsZero() && now.Sub(s.since) < period
}

// NewShardSizeTransitions returns a ShardSizeTransitions where a shard size change is a transition for the input
// period. The component labels the metrics, so that a querier and a store-gateway can share a registry in monolithic
// mode. The registerer can be nil.
func NewShardSizeTransitions(period time.Duration, component string, reg prometheus.Registerer, logger log.Logger) *ShardSizeTransitions {
	transitions := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "cortex_shard_size_transitions_total",
		Help:        "Total number of tenant store-gateway shard size transitions started and ended, as observed by this component. A transition starts when a change of the tenant shard size is observed, and ends when the transition period elapsed. The end of a transition is only recorded when the tenant is observed again after the period, which for queriers requires a query of the tenant.",
		ConstLabels: prometheus.Labels{"component": component},
	}, []string{"event"})

	return &ShardSizeTransitions{
		period:             period,
		now:                time.Now,
		logger:             logger,
		transitionsStarted: transitions.WithLabelValues("started"),
		transitionsEnded:   transitions.WithLabelValues("ended"),
		tenants:            map[string]*tenantShardSize{},
	}
}

// Observe records shardSize as the tenant's current shard size, and returns the previous one if it's in transition.
//
// If the shard size changes again during a transition, the period restarts and the previous shard size is still the
// one from before the first change, as it's the last one all store-gateways are known to have loaded.
func (t *ShardSizeTransitions) Observe(userID string, shardSize TenantShardSize) (previous TenantShardSize, inTransition bool) {
	if t == nil {
		return TenantShardSize{}, false
	}

	shardSize = shardSize.normalized()

	// Fast path, taken by tenants with an unchanged shard size: a shared lock, and no clock reads if settled.
	t.mtx.RLock()
	s, ok := t.tenants[userID]
	if ok && s.current == shardSize {
		if s.since.IsZero() {
			t.mtx.RUnlock()
			return TenantShardSize{}, false
		}
		if s.inTransition(t.now(), t.period) {
			previous = s.previous
			t.mtx.RUnlock()
			return previous, true
		}
	}
	t.mtx.RUnlock()

	previous, inTransition, started, ended := t.update(userID, shardSize)

	// Logged without holding the lock, because transitions are observed on the read path.
	if ended {
		level.Info(t.logger).Log("msg", "tenant shard size transition ended", "user", userID)
	}
	if started {
		level.Info(t.logger).Log("msg", "tenant shard size transition started", "user", userID, "previous_shard_size", previous, "shard_size", shardSize)
	}
	return previous, inTransition
}

func (t *ShardSizeTransitions) update(userID string, shardSize TenantShardSize) (previous TenantShardSize, inTransition, started, ended bool) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	now := t.now()

	s, ok := t.tenants[userID]
	if !ok {
		// Without history we can't know whether the shard size changed recently.
		t.tenants[userID] = &tenantShardSize{current: shardSize}
		return TenantShardSize{}, false, false, false
	}

	// A transition stays recorded until the first observation after its period elapsed.
	inTransition = !s.since.IsZero()
	if inTransition && !s.inTransition(now, t.period) {
		s.since = time.Time{}
		inTransition = false
		ended = true
		t.transitionsEnded.Inc()
	}

	if s.current != shardSize {
		if !inTransition {
			s.previous = s.current
			started = true
			t.transitionsStarted.Inc()
		}
		s.current = shardSize
		s.since = now
		inTransition = true
	}

	if !inTransition {
		return TenantShardSize{}, false, started, ended
	}
	return s.previous, true, started, ended
}
