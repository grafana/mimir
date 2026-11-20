// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sync"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ShardSizeTransitions tracks, in memory, the store-gateway shard size of each tenant and detects when it changes.
//
// After a change, store-gateways that become owners of a tenant's blocks load them only at their next sync, while
// queriers start using the new tenant shard immediately. For the duration of the transition, the previous shard size
// is reported so that store-gateways keep the blocks they already loaded, and queriers can fall back to them.
//
// The state is not persisted: a process that restarts has no history and behaves as if no transition is in progress.
// A nil *ShardSizeTransitions is valid and never reports a transition.
type ShardSizeTransitions struct {
	period time.Duration
	now    func() time.Time

	transitionsStarted prometheus.Counter
	transitionsEnded   prometheus.Counter

	mtx     sync.RWMutex
	tenants map[string]*tenantShardSize
}

// TenantShardSize is a tenant's store-gateway shard size as configured: either a shard size per zone, or a total
// shard size. It's tracked as configured, rather than as the total number of store-gateways it resolves to, so that
// adding or removing a zone isn't a shard size change when the shard size is per zone: the tenant's shard in the other
// zones doesn't change. It's comparable, and equal configurations are equal once normalized.
type TenantShardSize struct {
	// PerZone is the shard size per zone. If greater than 0, it takes precedence over Total.
	PerZone int

	// Total is the total shard size. A shard size of 0 means shuffle sharding is disabled.
	Total int
}

// normalized returns the shard size with only the field in use set, so that equivalent configurations are equal.
func (s TenantShardSize) normalized() TenantShardSize {
	if s.PerZone > 0 {
		return TenantShardSize{PerZone: s.PerZone}
	}
	return TenantShardSize{Total: max(s.Total, 0)}
}

// total returns the total number of store-gateways in the tenant shard of the ring r. A per-zone shard size is
// multiplied by the number of zones in the ring. ZonesCount() returns 0 only if there are no instances in the
// ring, which is counted as 1 zone. In non-zone-aware setups, ZonesCount() returns 1 because the empty
// zone string is counted as a zone.
func (s TenantShardSize) total(r *ring.Ring) int {
	if s.PerZone > 0 {
		return s.PerZone * max(r.ZonesCount(), 1)
	}
	return s.Total
}

type tenantShardSize struct {
	// current is the observed shard size during the request or sync.
	current TenantShardSize

	// previous is the last settled shard size before the current one. It's only meaningful during a transition.
	previous TenantShardSize

	// since is the time the current shard size was first observed. It's zero when the tenant is settled.
	since time.Time
}

func (s *tenantShardSize) inTransition(now time.Time, period time.Duration) bool {
	return !s.since.IsZero() && now.Sub(s.since) < period
}

// NewShardSizeTransitions returns a ShardSizeTransitions in which a shard size change is a transition for the
// input period. The component ("querier" or "store-gateway") labels the exported metrics, so that both can be
// registered in the same registry when running in monolithic mode. The registerer can be nil.
func NewShardSizeTransitions(period time.Duration, component string, reg prometheus.Registerer) *ShardSizeTransitions {
	transitions := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "cortex_shard_size_transitions_total",
		Help:        "Total number of tenant store-gateway shard size transitions started and ended, as observed by this component. A transition starts when a change of the tenant shard size is observed, and ends when the transition period elapsed or the shard size changed back. The end of a transition is only recorded when the tenant is observed again after the period, which for queriers requires a query of the tenant.",
		ConstLabels: prometheus.Labels{"component": component},
	}, []string{"event"})

	return &ShardSizeTransitions{
		period:             period,
		now:                time.Now,
		transitionsStarted: transitions.WithLabelValues("started"),
		transitionsEnded:   transitions.WithLabelValues("ended"),
		tenants:            map[string]*tenantShardSize{},
	}
}

// InTransition returns whether the tenant's last observed shard size changed less than the transition period ago.
// It doesn't record anything, so it reflects the last call to Observe.
func (t *ShardSizeTransitions) InTransition(userID string) bool {
	if t == nil {
		return false
	}

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	s, ok := t.tenants[userID]
	return ok && !s.since.IsZero() && s.previous != s.current && s.inTransition(t.now(), t.period)
}

// Observe records shardSize as the current shard size of the tenant. If it changed less than the transition period
// ago, it returns the shard size which was in use before the change and true.
//
// If the shard size changes again during a transition, the transition restarts and keeps reporting the shard size
// from before the first change, since that's the last one all store-gateways are known to have loaded.
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

	t.mtx.Lock()
	defer t.mtx.Unlock()

	now := t.now()

	s, ok = t.tenants[userID]
	if !ok {
		// Without history we can't know whether the shard size changed recently.
		t.tenants[userID] = &tenantShardSize{current: shardSize}
		return TenantShardSize{}, false
	}

	// A transition stays recorded until the first observation after its period elapsed.
	inTransition = !s.since.IsZero()
	if inTransition && !s.inTransition(now, t.period) {
		s.since = time.Time{}
		inTransition = false
		t.transitionsEnded.Inc()
	}

	if s.current != shardSize {
		if !inTransition {
			// The previous shard size is the last settled one, which keeps being reported if the shard size changes
			// again during the transition.
			s.previous = s.current
			t.transitionsStarted.Inc()
		}
		s.current = shardSize
		s.since = now
		inTransition = true

		// Changing back to the last settled shard size leaves nothing to transition from.
		if s.previous == s.current {
			s.since = time.Time{}
			inTransition = false
			t.transitionsEnded.Inc()
		}
	}

	if !inTransition {
		return TenantShardSize{}, false
	}
	return s.previous, true
}
