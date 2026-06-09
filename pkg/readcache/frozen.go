// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"math"
	"os"
	"time"

	"github.com/go-kit/log/level"
)

const (
	// frozenEpochReapInterval is how often the reaper scans frozen
	// epochs for ones whose data has aged out.
	frozenEpochReapInterval = 5 * time.Minute

	// frozenEpochReapGrace is added to LocalBlockRetention before a
	// frozen epoch is reaped, so an epoch is dropped only once its
	// newest sample is comfortably older than the readcache serving
	// horizon. This keeps the previous owner queryable for the whole
	// window the distributor may still route to it (the distributor
	// clamps queries to now-QueryIngestersWithin and the rebalancer
	// retains the lease over the same horizon), avoiding a gap where
	// the log still names this pod as a past owner but its slice is
	// already gone.
	frozenEpochReapGrace = 30 * time.Minute
)

// frozenEpoch is a read-only TSDB set retained after this pod stopped
// actively owning a partition. Each epoch corresponds to one ownership
// stint ({partition, epoch}); its per-tenant TSDBs keep serving the
// slice ingested during that stint until the reaper drops it.
//
// minT/maxT are the sample-time bounds captured at freeze time across
// all tenants in the epoch. The reaper uses maxT (absolute wallclock)
// to decide when the epoch has aged out of the readcache serving
// horizon — Prometheus's relative block retention never fires on a
// frozen DB because no new, newer blocks arrive to push the old ones
// past the retention window.
type frozenEpoch struct {
	partitionID int32
	epoch       int
	tenants     map[string]*partitionTSDB
	minT        int64
	maxT        int64
}

// freezePartition stops the partition's Kafka reader and moves its
// per-tenant TSDBs into a read-only frozen epoch instead of closing
// them, so the slice this pod ingested stays queryable after the
// partition moved to another readcache. The caller must guarantee p
// is no longer reachable through r.partitions before invoking this
// (removePartition deletes from the map under partitionMu first), so
// this runs WITHOUT holding partitionMu — required to avoid
// deadlocking the Kafka reader's stop path against in-flight pushers
// (see removePartition for the full rationale).
func (r *Readcache) freezePartition(partitionID int32, p *partitionState) error {
	if hook := r.stopPartitionHook; hook != nil {
		hook(partitionID)
	}

	var firstErr error
	if err := r.stopKafkaReaderLocked(p); err != nil {
		firstErr = err
		level.Warn(r.logger).Log("msg", "stopping partition reader before freeze", "partition", partitionID, "err", err)
	}

	// Detach the tenant TSDBs from the (now unreachable) live state.
	// The reader has returned from StopAndAwaitTerminated, so no
	// goroutine is appending; taking tenantsMu is safe.
	p.tenantsMu.Lock()
	tenants := p.tenants
	p.tenants = map[string]*partitionTSDB{}
	p.tenantsMu.Unlock()

	if len(tenants) == 0 {
		level.Info(r.logger).Log("msg", "readcache: partition removed (nothing to freeze)", "partition", partitionID)
		return firstErr
	}

	ep := &frozenEpoch{
		partitionID: partitionID,
		epoch:       p.epoch,
		tenants:     tenants,
		minT:        math.MaxInt64,
		maxT:        math.MinInt64,
	}
	for tenant, db := range tenants {
		mn, mx := db.sampleBounds()
		if mx < mn {
			continue // empty TSDB; bounds left at the sentinel
		}
		if mn < ep.minT {
			ep.minT = mn
		}
		if mx > ep.maxT {
			ep.maxT = mx
		}
		// Release the (tenant, partition) metrics key so the next live
		// epoch of this partition can register cleanly; the metrics
		// key does not encode the epoch.
		if r.tsdbMetrics != nil {
			r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, partitionID))
		}
	}

	r.frozenMu.Lock()
	r.frozen[partitionID] = append(r.frozen[partitionID], ep)
	r.frozenMu.Unlock()

	level.Info(r.logger).Log("msg", "readcache: partition frozen",
		"partition", partitionID, "epoch", p.epoch, "minT", ep.minT, "maxT", ep.maxT)
	return firstErr
}

// reapFrozenEpochs closes and deletes frozen epochs whose newest
// sample is older than the readcache serving horizon
// (LocalBlockRetention + grace) relative to now. Reaping is by
// absolute wallclock against the epoch's captured maxT: a frozen TSDB
// receives no new samples, so Prometheus's relative block retention
// would never delete its blocks on its own.
func (r *Readcache) reapFrozenEpochs(now time.Time) {
	cutoff := now.Add(-r.cfg.LocalBlockRetention - frozenEpochReapGrace).UnixMilli()

	var toClose []*frozenEpoch
	r.frozenMu.Lock()
	for partitionID, eps := range r.frozen {
		kept := eps[:0]
		for _, ep := range eps {
			if ep.maxT < cutoff {
				toClose = append(toClose, ep)
				continue
			}
			kept = append(kept, ep)
		}
		if len(kept) == 0 {
			delete(r.frozen, partitionID)
		} else {
			r.frozen[partitionID] = kept
		}
	}
	r.frozenMu.Unlock()

	for _, ep := range toClose {
		for tenant, db := range ep.tenants {
			if err := db.Close(); err != nil {
				level.Warn(r.logger).Log("msg", "closing reaped frozen epoch TSDB",
					"user", tenant, "partition", ep.partitionID, "epoch", ep.epoch, "err", err)
			}
			// Reclaim the on-disk directory: a reaped epoch is never
			// reopened (re-acquisition gets a fresh epoch number and
			// directory), so leaving it would leak disk on churn.
			if db.dir != "" {
				if err := os.RemoveAll(db.dir); err != nil {
					level.Warn(r.logger).Log("msg", "removing reaped frozen epoch dir",
						"user", tenant, "partition", ep.partitionID, "epoch", ep.epoch, "dir", db.dir, "err", err)
				}
			}
		}
		level.Info(r.logger).Log("msg", "readcache: frozen epoch reaped",
			"partition", ep.partitionID, "epoch", ep.epoch, "maxT", ep.maxT)
	}
}
