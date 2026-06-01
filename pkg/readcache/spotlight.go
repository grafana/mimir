// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

// readcacheSpotlight intervals. The cadence is intentionally
// looser than the rebalancer's because readcache observations are
// already point-in-time snapshots of per-(partition, range) EWMA
// state — emitting too frequently just inflates the log volume
// without adding signal.
const (
	readcacheSpotlightPollInterval = 10 * time.Second
	readcacheSpotlightEmitInterval = 30 * time.Second
	readcacheSpotlightPollTimeout  = 2 * time.Second
)

// readcacheSpotlightTracker holds the readcache-side cache of the
// rebalancer's spotlight set. Unlike the distributor tracker there
// is no per-write accumulator: emit walks the per-partition range
// state on each tick and projects observations for any range that
// overlaps any spotlight. The expensive bookkeeping (EWMA, series
// counts, example labels) is already maintained by the existing
// partition_ranges code path; we just intersect with the spotlight
// set and log.
type readcacheSpotlightTracker struct {
	mu         sync.RWMutex
	spotlights []rebalancer.SpotlightedRange
}

func newReadcacheSpotlightTracker() *readcacheSpotlightTracker {
	return &readcacheSpotlightTracker{}
}

func (t *readcacheSpotlightTracker) snapshot() []rebalancer.SpotlightedRange {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.spotlights
}

func (t *readcacheSpotlightTracker) setSpotlights(s []rebalancer.SpotlightedRange) {
	t.mu.Lock()
	t.spotlights = s
	t.mu.Unlock()
}

// runSpotlightLoop polls the rebalancer for the active spotlight
// set and periodically emits per-(spotlight, partition, range)
// observations. Exits when ctx is cancelled. No-op if the
// rebalancer connection is nil (cold start / tests).
func (r *Readcache) runSpotlightLoop(ctx context.Context) {
	if r.rebalancerConn == nil {
		return
	}
	cli := rebalancer.NewNautilusRebalancerClient(r.rebalancerConn)

	pollT := time.NewTicker(readcacheSpotlightPollInterval)
	emitT := time.NewTicker(readcacheSpotlightEmitInterval)
	defer pollT.Stop()
	defer emitT.Stop()

	r.pollSpotlights(ctx, cli)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollT.C:
			r.pollSpotlights(ctx, cli)
		case <-emitT.C:
			r.emitSpotlightObservations()
		}
	}
}

// pollSpotlights issues one GetSpotlightedRanges call and updates
// the cached set. Failures keep the previous cache (better stale
// than empty during transient rebalancer outages).
func (r *Readcache) pollSpotlights(ctx context.Context, cli rebalancer.NautilusRebalancerClient) {
	rpcCtx, cancel := context.WithTimeout(ctx, readcacheSpotlightPollTimeout)
	defer cancel()
	resp, err := cli.GetSpotlightedRanges(rpcCtx, &rebalancer.GetSpotlightedRangesRequest{})
	if err != nil {
		level.Debug(r.logger).Log("msg", "nautilus spotlight: poll failed", "err", err)
		return
	}
	r.spotlights.setSpotlights(resp.Ranges)
}

// emitSpotlightObservations walks every owned partition and, for
// each (current ∪ historical) range that overlaps any spotlighted
// range, emits one log line per (spotlight, partition, range)
// triple. The line includes the partition-local sample rate, head
// series count, and a representative example label string so an
// operator can quickly see what kind of traffic each spotlight is
// catching.
//
// We project the data we *already maintain* rather than tracking
// extra per-spotlight state in the ingest hot path: this keeps the
// observability layer additive (it adds reads but never extra writes
// on the per-sample path).
func (r *Readcache) emitSpotlightObservations() {
	spots := r.spotlights.snapshot()
	if len(spots) == 0 {
		return
	}

	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()
	if len(parts) == 0 {
		return
	}

	for _, p := range parts {
		current, historical := p.ranges.adminSnapshot()
		emitReadcacheSpotlightForPartition(r.logger, r.cfg.InstanceID, p.partitionID, spots, current, "current")
		emitReadcacheSpotlightForPartition(r.logger, r.cfg.InstanceID, p.partitionID, spots, historical, "historical")
	}
}

// emitReadcacheSpotlightForPartition emits one log line per
// overlap between rows and spots. rows is either current or
// historical hash-range counts for partitionID. lifecycle is
// "current" or "historical" and is emitted verbatim so consumers
// can distinguish active routing from residue on a partition that
// previously owned the range.
//
// Split out as a free function so the logic is testable without
// constructing a full *Readcache.
func emitReadcacheSpotlightForPartition(
	logger log.Logger,
	instanceID string,
	partitionID int32,
	spots []rebalancer.SpotlightedRange,
	rows []hashRangeCount,
	lifecycle string,
) {
	for _, row := range rows {
		for _, sp := range spots {
			if !hashRangesOverlapBounds(row.Range, sp.Lo, sp.Hi) {
				continue
			}
			level.Info(logger).Log(
				"msg", "nautilus spotlight: readcache observation",
				"spotlight_id", sp.TraceId,
				"reason", sp.Reason,
				"spotlight_lo", sp.Lo,
				"spotlight_hi", sp.Hi,
				"from_partition", sp.FromPartitionId,
				"to_partition", sp.ToPartitionId,
				"observed_partition", partitionID,
				"observed_range_lo", row.Range.Lo,
				"observed_range_hi", row.Range.Hi,
				"lifecycle", lifecycle,
				"sample_rate", row.SampleRate,
				"head_series", row.Count,
				"example_series", row.Example,
				"instance_id", instanceID,
			)
		}
	}
}

// hashRangesOverlapBounds reports whether [r.Lo, r.Hi] (closed) and
// [lo, hi] (closed) share at least one hash value. Mirrors the
// rebalancer's hashRangesOverlap so consumers can compare ranges
// independently of either codebase's HashRange type.
func hashRangesOverlapBounds(r assignment.HashRange, lo, hi uint32) bool {
	return r.Lo <= hi && lo <= r.Hi
}
