// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// reconstructRound dispatches to the readcache or ingester
// reconstruction path. Both produce a fresh assignment by walking
// what the owners locally remember owning. Returns nil to signal a
// fall-back to FineEvenSplit.
func (r *Rebalancer) reconstructRound(ctx context.Context, activePartitions []int32) *assignment.Assignment {
	if r.readcachePool != nil {
		return r.reconstructAssignmentFromReadcache(ctx, activePartitions)
	}
	return r.reconstructAssignment(ctx, activePartitions)
}

// reconstructAssignmentFromReadcache mirrors reconstructAssignment
// but iterates the readcache fleet (via the ring + pool) instead of
// the ingester ring. The partition->owner mapping comes from the
// readcache assignment log; readcache pods not currently mapped to
// any partition (cold-start with empty log) are ignored, mirroring
// the ingester path's "unmapped" handling.
func (r *Rebalancer) reconstructAssignmentFromReadcache(ctx context.Context, activePartitions []int32) *assignment.Assignment {
	instances, err := r.readcachePool.healthyInstances()
	if err != nil {
		level.Warn(r.logger).Log("msg", "reconstructAssignmentFromReadcache: ring lookup failed", "err", err)
		return nil
	}
	if len(instances) == 0 {
		return nil
	}

	// Build instanceID -> the FIRST partition that readcache owns.
	// Multi-partition ownership: a readcache pod may own several
	// partitions in the log, and we'll process each separately by
	// scanning the log per-pod.
	type partitionOwnership struct {
		instanceID string
		partition  int32
	}
	var ownerships []partitionOwnership
	now := time.Now()
	for _, entry := range r.readcacheStore.snapshot() {
		if !entry.ActiveAt(now) {
			continue
		}
		ownerships = append(ownerships, partitionOwnership{instanceID: entry.InstanceID, partition: entry.PartitionID})
	}
	if len(ownerships) == 0 {
		level.Info(r.logger).Log("msg", "reconstructAssignmentFromReadcache: empty log, falling back to even split")
		return nil
	}

	// Resolve to ring entries so we can dial. We need the address,
	// and we drop ownership entries whose readcache isn't currently
	// in the ring (e.g. drained pod whose log lease hasn't expired
	// yet).
	idToInst := make(map[string]ring.InstanceDesc, len(instances))
	for _, inst := range instances {
		idToInst[inst.Id] = inst
	}

	reports := make([][]reportedEntry, len(ownerships))
	var ok, failed, unmapped atomic.Int32

	_ = concurrency.ForEachJob(ctx, len(ownerships), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		o := ownerships[idx]

		inst, known := idToInst[o.instanceID]
		if !known {
			unmapped.Add(1)
			return nil
		}

		c, err := r.readcachePool.clientFor(jobCtx, inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "reconstructAssignmentFromReadcache: client error", "readcache", inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		resp, err := c.GetHashRanges(callCtx, &ingester_client.GetHashRangesRequest{})
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "reconstructAssignmentFromReadcache: GetHashRanges RPC failed", "readcache", inst.Addr, "err", err)
			return nil
		}

		entries := make([]reportedEntry, len(resp.Ranges))
		for i, hr := range resp.Ranges {
			entries[i] = reportedEntry{
				partitionID: o.partition,
				hr:          assignment.HashRange{Lo: hr.Lo, Hi: hr.Hi},
			}
		}
		reports[idx] = entries
		ok.Add(1)
		return nil
	})

	expected := int32(len(ownerships)) - unmapped.Load()
	if expected <= 0 {
		level.Info(r.logger).Log("msg", "reconstructAssignmentFromReadcache: no readcaches mapped to active partitions, falling back to even split")
		return nil
	}
	if int64(ok.Load())*int64(reconstructionQuorumDen) < int64(expected)*int64(reconstructionQuorumNum) {
		level.Warn(r.logger).Log(
			"msg", "reconstructAssignmentFromReadcache: not enough readcaches responded, falling back to even split",
			"ownerships", len(ownerships),
			"unmapped", unmapped.Load(),
			"ok", ok.Load(),
			"failed", failed.Load(),
		)
		return nil
	}

	// Deduplicate (partitionID, range) pairs across owner reports.
	type pRange struct {
		pid int32
		hr  assignment.HashRange
	}
	seen := make(map[pRange]struct{})
	var merged []reportedEntry
	for _, list := range reports {
		for _, e := range list {
			k := pRange{pid: e.partitionID, hr: e.hr}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			merged = append(merged, e)
		}
	}
	if len(merged) == 0 {
		level.Info(r.logger).Log("msg", "reconstructAssignmentFromReadcache: no ranges reported, falling back to even split")
		return nil
	}

	sortReportedEntries(merged)
	entries := stitchReportedEntries(merged, activePartitions, r.logger)

	a := &assignment.Assignment{Entries: entries}
	if err := a.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "reconstructAssignmentFromReadcache: stitched assignment invalid, falling back to even split", "err", err)
		return nil
	}
	return a
}

// collectRoundStats picks the right stats source for this rebalance
// round. With the readcache pool wired (production path), it queries
// readcache pods through the ring; otherwise it falls back to the
// legacy ingester-pool path (used by tests and pre-readcache
// phase 1 deployments).
func (r *Rebalancer) collectRoundStats(ctx context.Context) ([]rangeRate, map[string]int64, map[int32]int64, map[int32]float64, map[string]float64, error) {
	if r.readcachePool != nil {
		return r.collectRatesFromReadcache(ctx)
	}
	rates, instanceTotals, partitionQuerySamples, unnamedPerInstance, err := r.collectRates(ctx)
	return rates, instanceTotals, nil, partitionQuerySamples, unnamedPerInstance, err
}

// pushRanges dispatches to the readcache or the ingester push path
// based on whether the readcache pool is wired.
func (r *Rebalancer) pushRanges(ctx context.Context, a *assignment.Assignment, at time.Time) {
	if r.readcachePool != nil {
		r.pushRangesToReadcache(ctx, a, at)
		return
	}
	r.pushRangesToIngesters(ctx, a)
}

// partitionLByPID computes per-partition head-series load. With the
// readcache pool wired, L comes from partitionTotals reported by
// HashRangeStats (one entry per Kafka partition per readcache pod);
// otherwise the helper falls back to partitionL backed by the ingester
// partition ring and per-instance totals.
func (r *Rebalancer) partitionLByPID(instanceTotals map[string]int64, partitionTotals map[int32]int64, pRing partitionRingView, activePartitions []int32, at time.Time) map[int32]int64 {
	if r.readcachePool == nil {
		return partitionL(instanceTotals, pRing, activePartitions)
	}

	out := make(map[int32]int64, len(activePartitions))
	for _, pid := range activePartitions {
		out[pid] = partitionTotals[pid]
	}
	return out
}

// collectRatesFromReadcache queries all healthy readcache pods (via
// the readcache ring) for per-range ingestion rates and per-instance
// totals. Returns the same shape as the legacy collectRates path so
// the slicer is agnostic to source.
//
// Source-of-truth contract:
//
//   - rates: per hash range, summed across the readcache pods that
//     report it. In single-owner-per-partition mode each range
//     appears on exactly one pod, so the sum reduces to a passthrough.
//   - instanceTotals: readcache instance ID -> sum of head series
//     across that pod's owned partitions. Used for observability;
//     partition-level L uses partitionTotals instead.
//   - partitionTotals: per-partition head series, max across pods
//     that reported each partition (normally exactly one owner).
//   - partitionQuerySamples: per-partition query-load EWMA, summed
//     across pods that report the partition.
//   - unnamedPerInstance: per-readcache unnamed query EWMA, surfaced
//     for observability but not fed into the slicer.
//
// On any per-pod failure the round continues with whatever the
// other pods returned; a single misbehaving readcache cannot block
// the rebalance round behind TCP timeouts (see
// Config.IngesterRPCTimeout).
func (r *Rebalancer) collectRatesFromReadcache(ctx context.Context) ([]rangeRate, map[string]int64, map[int32]int64, map[int32]float64, map[string]float64, error) {
	instances, err := r.readcachePool.healthyInstances()
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	type result struct {
		instanceID      string
		totalSeries     int64
		rates           []rangeRate
		partitionSeries []ingester_client.PartitionActiveSeries
		partitionLoad   []ingester_client.PartitionQueryLoad
		unnamedLoad     float64
	}

	results := make([]result, len(instances))
	var ok, failed atomic.Int32

	_ = concurrency.ForEachJob(ctx, len(instances), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		inst := instances[idx]

		c, err := r.readcachePool.clientFor(jobCtx, inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "failed to get client for readcache", "readcache", inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		resp, err := c.HashRangeStats(callCtx, &ingester_client.HashRangeStatsRequest{})
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "HashRangeStats RPC failed", "readcache", inst.Addr, "err", err)
			return nil
		}

		rates := make([]rangeRate, len(resp.Rates))
		for i, rate := range resp.Rates {
			rates[i] = rangeRate{
				hr:     assignment.HashRange{Lo: rate.Lo, Hi: rate.Hi},
				series: rate.ActiveSeries,
			}
		}
		results[idx] = result{
			instanceID:      inst.Id,
			totalSeries:     resp.TotalActiveSeries,
			rates:           rates,
			partitionSeries: resp.PartitionActiveSeries,
			partitionLoad:   resp.PartitionQueryLoads,
			unnamedLoad:     resp.UnnamedQuerySamplesEwma,
		}
		ok.Add(1)
		return nil
	})

	var all []rangeRate
	instanceTotals := make(map[string]int64, len(instances))
	partitionTotals := map[int32]int64{}
	partitionQuerySamples := map[int32]float64{}
	unnamedPerInstance := map[string]float64{}
	for _, res := range results {
		if res.instanceID == "" {
			continue
		}
		instanceTotals[res.instanceID] = res.totalSeries
		all = append(all, res.rates...)
		for _, p := range res.partitionSeries {
			if p.ActiveSeries > partitionTotals[p.PartitionId] {
				partitionTotals[p.PartitionId] = p.ActiveSeries
			}
		}
		for _, p := range res.partitionLoad {
			partitionQuerySamples[p.PartitionId] += p.SamplesEwma
		}
		if res.unnamedLoad > 0 {
			unnamedPerInstance[res.instanceID] = res.unnamedLoad
		}
	}

	level.Info(r.logger).Log("msg", "collected readcache stats", "healthy", len(instances), "ok", ok.Load(), "failed", failed.Load())
	return all, instanceTotals, partitionTotals, partitionQuerySamples, unnamedPerInstance, nil
}

// pushRangesToReadcache calls SetHashRanges on each readcache that
// owns at least one partition in the new assignment, sending only
// the hash ranges for the partitions that pod is the current owner
// of (per the readcache assignment log).
//
// This is the readcache analogue of pushRangesToIngesters. It runs
// only when the readcache pool is wired; without it, the rebalancer
// falls back to pushRangesToIngesters which targets the legacy
// ingester ring.
//
// Ownership is resolved at `at` (typically time.Now() for the round):
// only LogEntries whose [From, To) brackets `at` count. If a
// partition has no active owner at `at`, its ranges are skipped this
// round; the next round's slicer pass will assign one.
func (r *Rebalancer) pushRangesToReadcache(ctx context.Context, a *assignment.Assignment, at time.Time) {
	// Build partition -> readcache instance ID from the live
	// readcache log. Single-owner mode means each pid maps to one
	// instance; if multi-owner gets enabled later we'd push to all
	// owners.
	ownerByPartition := make(map[int32]string)
	for _, entry := range r.readcacheStore.snapshot() {
		if entry.ActiveAt(at) {
			ownerByPartition[entry.PartitionID] = entry.InstanceID
		}
	}

	// Group hash ranges per readcache instance ID by walking the
	// assignment entries.
	rangesByInstance := make(map[string][]ingester_client.HashRangeEntry)
	for _, e := range a.Entries {
		owner, ok := ownerByPartition[e.PartitionID]
		if !ok || owner == "" {
			continue
		}
		rangesByInstance[owner] = append(rangesByInstance[owner],
			ingester_client.HashRangeEntry{Lo: e.Range.Lo, Hi: e.Range.Hi})
	}
	if len(rangesByInstance) == 0 {
		return
	}

	// Resolve instance IDs to ring entries (need Addr for dialling).
	instances, err := r.readcachePool.healthyInstances()
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to get healthy readcaches for push", "err", err)
		return
	}
	idToInst := make(map[string]ring.InstanceDesc, len(instances))
	for _, inst := range instances {
		idToInst[inst.Id] = inst
	}

	type job struct {
		instanceID string
		inst       ring.InstanceDesc
		ranges     []ingester_client.HashRangeEntry
	}
	jobs := make([]job, 0, len(rangesByInstance))
	for id, rs := range rangesByInstance {
		inst, ok := idToInst[id]
		if !ok {
			// Owner referenced in the log isn't in the ring right
			// now — either it just left and the log hasn't been
			// reshuffled, or the ring is stale. Either way, skip;
			// the next round will reconcile.
			level.Warn(r.logger).Log("msg", "skipping SetHashRanges: owner not in readcache ring", "owner", id, "partitions_with_ranges", len(rs))
			continue
		}
		jobs = append(jobs, job{instanceID: id, inst: inst, ranges: rs})
	}

	var ok, failed atomic.Int32
	_ = concurrency.ForEachJob(ctx, len(jobs), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		j := jobs[idx]

		c, err := r.readcachePool.clientFor(jobCtx, j.inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "SetHashRanges: failed to get client", "readcache", j.inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		if _, err := c.SetHashRanges(callCtx, &ingester_client.SetHashRangesRequest{Ranges: j.ranges}); err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "SetHashRanges RPC failed", "readcache", j.inst.Addr, "err", err)
			return nil
		}
		ok.Add(1)
		return nil
	})

	level.Info(r.logger).Log("msg", "pushed ranges to readcache", "ok", ok.Load(), "failed", failed.Load())
}
