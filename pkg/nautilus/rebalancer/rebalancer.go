// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// Config holds the configuration for the nautilus ingestion rebalancer.
type Config struct {
	RebalanceInterval time.Duration `yaml:"rebalance_interval"`
	MovementBudget    float64       `yaml:"movement_budget"`

	// IngesterRPCTimeout bounds each individual HashRangeStats /
	// SetHashRanges call to a single ingester. RPCs are issued in
	// parallel, but a stuck ingester (e.g. mid-rollout, with a stale
	// pool connection) would otherwise tie up the whole round behind
	// TCP-level timeouts. Set to 0 to disable per-call timeouts (not
	// recommended in production).
	IngesterRPCTimeout time.Duration `yaml:"ingester_rpc_timeout"`

	// IngesterRPCConcurrency caps the number of in-flight ingester
	// RPCs per round. A value <= 0 means "one job per ingester"
	// (effectively unbounded). Tune down if the rebalancer pod has
	// limited file descriptors or you want gentler load on the pool.
	IngesterRPCConcurrency int `yaml:"ingester_rpc_concurrency"`

	// MoveCooldown is the minimum time a hash range must sit on its new
	// partition after a move before it (or any range overlapping its
	// former boundaries) is eligible to be moved again. Acts as a
	// per-range anti-flap guard; the partition-level CompactionInterval
	// budget handles aggregate churn.
	MoveCooldown time.Duration `yaml:"move_cooldown"`

	// CompactionInterval bounds the window over which recent moves off
	// a source partition count against its movable budget. Series moved
	// off an ingester remain in its TSDB head (contributing to reported
	// L_pid) until the next head compaction, so during this window the
	// slicer must discount its apparent "above mean" budget by what's
	// already been moved off. Should match the ingester's TSDB head
	// compaction interval (default 2h).
	CompactionInterval time.Duration `yaml:"compaction_interval"`
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.RebalanceInterval, prefix+"rebalance-interval", 60*time.Second, "How often the rebalancer runs.")
	f.Float64Var(&cfg.MovementBudget, prefix+"movement-budget", 0.09, "Maximum fraction of the hash space that can be moved per round.")
	f.DurationVar(&cfg.IngesterRPCTimeout, prefix+"ingester-rpc-timeout", 4*time.Second, "Per-call timeout for HashRangeStats and SetHashRanges RPCs to each ingester. Prevents one stuck pod (e.g. mid-rollout) from stalling the whole rebalance round. 0 disables.")
	f.IntVar(&cfg.IngesterRPCConcurrency, prefix+"ingester-rpc-concurrency", 10, "Maximum concurrent ingester RPCs per round. 0 means one per ingester (unbounded).")
	f.DurationVar(&cfg.MoveCooldown, prefix+"move-cooldown", 90*time.Second, "Minimum time between consecutive moves of the same hash range (or any range overlapping it). Per-range anti-flap guard complementing the compaction-interval partition-level budget. 0 disables.")
	f.DurationVar(&cfg.CompactionInterval, prefix+"compaction-interval", 2*time.Hour, "Window over which recent moves off a source partition count against its movable budget. Series moved off an ingester stay in its TSDB head (reported L_pid) until head compaction GCs them. Should match the ingester's TSDB head compaction interval. 0 disables the movable-budget guard (moves are only gated by MovementBudget and MoveCooldown).")
}

// Rebalancer is a Mimir module that periodically queries ingesters for
// per-hash-range ingestion rates, rebalances hash-range-to-partition
// assignments, pushes ownership to ingesters, and serves the
// assignment history to distributors.
type Rebalancer struct {
	services.Service

	cfg    Config
	logger log.Logger

	ingesterRing  ring.ReadRing
	pool          *ring_client.Pool
	partitionRing *ring.PartitionInstanceRing

	store assignmentStore
	admin adminState

	// prevInstanceRanges tracks the last set of ranges pushed to each
	// ingester, keyed by instance ID, so we can log changes.
	prevInstanceRanges map[string][]ingester_client.HashRangeEntry

	// moveCooldowns records, for each hash range that was recently
	// moved, the wall-clock time at which it (and any range overlapping
	// its boundaries) becomes eligible to be moved again. Mutated only
	// by rebalance(), which runs single-threaded via TimerService.
	moveCooldowns map[assignment.HashRange]time.Time

	// recentMoves records, per *source* partition, all moves off that
	// partition within the current CompactionInterval window. Each
	// entry's series count counts against the partition's movable
	// budget until CompactionInterval elapses. Destinations do not need
	// cross-round state because a destination's L_pid updates within one
	// scrape interval as distributors route writes to it. Mutated only
	// by rebalance().
	recentMoves map[int32][]moveRecord
}

// moveRecord tracks one past move off a source partition during the
// CompactionInterval window. The source ingester's reported
// TotalActiveSeries does NOT drop until its next TSDB head compaction
// GCs the moved series, so during this window the slicer must discount
// the partition's apparent "above mean" budget by the sum of
// outstanding moveRecord.series.
type moveRecord struct {
	hr     assignment.HashRange
	series int64
	at     time.Time
}

// New creates and returns a new Rebalancer.
func New(cfg Config, ingesterRing ring.ReadRing, pool *ring_client.Pool, partitionRing *ring.PartitionInstanceRing, logger log.Logger) *Rebalancer {
	r := &Rebalancer{
		cfg:           cfg,
		logger:        logger,
		ingesterRing:  ingesterRing,
		pool:          pool,
		partitionRing: partitionRing,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
		recentMoves:   make(map[int32][]moveRecord),
	}

	r.Service = services.NewTimerService(cfg.RebalanceInterval, r.starting, r.rebalance, nil)
	return r
}

// GetAssignments implements NautilusRebalancerServer.
func (r *Rebalancer) GetAssignments(_ context.Context, _ *GetAssignmentsRequest) (*GetAssignmentsResponse, error) {
	snap := r.store.snapshot()
	return TimedAssignmentSetToProto(&snap), nil
}

func (r *Rebalancer) starting(_ context.Context) error {
	level.Info(r.logger).Log("msg", "nautilus rebalancer starting")
	return nil
}

func (r *Rebalancer) rebalance(ctx context.Context) error {
	// The ingester client pool requires an org ID in the context
	// (ClientUserHeaderInterceptor). Inject a synthetic one since
	// rebalancer RPCs are not tenant-scoped.
	ctx = user.InjectOrgID(ctx, "nautilus-rebalancer")

	pRing := r.partitionRing.PartitionRing()
	activePartitions := pRing.ActivePartitionIDs()
	if len(activePartitions) == 0 {
		level.Warn(r.logger).Log("msg", "no active partitions, skipping rebalance")
		return nil
	}

	current := r.store.latest()
	if current == nil {
		// Cold start: try to reconstruct the assignment from whatever
		// each ingester locally remembers (via GetHashRanges). On a
		// rolling rebalancer restart this preserves rebalanced state;
		// on a truly-cold cluster it falls back to FineEvenSplit.
		current = r.reconstructAssignment(ctx, activePartitions)
		if current == nil {
			current = assignment.FineEvenSplit(activePartitions, initialSlicesPerPartition)
			level.Info(r.logger).Log("msg", "initialized assignment with fine even split",
				"partitions", len(activePartitions),
				"slices_per_partition", initialSlicesPerPartition,
				"total_slices", len(current.Entries))
		} else {
			level.Info(r.logger).Log("msg", "initialized assignment from ingester reports",
				"partitions", len(activePartitions),
				"total_entries", len(current.Entries))
		}
		r.store.add(time.Now(), current)
		r.pushRangesToIngesters(ctx, current)
		return nil
	}

	rates, instanceTotals, err := r.collectRates(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to collect ingester rates", "err", err)
		return nil
	}

	now := time.Now()
	r.pruneExpiredCooldowns(now)
	r.pruneRecentMoves(now)

	// Compute per-partition L (head-series) from ingester totals, taking
	// the max over owners in each partition's replica set. Max rather
	// than mean captures worst-case memory pressure across replicas.
	partitionLByPID := partitionL(instanceTotals, pRing, activePartitions)

	lm := buildLoadMap(rates)
	r.admin.setLastStats(lm, partitionLByPID, r.recentMoves, activePartitions)

	// Snapshot pre-slicer state for the trace. Done BEFORE runSlicer
	// mutates anything (it gets a fresh assignment, but the cooldown
	// map and recentMoves map are inspected as-of `now` and we want the
	// snapshot to reflect what the slicer actually saw).
	startEntries := append([]assignment.Entry(nil), current.Entries...)
	cooldownsSnapshot := cooldownsToWire(r.moveCooldowns)
	recentMovesSnapshot := recentMovesToWire(r.recentMoves)

	newAssignment, actions := r.runSlicer(current, rates, partitionLByPID, r.recentMoves, activePartitions, now)
	if err := newAssignment.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "generated invalid assignment", "err", err)
		return nil
	}

	r.recordMoveCooldowns(now, actions)
	r.recordRecentMoves(now, actions, lm)

	r.store.add(now, newAssignment)
	r.pushRangesToIngesters(ctx, newAssignment)

	// Compute round summary stats using L (memory series) so the admin
	// view tracks the same quantity the slicer balances.
	var totalL, maxL, minL int64
	minL = math.MaxInt64
	for _, pid := range activePartitions {
		l := partitionLByPID[pid]
		totalL += l
		if l > maxL {
			maxL = l
		}
		if l < minL {
			minL = l
		}
	}
	if minL == math.MaxInt64 {
		minL = 0
	}
	var meanL int64
	if len(activePartitions) > 0 {
		meanL = totalL / int64(len(activePartitions))
	}
	// Per the Slicer paper, load imbalance is defined as max / mean.
	// 1.0 means perfectly balanced; values above 1.0 indicate the
	// hottest partition exceeds the average by that factor.
	imbalance := 0.0
	if meanL > 0 {
		imbalance = float64(maxL) / float64(meanL)
	}
	movedFraction := 0.0
	hashSpaceSize := float64(uint64(math.MaxUint32) + 1)
	for _, a := range actions {
		if a.Kind == ActionMove || a.Kind == ActionReassign {
			movedFraction += float64(a.Range.Size()) / hashSpaceSize
		}
	}

	round := RoundLog{
		Time:           now,
		TotalL:         totalL,
		MeanL:          meanL,
		MaxL:           maxL,
		MinL:           minL,
		ImbalanceRatio: imbalance,
		NumEntries:     len(newAssignment.Entries),
		NumPartitions:  len(activePartitions),
		MovedFraction:  movedFraction,
		Actions:        actions,
	}

	r.admin.addTrace(Trace{
		SlicerVersion:    SlicerVersion,
		Round:            round,
		Now:              now,
		Start:            startEntries,
		Rates:            ratesToWire(rates),
		PartitionL:       partitionLByPID,
		RecentMoves:      recentMovesSnapshot,
		ActivePartitions: append([]int32(nil), activePartitions...),
		Cooldowns:        cooldownsSnapshot,
		Config: ConfigSnapshot{
			MovementBudget:     r.cfg.MovementBudget,
			MoveCooldown:       r.cfg.MoveCooldown,
			CompactionInterval: r.cfg.CompactionInterval,
		},
		End: append([]assignment.Entry(nil), newAssignment.Entries...),
	})

	level.Info(r.logger).Log("msg", "rebalance complete", "entries", len(newAssignment.Entries), "total_assignments", len(r.store.snapshot().Assignments))
	return nil
}

// collectRates queries all ingesters for per-range ingestion rates and
// returns a global view: one rate per reported hash range. Since
// ingesters only report rates for ranges they own, the results are
// already partitioned; we aggregate into a single map keyed by range.
//
// RPCs are issued in parallel with a per-call timeout (see
// Config.IngesterRPCTimeout) so a single stuck ingester (e.g. mid-
// rollout, with a stale pool connection) cannot block the whole
// round behind TCP-level timeouts.
//
// The second return value is keyed by ingester instance ID and holds
// the total in-memory series count on that ingester (across all
// tenants and all hash buckets). It is the L_i signal that feeds into
// partitionL and the movable-budget calculation.
func (r *Rebalancer) collectRates(ctx context.Context) ([]rangeRate, map[string]int64, error) {
	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		return nil, nil, err
	}

	type result struct {
		instanceID  string
		totalSeries int64
		rates       []rangeRate
	}

	results := make([]result, len(instances.Instances))
	var ok, failed atomic.Int32

	_ = concurrency.ForEachJob(ctx, len(instances.Instances), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		inst := instances.Instances[idx]

		c, err := r.pool.GetClientForInstance(inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "failed to get client for ingester", "ingester", inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		resp, err := c.(ingester_client.IngesterClient).HashRangeStats(callCtx, &ingester_client.HashRangeStatsRequest{})
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "HashRangeStats RPC failed", "ingester", inst.Addr, "err", err)
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
			instanceID:  inst.GetId(),
			totalSeries: resp.TotalActiveSeries,
			rates:       rates,
		}
		ok.Add(1)
		return nil
	})

	var all []rangeRate
	instanceTotals := make(map[string]int64, len(instances.Instances))
	for _, res := range results {
		if res.instanceID == "" {
			continue
		}
		instanceTotals[res.instanceID] = res.totalSeries
		all = append(all, res.rates...)
	}

	level.Info(r.logger).Log("msg", "collected ingester stats", "healthy", len(instances.Instances), "ok", ok.Load(), "failed", failed.Load())
	return all, instanceTotals, nil
}

// withRPCTimeout wraps ctx with the configured per-call timeout. When
// the timeout is disabled (<=0), returns the parent context and a
// no-op cancel.
func (r *Rebalancer) withRPCTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.cfg.IngesterRPCTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.cfg.IngesterRPCTimeout)
}

// reconstructionQuorumNum / reconstructionQuorumDen together express the
// minimum fraction of expected ingesters that must successfully return
// their hash ranges for reconstruction to be trusted. Below this, we
// fall back to FineEvenSplit rather than take a destructive action
// (e.g. blowing up a range's ownership) on a minority view.
const (
	reconstructionQuorumNum = 1
	reconstructionQuorumDen = 2
)

// reconstructAssignment queries all healthy ingesters for their
// currently-owned hash ranges (via GetHashRanges) and reassembles a
// fleet-wide assignment by mapping each reported range to the
// partition its owner ingester belongs to. This lets the rebalancer
// resume from whatever assignment is actually in effect on the fleet,
// rather than destroying rebalanced state with a fresh even split on
// cold start.
//
// Policy:
//   - First-replica-wins on any overlap between different partitions:
//     whichever partition's range we encounter first (in sorted-Lo
//     order) keeps the overlapping hash space; any later range that
//     overlaps is truncated or dropped. The next pushRangesToIngesters
//     will reconcile the losing replica's view.
//   - Gaps (holes in [0, MaxUint32] after merging) are filled verbatim:
//     one entry per gap, round-robin across active partitions. The
//     slicer may subsequently split the filler as part of normal
//     Phase 4 (split hot slices) if it ends up carrying load.
//   - If fewer than reconstructionQuorumNum/reconstructionQuorumDen of
//     the expected ingesters respond, or if no ingester reports any
//     ranges (truly-cold cluster), returns nil so the caller falls
//     back to FineEvenSplit.
//   - Any ingester whose instance ID doesn't map to an active
//     partition (e.g. mid-scale-down) is silently ignored; its ranges
//     become gaps that get refilled from the active partition set.
func (r *Rebalancer) reconstructAssignment(ctx context.Context, activePartitions []int32) *assignment.Assignment {
	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		level.Warn(r.logger).Log("msg", "reconstructAssignment: failed to get healthy ingesters", "err", err)
		return nil
	}
	if len(instances.Instances) == 0 {
		return nil
	}

	pRing := r.partitionRing.PartitionRing()

	// Build instanceID -> partitionID from active partitions only. An
	// ingester not in this map is either unassigned or owns an
	// inactive partition; either way, we ignore its reported ranges.
	instanceToPartition := make(map[string]int32)
	for _, pid := range activePartitions {
		for _, ownerID := range pRing.PartitionOwnerIDs(pid) {
			instanceToPartition[ownerID] = pid
		}
	}

	reports := make([][]reportedEntry, len(instances.Instances))
	var ok, failed, unmapped atomic.Int32

	_ = concurrency.ForEachJob(ctx, len(instances.Instances), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		inst := instances.Instances[idx]

		pid, known := instanceToPartition[inst.GetId()]
		if !known {
			unmapped.Add(1)
			return nil
		}

		c, err := r.pool.GetClientForInstance(inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "reconstructAssignment: failed to get client", "ingester", inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		resp, err := c.(ingester_client.IngesterClient).GetHashRanges(callCtx, &ingester_client.GetHashRangesRequest{})
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "reconstructAssignment: GetHashRanges RPC failed", "ingester", inst.Addr, "err", err)
			return nil
		}

		entries := make([]reportedEntry, len(resp.Ranges))
		for i, hr := range resp.Ranges {
			entries[i] = reportedEntry{
				partitionID: pid,
				hr:          assignment.HashRange{Lo: hr.Lo, Hi: hr.Hi},
			}
		}
		reports[idx] = entries
		ok.Add(1)
		return nil
	})

	expected := int32(len(instances.Instances)) - unmapped.Load()
	if expected <= 0 {
		level.Info(r.logger).Log("msg", "reconstructAssignment: no ingesters mapped to active partitions, falling back to even split")
		return nil
	}
	// Quorum: ok.Load() / expected >= num/den.
	if int64(ok.Load())*int64(reconstructionQuorumDen) < int64(expected)*int64(reconstructionQuorumNum) {
		level.Warn(r.logger).Log(
			"msg", "reconstructAssignment: not enough ingesters responded, falling back to even split",
			"healthy", len(instances.Instances),
			"unmapped", unmapped.Load(),
			"ok", ok.Load(),
			"failed", failed.Load(),
			"quorum_num", reconstructionQuorumNum,
			"quorum_den", reconstructionQuorumDen,
		)
		return nil
	}

	// Deduplicate (partitionID, range) pairs — different replicas of
	// the same partition will redundantly report the same ranges.
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
		level.Info(r.logger).Log("msg", "reconstructAssignment: no ranges reported, falling back to even split")
		return nil
	}

	// Sort by (Lo, partitionID) so first-replica-wins has a stable,
	// deterministic ordering and tests are reproducible.
	sortReportedEntries(merged)

	entries := stitchReportedEntries(merged, activePartitions, r.logger)

	a := &assignment.Assignment{Entries: entries}
	if err := a.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "reconstructAssignment: stitched assignment invalid, falling back to even split", "err", err)
		return nil
	}

	level.Info(r.logger).Log(
		"msg", "reconstructAssignment: reassembled assignment from healthy ingesters",
		"healthy", len(instances.Instances),
		"unmapped", unmapped.Load(),
		"ok", ok.Load(),
		"failed", failed.Load(),
		"reported_entries", len(merged),
		"final_entries", len(entries),
	)
	return a
}

// reportedEntry is a (partition, range) pair collected from a
// GetHashRanges response during assignment reconstruction.
type reportedEntry struct {
	partitionID int32
	hr          assignment.HashRange
}

// sortReportedEntries sorts reported entries ascending by (Lo,
// partitionID). Used both in production (reconstructAssignment) and in
// tests to establish stitchReportedEntries' precondition.
func sortReportedEntries(entries []reportedEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].hr.Lo != entries[j].hr.Lo {
			return entries[i].hr.Lo < entries[j].hr.Lo
		}
		return entries[i].partitionID < entries[j].partitionID
	})
}

// stitchReportedEntries walks reported (partition, range) entries in
// sorted (Lo, partitionID) order and produces a sequence of entries
// covering [0, math.MaxUint32] with no gaps and no overlaps. Overlaps
// between different partitions are resolved first-replica-wins: the
// earlier entry keeps its coverage and any later entry is truncated
// to [cursor, Hi] or dropped entirely if fully covered. Gaps (including
// a leading gap before the first reported Lo and a trailing gap after
// the last reported Hi) are filled with single entries round-robin
// across activePartitions.
//
// Precondition: sorted is sorted ascending by (hr.Lo, partitionID).
// Returns entries that are sorted and non-overlapping, with the first
// Lo == 0 and the last Hi == math.MaxUint32, i.e. ready for
// Assignment.Validate.
func stitchReportedEntries(sorted []reportedEntry, activePartitions []int32, logger log.Logger) []assignment.Entry {
	out := make([]assignment.Entry, 0, len(sorted)+1)
	// gapRR is the round-robin cursor for assigning gap-filler entries
	// to active partitions. Advances only when a filler is emitted.
	gapRR := 0
	emitGap := func(lo, hi uint32) {
		if len(activePartitions) == 0 {
			return
		}
		pid := activePartitions[gapRR%len(activePartitions)]
		gapRR++
		out = append(out, assignment.Entry{
			Range:       assignment.HashRange{Lo: lo, Hi: hi},
			PartitionID: pid,
		})
	}

	// cursor tracks the next uncovered hash. Tracked as uint64 so we
	// can represent "past MaxUint32" (i.e. fully covered) without
	// overflow.
	var cursor uint64
	for _, e := range sorted {
		lo := uint64(e.hr.Lo)
		hi := uint64(e.hr.Hi)

		if lo > cursor {
			// Gap before this entry.
			emitGap(uint32(cursor), uint32(lo-1))
			cursor = lo
		}

		if hi < cursor {
			// Entry is entirely behind the cursor; first-replica
			// already won this span.
			level.Warn(logger).Log(
				"msg", "reconstructAssignment: dropping overlapped range",
				"partition", e.partitionID,
				"lo", e.hr.Lo,
				"hi", e.hr.Hi,
				"cursor", cursor,
			)
			continue
		}

		if lo < cursor {
			// Partial overlap: truncate to [cursor, hi].
			level.Warn(logger).Log(
				"msg", "reconstructAssignment: truncating overlapped range",
				"partition", e.partitionID,
				"orig_lo", e.hr.Lo,
				"orig_hi", e.hr.Hi,
				"truncated_lo", cursor,
			)
			lo = cursor
		}

		out = append(out, assignment.Entry{
			Range:       assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)},
			PartitionID: e.partitionID,
		})
		cursor = hi + 1
	}

	// Trailing gap up to MaxUint32, if any.
	if cursor <= uint64(math.MaxUint32) {
		emitGap(uint32(cursor), math.MaxUint32)
	}

	return out
}

// partitionL returns the per-partition L (TSDB head series) value,
// derived from each partition's owner ingesters. For replicated
// partitions (typically one owner per zone) the value is the max over
// owners — i.e., the worst-case memory pressure across replicas. The
// rebalancer balances on worst-case to keep any single ingester from
// approaching OOM, even if other zones are cooler.
//
// Returns a map keyed by partition ID. Partitions with no healthy
// owner in instanceTotals map to zero.
func partitionL(instanceTotals map[string]int64, pRing partitionRingView, activePartitions []int32) map[int32]int64 {
	if pRing == nil {
		return nil
	}
	m := make(map[int32]int64, len(activePartitions))
	for _, pid := range activePartitions {
		owners := pRing.PartitionOwnerIDs(pid)
		var worst int64
		for _, id := range owners {
			if v, ok := instanceTotals[id]; ok && v > worst {
				worst = v
			}
		}
		m[pid] = worst
	}
	return m
}

// partitionRingView is the subset of the partition ring API that the
// partitionL helper needs. Defined as an interface so tests can
// inject a stub without spinning up a full PartitionRing.
type partitionRingView interface {
	PartitionOwnerIDs(int32) []string
}

// pushRangesToIngesters calls SetHashRanges on each ingester with
// only the hash ranges belonging to the partitions that ingester owns.
func (r *Rebalancer) pushRangesToIngesters(ctx context.Context, a *assignment.Assignment) {
	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to get healthy ingesters for push", "err", err)
		return
	}

	pRing := r.partitionRing.PartitionRing()

	// Build instance ID → address lookup from the ingester ring.
	idToInst := make(map[string]ring.InstanceDesc, len(instances.Instances))
	for _, inst := range instances.Instances {
		idToInst[inst.GetId()] = inst
	}

	// Build partition → hash ranges from the assignment.
	partitionRanges := make(map[int32][]ingester_client.HashRangeEntry)
	for _, e := range a.Entries {
		partitionRanges[e.PartitionID] = append(partitionRanges[e.PartitionID],
			ingester_client.HashRangeEntry{Lo: e.Range.Lo, Hi: e.Range.Hi})
	}

	// For each active partition, find its owner ingester(s) and collect
	// the hash ranges that should be sent to each.
	instanceRanges := make(map[string][]ingester_client.HashRangeEntry)
	for _, pid := range pRing.ActivePartitionIDs() {
		owners := pRing.PartitionOwnerIDs(pid)
		ranges := partitionRanges[pid]
		for _, ownerID := range owners {
			instanceRanges[ownerID] = append(instanceRanges[ownerID], ranges...)
		}
	}

	// Flatten the per-instance work into an indexed slice so we can
	// fan out the SetHashRanges RPCs in parallel. Each job is bounded
	// by the configured per-call timeout; one stuck pod can no longer
	// stall the whole round behind TCP-level timeouts.
	type job struct {
		instanceID string
		inst       ring.InstanceDesc
		ranges     []ingester_client.HashRangeEntry
	}
	jobs := make([]job, 0, len(instanceRanges))
	for instanceID, ranges := range instanceRanges {
		inst, ok := idToInst[instanceID]
		if !ok {
			continue
		}

		prev := r.prevInstanceRanges[instanceID]
		added, removed, changed := diffRanges(prev, ranges)
		if changed {
			prevCoverage := hashSpaceCoverage(prev)
			newCoverage := hashSpaceCoverage(ranges)
			level.Info(r.logger).Log(
				"msg", "ingester assignment changed",
				"ingester", inst.Addr,
				"instance", instanceID,
				"prev_ranges", len(prev),
				"new_ranges", len(ranges),
				"added", added,
				"removed", removed,
				"prev_hash_space_pct", fmt.Sprintf("%.2f", prevCoverage*100),
				"new_hash_space_pct", fmt.Sprintf("%.2f", newCoverage*100),
			)
		}

		jobs = append(jobs, job{instanceID: instanceID, inst: inst, ranges: ranges})
	}

	var ok, failed atomic.Int32
	_ = concurrency.ForEachJob(ctx, len(jobs), r.cfg.IngesterRPCConcurrency, func(jobCtx context.Context, idx int) error {
		j := jobs[idx]

		c, err := r.pool.GetClientForInstance(j.inst)
		if err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "failed to get client for ingester", "ingester", j.inst.Addr, "err", err)
			return nil
		}

		callCtx, cancel := r.withRPCTimeout(jobCtx)
		defer cancel()

		if _, err := c.(ingester_client.IngesterClient).SetHashRanges(callCtx, &ingester_client.SetHashRangesRequest{Ranges: j.ranges}); err != nil {
			failed.Add(1)
			level.Warn(r.logger).Log("msg", "SetHashRanges RPC failed", "ingester", j.inst.Addr, "err", err)
			return nil
		}
		ok.Add(1)
		return nil
	})

	level.Info(r.logger).Log("msg", "pushed ranges to ingesters", "ok", ok.Load(), "failed", failed.Load())
	r.prevInstanceRanges = instanceRanges
}

const (
	// initialSlicesPerPartition is the number of sub-ranges each
	// partition gets in the first assignment. Starting fine gives
	// the move step enough granularity on the first round.
	initialSlicesPerPartition = 64

	// minSlicesPerPartition is the floor below which merging stops.
	// Matches Slicer paper's "50 slices per task" guideline.
	minSlicesPerPartition = 50

	// maxSlicesPerPartition is the ceiling above which splitting
	// stops. Matches Slicer paper's "150 slices per task" guideline.
	maxSlicesPerPartition = 150

	// mergeChurnBudget is the max fraction of keyspace that merging
	// may move (Slicer paper: 1%).
	mergeChurnBudget = 0.01
)

// rangeRate is one entry from an ingester's HashRangeStats response.
// Per the reframed load model only the per-range in-memory TSDB head
// series count (R_r) feeds the slicer.
type rangeRate struct {
	hr     assignment.HashRange
	series int64
}

// rangeLoad carries a single assignment entry alongside its cost
// metrics (head series count and the equivalent float used by Phase 2
// /4 scoring helpers). `load` is defined as float64(series) so Phase
// 2's merge score and Phase 4's split-threshold arithmetic don't need
// a separate scaling.
type rangeLoad struct {
	entry  assignment.Entry
	load   float64 // float view of series, used by merge/split scoring
	series int64   // raw in-memory TSDB head series count
}

// runSlicer implements the Slicer weighted-move algorithm (Adya et al.,
// OSDI'16, Section 4.4.1). Phases:
//
//  1. Reassign slices from inactive partitions.
//  2. Merge adjacent cold slices to defragment (cap: 1% churn, floor:
//     minSlicesPerPartition).
//  3. Weighted-move: greedily move slices from the hottest partition
//     (highest L_pid - recentMoves) to the coldest (lowest L_pid +
//     plannedAdded), gated by a per-source movable budget of
//     max(0, L_pid - meanL) - sumRecentMoves(pid). Budget:
//     cfg.MovementBudget (default 9% of hash space, hard global cap).
//  4. Split hot slices (>2× mean slice load) without changing
//     assignments. Cap: maxSlicesPerPartition.
//
// The `now` parameter is used to evaluate per-range move cooldowns
// (see Config.MoveCooldown). Pass time.Now() in production; tests can
// pass a deterministic value or the zero time to disable cooldown
// filtering.
//
// partitionLByPID provides L (TSDB head series) per partition, used for
// hot/cold selection and movable-budget computation. When nil or
// empty, the slicer falls back to computing per-partition load from
// the sum of per-range series (useful in unit tests that don't model
// the L_pid signal; production always provides it).
//
// recentMoves is the cross-round source-side budget state: moves off
// each source partition that are still within CompactionInterval, and
// therefore still counting against that partition's reported L_pid.
func (r *Rebalancer) runSlicer(
	current *assignment.Assignment,
	rates []rangeRate,
	partitionLByPID map[int32]int64,
	recentMoves map[int32][]moveRecord,
	activePartitions []int32,
	now time.Time,
) (*assignment.Assignment, []Action) {
	lm := buildLoadMap(rates)
	numPartitions := len(activePartitions)
	var actions []Action

	activeSet := make(map[int32]bool, numPartitions)
	for _, pid := range activePartitions {
		activeSet[pid] = true
	}

	// --- Phase 1: build entries, reassign inactive partitions ----------
	entries := make([]rangeLoad, len(current.Entries))
	rrIdx := 0
	for i, e := range current.Entries {
		series := lm.seriesAt(e.Range)
		entries[i] = rangeLoad{
			entry:  e,
			load:   float64(series),
			series: series,
		}
		if !activeSet[e.PartitionID] {
			newPID := activePartitions[rrIdx%numPartitions]
			actions = append(actions, Action{
				Kind:     ActionReassign,
				Range:    e.Range,
				FromPart: e.PartitionID,
				ToPart:   newPID,
				Detail:   fmt.Sprintf("inactive partition %d → %d", e.PartitionID, newPID),
			})
			entries[i].entry.PartitionID = newPID
			rrIdx++
		}
	}

	totalLoad := 0.0
	for _, rl := range entries {
		totalLoad += rl.load
	}
	targetLoad := totalLoad / float64(numPartitions)

	// --- Phase 2: merge adjacent cold slices (defragment) -------------
	if len(entries) > minSlicesPerPartition*numPartitions {
		meanSliceLoad := totalLoad / float64(len(entries))
		mergeMoveBudget := mergeChurnBudget * float64(uint64(math.MaxUint32)+1)
		var mergeActions []Action
		entries, mergeActions = mergeAdjacentCold(entries, meanSliceLoad, mergeMoveBudget, targetLoad, minSlicesPerPartition*numPartitions)
		actions = append(actions, mergeActions...)
	}

	// --- Phase 3: weighted-move using L_pid -----------------------------
	phase3Actions := r.runPhase3(entries, partitionLByPID, recentMoves, activePartitions, now)
	actions = append(actions, phase3Actions...)

	// --- Phase 4: split hot slices ----------------------------------------
	// Only split ranges on OVERLOADED partitions (>= target load).
	// Splitting ranges on cold partitions just adds fragmentation
	// without helping rebalancing.
	//
	// The split threshold is computed from ranges with non-zero load
	// to avoid the feedback loop where zero-load fragments from prior
	// splits drag down the mean and cause everything to look "hot".
	maxTotal := maxSlicesPerPartition * numPartitions
	if len(entries) < maxTotal {
		partitionLoads := computePartitionLoads(entries)

		var nonZeroCount int
		var nonZeroLoad float64
		for _, rl := range entries {
			if rl.load > 0 {
				nonZeroCount++
				nonZeroLoad += rl.load
			}
		}
		meanSliceLoad := nonZeroLoad / math.Max(float64(nonZeroCount), 1)
		splitThreshold := 2.0 * meanSliceLoad

		overloaded := make(map[int32]bool, numPartitions)
		for _, pid := range activePartitions {
			if partitionLoads[pid] >= targetLoad {
				overloaded[pid] = true
			}
		}

		var newEntries []rangeLoad
		for _, rl := range entries {
			if len(newEntries) >= maxTotal {
				newEntries = append(newEntries, rl)
				continue
			}
			if rl.load > splitThreshold && rl.entry.Range.Size() > 1 && overloaded[rl.entry.PartitionID] {
				mid := rl.entry.Range.Lo + uint32((uint64(rl.entry.Range.Hi)-uint64(rl.entry.Range.Lo))/2)
				left := assignment.HashRange{Lo: rl.entry.Range.Lo, Hi: mid}
				right := assignment.HashRange{Lo: mid + 1, Hi: rl.entry.Range.Hi}
				leftSeries := lm.seriesAt(left)
				rightSeries := lm.seriesAt(right)
				leftLoad := float64(leftSeries)
				rightLoad := float64(rightSeries)
				if leftLoad == 0 && rightLoad == 0 && rl.load > 0 {
					// Newly split sub-ranges have no per-range data yet.
					// Distribute the parent's load proportionally so the
					// next phase doesn't immediately re-merge them.
					leftFraction := float64(left.Size()) / float64(rl.entry.Range.Size())
					leftLoad = rl.load * leftFraction
					rightLoad = rl.load * (1 - leftFraction)
					leftSeries = int64(float64(rl.series) * leftFraction)
					rightSeries = rl.series - leftSeries
				}
				newEntries = append(newEntries,
					rangeLoad{entry: assignment.Entry{Range: left, PartitionID: rl.entry.PartitionID}, load: leftLoad, series: leftSeries},
					rangeLoad{entry: assignment.Entry{Range: right, PartitionID: rl.entry.PartitionID}, load: rightLoad, series: rightSeries},
				)
				actions = append(actions, Action{
					Kind:   ActionSplit,
					Range:  rl.entry.Range,
					ToPart: rl.entry.PartitionID,
					Detail: fmt.Sprintf("series=%d > threshold=%.0f, split on P%d", rl.series, splitThreshold, rl.entry.PartitionID),
				})
			} else {
				newEntries = append(newEntries, rl)
			}
		}
		entries = newEntries
	}

	// --- Build result --------------------------------------------------
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.Range.Lo < entries[j].entry.Range.Lo
	})

	result := &assignment.Assignment{
		Entries: make([]assignment.Entry, len(entries)),
	}
	for i, rl := range entries {
		result.Entries[i] = rl.entry
	}
	return result, actions
}

// runPhase3 runs the weighted-move phase and appends any resulting
// moves to actions. It mutates `entries` in place (updating PartitionID
// for moved ranges). The source/destination asymmetry is handled as
// follows:
//
//   - Source side: recentMoves (cross-round) plus moves accumulated in
//     this round's loop are summed into sumRecentMoves(pid). The
//     effective source L is `L_pid - sumRecentMoves(pid)`, the
//     partition's projected head-series count once the in-flight moves
//     compact away. A partition with exhausted budget has effective L
//     at or below meanL and is not selected as hot.
//
//   - Destination side: a within-round plannedAdded[pid] inflates the
//     effective cold L so the loop spreads moves across multiple cold
//     partitions rather than stacking on one. No cross-round
//     destination state: by the next round, the destination's L_pid
//     already reflects writes routed to it (within one scrape
//     interval), so there's nothing to carry forward.
//
// When partitionLByPID is nil/empty the slicer falls back to a
// per-range-sum approximation (useful for unit tests).
func (r *Rebalancer) runPhase3(
	entries []rangeLoad,
	partitionLByPID map[int32]int64,
	recentMoves map[int32][]moveRecord,
	activePartitions []int32,
	now time.Time,
) []Action {
	numPartitions := len(activePartitions)
	if numPartitions == 0 {
		return nil
	}

	// Build effective L snapshot. If no partitionLByPID was provided
	// (legacy callers), fall back to the per-range-sum approximation
	// so tests without full ingester totals still exercise the
	// phase. Production always provides partitionLByPID.
	useRealL := len(partitionLByPID) > 0
	effL := make(map[int32]int64, numPartitions)
	if useRealL {
		for _, pid := range activePartitions {
			effL[pid] = partitionLByPID[pid]
		}
	} else {
		for _, rl := range entries {
			effL[rl.entry.PartitionID] += rl.series
		}
	}

	var totalL int64
	for _, pid := range activePartitions {
		totalL += effL[pid]
	}
	meanL := int64(0)
	if numPartitions > 0 {
		meanL = totalL / int64(numPartitions)
	}

	// plannedAdded accumulates within-round additions per destination.
	// Reset each round by being local to this call.
	plannedAdded := make(map[int32]int64, numPartitions)
	// thisRoundMoves shadows the slicer's growing knowledge of moves
	// off each source during this phase. Combined with the cross-round
	// recentMoves passed in, it feeds sumRecentMoves.
	thisRoundMoves := make(map[int32]int64, numPartitions)

	sumRecentMoves := func(pid int32) int64 {
		var s int64
		for _, m := range recentMoves[pid] {
			s += m.series
		}
		s += thisRoundMoves[pid]
		return s
	}

	effectiveSource := func(pid int32) int64 {
		return effL[pid] - sumRecentMoves(pid)
	}

	effectiveDest := func(pid int32) int64 {
		return effL[pid] + plannedAdded[pid]
	}

	movable := func(pid int32) int64 {
		s := effectiveSource(pid)
		if s <= meanL {
			return 0
		}
		return s - meanL
	}

	movementBudget := r.cfg.MovementBudget * float64(uint64(math.MaxUint32)+1)
	var moved float64

	// Partitions excluded from "hottest" consideration: those that had
	// no profitable range to move when examined. Without this guard a
	// single budget-exhausted-but-still-nominally-hot partition could
	// stall the whole round. Separate from movable-based filtering so
	// that moving a range off P1 doesn't unintentionally unblock a
	// previously-exhausted P0 — once excluded, stays excluded.
	excludedHot := make(map[int32]bool)

	var actions []Action

	for iter := 0; iter < len(entries); iter++ {
		// Hot: argmax effectiveSource among partitions with movable > 0
		// and not excluded.
		var hotPID, coldPID int32
		hotL := int64(math.MinInt64)
		coldL := int64(math.MaxInt64)
		hotFound := false
		coldFound := false
		for _, pid := range activePartitions {
			if !excludedHot[pid] && movable(pid) > 0 {
				s := effectiveSource(pid)
				if s > hotL {
					hotL = s
					hotPID = pid
					hotFound = true
				}
			}
		}
		for _, pid := range activePartitions {
			d := effectiveDest(pid)
			if d < coldL {
				coldL = d
				coldPID = pid
				coldFound = true
			}
		}
		if !hotFound || !coldFound || hotPID == coldPID {
			break
		}

		mov := movable(hotPID)

		bestIdx := -1
		var bestScore float64
		for j, rl := range entries {
			if rl.entry.PartitionID != hotPID {
				continue
			}
			if r.isInMoveCooldown(now, rl.entry.Range) {
				continue
			}
			moveCost := float64(rl.entry.Range.Size())
			if moved+moveCost > movementBudget {
				continue
			}
			// Per-source movable budget: can't move more series off
			// than the "above mean" surplus (net of recent moves).
			if rl.series > mov {
				continue
			}
			// Imbalance improvement: the spread between hot and cold
			// should narrow after the move. Use absolute distance
			// from meanL on both sides.
			newHot := hotL - rl.series
			newCold := coldL + rl.series
			imbalanceBefore := absInt64(hotL-meanL) + absInt64(coldL-meanL)
			imbalanceAfter := absInt64(newHot-meanL) + absInt64(newCold-meanL)
			improvement := imbalanceBefore - imbalanceAfter
			if improvement <= 0 {
				continue
			}
			score := float64(improvement) / moveCost
			if score > bestScore {
				bestScore = score
				bestIdx = j
			}
		}

		if bestIdx < 0 {
			// Either every range is in cooldown, over-budget, too
			// large for the movable budget, or every potential move
			// yields no imbalance improvement. Exclude this hot and
			// try the next-hottest candidate; don't terminate Phase 3
			// because of one stuck source.
			excludedHot[hotPID] = true
			continue
		}

		fromPID := entries[bestIdx].entry.PartitionID
		moved += float64(entries[bestIdx].entry.Range.Size())
		seriesMoved := entries[bestIdx].series
		entries[bestIdx].entry.PartitionID = coldPID

		// Update phase-3 bookkeeping. plannedAdded delays re-picking
		// the same cold in the next iteration; thisRoundMoves feeds
		// movable() so the hot's budget shrinks as we move off it.
		plannedAdded[coldPID] += seriesMoved
		thisRoundMoves[fromPID] += seriesMoved

		actions = append(actions, Action{
			Kind:     ActionMove,
			Range:    entries[bestIdx].entry.Range,
			FromPart: fromPID,
			ToPart:   coldPID,
			Series:   seriesMoved,
			Detail:   fmt.Sprintf("L=%d meanL=%d, P%d→P%d, series=%d, movable=%d", hotL, meanL, fromPID, coldPID, seriesMoved, mov),
		})
	}

	return actions
}

// absInt64 returns the absolute value of x. Defined locally to avoid a
// math.Abs float round-trip for int64 operands.
func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// computePartitionLoads sums the per-range combined load for each
// partition. Used by the defragmentation (merge) and split phases.
// Phase 3 no longer uses this; it ranks by partitionLByPID (from
// ingester TotalActiveSeries) instead.
func computePartitionLoads(entries []rangeLoad) map[int32]float64 {
	m := make(map[int32]float64)
	for _, rl := range entries {
		m[rl.entry.PartitionID] += rl.load
	}
	return m
}

// loadMap holds per-range raw series counts. Construct via
// buildLoadMap and query via seriesAt().
type loadMap struct {
	series map[assignment.HashRange]int64
}

// buildLoadMap aggregates per-range series counts across all
// reporting ingesters by taking the max over replicas of the same
// range. Max (not sum) is intentional: every healthy owner of a
// partition reports counts for the same ranges (they are mirrors), so
// summing would scale the per-range signal by the replication factor
// while `partitionL` is on a 1× (max-over-owners) scale. Phase 3 of
// the slicer compares per-range `series` directly to the partition-
// level movable budget, so the two must share the same scale.
func buildLoadMap(rates []rangeRate) *loadMap {
	lm := &loadMap{series: make(map[assignment.HashRange]int64, len(rates))}
	for _, rr := range rates {
		if rr.series > lm.series[rr.hr] {
			lm.series[rr.hr] = rr.series
		}
	}
	return lm
}

// seriesAt returns the raw in-memory TSDB head series count for the
// given hash range, or 0 if the range is unknown.
func (lm *loadMap) seriesAt(hr assignment.HashRange) int64 {
	return lm.series[hr]
}

// isInMoveCooldown reports whether the given range overlaps any range
// that was moved within the configured cooldown window. Splits and
// merges that happen between the original move and now are handled
// implicitly: any overlap with a cooled-down ancestor's boundaries
// disqualifies the candidate. Always returns false when the cooldown
// is disabled (cfg.MoveCooldown <= 0) or no cooldowns are tracked.
func (r *Rebalancer) isInMoveCooldown(now time.Time, hr assignment.HashRange) bool {
	if r.cfg.MoveCooldown <= 0 || len(r.moveCooldowns) == 0 {
		return false
	}
	for cooled, deadline := range r.moveCooldowns {
		if !now.Before(deadline) {
			continue
		}
		if hashRangesOverlap(hr, cooled) {
			return true
		}
	}
	return false
}

// recordMoveCooldowns scans the slicer's actions and starts a cooldown
// timer for every ActionMove. ActionReassign is intentionally excluded:
// it's a recovery action triggered by an inactive partition, not a
// load-balancing decision, so cooldowning it would needlessly delay
// recovery.
func (r *Rebalancer) recordMoveCooldowns(now time.Time, actions []Action) {
	if r.cfg.MoveCooldown <= 0 {
		return
	}
	deadline := now.Add(r.cfg.MoveCooldown)
	for _, a := range actions {
		if a.Kind != ActionMove {
			continue
		}
		if r.moveCooldowns == nil {
			r.moveCooldowns = make(map[assignment.HashRange]time.Time)
		}
		// Use the post-move (current) range as the cooldown key. If a
		// later round splits or merges this range, the overlap test
		// in isInMoveCooldown will still match.
		r.moveCooldowns[a.Range] = deadline
	}
}

// pruneExpiredCooldowns drops cooldown entries whose deadline has passed.
func (r *Rebalancer) pruneExpiredCooldowns(now time.Time) {
	for hr, deadline := range r.moveCooldowns {
		if !now.Before(deadline) {
			delete(r.moveCooldowns, hr)
		}
	}
}

// recordRecentMoves appends move records for every ActionMove in the
// round's actions, keyed by the source partition. Each record counts
// against that source's movable budget in subsequent rounds until it
// ages out of CompactionInterval. ActionReassign is intentionally
// excluded (same rationale as recordMoveCooldowns).
func (r *Rebalancer) recordRecentMoves(now time.Time, actions []Action, lm *loadMap) {
	if r.cfg.CompactionInterval <= 0 {
		return
	}
	if r.recentMoves == nil {
		r.recentMoves = make(map[int32][]moveRecord)
	}
	for _, a := range actions {
		if a.Kind != ActionMove {
			continue
		}
		series := a.Series
		if series == 0 && lm != nil {
			// Defensive: if Action.Series wasn't populated (e.g.
			// legacy callers), fall back to looking up by range.
			series = lm.seriesAt(a.Range)
		}
		r.recentMoves[a.FromPart] = append(r.recentMoves[a.FromPart], moveRecord{
			hr:     a.Range,
			series: series,
			at:     now,
		})
	}
}

// pruneRecentMoves drops move records older than CompactionInterval.
// Partitions left with no surviving records have their slice deleted
// entirely to keep the map small.
func (r *Rebalancer) pruneRecentMoves(now time.Time) {
	if r.cfg.CompactionInterval <= 0 {
		// When the guard is disabled, also clear any accumulated
		// records so they don't silently gate moves if the operator
		// re-enables the feature later.
		for pid := range r.recentMoves {
			delete(r.recentMoves, pid)
		}
		return
	}
	cutoff := now.Add(-r.cfg.CompactionInterval)
	for pid, records := range r.recentMoves {
		keep := records[:0]
		for _, m := range records {
			if m.at.After(cutoff) {
				keep = append(keep, m)
			}
		}
		if len(keep) == 0 {
			delete(r.recentMoves, pid)
		} else {
			r.recentMoves[pid] = keep
		}
	}
}

// hashRangesOverlap returns true if the two ranges share at least one
// hash value (closed intervals on both sides).
func hashRangesOverlap(a, b assignment.HashRange) bool {
	return a.Lo <= b.Hi && b.Lo <= a.Hi
}

// mergeAdjacentCold merges adjacent cold slices to defragment the
// assignment, following the Slicer paper (Section 4.4.1, Phase 3).
//
// Same-partition adjacent slices are merged directly.
// Cross-partition adjacent slices are merged by moving the smaller
// slice onto the other's partition, then combining into one range.
//
// Constraints:
//   - merged load < meanSliceLoad
//   - receiving partition load stays below maxPartitionLoad (target * 1.5)
//   - total churn stays within churnBudget
//   - total entries don't drop below minEntries
func mergeAdjacentCold(entries []rangeLoad, meanSliceLoad, churnBudget, targetLoad float64, minEntries int) ([]rangeLoad, []Action) {
	if len(entries) <= 1 || len(entries) <= minEntries {
		return entries, nil
	}

	maxPartitionLoad := targetLoad * 1.5
	partitionLoads := computePartitionLoads(entries)
	var churned float64
	var actions []Action

	result := []rangeLoad{entries[0]}
	for i := 1; i < len(entries); i++ {
		if len(result)+len(entries)-i <= minEntries {
			result = append(result, entries[i:]...)
			break
		}
		prev := &result[len(result)-1]
		curr := entries[i]

		if prev.entry.Range.Hi+1 != curr.entry.Range.Lo {
			result = append(result, curr)
			continue
		}

		mergedLoad := prev.load + curr.load
		if mergedLoad >= meanSliceLoad {
			result = append(result, curr)
			continue
		}

		if prev.entry.PartitionID == curr.entry.PartitionID {
			if partitionLoads[prev.entry.PartitionID] <= maxPartitionLoad {
				mergeCost := float64(curr.entry.Range.Size())
				if churned+mergeCost <= churnBudget {
					merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
					actions = append(actions, Action{
						Kind:   ActionMerge,
						Range:  merged,
						ToPart: prev.entry.PartitionID,
						Detail: fmt.Sprintf("same-partition merge on P%d, combined load=%.4f", prev.entry.PartitionID, mergedLoad),
					})
					prev.entry.Range = merged
					prev.load = mergedLoad
					prev.series += curr.series
					churned += mergeCost
					continue
				}
			}
		} else {
			var receiverPID, donorPID int32
			var movedSize float64
			var donorLoad float64
			if prev.load >= curr.load {
				receiverPID = prev.entry.PartitionID
				donorPID = curr.entry.PartitionID
				movedSize = float64(curr.entry.Range.Size())
				donorLoad = curr.load
			} else {
				receiverPID = curr.entry.PartitionID
				donorPID = prev.entry.PartitionID
				movedSize = float64(prev.entry.Range.Size())
				donorLoad = prev.load
			}

			if partitionLoads[receiverPID]+donorLoad <= maxPartitionLoad && churned+movedSize <= churnBudget {
				partitionLoads[receiverPID] += donorLoad
				partitionLoads[donorPID] -= donorLoad

				merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
				actions = append(actions, Action{
					Kind:     ActionMerge,
					Range:    merged,
					FromPart: donorPID,
					ToPart:   receiverPID,
					Detail:   fmt.Sprintf("cross-partition merge P%d+P%d→P%d, combined load=%.4f", donorPID, receiverPID, receiverPID, mergedLoad),
				})
				prev.entry.Range = merged
				prev.entry.PartitionID = receiverPID
				prev.load = mergedLoad
				prev.series += curr.series
				churned += movedSize
				continue
			}
		}

		result = append(result, curr)
	}
	return result, actions
}

type rangeKey struct{ lo, hi uint32 }

// diffRanges returns the number of ranges added and removed between prev
// and next, and whether any change occurred at all.
func diffRanges(prev, next []ingester_client.HashRangeEntry) (added, removed int, changed bool) {
	prevSet := make(map[rangeKey]struct{}, len(prev))
	for _, r := range prev {
		prevSet[rangeKey{r.Lo, r.Hi}] = struct{}{}
	}
	nextSet := make(map[rangeKey]struct{}, len(next))
	for _, r := range next {
		nextSet[rangeKey{r.Lo, r.Hi}] = struct{}{}
	}
	for k := range nextSet {
		if _, ok := prevSet[k]; !ok {
			added++
		}
	}
	for k := range prevSet {
		if _, ok := nextSet[k]; !ok {
			removed++
		}
	}
	changed = added > 0 || removed > 0
	return
}

// hashSpaceCoverage returns the fraction of the 32-bit hash space covered
// by the given ranges.
func hashSpaceCoverage(ranges []ingester_client.HashRangeEntry) float64 {
	var total uint64
	for _, r := range ranges {
		total += uint64(r.Hi) - uint64(r.Lo) + 1
	}
	return float64(total) / float64(uint64(math.MaxUint32)+1)
}
