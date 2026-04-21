// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

	// MoveCooldown is the minimum time a hash range must sit on its new
	// partition after a move before it (or any range overlapping its
	// former boundaries) is eligible to be moved again. Prevents
	// over-correction during the warm-up window where the destination
	// ingester's reported load lags the actual post-move load (distributor
	// poll lag plus EWMA settle time). Set to 0 to disable.
	MoveCooldown time.Duration `yaml:"move_cooldown"`

	// LoadWeightSeries and LoadWeightSamples control how the two
	// per-range load signals (active series count and sample ingestion
	// rate) are combined into a single scalar load value used by the
	// slicer. Each component is first normalized by its global total
	// so that both contribute fractions of total load, then weighted.
	// Defaults give cardinality the dominant role since ingester memory
	// (and OOM risk) is the primary scaling dimension.
	LoadWeightSeries  float64 `yaml:"load_weight_series"`
	LoadWeightSamples float64 `yaml:"load_weight_samples"`
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.RebalanceInterval, prefix+"rebalance-interval", 60*time.Second, "How often the rebalancer runs.")
	f.Float64Var(&cfg.MovementBudget, prefix+"movement-budget", 0.09, "Maximum fraction of the hash space that can be moved per round.")
	f.DurationVar(&cfg.MoveCooldown, prefix+"move-cooldown", 90*time.Second, "Minimum time between consecutive moves of the same hash range (or any range overlapping it). Covers distributor poll lag plus ingester EWMA settle time. 0 disables.")
	f.Float64Var(&cfg.LoadWeightSeries, prefix+"load-weight-series", 0.8, "Weight of normalized active series count in the per-range combined load metric.")
	f.Float64Var(&cfg.LoadWeightSamples, prefix+"load-weight-samples", 0.2, "Weight of normalized sample ingestion rate in the per-range combined load metric.")
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
		current = assignment.FineEvenSplit(activePartitions, initialSlicesPerPartition)
		level.Info(r.logger).Log("msg", "initialized assignment with fine even split",
			"partitions", len(activePartitions),
			"slices_per_partition", initialSlicesPerPartition,
			"total_slices", len(current.Entries))
		r.store.add(time.Now(), current)
		r.pushRangesToIngesters(ctx, current)
		return nil
	}

	rates, instanceTotals, err := r.collectRates(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to collect ingester rates", "err", err)
		return nil
	}

	// Compute per-partition "orphan" series: in-memory series an
	// ingester still holds for hash ranges it no longer owns. These
	// linger until TSDB head compaction (~2h) and represent real
	// memory pressure on the source ingester even though no per-range
	// signal accounts for them.
	partitionOrphans := computePartitionOrphans(current, instanceTotals, rates, pRing)

	lm := buildLoadMapWithOrphans(rates, partitionOrphans, r.cfg.LoadWeightSeries, r.cfg.LoadWeightSamples)
	r.admin.setLastStats(lm)

	now := time.Now()
	r.pruneExpiredCooldowns(now)

	newAssignment, actions := r.runSlicer(current, rates, partitionOrphans, activePartitions, now)
	if err := newAssignment.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "generated invalid assignment", "err", err)
		return nil
	}

	r.recordMoveCooldowns(now, actions)

	r.store.add(now, newAssignment)
	r.pushRangesToIngesters(ctx, newAssignment)

	// Compute round summary stats.
	partLoads := lm.computePartitionLoads(buildRangeLoads(newAssignment, lm))
	var totalLoad, maxPL, minPL float64
	minPL = math.MaxFloat64
	for _, pl := range partLoads {
		totalLoad += pl
		if pl > maxPL {
			maxPL = pl
		}
		if pl < minPL {
			minPL = pl
		}
	}
	if minPL == math.MaxFloat64 {
		minPL = 0
	}
	meanPL := 0.0
	if len(partLoads) > 0 {
		meanPL = totalLoad / float64(len(partLoads))
	}
	imbalance := 0.0
	if meanPL > 0 {
		imbalance = (maxPL - minPL) / meanPL
	}
	movedFraction := 0.0
	hashSpaceSize := float64(uint64(math.MaxUint32) + 1)
	for _, a := range actions {
		if a.Kind == ActionMove || a.Kind == ActionReassign {
			movedFraction += float64(a.Range.Size()) / hashSpaceSize
		}
	}

	r.admin.addRound(RoundLog{
		Time:           now,
		TotalLoad:      totalLoad,
		MeanPartLoad:   meanPL,
		MaxPartLoad:    maxPL,
		MinPartLoad:    minPL,
		ImbalanceRatio: imbalance,
		NumEntries:     len(newAssignment.Entries),
		NumPartitions:  len(partLoads),
		MovedFraction:  movedFraction,
		Actions:        actions,
	})

	level.Info(r.logger).Log("msg", "rebalance complete", "entries", len(newAssignment.Entries), "total_assignments", len(r.store.snapshot().Assignments))
	return nil
}

// collectRates queries all ingesters for per-range ingestion rates and
// returns a global view: one rate per reported hash range. Since
// ingesters only report rates for ranges they own, the results are
// already partitioned; we aggregate into a single map keyed by range.
//
// The second return value is keyed by ingester instance ID and holds
// the total in-memory series count on that ingester (across all
// tenants and all hash buckets, including ones not currently owned).
// It is used to detect "orphan" series left behind by recent moves;
// see computePartitionOrphans.
func (r *Rebalancer) collectRates(ctx context.Context) ([]rangeRate, map[string]int64, error) {
	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		return nil, nil, err
	}

	var all []rangeRate
	instanceTotals := make(map[string]int64, len(instances.Instances))
	for _, inst := range instances.Instances {
		c, err := r.pool.GetClientForInstance(inst)
		if err != nil {
			level.Warn(r.logger).Log("msg", "failed to get client for ingester", "ingester", inst.Addr, "err", err)
			continue
		}

		resp, err := c.(ingester_client.IngesterClient).HashRangeStats(ctx, &ingester_client.HashRangeStatsRequest{})
		if err != nil {
			level.Warn(r.logger).Log("msg", "HashRangeStats RPC failed", "ingester", inst.Addr, "err", err)
			continue
		}

		instanceTotals[inst.GetId()] = resp.TotalActiveSeries
		for _, rate := range resp.Rates {
			all = append(all, rangeRate{
				hr:      assignment.HashRange{Lo: rate.Lo, Hi: rate.Hi},
				samples: rate.SamplesPerSecond,
				series:  rate.ActiveSeries,
			})
		}
	}

	return all, instanceTotals, nil
}

// computePartitionOrphans returns a per-partition estimate of "orphan"
// in-memory series: series the partition's owner ingester(s) still
// hold from a previously-owned hash range that has since been moved
// away. These series will be garbage-collected by the next TSDB head
// compaction (every ~2h by default), but in the meantime they
// represent genuine memory pressure on the source ingester.
//
// We compute the orphan count per ingester as
// (total in-memory series) - (sum of per-range series for ranges the
// ingester currently owns), and attribute it to the partition that
// instance owns. When a partition has multiple owners (e.g. one per
// zone), we take the worst (max) orphan count across owners — that's
// the partition's worst-case memory pressure.
//
// The orphan count is conservative when a single owner's per-range
// histogram approximation undercounts owned series; in that case we
// floor the orphan at zero to avoid negative values.
func computePartitionOrphans(current *assignment.Assignment, instanceTotals map[string]int64, rates []rangeRate, pRing partitionRingView) map[int32]int64 {
	if pRing == nil || len(instanceTotals) == 0 || current == nil {
		return nil
	}

	// Sum reported per-range series, keyed by range. An ingester
	// reports at most one entry per owned range.
	rangeSeries := make(map[assignment.HashRange]int64, len(rates))
	for _, rr := range rates {
		rangeSeries[rr.hr] += rr.series
	}

	// Sum the per-range series owned by each partition. Each entry
	// in the assignment maps a range to exactly one partition.
	partitionOwnedSeries := make(map[int32]int64)
	for _, e := range current.Entries {
		partitionOwnedSeries[e.PartitionID] += rangeSeries[e.Range]
	}

	// For each owner of a partition, the orphan count is total
	// in-memory series on that ingester minus what it should be
	// reporting for currently-owned ranges. Take the max across
	// owners — that's the partition's worst-case memory pressure.
	orphans := make(map[int32]int64)
	for pid, ownedSeries := range partitionOwnedSeries {
		owners := pRing.PartitionOwnerIDs(pid)
		var worst int64
		for _, ownerID := range owners {
			total, ok := instanceTotals[ownerID]
			if !ok {
				continue
			}
			orphan := total - ownedSeries
			if orphan < 0 {
				orphan = 0
			}
			if orphan > worst {
				worst = orphan
			}
		}
		if worst > 0 {
			orphans[pid] = worst
		}
	}
	return orphans
}

// partitionRingView is the subset of the partition ring API that the
// orphan-computation code needs. Defined as an interface so tests can
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

		c, err := r.pool.GetClientForInstance(inst)
		if err != nil {
			level.Warn(r.logger).Log("msg", "failed to get client for ingester", "ingester", inst.Addr, "err", err)
			continue
		}
		req := &ingester_client.SetHashRangesRequest{Ranges: ranges}
		if _, err := c.(ingester_client.IngesterClient).SetHashRanges(ctx, req); err != nil {
			level.Warn(r.logger).Log("msg", "SetHashRanges RPC failed", "ingester", inst.Addr, "err", err)
		}
	}

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
// It carries both raw load signals; the rebalancer combines them into
// a single scalar load using configurable weights.
type rangeRate struct {
	hr      assignment.HashRange
	samples float64
	series  int64
}

type rangeLoad struct {
	entry   assignment.Entry
	load    float64 // combined, weighted, normalized load
	samples float64 // raw samples/sec (for logging/admin)
	series  int64   // raw active series count (for logging/admin)
}

// runSlicer implements the Slicer weighted-move algorithm (Adya et al.,
// OSDI'16, Section 4.4.1). Phases:
//
//  1. Reassign slices from inactive partitions.
//  2. Merge adjacent cold slices to defragment (cap: 1% churn, floor:
//     minSlicesPerPartition).
//  3. Weighted-move: greedily move slices from the hottest partition to
//     the coldest, picking the move with the best imbalance-reduction
//     per unit of churn. Budget: cfg.MovementBudget (default 9%).
//  4. Split hot slices (>2× mean slice load) without changing
//     assignments. This creates finer load signals for the next round.
//     Cap: maxSlicesPerPartition.
//
// The `now` parameter is used to evaluate per-range move cooldowns
// (see Config.MoveCooldown). Pass time.Now() in production; tests can
// pass a deterministic value or the zero time to disable cooldown
// filtering.
func (r *Rebalancer) runSlicer(current *assignment.Assignment, rates []rangeRate, partitionOrphans map[int32]int64, activePartitions []int32, now time.Time) (*assignment.Assignment, []Action) {
	lm := buildLoadMapWithOrphans(rates, partitionOrphans, r.cfg.LoadWeightSeries, r.cfg.LoadWeightSamples)
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
		samples, series := lm.stat(e.Range)
		entries[i] = rangeLoad{
			entry:   e,
			load:    lm.load(e.Range),
			samples: samples,
			series:  series,
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

	// --- Phase 3: weighted-move from hottest to coldest ----------------
	movementBudget := r.cfg.MovementBudget * float64(uint64(math.MaxUint32)+1)
	var moved float64

	for iter := 0; iter < len(entries); iter++ {
		partitionLoads := lm.computePartitionLoads(entries)

		var hottestPID, coldestPID int32
		hottestLoad := -1.0
		coldestLoad := math.MaxFloat64
		for _, pid := range activePartitions {
			load := partitionLoads[pid]
			if load > hottestLoad {
				hottestLoad = load
				hottestPID = pid
			}
			if load < coldestLoad {
				coldestLoad = load
				coldestPID = pid
			}
		}

		if hottestPID == coldestPID || (hottestLoad-coldestLoad) <= targetLoad*0.05 {
			break
		}

		// Don't drain any partition below 25% of the target load.
		minSourceLoad := targetLoad * 0.25

		bestIdx := -1
		bestScore := 0.0
		for j, rl := range entries {
			if rl.entry.PartitionID != hottestPID {
				continue
			}
			if r.isInMoveCooldown(now, rl.entry.Range) {
				continue
			}
			moveCost := float64(rl.entry.Range.Size())
			if moved+moveCost > movementBudget {
				continue
			}
			newHotLoad := hottestLoad - rl.load
			if newHotLoad < minSourceLoad {
				continue
			}
			newColdLoad := coldestLoad + rl.load
			imbalanceBefore := math.Abs(hottestLoad-targetLoad) + math.Abs(coldestLoad-targetLoad)
			imbalanceAfter := math.Abs(newHotLoad-targetLoad) + math.Abs(newColdLoad-targetLoad)
			improvement := imbalanceBefore - imbalanceAfter
			if improvement <= 0 {
				continue
			}
			score := improvement / moveCost
			if score > bestScore {
				bestScore = score
				bestIdx = j
			}
		}

		if bestIdx < 0 {
			break
		}

		fromPID := entries[bestIdx].entry.PartitionID
		moved += float64(entries[bestIdx].entry.Range.Size())
		entries[bestIdx].entry.PartitionID = coldestPID
		actions = append(actions, Action{
			Kind:     ActionMove,
			Range:    entries[bestIdx].entry.Range,
			FromPart: fromPID,
			ToPart:   coldestPID,
			Detail:   fmt.Sprintf("load=%.1f/s, hot P%d→cold P%d", entries[bestIdx].load, fromPID, coldestPID),
		})
	}

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
		partitionLoads := lm.computePartitionLoads(entries)

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
				leftLoad := lm.load(left)
				rightLoad := lm.load(right)
				leftSamples, leftSeries := lm.stat(left)
				rightSamples, rightSeries := lm.stat(right)
				if leftLoad == 0 && rightLoad == 0 && rl.load > 0 {
					// Newly split sub-ranges have no per-range data yet.
					// Distribute the parent's load proportionally so the
					// next phase doesn't immediately re-merge them.
					leftFraction := float64(left.Size()) / float64(rl.entry.Range.Size())
					leftLoad = rl.load * leftFraction
					rightLoad = rl.load * (1 - leftFraction)
					leftSamples = rl.samples * leftFraction
					rightSamples = rl.samples * (1 - leftFraction)
					leftSeries = int64(float64(rl.series) * leftFraction)
					rightSeries = rl.series - leftSeries
				}
				newEntries = append(newEntries,
					rangeLoad{entry: assignment.Entry{Range: left, PartitionID: rl.entry.PartitionID}, load: leftLoad, samples: leftSamples, series: leftSeries},
					rangeLoad{entry: assignment.Entry{Range: right, PartitionID: rl.entry.PartitionID}, load: rightLoad, samples: rightSamples, series: rightSeries},
				)
				actions = append(actions, Action{
					Kind:   ActionSplit,
					Range:  rl.entry.Range,
					ToPart: rl.entry.PartitionID,
					Detail: fmt.Sprintf("load=%.1f/s > threshold=%.1f/s, split on P%d", rl.load, splitThreshold, rl.entry.PartitionID),
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

func buildRangeLoads(a *assignment.Assignment, lm *loadMap) []rangeLoad {
	entries := make([]rangeLoad, len(a.Entries))
	for i, e := range a.Entries {
		samples, series := lm.stat(e.Range)
		entries[i] = rangeLoad{
			entry:   e,
			load:    lm.load(e.Range),
			samples: samples,
			series:  series,
		}
	}
	return entries
}

// computePartitionLoads sums the per-range combined load for each
// partition. It does NOT include the orphan-series adjustment;
// callers that want the orphan-aware per-partition load should use
// loadMap.computePartitionLoads instead. Kept as a free function for
// the defragmentation (merge) phase, where orphan accounting is not
// useful.
func computePartitionLoads(entries []rangeLoad) map[int32]float64 {
	m := make(map[int32]float64)
	for _, rl := range entries {
		m[rl.entry.PartitionID] += rl.load
	}
	return m
}

// rangeStats holds the raw per-range signals reported by ingesters.
type rangeStats struct {
	samples float64
	series  int64
}

// loadMap holds per-range raw signals plus the global totals needed to
// normalize them into a combined weighted load. Construct via
// buildLoadMap (or buildLoadMapWithOrphans when partition-level
// orphan counts are available) and query via load() / stat().
//
// totalSeries includes both per-range reported series AND the
// per-partition orphan series (in-memory series an ingester still
// holds for hash ranges it no longer owns). Including orphans in the
// denominator keeps the load values normalized — the sum of all
// per-partition loads (including orphan contributions) stays equal
// to wSeries + wSamples regardless of how many series are orphaned.
type loadMap struct {
	stats            map[assignment.HashRange]rangeStats
	partitionOrphans map[int32]int64
	totalSamples     float64
	totalSeries      int64
	wSeries          float64
	wSamples         float64
}

func buildLoadMap(rates []rangeRate, wSeries, wSamples float64) *loadMap {
	return buildLoadMapWithOrphans(rates, nil, wSeries, wSamples)
}

func buildLoadMapWithOrphans(rates []rangeRate, partitionOrphans map[int32]int64, wSeries, wSamples float64) *loadMap {
	lm := &loadMap{
		stats:            make(map[assignment.HashRange]rangeStats, len(rates)),
		partitionOrphans: partitionOrphans,
		wSeries:          wSeries,
		wSamples:         wSamples,
	}
	for _, rr := range rates {
		s := lm.stats[rr.hr]
		s.samples += rr.samples
		s.series += rr.series
		lm.stats[rr.hr] = s
		lm.totalSamples += rr.samples
		lm.totalSeries += rr.series
	}
	for _, orphan := range partitionOrphans {
		lm.totalSeries += orphan
	}
	return lm
}

// load returns the combined weighted load for the given hash range.
// Returns 0 if the range is not present in the underlying map. The
// returned value has no physical units; it's the sum of two
// fraction-of-total components weighted by the configured weights, so
// the global total of load() across all ranges PLUS partitionOrphanLoad
// across all partitions is wSeries + wSamples (typically 1.0).
func (lm *loadMap) load(hr assignment.HashRange) float64 {
	s, ok := lm.stats[hr]
	if !ok {
		return 0
	}
	var l float64
	if lm.totalSeries > 0 {
		l += lm.wSeries * float64(s.series) / float64(lm.totalSeries)
	}
	if lm.totalSamples > 0 {
		l += lm.wSamples * s.samples / lm.totalSamples
	}
	return l
}

// stat returns the raw samples/sec and series count for the range.
func (lm *loadMap) stat(hr assignment.HashRange) (samples float64, series int64) {
	s := lm.stats[hr]
	return s.samples, s.series
}

// partitionOrphanLoad returns the load contribution from orphan
// series attributed to the given partition. Orphans are series an
// owner ingester still holds for ranges that have been moved to
// another partition; they will be GC'd by the next TSDB head
// compaction (~2h) but in the meantime represent real memory
// pressure on the source ingester. Returns 0 when the range
// component (wSeries) is zero, when no orphans were measured for the
// partition, or when totalSeries is zero.
func (lm *loadMap) partitionOrphanLoad(pid int32) float64 {
	if lm == nil || lm.wSeries == 0 || lm.totalSeries == 0 {
		return 0
	}
	orphan := lm.partitionOrphans[pid]
	if orphan <= 0 {
		return 0
	}
	return lm.wSeries * float64(orphan) / float64(lm.totalSeries)
}

// partitionOrphanSeries returns the raw orphan series count for the
// given partition (0 if unknown).
func (lm *loadMap) partitionOrphanSeries(pid int32) int64 {
	if lm == nil || lm.partitionOrphans == nil {
		return 0
	}
	return lm.partitionOrphans[pid]
}

// computePartitionLoads sums the per-range combined load for each
// partition AND adds each partition's orphan-load contribution, so
// the result reflects the source ingester's true memory pressure
// (current ranges + as-yet-uncompacted series from former ranges).
// Used by the slicer's hottest/coldest selection so we don't pile
// load onto a partition whose owner is still working off a backlog.
func (lm *loadMap) computePartitionLoads(entries []rangeLoad) map[int32]float64 {
	m := computePartitionLoads(entries)
	if lm == nil {
		return m
	}
	for pid := range lm.partitionOrphans {
		if extra := lm.partitionOrphanLoad(pid); extra > 0 {
			m[pid] += extra
		}
	}
	return m
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
					prev.samples += curr.samples
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
				prev.samples += curr.samples
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
