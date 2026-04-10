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
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.RebalanceInterval, prefix+"rebalance-interval", 60*time.Second, "How often the rebalancer runs.")
	f.Float64Var(&cfg.MovementBudget, prefix+"movement-budget", 0.09, "Maximum fraction of the hash space that can be moved per round.")
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

	// prevInstanceRanges tracks the last set of ranges pushed to each
	// ingester, keyed by instance ID, so we can log changes.
	prevInstanceRanges map[string][]ingester_client.HashRangeEntry
}

// New creates and returns a new Rebalancer.
func New(cfg Config, ingesterRing ring.ReadRing, pool *ring_client.Pool, partitionRing *ring.PartitionInstanceRing, logger log.Logger) *Rebalancer {
	r := &Rebalancer{
		cfg:           cfg,
		logger:        logger,
		ingesterRing:  ingesterRing,
		pool:          pool,
		partitionRing: partitionRing,
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

	rates, err := r.collectRates(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to collect ingester rates", "err", err)
		return nil
	}

	newAssignment := r.runSlicer(current, rates, activePartitions)
	if err := newAssignment.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "generated invalid assignment", "err", err)
		return nil
	}

	r.store.add(time.Now(), newAssignment)
	r.pushRangesToIngesters(ctx, newAssignment)

	level.Info(r.logger).Log("msg", "rebalance complete", "entries", len(newAssignment.Entries), "total_assignments", len(r.store.snapshot().Assignments))
	return nil
}

// collectRates queries all ingesters for per-range ingestion rates and
// returns a global view: one rate per reported hash range. Since
// ingesters only report rates for ranges they own, the results are
// already partitioned; we aggregate into a single map keyed by range.
func (r *Rebalancer) collectRates(ctx context.Context) ([]rangeRate, error) {
	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		return nil, err
	}

	var all []rangeRate
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

		for _, rate := range resp.Rates {
			all = append(all, rangeRate{
				hr:   assignment.HashRange{Lo: rate.Lo, Hi: rate.Hi},
				rate: rate.SamplesPerSecond,
			})
		}
	}

	return all, nil
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

type rangeRate struct {
	hr   assignment.HashRange
	rate float64
}

type rangeLoad struct {
	entry assignment.Entry
	load  float64
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
func (r *Rebalancer) runSlicer(current *assignment.Assignment, rates []rangeRate, activePartitions []int32) *assignment.Assignment {
	rateMap := buildRateMap(rates)
	numPartitions := len(activePartitions)

	activeSet := make(map[int32]bool, numPartitions)
	for _, pid := range activePartitions {
		activeSet[pid] = true
	}

	// --- Phase 1: build entries, reassign inactive partitions ----------
	entries := make([]rangeLoad, len(current.Entries))
	for i, e := range current.Entries {
		entries[i] = rangeLoad{
			entry: e,
			load:  lookupRate(e.Range, rateMap),
		}
		if !activeSet[e.PartitionID] {
			entries[i].entry.PartitionID = activePartitions[0]
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
		entries = mergeAdjacentCold(entries, meanSliceLoad, mergeMoveBudget, targetLoad, minSlicesPerPartition*numPartitions, rateMap)
	}

	// --- Phase 3: weighted-move from hottest to coldest ----------------
	movementBudget := r.cfg.MovementBudget * float64(uint64(math.MaxUint32)+1)
	var moved float64

	for iter := 0; iter < len(entries); iter++ {
		partitionLoads := computePartitionLoads(entries)

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

		if hottestPID == coldestPID || hottestLoad <= targetLoad*1.1 {
			break
		}

		bestIdx := -1
		bestScore := 0.0
		for j, rl := range entries {
			if rl.entry.PartitionID != hottestPID {
				continue
			}
			moveCost := float64(rl.entry.Range.Size())
			if moved+moveCost > movementBudget {
				continue
			}
			imbalanceBefore := math.Abs(hottestLoad-targetLoad) + math.Abs(coldestLoad-targetLoad)
			newHotLoad := hottestLoad - rl.load
			newColdLoad := coldestLoad + rl.load
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

		moved += float64(entries[bestIdx].entry.Range.Size())
		entries[bestIdx].entry.PartitionID = coldestPID
	}

	// --- Phase 4: split hot slices (for next round) -------------------
	maxTotal := maxSlicesPerPartition * numPartitions
	if len(entries) < maxTotal {
		meanSliceLoad := totalLoad / float64(len(entries))
		splitThreshold := 2.0 * meanSliceLoad

		var newEntries []rangeLoad
		for _, rl := range entries {
			if len(newEntries) >= maxTotal {
				newEntries = append(newEntries, rl)
				continue
			}
			if rl.load > splitThreshold && rl.entry.Range.Size() > 1 {
				mid := rl.entry.Range.Lo + uint32((uint64(rl.entry.Range.Hi)-uint64(rl.entry.Range.Lo))/2)
				left := assignment.HashRange{Lo: rl.entry.Range.Lo, Hi: mid}
				right := assignment.HashRange{Lo: mid + 1, Hi: rl.entry.Range.Hi}
				leftLoad := lookupRate(left, rateMap)
				rightLoad := lookupRate(right, rateMap)
				if leftLoad == 0 && rightLoad == 0 && rl.load > 0 {
					leftFraction := float64(left.Size()) / float64(rl.entry.Range.Size())
					leftLoad = rl.load * leftFraction
					rightLoad = rl.load * (1 - leftFraction)
				}
				newEntries = append(newEntries,
					rangeLoad{entry: assignment.Entry{Range: left, PartitionID: rl.entry.PartitionID}, load: leftLoad},
					rangeLoad{entry: assignment.Entry{Range: right, PartitionID: rl.entry.PartitionID}, load: rightLoad},
				)
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
	return result
}

func computePartitionLoads(entries []rangeLoad) map[int32]float64 {
	m := make(map[int32]float64)
	for _, rl := range entries {
		m[rl.entry.PartitionID] += rl.load
	}
	return m
}

// buildRateMap builds a lookup from hash range to rate.
func buildRateMap(rates []rangeRate) map[assignment.HashRange]float64 {
	m := make(map[assignment.HashRange]float64, len(rates))
	for _, rr := range rates {
		m[rr.hr] += rr.rate
	}
	return m
}

// lookupRate returns the rate for the given hash range. If an exact
// match exists, it's returned directly. Otherwise returns 0 (new
// ranges from splits won't have data until the next cycle).
func lookupRate(hr assignment.HashRange, rateMap map[assignment.HashRange]float64) float64 {
	if rate, ok := rateMap[hr]; ok {
		return rate
	}
	return 0
}

// mergeAdjacentCold merges adjacent slices on the same partition to
// defragment the assignment. Follows the Slicer paper's constraints:
//   - merged load < meanSliceLoad
//   - receiving partition load stays below maxPartitionLoad (target * 1.5)
//   - total churn stays within churnBudget
//   - total entries don't drop below minEntries
func mergeAdjacentCold(entries []rangeLoad, meanSliceLoad, churnBudget, targetLoad float64, minEntries int, rateMap map[assignment.HashRange]float64) []rangeLoad {
	if len(entries) <= 1 || len(entries) <= minEntries {
		return entries
	}

	maxPartitionLoad := targetLoad * 1.5
	partitionLoads := computePartitionLoads(entries)
	var churned float64

	result := []rangeLoad{entries[0]}
	for i := 1; i < len(entries); i++ {
		if len(result)+len(entries)-i <= minEntries {
			result = append(result, entries[i:]...)
			break
		}
		prev := &result[len(result)-1]
		curr := entries[i]

		if prev.entry.PartitionID == curr.entry.PartitionID &&
			prev.entry.Range.Hi+1 == curr.entry.Range.Lo {

			mergedLoad := prev.load + curr.load
			if mergedLoad < meanSliceLoad && partitionLoads[prev.entry.PartitionID] <= maxPartitionLoad {
				mergeCost := float64(curr.entry.Range.Size())
				if churned+mergeCost <= churnBudget {
					merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
					prev.entry.Range = merged
					prev.load = mergedLoad
					churned += mergeCost
					continue
				}
			}
		}
		result = append(result, curr)
	}
	return result
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
