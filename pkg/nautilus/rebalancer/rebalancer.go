// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"flag"
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
		current = assignment.EvenSplit(activePartitions)
		level.Info(r.logger).Log("msg", "initialized assignment with even split", "partitions", len(activePartitions))
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
}

type rangeRate struct {
	hr   assignment.HashRange
	rate float64
}

type rangeLoad struct {
	entry assignment.Entry
	load  float64
}

// runSlicer runs a simplified slicer algorithm: split, merge, and move to
// converge toward equal per-partition load. It uses the per-range rates
// collected from ingesters.
func (r *Rebalancer) runSlicer(current *assignment.Assignment, rates []rangeRate, activePartitions []int32) *assignment.Assignment {
	rateMap := buildRateMap(rates)

	activeSet := make(map[int32]bool, len(activePartitions))
	for _, pid := range activePartitions {
		activeSet[pid] = true
	}

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

	partitionLoads := make(map[int32]float64)
	for _, rl := range entries {
		partitionLoads[rl.entry.PartitionID] += rl.load
	}
	targetLoad := totalLoad / float64(len(activePartitions))

	movementBudget := r.cfg.MovementBudget * float64(uint64(math.MaxUint32)+1)
	var moved float64

	meanRangeLoad := totalLoad / float64(len(entries))
	splitThreshold := 2.0 * meanRangeLoad
	var newEntries []rangeLoad
	for _, rl := range entries {
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

	entries = mergeAdjacentUnderloaded(entries, meanRangeLoad*0.5, rateMap)

	for i := 0; i < len(entries) && moved < movementBudget; i++ {
		partitionLoads = make(map[int32]float64)
		for _, rl := range entries {
			partitionLoads[rl.entry.PartitionID] += rl.load
		}

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

func mergeAdjacentUnderloaded(entries []rangeLoad, threshold float64, rateMap map[assignment.HashRange]float64) []rangeLoad {
	if len(entries) <= 1 {
		return entries
	}

	result := []rangeLoad{entries[0]}
	for i := 1; i < len(entries); i++ {
		prev := &result[len(result)-1]
		curr := entries[i]

		if prev.entry.PartitionID == curr.entry.PartitionID &&
			prev.entry.Range.Hi+1 == curr.entry.Range.Lo &&
			prev.load < threshold && curr.load < threshold {
			merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
			prev.entry.Range = merged
			prev.load = lookupRate(merged, rateMap)
		} else {
			result = append(result, curr)
		}
	}
	return result
}
