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

	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

const hashBucketCount = 256

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
// per-hash-bucket ingestion rates and rebalances hash-range-to-partition
// assignments to distribute ingestion load evenly.
type Rebalancer struct {
	services.Service

	cfg    Config
	logger log.Logger

	ingesterRing ring.ReadRing
	pool         *ring_client.Pool
	distributor  *distributor.Distributor

	partitionRing *ring.PartitionInstanceRing

	currentAssignment *assignment.Assignment
}

// New creates and returns a new Rebalancer.
func New(cfg Config, ingesterRing ring.ReadRing, pool *ring_client.Pool, dist *distributor.Distributor, partitionRing *ring.PartitionInstanceRing, logger log.Logger) *Rebalancer {
	r := &Rebalancer{
		cfg:           cfg,
		logger:        logger,
		ingesterRing:  ingesterRing,
		pool:          pool,
		distributor:   dist,
		partitionRing: partitionRing,
	}

	r.Service = services.NewTimerService(cfg.RebalanceInterval, r.starting, r.rebalance, nil)
	return r
}

func (r *Rebalancer) starting(_ context.Context) error {
	level.Info(r.logger).Log("msg", "nautilus rebalancer starting")
	return nil
}

func (r *Rebalancer) rebalance(ctx context.Context) error {
	globalRates, err := r.collectRates(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to collect ingester rates", "err", err)
		return nil // don't stop the service
	}

	pRing := r.partitionRing.PartitionRing()
	activePartitions := pRing.ActivePartitionIDs()
	if len(activePartitions) == 0 {
		level.Warn(r.logger).Log("msg", "no active partitions, skipping rebalance")
		return nil
	}

	if r.currentAssignment == nil {
		r.currentAssignment = assignment.EvenSplit(activePartitions)
		level.Info(r.logger).Log("msg", "initialized assignment with even split", "partitions", len(activePartitions))
	}

	newAssignment := r.runSlicer(r.currentAssignment, globalRates, activePartitions)
	if err := newAssignment.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "generated invalid assignment", "err", err)
		return nil
	}

	r.currentAssignment = newAssignment
	r.distributor.SetNautilusAssignment(newAssignment)

	level.Info(r.logger).Log("msg", "rebalance complete", "entries", len(newAssignment.Entries))
	return nil
}

// collectRates queries all ingesters and aggregates per-hash-bucket rates.
func (r *Rebalancer) collectRates(ctx context.Context) ([hashBucketCount]float64, error) {
	var globalRates [hashBucketCount]float64

	instances, err := r.ingesterRing.GetAllHealthy(ring.Read)
	if err != nil {
		return globalRates, err
	}

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

		for i := 0; i < hashBucketCount && i < len(resp.SamplesPerSecond); i++ {
			globalRates[i] += resp.SamplesPerSecond[i]
		}
	}

	return globalRates, nil
}

type rangeLoad struct {
	entry assignment.Entry
	load  float64
}

// runSlicer runs a simplified slicer algorithm: split, merge, and move to
// converge toward equal per-partition load.
func (r *Rebalancer) runSlicer(current *assignment.Assignment, rates [hashBucketCount]float64, activePartitions []int32) *assignment.Assignment {
	activeSet := make(map[int32]bool, len(activePartitions))
	for _, pid := range activePartitions {
		activeSet[pid] = true
	}

	entries := make([]rangeLoad, len(current.Entries))
	for i, e := range current.Entries {
		entries[i] = rangeLoad{
			entry: e,
			load:  loadForRange(e.Range, rates),
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

	// Split: bisect any range whose load exceeds 2x the mean range load.
	meanRangeLoad := totalLoad / float64(len(entries))
	splitThreshold := 2.0 * meanRangeLoad
	var newEntries []rangeLoad
	for _, rl := range entries {
		if rl.load > splitThreshold && rl.entry.Range.Size() > 1 {
			mid := rl.entry.Range.Lo + uint32((uint64(rl.entry.Range.Hi)-uint64(rl.entry.Range.Lo))/2)
			left := assignment.HashRange{Lo: rl.entry.Range.Lo, Hi: mid}
			right := assignment.HashRange{Lo: mid + 1, Hi: rl.entry.Range.Hi}
			leftLoad := loadForRange(left, rates)
			rightLoad := loadForRange(right, rates)
			newEntries = append(newEntries,
				rangeLoad{entry: assignment.Entry{Range: left, PartitionID: rl.entry.PartitionID}, load: leftLoad},
				rangeLoad{entry: assignment.Entry{Range: right, PartitionID: rl.entry.PartitionID}, load: rightLoad},
			)
		} else {
			newEntries = append(newEntries, rl)
		}
	}
	entries = newEntries

	// Merge: combine adjacent ranges on the same partition if both are underloaded.
	entries = mergeAdjacentUnderloaded(entries, meanRangeLoad*0.5, rates)

	// Move: greedily move ranges from the most loaded partition to the least.
	for i := 0; i < len(entries) && moved < movementBudget; i++ {
		// Recalculate partition loads.
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

		// Find the best range to move from hottest to coldest.
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

	// Sort by Lo to produce a valid assignment.
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

func loadForRange(hr assignment.HashRange, rates [hashBucketCount]float64) float64 {
	bucketSize := (uint64(math.MaxUint32) + 1) / hashBucketCount
	var total float64

	startBucket := int(uint64(hr.Lo) / bucketSize)
	endBucket := int(uint64(hr.Hi) / bucketSize)

	if startBucket >= hashBucketCount {
		startBucket = hashBucketCount - 1
	}
	if endBucket >= hashBucketCount {
		endBucket = hashBucketCount - 1
	}

	for b := startBucket; b <= endBucket; b++ {
		bLo := uint64(b) * bucketSize
		bHi := bLo + bucketSize - 1

		overlapLo := uint64(hr.Lo)
		if bLo > overlapLo {
			overlapLo = bLo
		}
		overlapHi := uint64(hr.Hi)
		if bHi < overlapHi {
			overlapHi = bHi
		}

		if overlapLo > overlapHi {
			continue
		}

		fraction := float64(overlapHi-overlapLo+1) / float64(bucketSize)
		total += rates[b] * fraction
	}

	return total
}

func mergeAdjacentUnderloaded(entries []rangeLoad, threshold float64, rates [hashBucketCount]float64) []rangeLoad {
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
			prev.load = loadForRange(merged, rates)
		} else {
			result = append(result, curr)
		}
	}
	return result
}
