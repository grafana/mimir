// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// simulatedIngester represents a single ingester that owns one partition
// and tracks per-hash-range EWMA ingestion rates. The rate value is
// then reported to the rebalancer as the per-range "series" count —
// standing in for R_r in the new model, where the rebalancer balances
// on in-memory series rather than sample rate. For simulation purposes
// the distinction doesn't matter: the test cares about whether the
// slicer converges a skewed distribution, and series tracks samples
// closely enough in steady state.
type simulatedIngester struct {
	partitionID int32
	ranges      []assignment.HashRange
	rates       []*util_math.EwmaRate
}

func newSimulatedIngester(partitionID int32) *simulatedIngester {
	return &simulatedIngester{partitionID: partitionID}
}

func (si *simulatedIngester) setRanges(ranges []assignment.HashRange) {
	sorted := make([]assignment.HashRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Lo < sorted[j].Lo })

	old := make(map[assignment.HashRange]*util_math.EwmaRate, len(si.ranges))
	for i, r := range si.ranges {
		old[r] = si.rates[i]
	}

	newRates := make([]*util_math.EwmaRate, len(sorted))
	for i, r := range sorted {
		if existing, ok := old[r]; ok {
			newRates[i] = existing
		} else {
			newRates[i] = util_math.NewEWMARate(0.2, time.Second)
		}
	}
	si.ranges = sorted
	si.rates = newRates
}

func (si *simulatedIngester) recordSamples(hash uint32, count int) {
	if len(si.ranges) == 0 {
		return
	}
	idx := sort.Search(len(si.ranges), func(i int) bool {
		return si.ranges[i].Lo > hash
	}) - 1
	if idx >= 0 && si.ranges[idx].Contains(hash) {
		si.rates[idx].Add(int64(count))
	}
}

func (si *simulatedIngester) tick() {
	for _, r := range si.rates {
		r.Tick()
	}
}

// snapshot returns the ingester's per-range load as rangeRate values.
// The EWMA rate is coerced to int64 and reported as series; the
// simulation's concept of "load" is the per-range rate, which in the
// new model stands in for R_r. All ranges are attributed to this
// ingester's partition — the simulation models one partition per
// simulated ingester with no residue.
func (si *simulatedIngester) snapshot() []rangeRate {
	out := make([]rangeRate, 0, len(si.ranges))
	for i, r := range si.ranges {
		out = append(out, rangeRate{
			hr:          r,
			series:      int64(si.rates[i].Rate()),
			partitionID: si.partitionID,
		})
	}
	return out
}

// totalSeries returns the simulated ingester's TotalActiveSeries value
// (L_i): the sum of per-range rates it currently owns.
func (si *simulatedIngester) totalSeries() int64 {
	var total int64
	for _, r := range si.rates {
		total += int64(r.Rate())
	}
	return total
}

// loadSource represents a source of load: a set of hashes with a fixed
// samples-per-second rate, simulating traffic from specific metric names.
type loadSource struct {
	hash           uint32
	samplesPerTick int
}

// simulation ties together dummy ingesters, a distributor (just the
// assignment lookup), and the rebalancer's runSlicer to form a closed loop.
type simulation struct {
	partitions []int32
	ingesters  map[int32]*simulatedIngester
	rebalancer *Rebalancer
	assignment *assignment.Assignment
	sources    []loadSource
}

func newSimulation(numPartitions int, cfg Config) *simulation {
	partitions := make([]int32, numPartitions)
	ingesters := make(map[int32]*simulatedIngester, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitions[i] = int32(i)
		ingesters[int32(i)] = newSimulatedIngester(int32(i))
	}

	return &simulation{
		partitions: partitions,
		ingesters:  ingesters,
		rebalancer: &Rebalancer{cfg: cfg, recentMoves: make(map[int32][]moveRecord)},
		assignment: assignment.FineEvenSplit(partitions, initialSlicesPerPartition),
	}
}

func (s *simulation) pushRangesToIngesters() {
	partitionRanges := make(map[int32][]assignment.HashRange)
	for _, e := range s.assignment.Entries {
		partitionRanges[e.PartitionID] = append(partitionRanges[e.PartitionID], e.Range)
	}
	for pid, ing := range s.ingesters {
		ing.setRanges(partitionRanges[pid])
	}
}

// ingestTick simulates one second of ingestion: each load source's
// samples are routed through the assignment to the correct partition's
// ingester, then all ingesters tick their EWMAs.
func (s *simulation) ingestTick() {
	for _, src := range s.sources {
		pid, ok := s.assignment.Lookup(src.hash)
		if !ok {
			continue
		}
		ing := s.ingesters[pid]
		ing.recordSamples(src.hash, src.samplesPerTick)
	}
	for _, ing := range s.ingesters {
		ing.tick()
	}
}

func (s *simulation) collectRates() []rangeRate {
	var all []rangeRate
	for _, ing := range s.ingesters {
		all = append(all, ing.snapshot()...)
	}
	return all
}

// partitionLFromIngesters returns the simulated partitionLByPID the
// rebalancer would compute in production: per-partition L, max over
// owners. Since the simulation has a single owner per partition, this
// is just each ingester's totalSeries.
func (s *simulation) partitionLFromIngesters() map[int32]int64 {
	out := make(map[int32]int64, len(s.ingesters))
	for pid, ing := range s.ingesters {
		out[pid] = ing.totalSeries()
	}
	return out
}

func (s *simulation) rebalance() {
	rates := s.collectRates()
	now := time.Now()
	s.rebalancer.pruneExpiredCooldowns(now)
	s.rebalancer.pruneRecentMoves(now)
	partL := s.partitionLFromIngesters()
	var actions []Action
	s.assignment, actions = s.rebalancer.runSlicer(s.assignment, rates, partL, s.rebalancer.recentMoves, s.partitions, now)
	s.rebalancer.recordMoveCooldowns(now, actions)
	s.rebalancer.recordRecentMoves(now, actions, buildLoadMap(rates))
	s.pushRangesToIngesters()
}

func (s *simulation) partitionLoads() map[int32]float64 {
	rates := s.collectRates()
	lm := buildLoadMap(rates)
	loads := make(map[int32]float64)
	for _, e := range s.assignment.Entries {
		loads[e.PartitionID] += float64(lm.seriesAt(e.PartitionID, e.Range))
	}
	return loads
}

// imbalanceRatio returns max / mean partition load, per the Slicer
// paper's definition. 1.0 means perfectly balanced.
func (s *simulation) imbalanceRatio() float64 {
	loads := s.partitionLoads()
	if len(loads) == 0 {
		return 0
	}
	maxLoad := -1.0
	total := 0.0
	for _, l := range loads {
		if l > maxLoad {
			maxLoad = l
		}
		total += l
	}
	avg := total / float64(len(loads))
	if avg == 0 {
		return 0
	}
	return maxLoad / avg
}

func logRound(t *testing.T, round int, s *simulation) {
	loads := s.partitionLoads()
	total := 0.0
	parts := make([]string, 0, len(loads))
	for _, pid := range s.partitions {
		l := loads[pid]
		total += l
		parts = append(parts, fmt.Sprintf("p%d=%.0f", pid, l))
	}
	avg := total / float64(len(loads))
	imbalance := s.imbalanceRatio()
	t.Logf("Round %2d: %s | total=%.0f avg=%.0f imbalance=%.2f entries=%d",
		round, strings.Join(parts, " "), total, avg, imbalance, len(s.assignment.Entries))
}

// simCfg returns a Config suited for the simulation: series-only load
// model with the movable-budget guard disabled. The simulation's
// ingesters update their reported L immediately when ranges move
// (no TSDB head compaction lag), so the recentMoves accounting —
// which exists specifically to compensate for that lag in production
// — would over-constrain the simulation and prevent convergence.
// MovementBudget and MoveCooldown remain as churn guards.
func simCfg(movementBudget float64) Config {
	return Config{
		MovementBudget:     movementBudget,
		CompactionInterval: 0,
	}
}

// TestSimulation_SkewedLoadConverges simulates a realistic scenario:
// 4 partitions, 1000 metric "sources" where ~10% of sources produce
// 90% of the load (a common production pattern). The sources have
// hashes that initially land on partition 0's hash space,
// creating heavy skew. We verify that the rebalancer converges
// to within 30% imbalance over multiple rounds.
func TestSimulation_SkewedLoadConverges(t *testing.T) {
	const (
		numPartitions   = 4
		numSources      = 500
		ewmaWarmupTicks = 30
		rebalanceRounds = 20
		ticksPerRound   = 60
	)

	sim := newSimulation(numPartitions, simCfg(0.09))

	// Create load sources concentrated in partition 0's initial hash space.
	// Use FineEvenSplit: partition 0 owns hashes [0, MaxUint32/4).
	p0MaxHash := uint32(math.MaxUint32 / 4)

	for i := 0; i < numSources; i++ {
		hash := uint32(float64(i) / float64(numSources) * float64(p0MaxHash))
		rate := 10 // base: 10 samples/sec
		if i < numSources/10 {
			rate = 900 // top 10% produce 90x more
		}
		sim.sources = append(sim.sources, loadSource{
			hash:           hash,
			samplesPerTick: rate,
		})
	}

	sim.pushRangesToIngesters()

	for tick := 0; tick < ewmaWarmupTicks; tick++ {
		sim.ingestTick()
	}

	t.Logf("--- Initial state (all load on partition 0) ---")
	logRound(t, 0, sim)

	initialImbalance := sim.imbalanceRatio()
	require.Greater(t, initialImbalance, 2.0,
		"initial setup should be heavily imbalanced (max/mean > 2.0)")

	for round := 1; round <= rebalanceRounds; round++ {
		sim.rebalance()

		for tick := 0; tick < ticksPerRound; tick++ {
			sim.ingestTick()
		}

		logRound(t, round, sim)
	}

	finalImbalance := sim.imbalanceRatio()
	t.Logf("--- Final: imbalance went from %.2f to %.2f ---", initialImbalance, finalImbalance)

	require.Less(t, finalImbalance, 1.3,
		"rebalancer should converge to max/mean < 1.3, got %.2f", finalImbalance)
	// The deviation from perfectly balanced (max/mean - 1) should drop
	// by at least 50%.
	require.Less(t, finalImbalance-1.0, (initialImbalance-1.0)*0.5,
		"rebalancer should halve the deviation from balanced")
}

// TestSimulation_RealisticProductionWorkload simulates a scenario matching
// production observations: ~50k samples/sec total across 6 partitions,
// with heavy skew where one partition gets 5x the average.
func TestSimulation_RealisticProductionWorkload(t *testing.T) {
	const (
		numPartitions   = 6
		ewmaWarmupTicks = 30
		rebalanceRounds = 30
		ticksPerRound   = 60
	)

	sim := newSimulation(numPartitions, simCfg(0.09))

	// Spread 200 sources across the hash space with varying intensities.
	// Cluster hot sources in the first 1/6 of the space (partition 0).
	hashSpacePerPartition := uint64(math.MaxUint32+1) / uint64(numPartitions)

	// Hot sources: 20 sources in partition 0's space, each doing 2000 samples/sec
	for i := 0; i < 20; i++ {
		hash := uint32(uint64(i) * hashSpacePerPartition / 25)
		sim.sources = append(sim.sources, loadSource{
			hash:           hash,
			samplesPerTick: 2000,
		})
	}

	// Medium sources: 30 sources in partition 1's space, each doing 200 samples/sec
	for i := 0; i < 30; i++ {
		hash := uint32(hashSpacePerPartition + uint64(i)*hashSpacePerPartition/35)
		sim.sources = append(sim.sources, loadSource{
			hash:           hash,
			samplesPerTick: 200,
		})
	}

	// Light sources: spread across remaining partitions, 50 samples/sec each
	for pid := int32(2); pid < int32(numPartitions); pid++ {
		for i := 0; i < 10; i++ {
			hash := uint32(uint64(pid)*hashSpacePerPartition + uint64(i)*hashSpacePerPartition/15)
			sim.sources = append(sim.sources, loadSource{
				hash:           hash,
				samplesPerTick: 50,
			})
		}
	}

	sim.pushRangesToIngesters()

	for tick := 0; tick < ewmaWarmupTicks; tick++ {
		sim.ingestTick()
	}

	t.Logf("--- Initial state ---")
	logRound(t, 0, sim)

	initialImbalance := sim.imbalanceRatio()

	for round := 1; round <= rebalanceRounds; round++ {
		sim.rebalance()
		for tick := 0; tick < ticksPerRound; tick++ {
			sim.ingestTick()
		}
		logRound(t, round, sim)
	}

	finalImbalance := sim.imbalanceRatio()
	t.Logf("--- Final: imbalance %.2f -> %.2f ---", initialImbalance, finalImbalance)

	require.Less(t, finalImbalance, 1.5,
		"should converge to max/mean < 1.5, got %.2f", finalImbalance)
}

// TestSimulation_LoadShiftMidway verifies that the rebalancer adapts
// when load patterns change: first partition 0 is hot, then after
// stabilization, load shifts to partition 3.
func TestSimulation_LoadShiftMidway(t *testing.T) {
	const (
		numPartitions   = 4
		ewmaWarmupTicks = 30
		ticksPerRound   = 60
	)

	sim := newSimulation(numPartitions, simCfg(0.09))

	hashSpacePerPartition := uint64(math.MaxUint32+1) / uint64(numPartitions)

	// Phase 1: all hot load on partition 0.
	for i := 0; i < 20; i++ {
		hash := uint32(uint64(i) * hashSpacePerPartition / 25)
		sim.sources = append(sim.sources, loadSource{hash: hash, samplesPerTick: 500})
	}
	// Light load on all partitions.
	for pid := int32(0); pid < int32(numPartitions); pid++ {
		for i := 0; i < 5; i++ {
			hash := uint32(uint64(pid)*hashSpacePerPartition + uint64(i)*hashSpacePerPartition/10)
			sim.sources = append(sim.sources, loadSource{hash: hash, samplesPerTick: 20})
		}
	}

	sim.pushRangesToIngesters()
	for tick := 0; tick < ewmaWarmupTicks; tick++ {
		sim.ingestTick()
	}

	t.Logf("--- Phase 1: hot load on partition 0 ---")
	logRound(t, 0, sim)

	for round := 1; round <= 10; round++ {
		sim.rebalance()
		for tick := 0; tick < ticksPerRound; tick++ {
			sim.ingestTick()
		}
		logRound(t, round, sim)
	}

	phase1Imbalance := sim.imbalanceRatio()
	require.Less(t, phase1Imbalance, 1.4,
		"phase 1 should converge to max/mean < 1.4, got %.2f", phase1Imbalance)

	// Phase 2: shift load -- partition 0's hot sources go cold,
	// partition 3 gets new hot sources.
	t.Logf("--- Phase 2: shifting hot load to partition 3 ---")
	for i := 0; i < 20; i++ {
		sim.sources[i].samplesPerTick = 10 // cool down p0
	}
	newHotStart := uint32(3 * uint64(hashSpacePerPartition))
	for i := 0; i < 20; i++ {
		hash := newHotStart + uint32(uint64(i)*hashSpacePerPartition/25)
		sim.sources = append(sim.sources, loadSource{hash: hash, samplesPerTick: 500})
	}

	// Let EWMA adapt.
	for tick := 0; tick < ewmaWarmupTicks; tick++ {
		sim.ingestTick()
	}

	logRound(t, 11, sim)

	for round := 12; round <= 25; round++ {
		sim.rebalance()
		for tick := 0; tick < ticksPerRound; tick++ {
			sim.ingestTick()
		}
		logRound(t, round, sim)
	}

	phase2Imbalance := sim.imbalanceRatio()
	t.Logf("--- Phase 2 final imbalance: %.2f ---", phase2Imbalance)
	require.Less(t, phase2Imbalance, 1.5,
		"phase 2 should re-converge to max/mean < 1.5, got %.2f", phase2Imbalance)
}

// TestSimulation_EvenLoadStaysStable verifies that an already-balanced
// system is not disrupted by the rebalancer.
func TestSimulation_EvenLoadStaysStable(t *testing.T) {
	const (
		numPartitions   = 4
		ewmaWarmupTicks = 30
		rebalanceRounds = 10
		ticksPerRound   = 60
	)

	sim := newSimulation(numPartitions, simCfg(0.09))

	// Spread sources evenly across all partitions.
	hashSpacePerPartition := uint64(math.MaxUint32+1) / uint64(numPartitions)
	for pid := int32(0); pid < int32(numPartitions); pid++ {
		for i := 0; i < 25; i++ {
			hash := uint32(uint64(pid)*hashSpacePerPartition + uint64(i)*hashSpacePerPartition/30)
			sim.sources = append(sim.sources, loadSource{
				hash:           hash,
				samplesPerTick: 100,
			})
		}
	}

	sim.pushRangesToIngesters()
	for tick := 0; tick < ewmaWarmupTicks; tick++ {
		sim.ingestTick()
	}

	logRound(t, 0, sim)

	for round := 1; round <= rebalanceRounds; round++ {
		sim.rebalance()
		for tick := 0; tick < ticksPerRound; tick++ {
			sim.ingestTick()
		}
		logRound(t, round, sim)
	}

	finalImbalance := sim.imbalanceRatio()
	require.Less(t, finalImbalance, 1.3,
		"balanced load should stay balanced (max/mean < 1.3), got %.2f", finalImbalance)
}
