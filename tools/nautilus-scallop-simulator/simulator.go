// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

type dummyReadcache struct {
	ID             string
	Partitions     []int32
	RangeLoads     map[scallop.RangeKey]float64
	PartitionLoads map[int32]float64
	TotalLoad      float64
}

type loadView struct {
	RangeLoads     map[scallop.RangeKey]float64
	PartitionLoads map[int32]float64
	ReplicaLoads   map[string]float64
	TotalLoad      float64
}

type ImbalancePoint struct {
	Partition float64 `json:"partition"`
	Replica   float64 `json:"replica"`
	// PartitionFloor is the coldest partition's load over the mean; peak excess
	// alone cannot see partitions drained toward zero.
	PartitionFloor float64 `json:"partition_floor"`
}

type RoundRecord struct {
	Tick int       `json:"tick"`
	Time time.Time `json:"time"`

	WorkloadChanged bool           `json:"workload_changed"`
	Static          ImbalancePoint `json:"static"`
	PrePlan         ImbalancePoint `json:"pre_plan"`
	PostPlan        ImbalancePoint `json:"post_plan"`

	TotalLoad         float64          `json:"total_load"`
	RangeCount        int              `json:"range_count"`
	TenantPartitions  int              `json:"tenant_partitions"`
	TenantRangeCounts map[string]int   `json:"tenant_range_counts"`
	PartitionOwners   map[int32]string `json:"partition_owners"`

	Actions         []scallop.Action                   `json:"actions"`
	InitialCost     scallop.CostBreakdown              `json:"initial_cost"`
	FinalCost       scallop.CostBreakdown              `json:"final_cost"`
	CandidateSearch scallop.CandidateSearchDiagnostics `json:"candidate_search"`
}

type CandidateSearchSummary struct {
	Limits                         scallop.CandidateSearchLimits   `json:"limits"`
	RoundsTruncated                int                             `json:"rounds_truncated"`
	Iterations                     int                             `json:"iterations"`
	LegalMoveSources               int                             `json:"legal_move_sources"`
	LegalMoveDestinations          int                             `json:"legal_move_destinations"`
	LegalPartitionMoveSources      int                             `json:"legal_partition_move_sources"`
	LegalPartitionMoveDestinations int                             `json:"legal_partition_move_destinations"`
	Legal                          scallop.CandidateCounts         `json:"legal"`
	Admitted                       scallop.CandidateCounts         `json:"admitted"`
	FullyScored                    scallop.CandidateCounts         `json:"fully_scored"`
	Discarded                      scallop.CandidateCounts         `json:"discarded"`
	DiscardedByBudget              scallop.CandidateBudgetDiscards `json:"discarded_by_budget"`
	Work                           scallop.CandidateSearchWork     `json:"work"`
	Pruned                         scallop.CandidateSearchPruned   `json:"pruned"`
}

type SimulationResult struct {
	FixtureName     string                 `json:"fixture"`
	TrafficNoise    TrafficNoiseConfig     `json:"traffic_noise"`
	Policy          scallop.Policy         `json:"policy"`
	Rounds          []RoundRecord          `json:"rounds"`
	Evaluation      EvaluationReport       `json:"evaluation"`
	CandidateSearch CandidateSearchSummary `json:"candidate_search"`
	Utility         float64                `json:"utility"`
}

type simulator struct {
	fixture Fixture

	now time.Time

	assignment            *assignment.Assignment
	staticAssignment      *assignment.Assignment
	partitions            []int32
	partitionOwners       map[int32]string
	staticPartitionOwners map[int32]string
	replicas              []string
	readcaches            []*dummyReadcache
	workloads             map[string]TenantWorkload
	lastHostedAt          map[string]map[string]time.Time
}

// newSimulator builds deterministic topology, initial placement, workloads, and locality history for a fixture.
func newSimulator(fixture Fixture) (*simulator, error) {
	if err := fixture.validate(); err != nil {
		return nil, err
	}
	fixture = prepareFixtureTrafficNoise(fixture)
	partitions := make([]int32, fixture.Partitions)
	owners := make(map[int32]string, fixture.Partitions)
	replicas := make([]string, fixture.Readcaches)
	readcaches := make([]*dummyReadcache, fixture.Readcaches)
	for i := range replicas {
		replicas[i] = fmt.Sprintf("readcache-%d", i)
		readcaches[i] = &dummyReadcache{ID: replicas[i]}
	}
	for i := range partitions {
		partitions[i] = int32(i)
		owner := replicas[i%len(replicas)]
		owners[int32(i)] = owner
		readcaches[i%len(readcaches)].Partitions = append(readcaches[i%len(readcaches)].Partitions, int32(i))
	}

	var entries []assignment.Entry
	workloads := make(map[string]TenantWorkload, len(fixture.Tenants))
	spreadCursor := 0
	for tenantIndex, workload := range fixture.Tenants {
		workloads[workload.ID] = workload
		destinations := make([]int32, fixture.initialRangesForTenant(workload))
		if fixture.InitialPlacement == initialPlacementSpread {
			for i := range destinations {
				destinations[i] = partitions[spreadCursor%len(partitions)]
				spreadCursor++
			}
		} else {
			initialReplica := replicas[tenantIndex%len(replicas)]
			initialPartitions := make([]int32, 0, len(partitions))
			for _, partitionID := range partitions {
				if owners[partitionID] == initialReplica {
					initialPartitions = append(initialPartitions, partitionID)
				}
			}
			for i := range destinations {
				destinations[i] = initialPartitions[i%len(initialPartitions)]
			}
		}
		initial := assignment.EvenSplitForTenant(workload.ID, destinations)
		entries = append(entries, initial.Entries...)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TenantID != entries[j].TenantID {
			return entries[i].TenantID < entries[j].TenantID
		}
		return entries[i].Range.Lo < entries[j].Range.Lo
	})
	initial := &assignment.Assignment{Entries: entries}
	if err := initial.Validate(); err != nil {
		return nil, fmt.Errorf("initial assignment: %w", err)
	}

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sim := &simulator{
		fixture:               fixture,
		now:                   start,
		assignment:            cloneAssignment(initial),
		staticAssignment:      cloneAssignment(initial),
		partitions:            partitions,
		partitionOwners:       owners,
		staticPartitionOwners: cloneOwners(owners),
		replicas:              replicas,
		readcaches:            readcaches,
		workloads:             workloads,
		lastHostedAt:          map[string]map[string]time.Time{},
	}
	sim.updateLocality(initial, start)
	return sim, nil
}

// simulateFixture runs the observe-plan-apply loop and evaluates the resulting trajectory for one policy.
func simulateFixture(fixture Fixture, policy scallop.Policy) (SimulationResult, error) {
	sim, err := newSimulator(fixture)
	if err != nil {
		return SimulationResult{}, err
	}
	rounds := make([]RoundRecord, 0, fixture.Ticks)
	for tick := 0; tick < fixture.Ticks; tick++ {
		pre, err := sim.observe(sim.assignment, tick)
		if err != nil {
			return SimulationResult{}, err
		}
		static, err := sim.observeWithOwners(sim.staticAssignment, tick, sim.staticPartitionOwners)
		if err != nil {
			return SimulationResult{}, err
		}
		snapshot := scallop.Snapshot{
			At:               sim.now,
			Assignment:       cloneAssignment(sim.assignment),
			RangeLoads:       sim.observedRangeLoads(pre, tick),
			ActivePartitions: append([]int32(nil), sim.partitions...),
			PartitionOwners:  cloneOwners(sim.partitionOwners),
			ActiveReplicas:   append([]string(nil), sim.replicas...),
			LastHostedAt:     cloneLocality(sim.lastHostedAt),
		}
		plan, err := scallop.Plan(snapshot, policy)
		if err != nil {
			return SimulationResult{}, fmt.Errorf("fixture %s tick %d: %w", fixture.Name, tick, err)
		}
		sim.assignment = cloneAssignment(plan.Assignment)
		sim.setPartitionOwners(plan.PartitionOwners)
		post, err := sim.observe(sim.assignment, tick)
		if err != nil {
			return SimulationResult{}, err
		}
		sim.updateLocality(sim.assignment, sim.now)

		rounds = append(rounds, RoundRecord{
			Tick:              tick,
			Time:              sim.now,
			WorkloadChanged:   sim.fixture.workloadChangedAt(tick),
			Static:            imbalancePoint(static),
			PrePlan:           imbalancePoint(pre),
			PostPlan:          imbalancePoint(post),
			TotalLoad:         post.TotalLoad,
			RangeCount:        len(sim.assignment.Entries),
			TenantPartitions:  tenantPartitionCount(sim.assignment),
			TenantRangeCounts: tenantRangeCounts(sim.assignment),
			PartitionOwners:   cloneOwners(sim.partitionOwners),
			Actions:           plan.Actions,
			InitialCost:       plan.InitialCost,
			FinalCost:         plan.FinalCost,
			CandidateSearch:   plan.CandidateSearch,
		})
		sim.now = sim.now.Add(time.Duration(fixture.TickSeconds) * time.Second)
	}

	evaluation := evaluate(sim.fixture, rounds)
	return SimulationResult{
		FixtureName:     fixture.Name,
		TrafficNoise:    sim.fixture.effectiveTrafficNoise(),
		Policy:          policy,
		Rounds:          rounds,
		Evaluation:      evaluation,
		CandidateSearch: summarizeCandidateSearch(rounds, policy.CandidateSearch),
		Utility:         fixedUtility(evaluation),
	}, nil
}

// summarizeCandidateSearch aggregates per-plan pruning diagnostics for one fixture run.
func summarizeCandidateSearch(rounds []RoundRecord, limits scallop.CandidateSearchLimits) CandidateSearchSummary {
	out := CandidateSearchSummary{Limits: limits}
	for _, round := range rounds {
		diagnostics := round.CandidateSearch
		if diagnostics.Truncated {
			out.RoundsTruncated++
		}
		out.Iterations += diagnostics.Iterations
		out.LegalMoveSources += diagnostics.LegalMoveSources
		out.LegalMoveDestinations += diagnostics.LegalMoveDestinations
		out.LegalPartitionMoveSources += diagnostics.LegalPartitionMoveSources
		out.LegalPartitionMoveDestinations += diagnostics.LegalPartitionMoveDestinations
		addCandidateCounts(&out.Legal, diagnostics.Legal)
		addCandidateCounts(&out.Admitted, diagnostics.Admitted)
		addCandidateCounts(&out.FullyScored, diagnostics.FullyScored)
		addCandidateCounts(&out.Discarded, diagnostics.Discarded)
		out.DiscardedByBudget.MoveSources += diagnostics.DiscardedByBudget.MoveSources
		out.DiscardedByBudget.Destinations += diagnostics.DiscardedByBudget.Destinations
		out.DiscardedByBudget.Merges += diagnostics.DiscardedByBudget.Merges
		out.DiscardedByBudget.PartitionMoveSources += diagnostics.DiscardedByBudget.PartitionMoveSources
		out.DiscardedByBudget.PartitionMoveDestinations += diagnostics.DiscardedByBudget.PartitionMoveDestinations
		out.DiscardedByBudget.PartitionMoves += diagnostics.DiscardedByBudget.PartitionMoves
		out.DiscardedByBudget.FullyScored += diagnostics.DiscardedByBudget.FullyScored
		out.Work.OffendersExpanded.Partitions += diagnostics.Work.OffendersExpanded.Partitions
		out.Work.OffendersExpanded.Replicas += diagnostics.Work.OffendersExpanded.Replicas
		out.Work.OffendersExpanded.Tenants += diagnostics.Work.OffendersExpanded.Tenants
		out.Work.RangesInspected += diagnostics.Work.RangesInspected
		out.Work.AdjacencyEdgesInspected += diagnostics.Work.AdjacencyEdgesInspected
		out.Work.DestinationsInspected += diagnostics.Work.DestinationsInspected
		addCandidateCounts(&out.Work.Yielded, diagnostics.Work.Yielded)
		out.Work.ExactDeltaScores += diagnostics.Work.ExactDeltaScores
		out.Work.CompleteProjections += diagnostics.Work.CompleteProjections
		out.Pruned.PartitionOffenders += diagnostics.Pruned.PartitionOffenders
		out.Pruned.ReplicaOffenders += diagnostics.Pruned.ReplicaOffenders
		out.Pruned.TenantOffenders += diagnostics.Pruned.TenantOffenders
		out.Pruned.Ranges += diagnostics.Pruned.Ranges
		out.Pruned.AdjacencyEdges += diagnostics.Pruned.AdjacencyEdges
		out.Pruned.DestinationPartitions += diagnostics.Pruned.DestinationPartitions
		out.Pruned.DestinationReplicas += diagnostics.Pruned.DestinationReplicas
	}
	return out
}

func addCandidateCounts(dst *scallop.CandidateCounts, src scallop.CandidateCounts) {
	dst.Move += src.Move
	dst.Split += src.Split
	dst.Merge += src.Merge
	dst.MovePartition += src.MovePartition
	dst.Total += src.Total
}

// observe analytically computes the exact load each range, partition, and dummy readcache owns at a tick.
func (s *simulator) observe(a *assignment.Assignment, tick int) (loadView, error) {
	return s.observeWithOwners(a, tick, s.partitionOwners)
}

// observeWithOwners computes exact load against an explicit projected or static ownership map.
func (s *simulator) observeWithOwners(
	a *assignment.Assignment,
	tick int,
	owners map[int32]string,
) (loadView, error) {
	for _, readcache := range s.readcaches {
		readcache.RangeLoads = map[scallop.RangeKey]float64{}
		readcache.PartitionLoads = map[int32]float64{}
		readcache.TotalLoad = 0
	}
	readcacheByID := make(map[string]*dummyReadcache, len(s.readcaches))
	for _, readcache := range s.readcaches {
		readcacheByID[readcache.ID] = readcache
	}

	view := loadView{
		RangeLoads:     make(map[scallop.RangeKey]float64, len(a.Entries)),
		PartitionLoads: make(map[int32]float64, len(s.partitions)),
		ReplicaLoads:   make(map[string]float64, len(s.replicas)),
	}
	for _, partitionID := range s.partitions {
		view.PartitionLoads[partitionID] = 0
	}
	for _, replica := range s.replicas {
		view.ReplicaLoads[replica] = 0
	}

	for _, entry := range a.Entries {
		workload, ok := s.workloads[entry.TenantID]
		if !ok {
			return loadView{}, fmt.Errorf("tenant %q has no workload", entry.TenantID)
		}
		load := workload.integrate(entry.Range, tick) * s.fixture.loadScale()
		key := scallop.RangeKey{TenantID: entry.TenantID, Range: entry.Range}
		view.RangeLoads[key] = load
		view.PartitionLoads[entry.PartitionID] += load
		view.TotalLoad += load

		owner := owners[entry.PartitionID]
		readcache := readcacheByID[owner]
		readcache.RangeLoads[key] = load
		readcache.PartitionLoads[entry.PartitionID] += load
		readcache.TotalLoad += load
	}
	for _, readcache := range s.readcaches {
		view.ReplicaLoads[readcache.ID] = readcache.TotalLoad
	}
	return view, nil
}

// observedRangeLoads returns the planner's view of exact range loads. Like
// windowed sample counting, each range's error variance is proportional to its
// load, so a partition's observed total has the configured relative error
// however finely its load is divided.
func (s *simulator) observedRangeLoads(exact loadView, tick int) map[scallop.RangeKey]float64 {
	noise := s.fixture.ObservationNoise
	if noise == nil || noise.PartitionCoefficientOfVariation == 0 || exact.TotalLoad <= 0 {
		return exact.RangeLoads
	}
	meanPartitionLoad := exact.TotalLoad / float64(len(s.partitions))
	out := make(map[scallop.RangeKey]float64, len(exact.RangeLoads))
	for key, load := range exact.RangeLoads {
		deviation := noise.PartitionCoefficientOfVariation * math.Sqrt(meanPartitionLoad*load)
		out[key] = math.Max(0, load+deviation*observationNormal(noise.Seed, tick, key))
	}
	return out
}

// observationNormal returns a standard-normal draw fixed by seed, tick, and range identity.
func observationNormal(seed int64, tick int, key scallop.RangeKey) float64 {
	hash := fnv.New64a()
	var encoded [8]byte
	for _, value := range []uint64{uint64(seed), uint64(tick), uint64(key.Range.Lo)<<32 | uint64(key.Range.Hi)} {
		binary.LittleEndian.PutUint64(encoded[:], value)
		_, _ = hash.Write(encoded[:])
	}
	_, _ = hash.Write([]byte(key.TenantID))
	state := hash.Sum64()
	u1 := splitMixUniform(&state)
	u2 := splitMixUniform(&state)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// setPartitionOwners applies projected logical ownership to simulator readcaches.
func (s *simulator) setPartitionOwners(owners map[int32]string) {
	s.partitionOwners = cloneOwners(owners)
	for _, readcache := range s.readcaches {
		readcache.Partitions = readcache.Partitions[:0]
	}
	byID := make(map[string]*dummyReadcache, len(s.readcaches))
	for _, readcache := range s.readcaches {
		byID[readcache.ID] = readcache
	}
	for _, partitionID := range s.partitions {
		owner := s.partitionOwners[partitionID]
		byID[owner].Partitions = append(byID[owner].Partitions, partitionID)
	}
}

// updateLocality records which logical replicas currently host each tenant for future move-cost decisions.
func (s *simulator) updateLocality(a *assignment.Assignment, at time.Time) {
	for _, entry := range a.Entries {
		byReplica := s.lastHostedAt[entry.TenantID]
		if byReplica == nil {
			byReplica = map[string]time.Time{}
			s.lastHostedAt[entry.TenantID] = byReplica
		}
		byReplica[s.partitionOwners[entry.PartitionID]] = at
	}
}

// imbalancePoint derives the partition and replica peak-over-mean excess reported for one observation.
func imbalancePoint(view loadView) ImbalancePoint {
	return ImbalancePoint{
		Partition:      peakExcess(view.PartitionLoads),
		Replica:        peakExcess(view.ReplicaLoads),
		PartitionFloor: floorRatio(view.PartitionLoads),
	}
}

// floorRatio computes min/mean, with one representing perfect or zero-load balance.
func floorRatio[K comparable](loads map[K]float64) float64 {
	if len(loads) == 0 {
		return 1
	}
	values := make([]float64, 0, len(loads))
	for _, load := range loads {
		values = append(values, load)
	}
	sort.Float64s(values)
	total := 0.0
	for _, load := range values {
		total += load
	}
	if total == 0 {
		return 1
	}
	return values[0] / (total / float64(len(loads)))
}

// peakExcess computes max/mean minus one, with zero representing perfect or zero-load balance.
func peakExcess[K comparable](loads map[K]float64) float64 {
	if len(loads) == 0 {
		return 0
	}
	values := make([]float64, 0, len(loads))
	for _, load := range loads {
		values = append(values, load)
	}
	sort.Float64s(values)
	total, maximum := 0.0, 0.0
	for _, load := range values {
		total += load
		if load > maximum {
			maximum = load
		}
	}
	if total == 0 {
		return 0
	}
	return maximum/(total/float64(len(loads))) - 1
}

// tenantPartitionCount measures current tenant fanout across partitions for structural evaluation.
func tenantPartitionCount(a *assignment.Assignment) int {
	seen := map[struct {
		tenant    string
		partition int32
	}]struct{}{}
	for _, entry := range a.Entries {
		seen[struct {
			tenant    string
			partition int32
		}{entry.TenantID, entry.PartitionID}] = struct{}{}
	}
	return len(seen)
}

// tenantRangeCounts exposes per-tenant structural progress for consolidation fixtures.
func tenantRangeCounts(a *assignment.Assignment) map[string]int {
	counts := map[string]int{}
	for _, entry := range a.Entries {
		counts[entry.TenantID]++
	}
	return counts
}

func cloneAssignment(a *assignment.Assignment) *assignment.Assignment {
	return &assignment.Assignment{Entries: append([]assignment.Entry(nil), a.Entries...)}
}

func cloneOwners(in map[int32]string) map[int32]string {
	out := make(map[int32]string, len(in))
	for partitionID, owner := range in {
		out[partitionID] = owner
	}
	return out
}

func cloneLocality(in map[string]map[string]time.Time) map[string]map[string]time.Time {
	out := make(map[string]map[string]time.Time, len(in))
	for tenant, byReplica := range in {
		out[tenant] = make(map[string]time.Time, len(byReplica))
		for replica, at := range byReplica {
			out[tenant][replica] = at
		}
	}
	return out
}
