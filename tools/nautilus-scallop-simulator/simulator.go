// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
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
}

type RoundRecord struct {
	Tick int       `json:"tick"`
	Time time.Time `json:"time"`

	Static   ImbalancePoint `json:"static"`
	PrePlan  ImbalancePoint `json:"pre_plan"`
	PostPlan ImbalancePoint `json:"post_plan"`

	TotalLoad        float64 `json:"total_load"`
	RangeCount       int     `json:"range_count"`
	TenantPartitions int     `json:"tenant_partitions"`

	Actions []scallop.Action `json:"actions"`
}

type SimulationResult struct {
	FixtureName string           `json:"fixture"`
	Policy      scallop.Policy   `json:"policy"`
	Rounds      []RoundRecord    `json:"rounds"`
	Evaluation  EvaluationReport `json:"evaluation"`
	Utility     float64          `json:"utility"`
}

type simulator struct {
	fixture Fixture

	now time.Time

	assignment       *assignment.Assignment
	staticAssignment *assignment.Assignment
	partitions       []int32
	partitionOwners  map[int32]string
	replicas         []string
	readcaches       []*dummyReadcache
	workloads        map[string]TenantWorkload
	lastHostedAt     map[string]map[string]time.Time
}

// newSimulator builds deterministic topology, initial placement, workloads, and locality history for a fixture.
func newSimulator(fixture Fixture) (*simulator, error) {
	if err := fixture.validate(); err != nil {
		return nil, err
	}
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
	for tenantIndex, workload := range fixture.Tenants {
		workloads[workload.ID] = workload
		initialReplica := replicas[tenantIndex%len(replicas)]
		initialPartitions := make([]int32, 0, len(partitions))
		for _, partitionID := range partitions {
			if owners[partitionID] == initialReplica {
				initialPartitions = append(initialPartitions, partitionID)
			}
		}
		destinations := make([]int32, fixture.InitialRanges)
		for i := range destinations {
			destinations[i] = initialPartitions[i%len(initialPartitions)]
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
		fixture:          fixture,
		now:              start,
		assignment:       cloneAssignment(initial),
		staticAssignment: cloneAssignment(initial),
		partitions:       partitions,
		partitionOwners:  owners,
		replicas:         replicas,
		readcaches:       readcaches,
		workloads:        workloads,
		lastHostedAt:     map[string]map[string]time.Time{},
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
		static, err := sim.observe(sim.staticAssignment, tick)
		if err != nil {
			return SimulationResult{}, err
		}
		snapshot := scallop.Snapshot{
			At:               sim.now,
			Assignment:       cloneAssignment(sim.assignment),
			RangeLoads:       pre.RangeLoads,
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
		post, err := sim.observe(sim.assignment, tick)
		if err != nil {
			return SimulationResult{}, err
		}
		sim.updateLocality(sim.assignment, sim.now)

		rounds = append(rounds, RoundRecord{
			Tick:             tick,
			Time:             sim.now,
			Static:           imbalancePoint(static),
			PrePlan:          imbalancePoint(pre),
			PostPlan:         imbalancePoint(post),
			TotalLoad:        post.TotalLoad,
			RangeCount:       len(sim.assignment.Entries),
			TenantPartitions: tenantPartitionCount(sim.assignment),
			Actions:          plan.Actions,
		})
		sim.now = sim.now.Add(time.Duration(fixture.TickSeconds) * time.Second)
	}

	evaluation := evaluate(fixture, rounds)
	return SimulationResult{
		FixtureName: fixture.Name,
		Policy:      policy,
		Rounds:      rounds,
		Evaluation:  evaluation,
		Utility:     fixedUtility(evaluation),
	}, nil
}

// observe analytically computes the exact load each range, partition, and dummy readcache owns at a tick.
func (s *simulator) observe(a *assignment.Assignment, tick int) (loadView, error) {
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
		load := workload.integrate(entry.Range, tick)
		key := scallop.RangeKey{TenantID: entry.TenantID, Range: entry.Range}
		view.RangeLoads[key] = load
		view.PartitionLoads[entry.PartitionID] += load
		view.TotalLoad += load

		owner := s.partitionOwners[entry.PartitionID]
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
		Partition: peakExcess(view.PartitionLoads),
		Replica:   peakExcess(view.ReplicaLoads),
	}
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
