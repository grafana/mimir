// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestGaussianIntegrationAndTemporalProfiles(t *testing.T) {
	workload := TenantWorkload{
		ID:       "tenant-a",
		Baseline: 40,
		Components: []GaussianComponent{{
			Center: 0.2,
			Width:  0.05,
			Amplitude: TemporalAmplitude{
				Kind:  "constant",
				Value: 100,
			},
		}},
	}
	full := workload.integrate(assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, 0)
	require.InDelta(t, 140, full, 1e-9)

	growing := TemporalAmplitude{Kind: "linear", StartTick: 0, EndTick: 10, StartValue: 10, EndValue: 30}
	require.Equal(t, 10.0, growing.at(-1))
	require.Equal(t, 20.0, growing.at(5))
	require.Equal(t, 30.0, growing.at(11))

	decaying := TemporalAmplitude{Kind: "linear", StartTick: 0, EndTick: 10, StartValue: 30, EndValue: 10}
	require.Equal(t, 20.0, decaying.at(5))

	envelope := TemporalAmplitude{Kind: "gaussian", CenterTick: 5, TimeWidth: 2, Peak: 50}
	require.Equal(t, 50.0, envelope.at(5))
	require.Less(t, envelope.at(0), envelope.at(5))
}

func TestTrafficNoiseIsDeterministicCorrelatedAndMeanPreserving(t *testing.T) {
	fixture := Fixture{
		Ticks: 20_000,
		TrafficNoise: &TrafficNoiseConfig{
			CoefficientOfVariation: 0.12,
			CorrelationTicks:       2,
			Seed:                   30,
		},
		Tenants: []TenantWorkload{{
			ID: "tenant-a",
			Components: []GaussianComponent{
				{Amplitude: TemporalAmplitude{Kind: "constant", Value: 1}},
				{Amplitude: TemporalAmplitude{Kind: "constant", Value: 1}},
			},
		}, {
			ID: "tenant-b",
			Components: []GaussianComponent{{
				Amplitude: TemporalAmplitude{Kind: "constant", Value: 1},
			}},
		}},
	}
	first := prepareFixtureTrafficNoise(fixture)
	second := prepareFixtureTrafficNoise(fixture)
	require.Equal(t, first.Tenants[0].Components[0].noiseMultipliers, second.Tenants[0].Components[0].noiseMultipliers)

	values := first.Tenants[0].Components[0].noiseMultipliers
	require.NotEqual(t, values, first.Tenants[0].Components[1].noiseMultipliers)
	require.NotEqual(t, values, first.Tenants[1].Components[0].noiseMultipliers)
	sum := 0.0
	logs := make([]float64, len(values))
	for i, value := range values {
		require.Positive(t, value)
		sum += value
		logs[i] = math.Log(value)
	}
	require.InDelta(t, 1, sum/float64(len(values)), 0.01)
	require.InDelta(t, math.Exp(-0.5), lagOneCorrelation(logs), 0.03)

	fixture.TrafficNoise.Seed++
	different := prepareFixtureTrafficNoise(fixture)
	require.NotEqual(t, values, different.Tenants[0].Components[0].noiseMultipliers)
}

func TestTrafficNoiseDefaultsAndExplicitDisable(t *testing.T) {
	fixture := Fixture{Ticks: 3, Tenants: []TenantWorkload{{
		ID: "tenant-a",
		Components: []GaussianComponent{{
			Amplitude: TemporalAmplitude{Kind: "constant", Value: 10},
		}},
	}}}
	defaulted := prepareFixtureTrafficNoise(fixture)
	require.Equal(t, defaultTrafficNoiseCV, defaulted.effectiveTrafficNoise().CoefficientOfVariation)
	require.Len(t, defaulted.Tenants[0].Components[0].noiseMultipliers, fixture.Ticks)

	fixture.TrafficNoise = &TrafficNoiseConfig{CoefficientOfVariation: 0, CorrelationTicks: 1, Seed: 99}
	disabled := prepareFixtureTrafficNoise(fixture)
	require.Empty(t, disabled.Tenants[0].Components[0].noiseMultipliers)
	require.Equal(t, 10.0, disabled.Tenants[0].Components[0].at(2))
}

func TestNoisyGaussianIntegrationConservesSplitAndMergeLoad(t *testing.T) {
	fixture := prepareFixtureTrafficNoise(Fixture{
		Ticks: 2,
		Tenants: []TenantWorkload{{
			ID:       "tenant-a",
			Baseline: 7,
			Components: []GaussianComponent{{
				Center:    0.23,
				Width:     0.04,
				Amplitude: TemporalAmplitude{Kind: "constant", Value: 100},
			}},
		}},
	})
	workload := fixture.Tenants[0]
	left := assignment.HashRange{Lo: 0, Hi: math.MaxInt32}
	right := assignment.HashRange{Lo: math.MaxInt32 + 1, Hi: math.MaxUint32}
	full := assignment.HashRange{Lo: 0, Hi: math.MaxUint32}
	for tick := range fixture.Ticks {
		first := workload.integrate(full, tick)
		second := workload.integrate(full, tick)
		require.Equal(t, first, second, "same-tick integration must be stable")
		require.InDelta(t, first, workload.integrate(left, tick)+workload.integrate(right, tick), 1e-10)
	}
}

func TestDummyReadcacheImmediatelyExhibitsMovedGaussianLoad(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture := fixtureByName(t, fixtures, "single-tenant-static")
	sim, err := newSimulator(fixture)
	require.NoError(t, err)

	before, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)
	moved := sim.assignment.Entries[0]
	oldPartition := moved.PartitionID
	newPartition := int32(1)
	require.NotEqual(t, sim.partitionOwners[oldPartition], sim.partitionOwners[newPartition])
	load := before.RangeLoads[scallop.RangeKey{TenantID: moved.TenantID, Range: moved.Range}]

	afterAssignment := cloneAssignment(sim.assignment)
	afterAssignment.Entries[0].PartitionID = newPartition
	after, err := sim.observe(afterAssignment, 0)
	require.NoError(t, err)

	require.InDelta(t, before.PartitionLoads[oldPartition]-load, after.PartitionLoads[oldPartition], 1e-12)
	require.InDelta(t, before.PartitionLoads[newPartition]+load, after.PartitionLoads[newPartition], 1e-12)
	require.InDelta(t, before.TotalLoad, after.TotalLoad, 1e-12)
	require.InDelta(t, load, after.RangeLoads[scallop.RangeKey{TenantID: moved.TenantID, Range: moved.Range}], 1e-12)
}

func TestSplitChildrenBecomeObservedOnNextSimulatorObservation(t *testing.T) {
	fixture := Fixture{
		Name:               "split-observation",
		Ticks:              2,
		TickSeconds:        60,
		Partitions:         2,
		Readcaches:         2,
		InitialRanges:      2,
		ImbalanceThreshold: 0.2,
		Tenants: []TenantWorkload{{
			ID: "tenant-a",
			Components: []GaussianComponent{{
				Center:    0.2,
				Width:     0.05,
				Amplitude: TemporalAmplitude{Kind: "constant", Value: 100},
			}},
		}},
	}
	sim, err := newSimulator(fixture)
	require.NoError(t, err)
	observed, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)

	policy := scallop.Policy{
		Weights:           scallop.Weights{Resolution: 1},
		ActionMultipliers: scallop.ActionMultipliers{Move: 1, Split: 1, Merge: 1, MovePartition: 1},
		CandidateSearch:   scallop.DefaultCandidateSearchLimits(),
		ActionLimits:      simulatorActionLimits(4),
	}
	policy.ActionLimits.Move = 0
	plan, err := scallop.Plan(scallop.Snapshot{
		At:               sim.now,
		Assignment:       sim.assignment,
		RangeLoads:       observed.RangeLoads,
		ActivePartitions: sim.partitions,
		PartitionOwners:  sim.partitionOwners,
		ActiveReplicas:   sim.replicas,
		LastHostedAt:     sim.lastHostedAt,
	}, policy)
	require.NoError(t, err)
	require.Len(t, plan.Actions, 2, "each original observed range may split once")
	require.Equal(t, scallop.ActionSplit, plan.Actions[0].Kind)
	require.Len(t, plan.Assignment.Entries, 4)

	nextObservation, err := sim.observe(plan.Assignment, 1)
	require.NoError(t, err)
	for _, entry := range plan.Assignment.Entries {
		_, ok := nextObservation.RangeLoads[scallop.RangeKey{TenantID: entry.TenantID, Range: entry.Range}]
		require.True(t, ok)
	}
}

func TestSimulatorAppliesProjectedPartitionOwnersBeforePostPlanObservation(t *testing.T) {
	fixture := Fixture{
		Name:               "partition-move",
		Ticks:              2,
		TickSeconds:        60,
		Partitions:         4,
		Readcaches:         2,
		InitialRanges:      4,
		ImbalanceThreshold: 0.2,
		Tenants: []TenantWorkload{{
			ID:       "tenant-a",
			Baseline: 100,
		}},
	}
	policy := scallop.DefaultPolicy()
	policy.Weights = scallop.Weights{ReplicaBalance: 1}
	policy.ActionLimits = scallop.ActionLimits{Total: 2, MovePartition: 2}

	result, err := simulateFixture(fixture, policy)
	require.NoError(t, err)
	require.Greater(t, result.Evaluation.RebalancingWork.PartitionMoves, 0)
	require.Equal(t, result.Rounds[0].PrePlan.Partition, result.Rounds[0].PostPlan.Partition)
	require.Less(t, result.Rounds[0].PostPlan.Replica, result.Rounds[0].PrePlan.Replica)
	require.Greater(t, result.Evaluation.RebalancingWork.PartitionMovedLoadFraction, 0.0)
	ticks, _ := buildFixtureRecords(fixture, result)
	require.Greater(t, ticks[0].PartitionMovedLoad, 0.0)
	require.Greater(t, ticks[0].PartitionMovedLoadFraction, 0.0)
}

// TestActionColdMovedLoadHandlesPartialPartitionWarmth preserves load-weighted partition locality.
func TestActionColdMovedLoadHandlesPartialPartitionWarmth(t *testing.T) {
	action := scallop.Action{
		MovedLoad: 8,
		Transition: scallop.CostBreakdown{
			TransitionLoad: 1,
			LocalityMiss:   0.25,
		},
	}
	require.Equal(t, 2.0, actionColdMovedLoad(action))
}

func TestRequiredFixturesRunClosedLoopAndEmitSevenGroups(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	require.Len(t, fixtures, 15)
	policy := scallop.DefaultPolicy()
	policy.ActionLimits = simulatorActionLimits(4)
	policy.Weights.Resolution = 0.005
	policy.Weights.Fragmentation = 0.001
	policy.Weights.TransitionEvents = 0
	policy.ActionLimits.MovePartition = 0
	observedLocalityDisruption := false

	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			result, err := simulateFixture(fixture, policy)
			require.NoError(t, err)
			require.Equal(t, fixture.effectiveTrafficNoise(), result.TrafficNoise)
			require.Equal(t, defaultTrafficNoiseCV, result.TrafficNoise.CoefficientOfVariation)
			require.Len(t, result.Rounds, fixture.Ticks)
			if fixture.Name == "balanced-no-op" {
				require.Zero(t, result.Evaluation.PeakImbalance.Partition.Integral)
				require.Zero(t, result.Evaluation.PeakImbalance.Replica.Integral)
			} else {
				require.Greater(t, result.Evaluation.PeakImbalance.Partition.Integral, 0.0)
			}
			require.Greater(t, result.Evaluation.StructuralFootprint.RangeSeconds, 0.0)
			observedLocalityDisruption = observedLocalityDisruption ||
				result.Evaluation.LocalityDisruption.ColdMovedLoad > 0

			requireSevenEvaluationGroups(t, result.Evaluation)

			switch fixture.Name {
			case "single-tenant-static":
				require.True(t, result.Rounds[1].WorkloadChanged,
					"static Gaussian shape should still receive default traffic variation")
			case "single-tenant-growing":
				require.Greater(t,
					result.Rounds[len(result.Rounds)-1].TotalLoad,
					result.Rounds[0].TotalLoad,
					"growing hotspot should increase observed load")
			case "two-tenant-opposing":
				actedTenants := actionTenants(result.Rounds)
				require.Contains(t, actedTenants, "tenant-growing")
				require.Contains(t, actedTenants, "tenant-shrinking")
			}
		})
	}
	require.True(t, observedLocalityDisruption, "fixtures must exercise the locality cost")
}

func TestEmbeddedFixtureCatalog(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)

	names := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		names = append(names, fixture.Name)
		require.NotNil(t, fixture.TrafficNoise, "fixture %s must check in its deterministic noise configuration", fixture.Name)
		require.Equal(t, TrafficNoiseConfig{
			CoefficientOfVariation: defaultTrafficNoiseCV,
			CorrelationTicks:       defaultTrafficNoiseCorrelationTicks,
			Seed:                   defaultTrafficNoiseSeed,
		}, *fixture.TrafficNoise)
	}
	require.Equal(t, []string{
		"balanced-no-op",
		"dev30-fluctuating",
		"dev30-overfragmented-recovery",
		"dev30-steady-state",
		"hotspot-shift",
		"large-cell-static",
		"many-tiny-tenants-consolidating",
		"mixed-heavy-tiny",
		"multi-modal",
		"same-workload-coarse",
		"same-workload-fine",
		"single-tenant-growing",
		"single-tenant-static",
		"temporal-burst",
		"two-tenant-opposing",
	}, names)
}

func TestTenantInitialRangeOverridesFlowThroughSimulationAndEvaluation(t *testing.T) {
	fixture := Fixture{
		Name:               "tenant-range-overrides",
		Ticks:              2,
		TickSeconds:        1,
		Partitions:         2,
		Readcaches:         1,
		InitialRanges:      4,
		ImbalanceThreshold: 0.2,
		Tenants: []TenantWorkload{
			{ID: "default"},
			{ID: "overridden", InitialRanges: 7},
		},
	}
	require.NoError(t, fixture.validate())

	sim, err := newSimulator(fixture)
	require.NoError(t, err)
	counts := tenantRangeCounts(sim.assignment)
	require.Equal(t, map[string]int{"default": 4, "overridden": 7}, counts)

	rounds := []RoundRecord{{
		RangeCount:        11,
		TenantPartitions:  2,
		TenantRangeCounts: counts,
	}}
	structure := evaluateStructure(fixture, rounds, 1)
	require.Equal(t, counts, structure.InitialRangeCounts)
	_, summary := buildFixtureRecords(fixture, SimulationResult{Rounds: rounds})
	require.Equal(t, counts, summary.Baseline.InitialRangeCounts)

	fixture.SettledRanges = 2
	fixture.Tenants[1].SettledRanges = 7
	require.Equal(t, 1, countUnsettled(counts, fixture))
	structure = evaluateStructure(fixture, rounds, 1)
	require.Equal(t, 1, structure.UnsettledTenants)
	require.Equal(t, 0, structure.SettleTicks["overridden"])

	fixture.Tenants[1].SettledRanges = -1
	require.EqualError(t, fixture.validate(), `tenant "overridden" settled ranges must be non-negative`)

	fixture.Tenants[1].SettledRanges = 0
	fixture.Tenants[1].InitialRanges = -1
	require.EqualError(t, fixture.validate(), `tenant "overridden" initial ranges must be non-negative`)
}

func TestLargeCellFixtureTopologyAndWorkloads(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture := fixtureByName(t, fixtures, "large-cell-static")
	require.Equal(t, 500, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Len(t, fixture.Tenants, 50)

	centers := map[float64]struct{}{}
	amplitudes := map[float64]struct{}{}
	for _, tenant := range fixture.Tenants {
		require.Len(t, tenant.Components, 1)
		component := tenant.Components[0]
		require.Equal(t, "constant", component.Amplitude.Kind)
		centers[component.Center] = struct{}{}
		amplitudes[component.Amplitude.Value] = struct{}{}
	}
	require.Len(t, centers, len(fixture.Tenants))
	require.Greater(t, len(amplitudes), 10)

	sim, err := newSimulator(fixture)
	require.NoError(t, err)
	observation, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)
	require.Len(t, observation.PartitionLoads, fixture.Partitions)
	require.Len(t, observation.ReplicaLoads, fixture.Readcaches)
	require.Len(t, observation.RangeLoads, fixture.InitialRanges*len(fixture.Tenants))
	require.Greater(t, observation.TotalLoad, 0.0)

	policy := scallop.DefaultPolicy()
	policy.ActionLimits = simulatorActionLimits(4)
	first, err := simulateFixture(fixture, policy)
	require.NoError(t, err)
	second, err := simulateFixture(fixture, policy)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first.Rounds, fixture.Ticks)
	requireSevenEvaluationGroups(t, first.Evaluation)
	require.Equal(t, fixture.Ticks, first.CandidateSearch.RoundsTruncated)
	require.LessOrEqual(t,
		first.CandidateSearch.FullyScored.Total,
		first.CandidateSearch.Iterations*policy.CandidateSearch.MaxFullyScored,
	)
	require.Greater(t, first.CandidateSearch.Discarded.Total, 0)
	require.Equal(t, first.CandidateSearch.Discarded.Total,
		first.CandidateSearch.DiscardedByBudget.MoveSources+
			first.CandidateSearch.DiscardedByBudget.Destinations+
			first.CandidateSearch.DiscardedByBudget.Merges+
			first.CandidateSearch.DiscardedByBudget.PartitionMoveSources+
			first.CandidateSearch.DiscardedByBudget.PartitionMoveDestinations+
			first.CandidateSearch.DiscardedByBudget.PartitionMoves+
			first.CandidateSearch.DiscardedByBudget.FullyScored,
	)
	for _, round := range first.Rounds {
		require.LessOrEqual(t,
			round.CandidateSearch.FullyScored.Total,
			round.CandidateSearch.Iterations*policy.CandidateSearch.MaxFullyScored,
		)
	}
}

func TestDev30FluctuatingFixtureTopology(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture := fixtureByName(t, fixtures, "dev30-fluctuating")
	require.Equal(t, 300, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Equal(t, 4, fixture.InitialRanges)
	require.Equal(t, 30, fixture.TickSeconds)
	require.GreaterOrEqual(t, fixture.Ticks, 8)
	require.Len(t, fixture.Tenants, 200)
	require.Equal(t, defaultTrafficNoiseCV, fixture.effectiveTrafficNoise().CoefficientOfVariation)
	centers := map[float64]struct{}{}
	loads := map[float64]struct{}{}
	for _, tenant := range fixture.Tenants {
		require.Len(t, tenant.Components, 1)
		component := tenant.Components[0]
		require.Equal(t, "constant", component.Amplitude.Kind)
		centers[component.Center] = struct{}{}
		loads[component.Amplitude.Value] = struct{}{}
	}
	require.Greater(t, len(centers), 100)
	require.Greater(t, len(loads), 100)
}

func TestExpandedFixtureWorkloadShapes(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)

	balanced := fixtureByName(t, fixtures, "balanced-no-op")
	sim, err := newSimulator(balanced)
	require.NoError(t, err)
	observation, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)
	require.Equal(t, ImbalancePoint{PartitionFloor: 1}, imbalancePoint(observation))
	result, err := simulateFixture(balanced, scallop.DefaultPolicy())
	require.NoError(t, err)
	for _, round := range result.Rounds {
		require.Empty(t, round.Actions)
	}

	multiModal := fixtureByName(t, fixtures, "multi-modal")
	require.GreaterOrEqual(t, len(multiModal.Tenants[0].Components), 3)

	burst := fixtureByName(t, fixtures, "temporal-burst")
	require.Equal(t, "gaussian", burst.Tenants[0].Components[0].Amplitude.Kind)
	require.Greater(t, burst.Tenants[0].Components[0].Amplitude.at(6),
		burst.Tenants[0].Components[0].Amplitude.at(0))

	shift := fixtureByName(t, fixtures, "hotspot-shift")
	require.Len(t, shift.Tenants[0].Components, 2)
	oldHotspot, newHotspot := shift.Tenants[0].Components[0], shift.Tenants[0].Components[1]
	require.NotEqual(t, oldHotspot.Center, newHotspot.Center)
	require.Greater(t, oldHotspot.Amplitude.at(0), oldHotspot.Amplitude.at(shift.Ticks-1))
	require.Less(t, newHotspot.Amplitude.at(0), newHotspot.Amplitude.at(shift.Ticks-1))

	mixed := fixtureByName(t, fixtures, "mixed-heavy-tiny")
	heavyLoad := mixed.Tenants[0].integrate(assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, 0)
	tinyLoad := mixed.Tenants[2].integrate(assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, 0)
	require.Greater(t, heavyLoad, 1_000*tinyLoad)
	require.Equal(t, 16, mixed.initialRangesForTenant(mixed.Tenants[2]))
}

func TestDev30OverfragmentedRecoveryFixtureDemonstratesSustainedRecovery(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture := fixtureByName(t, fixtures, "dev30-overfragmented-recovery")
	require.Equal(t, 300, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Equal(t, 45_000, fixture.initialRangesForTenant(fixture.Tenants[0]))
	require.Equal(t, 30_000, fixture.initialRangesForTenant(fixture.Tenants[1]))
	for _, tenant := range fixture.Tenants[2:] {
		require.Equal(t, fixture.InitialRanges, fixture.initialRangesForTenant(tenant))
	}

	result, err := simulateFixture(fixture, scallop.DefaultPolicy())
	require.NoError(t, err)
	structure := result.Evaluation.StructuralFootprint
	require.Equal(t, 45_000, structure.InitialRangeCounts["dominant-45k"])
	require.Equal(t, 30_000, structure.InitialRangeCounts["dominant-30k"])
	require.Positive(t, structure.MergesPerTenant["dominant-45k"])
	require.Positive(t, structure.MergesPerTenant["dominant-30k"])
	require.LessOrEqual(t, structure.MergesPerTenant["dominant-45k"],
		fixture.Ticks*scallop.DefaultPolicy().ActionLimits.MergePerTenant)
	require.LessOrEqual(t, structure.MergesPerTenant["dominant-30k"],
		fixture.Ticks*scallop.DefaultPolicy().ActionLimits.MergePerTenant)
	require.GreaterOrEqual(t, structure.MergesPerTenant["dominant-45k"], 1_000)
	require.GreaterOrEqual(t, structure.MergesPerTenant["dominant-30k"], 1_000)
	require.Less(t, structure.FinalRangeCounts["dominant-45k"], 44_000)
	require.Less(t, structure.FinalRangeCounts["dominant-30k"], 29_000)
	for _, tenantID := range []string{"dominant-45k", "dominant-30k"} {
		trajectory := structure.TenantRangeCountTrajectories[tenantID]
		for i := 1; i < len(trajectory); i++ {
			require.Less(t, trajectory[i], trajectory[i-1])
		}
	}
	require.Greater(t, result.CandidateSearch.RoundsTruncated, 0)
}

func TestAdaptationSeparatesRangeAndPartitionReversals(t *testing.T) {
	rounds := make([]RoundRecord, 8)
	rounds[0].Actions = []scallop.Action{{
		Kind: scallop.ActionMovePartition, PartitionID: 7, FromReplica: "a", ToReplica: "b",
		Transition: scallop.CostBreakdown{TransitionLoad: 0.2},
	}}
	rounds[2].Actions = []scallop.Action{{
		Kind: scallop.ActionMovePartition, PartitionID: 7, FromReplica: "b", ToReplica: "a",
		Transition: scallop.CostBreakdown{TransitionLoad: 0.3},
	}}
	movedRange := assignment.HashRange{Lo: 0, Hi: 99}
	rounds[3].Actions = []scallop.Action{{
		Kind: scallop.ActionMove, TenantID: "tenant-a", Range: movedRange, FromPartition: 1, ToPartition: 2,
	}}
	rounds[5].Actions = []scallop.Action{{
		Kind: scallop.ActionMove, TenantID: "tenant-a", Range: movedRange, FromPartition: 2, ToPartition: 1,
	}}
	rounds[7].Actions = []scallop.Action{{
		Kind: scallop.ActionMovePartition, PartitionID: 9, FromReplica: "a", ToReplica: "b",
	}}

	got := evaluateAdaptation(Fixture{TickSeconds: 1}, rounds)
	require.Equal(t, 1, got.RepeatedRangeMoves)
	require.Equal(t, 1, got.ReversedRangeMoves)
	require.Equal(t, 1, got.RepeatedPartitionMoves)
	require.Equal(t, 1, got.ReversedPartitionMoves)
	require.InDelta(t, 0.3, got.ReturnedPartitionLoadFraction, 1e-12)
	require.Equal(t, 1, got.StationaryTailPartitionMoves)
}

func TestFixedUtilityPricesPartitionMoveChurn(t *testing.T) {
	rangeWork := EvaluationReport{
		RebalancingWork: RebalancingWorkEvaluation{Moves: 64},
	}
	partitionWork := EvaluationReport{
		RebalancingWork: RebalancingWorkEvaluation{PartitionMoves: 1},
	}
	require.Equal(t, fixedUtility(rangeWork), fixedUtility(partitionWork))

	partitionWork.AdaptationStability.ReversedPartitionMoves = 1
	partitionWork.AdaptationStability.ReturnedPartitionLoadFraction = 0.1
	require.Greater(t, fixedUtility(partitionWork), fixedUtility(rangeWork))
}

func TestPolicyIdentityIncludesPartitionMoveMultiplier(t *testing.T) {
	first := scallop.DefaultPolicy()
	second := first
	second.ActionMultipliers.MovePartition *= 2
	require.NotEqual(t, policyKey(first), policyKey(second))
}

func TestCheckedInFixturesCompareCoarseAndFineInitialGranularities(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	coarse := fixtureByName(t, fixtures, "same-workload-coarse")
	fine := fixtureByName(t, fixtures, "same-workload-fine")
	require.Equal(t, coarse.Tenants, fine.Tenants)
	require.Equal(t, coarse.Partitions, fine.Partitions)
	require.Equal(t, coarse.Readcaches, fine.Readcaches)
	require.Equal(t, coarse.Ticks, fine.Ticks)
	require.Equal(t, coarse.TickSeconds, fine.TickSeconds)
	require.Equal(t, coarse.TrafficNoise, fine.TrafficNoise)
	require.Less(t, coarse.InitialRanges, fine.InitialRanges)

	policy := scallop.DefaultPolicy()
	policy.ActionLimits = simulatorActionLimits(4)
	coarseResult, err := simulateFixture(coarse, policy)
	require.NoError(t, err)
	fineResult, err := simulateFixture(fine, policy)
	require.NoError(t, err)
	require.Equal(t, coarse.InitialRanges,
		coarseResult.Evaluation.StructuralFootprint.InitialRangeCounts["granularity-tenant"])
	require.Equal(t, fine.InitialRanges,
		fineResult.Evaluation.StructuralFootprint.InitialRangeCounts["granularity-tenant"])
	require.Greater(t, fineResult.Evaluation.StructuralFootprint.RangeSeconds,
		coarseResult.Evaluation.StructuralFootprint.RangeSeconds)
}

func TestCompleteWeightSearchIsDeterministicAndWritesReports(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	seed := scallop.DefaultPolicy()
	config := defaultSearchConfig()

	first, err := runWeightSearch(fixtures, seed, config)
	require.NoError(t, err)
	replayed, err := evaluatePolicy(fixtures, first.Recommended.Policy, first.Recommended.Generation)
	require.NoError(t, err)
	require.GreaterOrEqual(t, first.EvaluatedPolicies, 100)
	require.Equal(t, first.Recommended, replayed)
	require.Greater(t, first.Recommended.Policy.ActionMultipliers.MovePartition, 1.0)
	recommendedDev30 := first.Recommended.Fixtures[0]
	for _, fixture := range first.Recommended.Fixtures {
		if fixture.FixtureName == "dev30-fluctuating" {
			recommendedDev30 = fixture
			break
		}
	}
	require.Equal(t, "dev30-fluctuating", recommendedDev30.FixtureName)
	require.Zero(t, recommendedDev30.Evaluation.AdaptationStability.ReversedPartitionMoves)
	require.Less(t, recommendedDev30.Evaluation.RebalancingWork.PartitionMoves, 56)
	for _, policy := range first.Ranked {
		for _, fixture := range policy.Fixtures {
			requireSevenEvaluationGroups(t, fixture.Evaluation)
			if fixture.FixtureName == "large-cell-static" ||
				fixture.FixtureName == "many-tiny-tenants-consolidating" ||
				fixture.FixtureName == "dev30-fluctuating" ||
				fixture.FixtureName == "dev30-overfragmented-recovery" {
				require.Greater(t, fixture.CandidateSearch.RoundsTruncated, 0)
			}
			require.LessOrEqual(t,
				fixture.CandidateSearch.Work.CompleteProjections,
				fixture.CandidateSearch.Iterations,
			)
		}
	}
	recommendation := buildRecommendationReport(first)
	require.Len(t, recommendation.Fixtures, len(fixtures))
	require.Len(t, recommendation.Replays, 7)
	require.Len(t, recommendation.NearestCompetitors, 5)
	require.Len(t, recommendation.Sensitivity.Cases, policyDimensions*2+6)

	outputDirectory := t.TempDir()
	require.NoError(t, writeReports(outputDirectory, first))
	for _, name := range []string{
		"search-report.json",
		"search-summary.csv",
		"recommended-policy.json",
		"sensitivity-summary.csv",
	} {
		info, err := os.Stat(filepath.Join(outputDirectory, name))
		require.NoError(t, err)
		require.Greater(t, info.Size(), int64(0))
	}

	summary, err := os.Open(filepath.Join(outputDirectory, "search-summary.csv"))
	require.NoError(t, err)
	defer summary.Close()
	records, err := csv.NewReader(summary).ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(records), 1)
	for _, record := range records[1:] {
		require.Len(t, record, len(records[0]))
	}

	sensitivity, err := os.Open(filepath.Join(outputDirectory, "sensitivity-summary.csv"))
	require.NoError(t, err)
	defer sensitivity.Close()
	sensitivityRecords, err := csv.NewReader(sensitivity).ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(sensitivityRecords), 1)
	for _, record := range sensitivityRecords[1:] {
		require.Len(t, record, len(sensitivityRecords[0]))
	}
}

func simulatorActionLimits(limit int) scallop.ActionLimits {
	return scallop.ActionLimits{
		Total:          limit,
		Move:           limit,
		MovePerTenant:  limit,
		Split:          limit,
		SplitPerTenant: limit,
		Merge:          limit,
		MergePerTenant: limit,
		MovePartition:  limit,
	}
}

func lagOneCorrelation(values []float64) float64 {
	leftMean, rightMean := 0.0, 0.0
	for i := 1; i < len(values); i++ {
		leftMean += values[i-1]
		rightMean += values[i]
	}
	count := float64(len(values) - 1)
	leftMean /= count
	rightMean /= count
	covariance, leftVariance, rightVariance := 0.0, 0.0, 0.0
	for i := 1; i < len(values); i++ {
		left := values[i-1] - leftMean
		right := values[i] - rightMean
		covariance += left * right
		leftVariance += left * left
		rightVariance += right * right
	}
	return covariance / math.Sqrt(leftVariance*rightVariance)
}

func requireSevenEvaluationGroups(t *testing.T, evaluation EvaluationReport) {
	t.Helper()
	encoded, err := json.Marshal(evaluation)
	require.NoError(t, err)
	for _, name := range []string{
		"peak_imbalance",
		"rebalancing_work",
		"structural_footprint",
		"locality_disruption",
		"adaptation_stability",
		"action_effectiveness",
		"imbalance_tracking",
	} {
		require.Contains(t, string(encoded), `"`+name+`"`)
	}
}

func fixtureByName(t *testing.T, fixtures []Fixture, name string) Fixture {
	t.Helper()
	for _, fixture := range fixtures {
		if fixture.Name == name {
			return fixture
		}
	}
	t.Fatalf("fixture %q not found", name)
	return Fixture{}
}

func actionTenants(rounds []RoundRecord) map[string]struct{} {
	out := map[string]struct{}{}
	for _, round := range rounds {
		for _, action := range round.Actions {
			out[action.TenantID] = struct{}{}
		}
	}
	return out
}
