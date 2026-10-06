// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	require.Len(t, fixtures, 6)
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
			require.Greater(t, result.Evaluation.PeakImbalance.Partition.Integral, 0.0)
			require.Greater(t, result.Evaluation.StructuralFootprint.RangeSeconds, 0.0)
			observedLocalityDisruption = observedLocalityDisruption ||
				result.Evaluation.LocalityDisruption.ColdMovedLoad > 0

			requireSevenEvaluationGroups(t, result.Evaluation)

			switch fixture.Name {
			case "single-tenant-static":
				require.True(t, result.Rounds[1].WorkloadChanged,
					"static Gaussian shape should still receive default traffic variation")
			case "single-tenant-growing":
				require.True(t, hasActionAfter(result.Rounds, fixture.Ticks/2),
					"growing hotspot should trigger later actions from new observations")
			case "two-tenant-opposing":
				actedTenants := actionTenants(result.Rounds)
				require.Contains(t, actedTenants, "tenant-growing")
				require.Contains(t, actedTenants, "tenant-shrinking")
			}
		})
	}
	require.True(t, observedLocalityDisruption, "fixtures must exercise the locality cost")
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
			first.CandidateSearch.DiscardedByBudget.Splits+
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

func TestClosedLoopSupportsMultipleInitialGranularities(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	base := fixtureByName(t, fixtures, "single-tenant-static")
	policy := scallop.DefaultPolicy()
	policy.ActionLimits = simulatorActionLimits(4)

	for _, initialRanges := range []int{4, 8, 16} {
		t.Run(fmt.Sprintf("%d-ranges", initialRanges), func(t *testing.T) {
			fixture := base
			fixture.InitialRanges = initialRanges
			result, err := simulateFixture(fixture, policy)
			require.NoError(t, err)
			require.Len(t, result.Rounds, fixture.Ticks)
			require.Greater(t, result.Evaluation.StructuralFootprint.RangeSeconds, 0.0)
		})
	}
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
				fixture.FixtureName == "dev30-fluctuating" {
				require.Greater(t, fixture.CandidateSearch.RoundsTruncated, 0)
			} else {
				require.Zero(t, fixture.CandidateSearch.RoundsTruncated,
					"small fixtures should fit entirely within candidate limits")
			}
		}
	}
	recommendation := buildRecommendationReport(first)
	require.Len(t, recommendation.Fixtures, len(fixtures))
	require.Len(t, recommendation.NearestCompetitors, 5)

	outputDirectory := t.TempDir()
	require.NoError(t, writeReports(outputDirectory, first))
	for _, name := range []string{"search-report.json", "search-summary.csv", "recommended-policy.json"} {
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
}

func simulatorActionLimits(limit int) scallop.ActionLimits {
	return scallop.ActionLimits{
		Total:          limit,
		Move:           limit,
		Split:          limit,
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

func hasActionAfter(rounds []RoundRecord, tick int) bool {
	for _, round := range rounds {
		if round.Tick >= tick && len(round.Actions) > 0 {
			return true
		}
	}
	return false
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
