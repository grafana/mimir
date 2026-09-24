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
		ActionMultipliers: scallop.ActionMultipliers{Move: 1, Split: 1, Merge: 1},
		CandidateSearch:   scallop.DefaultCandidateSearchLimits(),
		MaxActions:        4,
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

func TestRequiredFixturesRunClosedLoopAndEmitSevenGroups(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	require.Len(t, fixtures, 5)
	policy := scallop.DefaultPolicy()
	policy.MaxActions = 4
	policy.Weights.Resolution = 0.005
	observedLocalityDisruption := false

	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			result, err := simulateFixture(fixture, policy)
			require.NoError(t, err)
			require.Len(t, result.Rounds, fixture.Ticks)
			require.Greater(t, result.Evaluation.PeakImbalance.Partition.Integral, 0.0)
			require.Greater(t, result.Evaluation.StructuralFootprint.RangeSeconds, 0.0)
			observedLocalityDisruption = observedLocalityDisruption ||
				result.Evaluation.LocalityDisruption.ColdMovedLoad > 0

			requireSevenEvaluationGroups(t, result.Evaluation)

			switch fixture.Name {
			case "single-tenant-static":
				for _, round := range result.Rounds[len(result.Rounds)-4:] {
					require.Empty(t, round.Actions, "stationary fixture should converge to no-op rounds")
				}
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
	policy.MaxActions = 4
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
			first.CandidateSearch.DiscardedByBudget.FullyScored,
	)
	for _, round := range first.Rounds {
		require.LessOrEqual(t,
			round.CandidateSearch.FullyScored.Total,
			round.CandidateSearch.Iterations*policy.CandidateSearch.MaxFullyScored,
		)
	}
}

func TestClosedLoopSupportsMultipleInitialGranularities(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	base := fixtureByName(t, fixtures, "single-tenant-static")
	policy := scallop.DefaultPolicy()
	policy.MaxActions = 4

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
	seed.MaxActions = 4
	config := defaultSearchConfig()

	first, err := runWeightSearch(fixtures, seed, config)
	require.NoError(t, err)
	second, err := runWeightSearch(fixtures, seed, config)
	require.NoError(t, err)
	require.GreaterOrEqual(t, first.EvaluatedPolicies, 100)
	require.Equal(t, first.EvaluatedPolicies, second.EvaluatedPolicies)
	require.Equal(t, policyKey(first.Recommended.Policy), policyKey(second.Recommended.Policy))
	require.Equal(t, first.Recommended.AggregateUtility, second.Recommended.AggregateUtility)
	require.Equal(t, rankedPolicyKeys(first), rankedPolicyKeys(second))
	for _, policy := range first.Ranked {
		for _, fixture := range policy.Fixtures {
			requireSevenEvaluationGroups(t, fixture.Evaluation)
			if fixture.FixtureName == "large-cell-static" ||
				fixture.FixtureName == "many-tiny-tenants-consolidating" {
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

func rankedPolicyKeys(result SearchResult) []string {
	keys := make([]string, len(result.Ranked))
	for i, policy := range result.Ranked {
		keys[i] = policyKey(policy.Policy)
	}
	return keys
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
