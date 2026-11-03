// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestReplayCorpusIsVersionedDeterministicAndMatchesDirectReplay(t *testing.T) {
	cases, err := loadReplayCorpus()
	require.NoError(t, err)
	require.Len(t, cases, 7)

	first, err := evaluateReplayCorpus(scallop.DefaultPolicy())
	require.NoError(t, err)
	second, err := evaluateReplayCorpus(scallop.DefaultPolicy())
	require.NoError(t, err)
	assert.Equal(t, first, second)
	byName := map[string]ReplayEvaluation{}
	for _, evaluation := range first {
		byName[evaluation.Name] = evaluation
	}
	assert.Less(t, replayLocalityMiss(byName["locality-hit"]), replayLocalityMiss(byName["locality-miss"]))

	for i, replayCase := range cases {
		t.Run(replayCase.Name, func(t *testing.T) {
			require.Equal(t, scallop.ReplaySnapshotVersion, replayCase.Version)
			direct := first[i]
			require.True(t, direct.Passed, direct.GateErrors)
			command := runReplayForTest(t, scallop.NewReplaySnapshotEnvelope(
				replayCase.snapshot(),
				scallop.DefaultPolicy(),
			))
			assert.Equal(t, direct.InputID, command.InputID)
			assert.Equal(t, direct.Actions, command.Actions)
			assert.Equal(t, direct.ProjectedAssignment, command.ProjectedAssignment)
			assert.Equal(t, direct.ProjectedOwners, command.ProjectedOwners)
			assert.Equal(t, direct.InitialCost, command.InitialCost)
			assert.Equal(t, direct.FinalCost, command.FinalCost)
			assert.Equal(t, direct.CandidateSearch, command.CandidateSearch)
		})
	}
}

func replayLocalityMiss(evaluation ReplayEvaluation) float64 {
	total := 0.0
	for _, action := range evaluation.Actions {
		total += action.Transition.LocalityMiss
	}
	return total
}

func TestDefaultPolicySatisfiesPhase8FixtureGates(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	policy := scallop.DefaultPolicy()
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			result, err := simulateFixture(fixture, policy)
			require.NoError(t, err)
			require.Empty(t, fixtureConstraintErrors(fixture, result))
		})
	}
}

func TestSensitivityEvaluationReportsPerFixtureAndWorkDeltas(t *testing.T) {
	baseline := PolicyEvaluation{
		AggregateUtility: 10,
		Fixtures: []SimulationResult{{
			FixtureName: "fixture-a",
			Utility:     4,
			Rounds: []RoundRecord{{
				FinalCost: scallop.CostBreakdown{WeightedTotal: 7},
			}},
			CandidateSearch: CandidateSearchSummary{
				Work: scallop.CandidateSearchWork{
					ExactDeltaScores:    10,
					CompleteProjections: 2,
				},
			},
		}},
	}
	nearby := PolicyEvaluation{
		AggregateUtility: 9,
		Fixtures: []SimulationResult{{
			FixtureName: "fixture-a",
			Utility:     3,
			Rounds: []RoundRecord{{
				FinalCost: scallop.CostBreakdown{WeightedTotal: 5},
			}},
			CandidateSearch: CandidateSearchSummary{
				Work: scallop.CandidateSearchWork{
					ExactDeltaScores:    14,
					CompleteProjections: 3,
				},
			},
		}},
	}

	got := buildSensitivityEvaluation("resolution_down", baseline, nearby)
	require.Equal(t, -1.0, got.AggregateUtilityDelta)
	require.Equal(t, []string{"fixture-a"}, got.WinsFixtures)
	require.Len(t, got.FixtureDeltas, 1)
	assert.Equal(t, -1.0, got.FixtureDeltas[0].UtilityDelta)
	assert.Equal(t, 4, got.FixtureDeltas[0].ExactDeltaScoresDelta)
	assert.Equal(t, 1, got.FixtureDeltas[0].CompleteProjectionsDelta)
	assert.Equal(t, -2.0, got.FixtureDeltas[0].FinalWeightedCostDelta)
	require.Len(t, got.FixtureDeltas[0].RoundCosts, 1)
	assert.Equal(t, 7.0, got.FixtureDeltas[0].RoundCosts[0].BaselineFinalCost.WeightedTotal)
	assert.Equal(t, 5.0, got.FixtureDeltas[0].RoundCosts[0].FinalCost.WeightedTotal)
}

func TestPolicyRankingKeepsSeedAcrossUtilityFlatDimensions(t *testing.T) {
	seed := scallop.DefaultPolicy()
	lower := seed
	lower.Weights.LocalityMiss /= 10
	results := []PolicyEvaluation{
		{Policy: lower, Eligible: true, AggregateUtility: 10},
		{Policy: seed, Eligible: true, AggregateUtility: 10},
	}

	sortPolicyEvaluations(results, seed)
	require.Equal(t, seed, results[0].Policy)
}

func TestSearchReportStreamsTheCompleteSearchResult(t *testing.T) {
	policy := scallop.DefaultPolicy()
	result := SearchResult{
		Config:            defaultSearchConfig(),
		EvaluatedPolicies: 2,
		Recommended:       PolicyEvaluation{Policy: policy, Eligible: true, AggregateUtility: 1},
		Ranked: []PolicyEvaluation{
			{Policy: policy, Eligible: true, AggregateUtility: 1},
			{Policy: policy, ConstraintErrors: []string{"gate"}, AggregateUtility: 2},
		},
		Sensitivity: SensitivityAnalysis{Log10Step: 0.15, Cases: []SensitivityEvaluation{{Label: "case"}}},
	}
	path := filepath.Join(t.TempDir(), "search-report.json")
	require.NoError(t, writeSearchReport(path, result))

	streamed, err := os.ReadFile(path)
	require.NoError(t, err)
	expected, err := json.Marshal(result)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(streamed))

	require.NoError(t, writeSearchReport(path, SearchResult{}))
	streamed, err = os.ReadFile(path)
	require.NoError(t, err)
	expected, err = json.Marshal(SearchResult{})
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(streamed))
}
