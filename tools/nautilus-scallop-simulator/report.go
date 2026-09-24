// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

// writeReports emits the full experiment, compact recommendation, and flattened CSV summary.
func writeReports(outputDirectory string, result SearchResult) error {
	if err := os.MkdirAll(outputDirectory, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDirectory, "search-report.json"), result); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDirectory, "recommended-policy.json"), buildRecommendationReport(result)); err != nil {
		return err
	}
	return writeCSV(filepath.Join(outputDirectory, "search-summary.csv"), result)
}

type RecommendationReport struct {
	EvaluatedPolicies  int                        `json:"evaluated_policies"`
	Policy             scallop.Policy             `json:"policy"`
	MeanUtility        float64                    `json:"mean_utility"`
	WorstUtility       float64                    `json:"worst_utility"`
	AggregateUtility   float64                    `json:"aggregate_utility"`
	Fixtures           []RecommendationFixture    `json:"fixtures"`
	NearestCompetitors []RecommendationCompetitor `json:"nearest_competitors"`
}

type RecommendationFixture struct {
	Name            string                 `json:"name"`
	Utility         float64                `json:"utility"`
	Evaluation      EvaluationReport       `json:"evaluation"`
	CandidateSearch CandidateSearchSummary `json:"candidate_search"`
}

type RecommendationCompetitor struct {
	Policy           scallop.Policy `json:"policy"`
	AggregateUtility float64        `json:"aggregate_utility"`
}

// buildRecommendationReport removes per-round bulk while preserving the winner and nearest competitors.
func buildRecommendationReport(result SearchResult) RecommendationReport {
	recommended := result.Recommended
	out := RecommendationReport{
		EvaluatedPolicies: result.EvaluatedPolicies,
		Policy:            recommended.Policy,
		MeanUtility:       recommended.MeanUtility,
		WorstUtility:      recommended.WorstUtility,
		AggregateUtility:  recommended.AggregateUtility,
		Fixtures:          make([]RecommendationFixture, 0, len(recommended.Fixtures)),
	}
	for _, fixture := range recommended.Fixtures {
		out.Fixtures = append(out.Fixtures, RecommendationFixture{
			Name:            fixture.FixtureName,
			Utility:         fixture.Utility,
			Evaluation:      fixture.Evaluation,
			CandidateSearch: fixture.CandidateSearch,
		})
	}
	for i := 1; i < min(6, len(result.Ranked)); i++ {
		out.NearestCompetitors = append(out.NearestCompetitors, RecommendationCompetitor{
			Policy:           result.Ranked[i].Policy,
			AggregateUtility: result.Ranked[i].AggregateUtility,
		})
	}
	return out
}

// writeJSON serializes an indented machine-readable report to one path.
func writeJSON(path string, value any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

// writeCSV flattens every policy-fixture evaluation so search results are easy to compare and graph.
func writeCSV(path string, result SearchResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"rank", "generation", "policy", "fixture",
		"aggregate_utility", "mean_utility", "worst_utility", "fixture_utility",
		"partition_imbalance_integral", "partition_imbalance_worst", "partition_imbalance_p95",
		"replica_imbalance_integral", "replica_imbalance_worst", "replica_imbalance_p95",
		"moved_load_fraction", "moved_hash_fraction", "moves", "splits", "merges",
		"range_seconds", "tenant_partition_seconds",
		"cold_moved_load", "cold_moved_load_fraction",
		"first_change_tick", "recovery_ticks", "post_change_area",
		"stationary_tail_actions", "repeated_lineage_moves", "reversed_moves",
		"imbalance_reduction", "reduction_per_action", "reduction_per_moved_load",
		"partition_tracking_score", "partition_tracking_max_error", "partition_tracking_p95_error",
		"partition_time_above_threshold", "partition_corrective_action_delay",
		"partition_post_action_slope", "partition_mean_corrected_fraction",
		"replica_tracking_score", "replica_tracking_max_error", "replica_tracking_p95_error",
		"replica_time_above_threshold", "replica_corrective_action_delay",
		"replica_post_action_slope", "replica_mean_corrected_fraction",
		"candidate_iterations", "candidate_rounds_truncated",
		"candidate_legal_total", "candidate_admitted_total", "candidate_fully_scored_total", "candidate_discarded_total",
		"candidate_fully_scored_moves", "candidate_fully_scored_splits", "candidate_fully_scored_merges",
		"candidate_discarded_moves", "candidate_discarded_splits", "candidate_discarded_merges",
		"discarded_by_move_source_budget", "discarded_by_destination_budget",
		"discarded_by_split_budget", "discarded_by_merge_budget", "discarded_by_fully_scored_budget",
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for rank, policy := range result.Ranked {
		for _, fixture := range policy.Fixtures {
			row := csvEvaluationRow(
				rank+1, policy.Generation, policy.Policy, fixture,
				policy.AggregateUtility, policy.MeanUtility, policy.WorstUtility,
			)
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	}
	return writer.Error()
}

func csvEvaluationRow(
	rank, generation int,
	policy scallop.Policy,
	fixture SimulationResult,
	aggregateUtility, meanUtility, worstUtility float64,
) []string {
	e := fixture.Evaluation
	search := fixture.CandidateSearch
	return []string{
		strconv.Itoa(rank),
		strconv.Itoa(generation),
		policyKey(policy),
		fixture.FixtureName,
		formatFloat(aggregateUtility),
		formatFloat(meanUtility),
		formatFloat(worstUtility),
		formatFloat(fixture.Utility),
		formatFloat(e.PeakImbalance.Partition.Integral),
		formatFloat(e.PeakImbalance.Partition.Worst),
		formatFloat(e.PeakImbalance.Partition.P95),
		formatFloat(e.PeakImbalance.Replica.Integral),
		formatFloat(e.PeakImbalance.Replica.Worst),
		formatFloat(e.PeakImbalance.Replica.P95),
		formatFloat(e.RebalancingWork.MovedLoadFraction),
		formatFloat(e.RebalancingWork.MovedHashFraction),
		strconv.Itoa(e.RebalancingWork.Moves),
		strconv.Itoa(e.RebalancingWork.Splits),
		strconv.Itoa(e.RebalancingWork.Merges),
		formatFloat(e.StructuralFootprint.RangeSeconds),
		formatFloat(e.StructuralFootprint.TenantPartitionSeconds),
		formatFloat(e.LocalityDisruption.ColdMovedLoad),
		formatFloat(e.LocalityDisruption.ColdMovedLoadFraction),
		strconv.Itoa(e.AdaptationStability.FirstChangeTick),
		strconv.Itoa(e.AdaptationStability.RecoveryTicks),
		formatFloat(e.AdaptationStability.PostChangeArea),
		strconv.Itoa(e.AdaptationStability.StationaryTailActions),
		strconv.Itoa(e.AdaptationStability.RepeatedLineageMoves),
		strconv.Itoa(e.AdaptationStability.ReversedMoves),
		formatFloat(e.ActionEffectiveness.TotalImbalanceReduction),
		formatFloat(e.ActionEffectiveness.ReductionPerAction),
		formatFloat(e.ActionEffectiveness.ReductionPerMovedLoad),
		formatOptionalFloat(e.ImbalanceTracking.Partition.Score),
		formatFloat(e.ImbalanceTracking.Partition.MaxError),
		formatFloat(e.ImbalanceTracking.Partition.P95Error),
		strconv.Itoa(e.ImbalanceTracking.Partition.TimeAboveThreshold),
		strconv.Itoa(e.ImbalanceTracking.Partition.CorrectiveActionDelay),
		formatFloat(e.ImbalanceTracking.Partition.PostActionSlope),
		formatFloat(e.ImbalanceTracking.Partition.MeanCorrectedFraction),
		formatOptionalFloat(e.ImbalanceTracking.Replica.Score),
		formatFloat(e.ImbalanceTracking.Replica.MaxError),
		formatFloat(e.ImbalanceTracking.Replica.P95Error),
		strconv.Itoa(e.ImbalanceTracking.Replica.TimeAboveThreshold),
		strconv.Itoa(e.ImbalanceTracking.Replica.CorrectiveActionDelay),
		formatFloat(e.ImbalanceTracking.Replica.PostActionSlope),
		formatFloat(e.ImbalanceTracking.Replica.MeanCorrectedFraction),
		strconv.Itoa(search.Iterations),
		strconv.Itoa(search.RoundsTruncated),
		strconv.Itoa(search.Legal.Total),
		strconv.Itoa(search.Admitted.Total),
		strconv.Itoa(search.FullyScored.Total),
		strconv.Itoa(search.Discarded.Total),
		strconv.Itoa(search.FullyScored.Move),
		strconv.Itoa(search.FullyScored.Split),
		strconv.Itoa(search.FullyScored.Merge),
		strconv.Itoa(search.Discarded.Move),
		strconv.Itoa(search.Discarded.Split),
		strconv.Itoa(search.Discarded.Merge),
		strconv.Itoa(search.DiscardedByBudget.MoveSources),
		strconv.Itoa(search.DiscardedByBudget.Destinations),
		strconv.Itoa(search.DiscardedByBudget.Splits),
		strconv.Itoa(search.DiscardedByBudget.Merges),
		strconv.Itoa(search.DiscardedByBudget.FullyScored),
	}
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', 12, 64)
}

func formatOptionalFloat(value *float64) string {
	if value == nil {
		return ""
	}
	return formatFloat(*value)
}

// recommendationSummary formats the concise terminal result shown after a complete search.
func recommendationSummary(result SearchResult) string {
	policy := result.Recommended.Policy
	return fmt.Sprintf(
		"evaluated=%d aggregate=%.6f weights={replica=%.6g events=%.6g load=%.6g hash=%.6g locality=%.6g fragmentation=%.6g resolution=%.6g}",
		result.EvaluatedPolicies,
		result.Recommended.AggregateUtility,
		policy.Weights.ReplicaBalance,
		policy.Weights.TransitionEvents,
		policy.Weights.TransitionLoad,
		policy.Weights.TransitionHashSpace,
		policy.Weights.LocalityMiss,
		policy.Weights.Fragmentation,
		policy.Weights.Resolution,
	)
}
