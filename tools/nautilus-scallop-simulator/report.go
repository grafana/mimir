// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
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
	if err := writeSearchReport(filepath.Join(outputDirectory, "search-report.json"), result); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(outputDirectory, "recommended-policy.json"), buildRecommendationReport(result)); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(outputDirectory, "search-summary.csv"), result); err != nil {
		return err
	}
	return writeSensitivityCSV(filepath.Join(outputDirectory, "sensitivity-summary.csv"), result.Sensitivity)
}

type RecommendationReport struct {
	EvaluatedPolicies  int                        `json:"evaluated_policies"`
	Policy             scallop.Policy             `json:"policy"`
	MeanUtility        float64                    `json:"mean_utility"`
	WorstUtility       float64                    `json:"worst_utility"`
	AggregateUtility   float64                    `json:"aggregate_utility"`
	Fixtures           []RecommendationFixture    `json:"fixtures"`
	Replays            []ReplayEvaluation         `json:"replays"`
	NearestCompetitors []RecommendationCompetitor `json:"nearest_competitors"`
	Sensitivity        SensitivityAnalysis        `json:"sensitivity"`
}

type RecommendationFixture struct {
	Name            string                 `json:"name"`
	TrafficNoise    TrafficNoiseConfig     `json:"traffic_noise"`
	Utility         float64                `json:"utility"`
	Evaluation      EvaluationReport       `json:"evaluation"`
	CandidateSearch CandidateSearchSummary `json:"candidate_search"`
}

type RecommendationCompetitor struct {
	Policy                scallop.Policy            `json:"policy"`
	Eligible              bool                      `json:"eligible"`
	ConstraintErrors      []string                  `json:"constraint_errors,omitempty"`
	MeanUtility           float64                   `json:"mean_utility"`
	WorstUtility          float64                   `json:"worst_utility"`
	AggregateUtility      float64                   `json:"aggregate_utility"`
	AggregateUtilityDelta float64                   `json:"aggregate_utility_delta"`
	FixtureDeltas         []SensitivityFixtureDelta `json:"fixture_deltas"`
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
		Replays:           recommended.Replays,
		Sensitivity:       result.Sensitivity,
	}
	for _, fixture := range recommended.Fixtures {
		out.Fixtures = append(out.Fixtures, RecommendationFixture{
			Name:            fixture.FixtureName,
			TrafficNoise:    fixture.TrafficNoise,
			Utility:         fixture.Utility,
			Evaluation:      fixture.Evaluation,
			CandidateSearch: fixture.CandidateSearch,
		})
	}
	for i := 1; i < min(6, len(result.Ranked)); i++ {
		competitor := result.Ranked[i]
		out.NearestCompetitors = append(out.NearestCompetitors, RecommendationCompetitor{
			Policy:                competitor.Policy,
			Eligible:              competitor.Eligible,
			ConstraintErrors:      competitor.ConstraintErrors,
			MeanUtility:           competitor.MeanUtility,
			WorstUtility:          competitor.WorstUtility,
			AggregateUtility:      competitor.AggregateUtility,
			AggregateUtilityDelta: competitor.AggregateUtility - recommended.AggregateUtility,
			FixtureDeltas:         buildSensitivityEvaluation("", recommended, competitor).FixtureDeltas,
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

// writeSearchReport streams SearchResult as compact JSON one ranked policy at a
// time; encoding the complete search in one buffer needs several gigabytes.
func writeSearchReport(path string, result SearchResult) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	writeField := func(prefix string, value any) error {
		if _, err := writer.WriteString(prefix); err != nil {
			return err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = writer.Write(data)
		return err
	}
	if err := writeField(`{"config":`, result.Config); err != nil {
		return err
	}
	if err := writeField(`,"evaluated_policies":`, result.EvaluatedPolicies); err != nil {
		return err
	}
	if err := writeField(`,"recommended":`, result.Recommended); err != nil {
		return err
	}
	if result.Ranked == nil {
		if err := writeField(`,"ranked":`, nil); err != nil {
			return err
		}
	} else {
		for i, evaluation := range result.Ranked {
			separator := ","
			if i == 0 {
				separator = `,"ranked":[`
			}
			if err := writeField(separator, evaluation); err != nil {
				return err
			}
		}
		if len(result.Ranked) == 0 {
			if _, err := writer.WriteString(`,"ranked":[`); err != nil {
				return err
			}
		}
		if _, err := writer.WriteString("]"); err != nil {
			return err
		}
	}
	if err := writeField(`,"sensitivity":`, result.Sensitivity); err != nil {
		return err
	}
	if _, err := writer.WriteString("}\n"); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Close()
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
		"move_partition_multiplier", "traffic_noise_cv", "traffic_noise_correlation_ticks", "traffic_noise_seed",
		"aggregate_utility", "mean_utility", "worst_utility", "fixture_utility",
		"partition_imbalance_integral", "partition_imbalance_worst", "partition_imbalance_p95",
		"replica_imbalance_integral", "replica_imbalance_worst", "replica_imbalance_p95",
		"moved_load_fraction", "moved_hash_fraction", "moves", "splits", "merges",
		"partition_moves", "partition_moved_load_fraction",
		"range_seconds", "tenant_partition_seconds", "max_settle_tick", "p95_settle_tick",
		"longest_no_progress_ticks", "unsettled_tenants",
		"cold_moved_load", "cold_moved_load_fraction",
		"first_change_tick", "recovery_ticks", "post_change_area",
		"stationary_tail_actions", "stationary_tail_partition_moves", "stationary_tail_partition_load_fraction",
		"repeated_range_moves", "reversed_range_moves",
		"repeated_partition_moves", "reversed_partition_moves", "returned_partition_load_fraction",
		"imbalance_reduction", "reduction_per_action", "reduction_per_moved_load",
		"partition_tracking_score", "partition_tracking_max_error", "partition_tracking_p95_error",
		"partition_time_above_threshold", "partition_corrective_action_delay",
		"partition_post_action_slope", "partition_mean_corrected_fraction",
		"replica_tracking_score", "replica_tracking_max_error", "replica_tracking_p95_error",
		"replica_time_above_threshold", "replica_corrective_action_delay",
		"replica_post_action_slope", "replica_mean_corrected_fraction",
		"candidate_iterations", "candidate_rounds_truncated",
		"candidate_legal_total", "candidate_admitted_total", "candidate_fully_scored_total", "candidate_discarded_total",
		"candidate_fully_scored_moves", "candidate_fully_scored_splits", "candidate_fully_scored_merges", "candidate_fully_scored_partition_moves",
		"candidate_discarded_moves", "candidate_discarded_splits", "candidate_discarded_merges", "candidate_discarded_partition_moves",
		"discarded_by_move_source_budget", "discarded_by_destination_budget",
		"discarded_by_merge_budget",
		"discarded_by_partition_source_budget", "discarded_by_partition_destination_budget",
		"discarded_by_partition_move_budget", "discarded_by_fully_scored_budget",
		"partition_offenders_expanded", "replica_offenders_expanded", "tenant_offenders_expanded",
		"ranges_inspected", "adjacency_edges_inspected", "destinations_inspected",
		"candidates_yielded", "exact_delta_scores", "complete_projections",
		"pruned_partition_offenders", "pruned_replica_offenders", "pruned_tenant_offenders",
		"pruned_ranges", "pruned_adjacency_edges",
		"pruned_destination_partitions", "pruned_destination_replicas",
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

func writeSensitivityCSV(path string, sensitivity SensitivityAnalysis) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"label", "policy", "eligible", "constraint_errors",
		"aggregate_utility", "aggregate_utility_delta", "fixture",
		"fixture_utility", "fixture_utility_delta",
		"exact_delta_scores_delta", "complete_projections_delta",
		"final_weighted_cost", "final_weighted_cost_delta", "nearby_wins_fixture",
	}); err != nil {
		return err
	}
	for _, sensitivityCase := range sensitivity.Cases {
		wins := make(map[string]struct{}, len(sensitivityCase.WinsFixtures))
		for _, fixture := range sensitivityCase.WinsFixtures {
			wins[fixture] = struct{}{}
		}
		for _, fixture := range sensitivityCase.FixtureDeltas {
			_, nearbyWins := wins[fixture.Name]
			if err := writer.Write([]string{
				sensitivityCase.Label,
				policyKey(sensitivityCase.Policy),
				strconv.FormatBool(sensitivityCase.Eligible),
				fmt.Sprint(sensitivityCase.ConstraintErrors),
				formatFloat(sensitivityCase.AggregateUtility),
				formatFloat(sensitivityCase.AggregateUtilityDelta),
				fixture.Name,
				formatFloat(fixture.Utility),
				formatFloat(fixture.UtilityDelta),
				strconv.Itoa(fixture.ExactDeltaScoresDelta),
				strconv.Itoa(fixture.CompleteProjectionsDelta),
				formatFloat(fixture.FinalWeightedCost),
				formatFloat(fixture.FinalWeightedCostDelta),
				strconv.FormatBool(nearbyWins),
			}); err != nil {
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
		formatFloat(policy.ActionMultipliers.MovePartition),
		formatFloat(fixture.TrafficNoise.CoefficientOfVariation),
		formatFloat(fixture.TrafficNoise.CorrelationTicks),
		strconv.FormatInt(fixture.TrafficNoise.Seed, 10),
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
		strconv.Itoa(e.RebalancingWork.PartitionMoves),
		formatFloat(e.RebalancingWork.PartitionMovedLoadFraction),
		formatFloat(e.StructuralFootprint.RangeSeconds),
		formatFloat(e.StructuralFootprint.TenantPartitionSeconds),
		strconv.Itoa(e.StructuralFootprint.MaxSettleTick),
		strconv.Itoa(e.StructuralFootprint.P95SettleTick),
		strconv.Itoa(e.StructuralFootprint.LongestNoProgressTicks),
		strconv.Itoa(e.StructuralFootprint.UnsettledTenants),
		formatFloat(e.LocalityDisruption.ColdMovedLoad),
		formatFloat(e.LocalityDisruption.ColdMovedLoadFraction),
		strconv.Itoa(e.AdaptationStability.FirstChangeTick),
		strconv.Itoa(e.AdaptationStability.RecoveryTicks),
		formatFloat(e.AdaptationStability.PostChangeArea),
		strconv.Itoa(e.AdaptationStability.StationaryTailActions),
		strconv.Itoa(e.AdaptationStability.StationaryTailPartitionMoves),
		formatFloat(e.AdaptationStability.StationaryTailPartitionLoadFraction),
		strconv.Itoa(e.AdaptationStability.RepeatedRangeMoves),
		strconv.Itoa(e.AdaptationStability.ReversedRangeMoves),
		strconv.Itoa(e.AdaptationStability.RepeatedPartitionMoves),
		strconv.Itoa(e.AdaptationStability.ReversedPartitionMoves),
		formatFloat(e.AdaptationStability.ReturnedPartitionLoadFraction),
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
		strconv.Itoa(search.FullyScored.MovePartition),
		strconv.Itoa(search.Discarded.Move),
		strconv.Itoa(search.Discarded.Split),
		strconv.Itoa(search.Discarded.Merge),
		strconv.Itoa(search.Discarded.MovePartition),
		strconv.Itoa(search.DiscardedByBudget.MoveSources),
		strconv.Itoa(search.DiscardedByBudget.Destinations),
		strconv.Itoa(search.DiscardedByBudget.Merges),
		strconv.Itoa(search.DiscardedByBudget.PartitionMoveSources),
		strconv.Itoa(search.DiscardedByBudget.PartitionMoveDestinations),
		strconv.Itoa(search.DiscardedByBudget.PartitionMoves),
		strconv.Itoa(search.DiscardedByBudget.FullyScored),
		strconv.Itoa(search.Work.OffendersExpanded.Partitions),
		strconv.Itoa(search.Work.OffendersExpanded.Replicas),
		strconv.Itoa(search.Work.OffendersExpanded.Tenants),
		strconv.Itoa(search.Work.RangesInspected),
		strconv.Itoa(search.Work.AdjacencyEdgesInspected),
		strconv.Itoa(search.Work.DestinationsInspected),
		strconv.Itoa(search.Work.Yielded.Total),
		strconv.Itoa(search.Work.ExactDeltaScores),
		strconv.Itoa(search.Work.CompleteProjections),
		strconv.Itoa(search.Pruned.PartitionOffenders),
		strconv.Itoa(search.Pruned.ReplicaOffenders),
		strconv.Itoa(search.Pruned.TenantOffenders),
		strconv.Itoa(search.Pruned.Ranges),
		strconv.Itoa(search.Pruned.AdjacencyEdges),
		strconv.Itoa(search.Pruned.DestinationPartitions),
		strconv.Itoa(search.Pruned.DestinationReplicas),
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
		"evaluated=%d aggregate=%.6f weights={replica=%.6g events=%.6g load=%.6g hash=%.6g locality=%.6g fragmentation=%.6g resolution=%.6g} action_multipliers={move_partition=%.6g} max_range_partition_share=%.6g",
		result.EvaluatedPolicies,
		result.Recommended.AggregateUtility,
		policy.Weights.ReplicaBalance,
		policy.Weights.TransitionEvents,
		policy.Weights.TransitionLoad,
		policy.Weights.TransitionHashSpace,
		policy.Weights.LocalityMiss,
		policy.Weights.Fragmentation,
		policy.Weights.Resolution,
		policy.ActionMultipliers.MovePartition,
		policy.MaxRangePartitionShare,
	)
}
