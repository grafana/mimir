// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

type EvaluationReport struct {
	PeakImbalance       PeakImbalanceEvaluation       `json:"peak_imbalance"`
	RebalancingWork     RebalancingWorkEvaluation     `json:"rebalancing_work"`
	StructuralFootprint StructuralFootprintEvaluation `json:"structural_footprint"`
	LocalityDisruption  LocalityDisruptionEvaluation  `json:"locality_disruption"`
	AdaptationStability AdaptationStabilityEvaluation `json:"adaptation_stability"`
	ActionEffectiveness ActionEffectivenessEvaluation `json:"action_effectiveness"`
	ImbalanceTracking   ImbalanceTrackingEvaluation   `json:"imbalance_tracking"`
}

type SeriesSummary struct {
	Integral float64 `json:"integral"`
	Worst    float64 `json:"worst"`
	P95      float64 `json:"p95"`
}

type PeakImbalanceEvaluation struct {
	Partition SeriesSummary `json:"partition"`
	Replica   SeriesSummary `json:"replica"`
}

type RebalancingWorkEvaluation struct {
	MovedLoadFraction float64 `json:"moved_load_fraction"`
	MovedHashFraction float64 `json:"moved_hash_fraction"`
	Moves             int     `json:"moves"`
	Splits            int     `json:"splits"`
	Merges            int     `json:"merges"`
}

type StructuralFootprintEvaluation struct {
	RangeSeconds           float64 `json:"range_seconds"`
	TenantPartitionSeconds float64 `json:"tenant_partition_seconds"`
}

type LocalityDisruptionEvaluation struct {
	ColdMovedLoad         float64 `json:"cold_moved_load"`
	ColdMovedLoadFraction float64 `json:"cold_moved_load_fraction"`
}

type AdaptationStabilityEvaluation struct {
	FirstChangeTick       int     `json:"first_change_tick"`
	RecoveryTicks         int     `json:"recovery_ticks"`
	PostChangeArea        float64 `json:"post_change_area"`
	StationaryTailActions int     `json:"stationary_tail_actions"`
	RepeatedLineageMoves  int     `json:"repeated_lineage_moves"`
	ReversedMoves         int     `json:"reversed_moves"`
}

type ActionEffectivenessEvaluation struct {
	TotalImbalanceReduction float64 `json:"total_imbalance_reduction"`
	ReductionPerAction      float64 `json:"reduction_per_action"`
	ReductionPerMovedLoad   float64 `json:"reduction_per_moved_load"`
}

type TrackingSeriesEvaluation struct {
	Score                 *float64 `json:"score"`
	MaxError              float64  `json:"max_error"`
	P95Error              float64  `json:"p95_error"`
	TimeAboveThreshold    int      `json:"time_above_threshold"`
	CorrectiveActionDelay int      `json:"corrective_action_delay"`
	PostActionSlope       float64  `json:"post_action_slope"`
	MeanCorrectedFraction float64  `json:"mean_corrected_fraction"`
}

type ImbalanceTrackingEvaluation struct {
	Partition TrackingSeriesEvaluation `json:"partition"`
	Replica   TrackingSeriesEvaluation `json:"replica"`
}

// evaluate derives all seven policy-independent quality groups from a completed fixture trajectory.
func evaluate(fixture Fixture, rounds []RoundRecord) EvaluationReport {
	tickSeconds := float64(fixture.TickSeconds)
	partitionPost := make([]float64, len(rounds))
	replicaPost := make([]float64, len(rounds))
	partitionStatic := make([]float64, len(rounds))
	replicaStatic := make([]float64, len(rounds))
	for i, round := range rounds {
		partitionPost[i] = round.PostPlan.Partition
		replicaPost[i] = round.PostPlan.Replica
		partitionStatic[i] = round.Static.Partition
		replicaStatic[i] = round.Static.Replica
	}

	report := EvaluationReport{
		PeakImbalance: PeakImbalanceEvaluation{
			Partition: summarizeSeries(partitionPost, tickSeconds),
			Replica:   summarizeSeries(replicaPost, tickSeconds),
		},
		RebalancingWork:     evaluateWork(rounds),
		StructuralFootprint: evaluateStructure(rounds, tickSeconds),
		AdaptationStability: evaluateAdaptation(fixture, rounds),
		ImbalanceTracking: ImbalanceTrackingEvaluation{
			Partition: evaluateTracking(fixture, rounds, partitionStatic, partitionPost, func(r RoundRecord) float64 {
				return r.PostPlan.Partition
			}),
			Replica: evaluateTracking(fixture, rounds, replicaStatic, replicaPost, func(r RoundRecord) float64 {
				return r.PostPlan.Replica
			}),
		},
	}
	report.LocalityDisruption = evaluateLocality(rounds)
	report.ActionEffectiveness = evaluateEffectiveness(rounds)
	return report
}

// summarizeSeries reports time-integrated, worst, and p95 values for an imbalance series.
func summarizeSeries(values []float64, tickSeconds float64) SeriesSummary {
	out := SeriesSummary{}
	for _, value := range values {
		out.Integral += value * tickSeconds
		out.Worst = math.Max(out.Worst, value)
	}
	out.P95 = percentile(values, 0.95)
	return out
}

// evaluateWork totals relocation volume and action counts to quantify rebalancing cost.
func evaluateWork(rounds []RoundRecord) RebalancingWorkEvaluation {
	out := RebalancingWorkEvaluation{}
	for _, round := range rounds {
		for _, action := range round.Actions {
			out.MovedLoadFraction += action.Transition.TransitionLoad
			out.MovedHashFraction += action.MovedHashFraction
			switch action.Kind {
			case scallop.ActionMove:
				out.Moves++
			case scallop.ActionSplit:
				out.Splits++
			case scallop.ActionMerge:
				out.Merges++
			}
		}
	}
	return out
}

// evaluateStructure integrates range count and tenant fanout to expose persistent structural overhead.
func evaluateStructure(rounds []RoundRecord, tickSeconds float64) StructuralFootprintEvaluation {
	out := StructuralFootprintEvaluation{}
	for _, round := range rounds {
		out.RangeSeconds += float64(round.RangeCount) * tickSeconds
		out.TenantPartitionSeconds += float64(round.TenantPartitions) * tickSeconds
	}
	return out
}

// evaluateLocality measures load moved onto replicas without recent tenant history.
func evaluateLocality(rounds []RoundRecord) LocalityDisruptionEvaluation {
	out := LocalityDisruptionEvaluation{}
	totalMovedLoad := 0.0
	for _, round := range rounds {
		for _, action := range round.Actions {
			totalMovedLoad += action.MovedLoad
			if action.Transition.LocalityMiss > 0 {
				out.ColdMovedLoad += action.MovedLoad
			}
		}
	}
	if totalMovedLoad > 0 {
		out.ColdMovedLoadFraction = out.ColdMovedLoad / totalMovedLoad
	}
	return out
}

// evaluateAdaptation measures recovery, tail churn, and repeated or reversed lineage movement.
func evaluateAdaptation(fixture Fixture, rounds []RoundRecord) AdaptationStabilityEvaluation {
	out := AdaptationStabilityEvaluation{FirstChangeTick: -1, RecoveryTicks: -1}
	for tick := range rounds {
		if fixture.workloadChangedAt(tick) {
			out.FirstChangeTick = tick
			break
		}
	}
	if out.FirstChangeTick >= 0 {
		recoveredAt := -1
		for i := out.FirstChangeTick; i < len(rounds); i++ {
			excess := rounds[i].PostPlan.Partition + rounds[i].PostPlan.Replica
			out.PostChangeArea += excess * float64(fixture.TickSeconds)
			if recoveredAt < 0 &&
				rounds[i].PostPlan.Partition <= fixture.ImbalanceThreshold &&
				rounds[i].PostPlan.Replica <= fixture.ImbalanceThreshold {
				recoveredAt = i
			}
		}
		if recoveredAt >= 0 {
			out.RecoveryTicks = recoveredAt - out.FirstChangeTick
		}
	}

	tailStart := len(rounds) * 3 / 4
	seen := map[string]scallop.Action{}
	for i, round := range rounds {
		if i >= tailStart {
			out.StationaryTailActions += len(round.Actions)
		}
		for _, action := range round.Actions {
			key := actionLineageKey(action)
			if previous, ok := seen[key]; ok {
				out.RepeatedLineageMoves++
				if action.Kind == scallop.ActionMove &&
					previous.Kind == scallop.ActionMove &&
					action.FromPartition == previous.ToPartition &&
					action.ToPartition == previous.FromPartition {
					out.ReversedMoves++
				}
			}
			seen[key] = action
		}
	}
	return out
}

// evaluateEffectiveness relates same-tick imbalance reduction to actions and affected load.
func evaluateEffectiveness(rounds []RoundRecord) ActionEffectivenessEvaluation {
	out := ActionEffectivenessEvaluation{}
	actionCount := 0
	totalMovedLoad := 0.0
	for _, round := range rounds {
		out.TotalImbalanceReduction +=
			(round.PrePlan.Partition + round.PrePlan.Replica) -
				(round.PostPlan.Partition + round.PostPlan.Replica)
		actionCount += len(round.Actions)
		for _, action := range round.Actions {
			totalMovedLoad += action.MovedLoad
		}
	}
	if actionCount > 0 {
		out.ReductionPerAction = out.TotalImbalanceReduction / float64(actionCount)
	}
	if totalMovedLoad > 0 {
		out.ReductionPerMovedLoad = out.TotalImbalanceReduction / totalMovedLoad
	}
	return out
}

// evaluateTracking compares Scallop's imbalance trajectory with an identical no-rebalancing baseline.
func evaluateTracking(
	fixture Fixture,
	rounds []RoundRecord,
	staticValues []float64,
	actualValues []float64,
	value func(RoundRecord) float64,
) TrackingSeriesEvaluation {
	out := TrackingSeriesEvaluation{CorrectiveActionDelay: -1}
	staticIntegral, actualIntegral := 0.0, 0.0
	correctedTotal := 0.0
	correctedCount := 0
	firstChange := -1
	firstAction := -1
	for i, round := range rounds {
		staticIntegral += staticValues[i] * float64(fixture.TickSeconds)
		actualIntegral += actualValues[i] * float64(fixture.TickSeconds)
		out.MaxError = math.Max(out.MaxError, actualValues[i])
		if actualValues[i] > fixture.ImbalanceThreshold {
			out.TimeAboveThreshold++
		}
		if staticValues[i] > 0 {
			correctedTotal += (staticValues[i] - actualValues[i]) / staticValues[i]
			correctedCount++
		}
		if firstChange < 0 && fixture.workloadChangedAt(i) {
			firstChange = i
		}
		if firstChange >= 0 && firstAction < 0 && len(round.Actions) > 0 {
			firstAction = i
		}
	}
	if staticIntegral > 0 {
		score := 1 - actualIntegral/staticIntegral
		out.Score = &score
	}
	out.P95Error = percentile(actualValues, 0.95)
	if correctedCount > 0 {
		out.MeanCorrectedFraction = correctedTotal / float64(correctedCount)
	}
	if firstChange >= 0 && firstAction >= 0 {
		out.CorrectiveActionDelay = firstAction - firstChange
	}
	slopeStart := firstAction
	if slopeStart < 0 {
		slopeStart = firstChange
	}
	if slopeStart < 0 {
		slopeStart = 0
	}
	out.PostActionSlope = linearSlope(rounds[slopeStart:], value)
	return out
}

// percentile returns the nearest-rank quantile of a copied and sorted value series.
func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// linearSlope fits imbalance against round index to show whether post-action skew is improving.
func linearSlope(rounds []RoundRecord, value func(RoundRecord) float64) float64 {
	if len(rounds) < 2 {
		return 0
	}
	var sumX, sumY, sumXY, sumXX float64
	for i, round := range rounds {
		x := float64(i)
		y := value(round)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	n := float64(len(rounds))
	denominator := n*sumXX - sumX*sumX
	if denominator == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denominator
}

// actionLineageKey groups repeated actions affecting the same tenant range lineage.
func actionLineageKey(action scallop.Action) string {
	return fmt.Sprintf("%s/%d/%d", action.TenantID, action.Range.Lo, action.Range.Hi)
}

// fixedUtility ranks policies with an external objective independent of the planner's own weighted cost.
func fixedUtility(report EvaluationReport) float64 {
	work := report.RebalancingWork
	structure := report.StructuralFootprint
	return report.PeakImbalance.Partition.Integral +
		report.PeakImbalance.Replica.Integral +
		0.1*work.MovedLoadFraction +
		0.05*work.MovedHashFraction +
		0.01*float64(work.Moves+work.Splits+work.Merges) +
		0.1*report.LocalityDisruption.ColdMovedLoadFraction +
		0.00001*structure.RangeSeconds +
		0.00001*structure.TenantPartitionSeconds
}
