// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

// This file runs one production round through Scallop without legacy planning.

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

type scallopRoundInput struct {
	now                      time.Time
	current                  *assignment.Assignment
	rates                    []rangeRate
	activePartitions         []int32
	activeReplicas           []string
	replicaMap               readcacheassignment.ReplicaMap
	statsReadiness           readcacheStatsReadiness
	failedConcreteReadcaches map[string]struct{}
	partitionL               map[int32]int64
	partitionQuerySamples    map[int32]float64
	unnamedQuerySamples      map[string]float64
}

// seedScallopReadcacheCoverage handles startup before any readcache assignment
// exists. It distributes active Kafka partitions evenly across the available
// logical readcache replicas so Scallop has a complete initial placement to
// observe and improve.
func (r *Rebalancer) seedScallopReadcacheCoverage(now time.Time, partitions []int32, replicas []string) bool {
	if len(partitions) == 0 || len(replicas) == 0 {
		return false
	}
	sortedPartitions := append([]int32(nil), partitions...)
	sort.Slice(sortedPartitions, func(i, j int) bool { return sortedPartitions[i] < sortedPartitions[j] })
	sortedReplicas := append([]string(nil), replicas...)
	sort.Strings(sortedReplicas)
	entries := make([]readcacheassignment.AssignmentEntry, len(sortedPartitions))
	for index, partitionID := range sortedPartitions {
		entries[index] = readcacheassignment.AssignmentEntry{
			PartitionID: partitionID,
			InstanceID:  sortedReplicas[index%len(sortedReplicas)],
		}
	}
	r.readcacheApplyMu.Lock()
	defer r.readcacheApplyMu.Unlock()
	return r.readcacheStore.apply(
		now,
		&readcacheassignment.Assignment{Entries: entries},
		r.cfg.LeaseDuration,
		r.readcacheLeaseLookahead(),
		r.cfg.EntryRetention,
		r.cfg.ReadcacheMoveSafetyWindow,
	)
}

// runScallopRound builds one snapshot, plans once, publishes both logs, then pushes.
func (r *Rebalancer) runScallopRound(ctx context.Context, input scallopRoundInput) error {
	policy := scallop.DefaultPolicy()
	unavailable := input.failedConcreteReadcaches
	if r.cfg.ReadcacheSlicer.DesiredReplicas > 0 {
		unavailable = excludeLogicalTargetsFromConcreteFailures(
			input.failedConcreteReadcaches,
			input.replicaMap,
			r.healthyConcreteSet(),
		)
	}
	if len(input.statsReadiness.unreadyLogicalTargets) > 0 {
		if unavailable == nil {
			unavailable = make(map[string]struct{}, len(input.statsReadiness.unreadyLogicalTargets))
		} else {
			unavailable = cloneStringSet(unavailable)
		}
		for replicaID := range input.statsReadiness.unreadyLogicalTargets {
			unavailable[replicaID] = struct{}{}
		}
	}

	snapshot, err := buildScallopSnapshot(scallopSnapshotInput{
		now:                 input.now,
		current:             input.current,
		rates:               input.rates,
		activePartitions:    input.activePartitions,
		activeReplicas:      input.activeReplicas,
		readcacheLog:        r.readcacheStore.snapshot(),
		unreadyPartitions:   input.statsReadiness.unreadyPartitions,
		unavailableReplicas: unavailable,
		hashLog:             r.store.snapshot(),
	}, policy)
	if err != nil {
		kind, reason := scallopSnapshotFailure(err)
		level.Warn(r.logger).Log("msg", "Scallop snapshot is not ready; keeping assignments unchanged", "kind", kind, "reason", reason, "err", err)
		r.metrics.recordPlannerRound(plannerScallop, "skipped", reason)
		r.refreshCurrentLeases(input.now, input.current)
		return nil
	}

	planFn := r.scallopPlan
	if planFn == nil {
		planFn = scallop.Plan
	}
	planningStarted := time.Now()
	result, err := planFn(snapshot, policy)
	planningDuration := time.Since(planningStarted)
	if err != nil {
		level.Error(r.logger).Log("msg", "Scallop planning failed; keeping assignments unchanged", "planning_duration", planningDuration, "err", err)
		r.metrics.recordPlannerRound(plannerScallop, "error", "planning_error")
		r.refreshCurrentLeases(input.now, input.current)
		return nil
	}
	translation, err := translateScallopPlan(snapshot, result, input.activePartitions)
	if err != nil {
		level.Error(r.logger).Log("msg", "Scallop plan translation failed; keeping assignments unchanged", "err", err)
		r.metrics.recordPlannerRound(plannerScallop, "error", "translation_error")
		r.refreshCurrentLeases(input.now, input.current)
		return nil
	}

	// The readcache mutex keeps admin resets from interleaving with the
	// readcache-first/hash-second publication order.
	r.readcacheApplyMu.Lock()
	readcacheChanged := r.readcacheStore.apply(
		input.now,
		translation.readcacheAssignment,
		r.cfg.LeaseDuration,
		r.readcacheLeaseLookahead(),
		r.cfg.EntryRetention,
		r.cfg.ReadcacheMoveSafetyWindow,
	)
	hashChanged := r.store.apply(
		input.now,
		translation.hashAssignment,
		r.cfg.LeaseDuration,
		r.hashLeaseLookahead(),
		r.cfg.EntryRetention,
	)
	r.readcacheApplyMu.Unlock()

	r.metrics.updateTenantHashRanges(translation.hashAssignment)
	r.metrics.recordPlannerRound(plannerScallop, "success", "planned")
	r.metrics.recordPlannerActions(plannerScallop, translation.actions)
	r.pushScallopRanges(ctx, translation.pushIntents, input.replicaMap)
	r.addScallopTrace(input, snapshot, policy, result, translation)
	actionCounts := countScallopActions(translation.actions)

	level.Info(r.logger).Log(
		"msg", "Scallop rebalance complete",
		"planner", plannerScallop,
		"planning_duration", planningDuration,
		"actions", len(translation.actions),
		"moves", actionCounts[scallop.ActionMove],
		"splits", actionCounts[scallop.ActionSplit],
		"merges", actionCounts[scallop.ActionMerge],
		"partition_moves", actionCounts[scallop.ActionMovePartition],
		"entries", len(translation.hashAssignment.Entries),
		"hash_log_changed", hashChanged,
		"readcache_log_changed", readcacheChanged,
		"initial_cost", result.InitialCost.WeightedTotal,
		"final_cost", result.FinalCost.WeightedTotal,
		"candidate_admitted", result.CandidateSearch.Admitted.Total,
		"candidate_fully_scored", result.CandidateSearch.FullyScored.Total,
		"candidate_discarded", result.CandidateSearch.Discarded.Total,
	)
	return nil
}

func countScallopActions(actions []scallop.Action) map[scallop.ActionKind]int {
	counts := make(map[scallop.ActionKind]int, 4)
	for _, action := range actions {
		counts[action.Kind]++
	}
	return counts
}

// refreshCurrentLeases preserves both active assignments after a skipped Scallop round.
func (r *Rebalancer) refreshCurrentLeases(now time.Time, current *assignment.Assignment) {
	r.store.apply(now, current, r.cfg.LeaseDuration, r.hashLeaseLookahead(), r.cfg.EntryRetention)
	r.refreshReadcacheLeases()
}

// addScallopTrace records joint-planner evidence without populating legacy actions.
func (r *Rebalancer) addScallopTrace(
	input scallopRoundInput,
	snapshot scallop.Snapshot,
	policy scallop.Policy,
	result scallop.PlanResult,
	translation scallopPlanTranslation,
) {
	var totalL, maxL, minL int64
	minL = math.MaxInt64
	for _, partitionID := range input.activePartitions {
		load := input.partitionL[partitionID]
		totalL += load
		maxL = max(maxL, load)
		minL = min(minL, load)
	}
	if minL == math.MaxInt64 {
		minL = 0
	}
	var meanL int64
	if len(input.activePartitions) > 0 {
		meanL = totalL / int64(len(input.activePartitions))
	}
	imbalance := 0.0
	if meanL > 0 {
		imbalance = float64(maxL) / float64(meanL)
	}
	movedFraction := 0.0
	for _, action := range translation.actions {
		movedFraction += action.MovedHashFraction
	}
	round := RoundLog{
		Planner:        plannerScallop,
		Time:           input.now,
		TotalL:         totalL,
		MeanL:          meanL,
		MaxL:           maxL,
		MinL:           minL,
		ImbalanceRatio: imbalance,
		NumEntries:     len(translation.hashAssignment.Entries),
		NumPartitions:  len(input.activePartitions),
		MovedFraction:  movedFraction,
		ScallopActions: append([]scallop.Action(nil), translation.actions...),
	}
	r.admin.addTrace(Trace{
		SlicerVersion:         SlicerVersion,
		Planner:               plannerScallop,
		Round:                 round,
		Now:                   input.now,
		Start:                 append([]assignment.Entry(nil), input.current.Entries...),
		Rates:                 ratesToWire(input.rates),
		PartitionL:            input.partitionL,
		PartitionQuerySamples: input.partitionQuerySamples,
		UnnamedQuerySamples:   input.unnamedQuerySamples,
		ActivePartitions:      append([]int32(nil), input.activePartitions...),
		Scallop: &ScallopTrace{
			Policy:               policy,
			InputPartitionOwners: cloneTracePartitionOwners(snapshot.PartitionOwners),
			ActiveReplicas:       append([]string(nil), snapshot.ActiveReplicas...),
			LastHostedAt:         cloneTraceLocality(snapshot.LastHostedAt),
			PartitionOwners:      result.PartitionOwners,
			InitialCost:          result.InitialCost,
			FinalCost:            result.FinalCost,
			CandidateSearch:      result.CandidateSearch,
		},
		End: append([]assignment.Entry(nil), translation.hashAssignment.Entries...),
	})
}

func cloneStringSet(input map[string]struct{}) map[string]struct{} {
	if input == nil {
		return nil
	}
	out := make(map[string]struct{}, len(input))
	for value := range input {
		out[value] = struct{}{}
	}
	return out
}
