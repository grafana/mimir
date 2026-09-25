// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

// This file adapts production lease logs and observations to Scallop's pure API.

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

type scallopSnapshotErrorKind string

const (
	scallopSnapshotInvalid  scallopSnapshotErrorKind = "invalid"
	scallopSnapshotNotReady scallopSnapshotErrorKind = "not_ready"
)

// scallopSnapshotError classifies bounded-cardinality adapter failures.
type scallopSnapshotError struct {
	kind   scallopSnapshotErrorKind
	reason string
	err    error
}

func (e *scallopSnapshotError) Error() string {
	if e.err == nil {
		return e.reason
	}
	return fmt.Sprintf("%s: %v", e.reason, e.err)
}

func (e *scallopSnapshotError) Unwrap() error { return e.err }

// scallopSnapshotFailure returns stable metric labels for an adapter error.
func scallopSnapshotFailure(err error) (scallopSnapshotErrorKind, string) {
	var snapshotErr *scallopSnapshotError
	if errors.As(err, &snapshotErr) {
		return snapshotErr.kind, snapshotErr.reason
	}
	return scallopSnapshotInvalid, "planning_error"
}

// scallopSnapshotInput contains one immutable production round observation.
type scallopSnapshotInput struct {
	now                 time.Time
	current             *assignment.Assignment
	rates               []rangeRate
	activePartitions    []int32
	activeReplicas      []string
	readcacheLog        []readcacheassignment.LogEntry
	unreadyPartitions   map[int32]bool
	unavailableReplicas map[string]struct{}
	hashLog             []assignment.LogEntry
}

// buildScallopSnapshot validates and deterministically converts production state.
func buildScallopSnapshot(input scallopSnapshotInput, policy scallop.Policy) (scallop.Snapshot, error) {
	if input.now.IsZero() {
		return scallop.Snapshot{}, snapshotInvalid("round_time", errors.New("round time is required"))
	}
	if input.current == nil {
		return scallop.Snapshot{}, snapshotNotReady("assignment_missing", errors.New("active tenant assignment is unavailable"))
	}

	current := cloneAndSortAssignment(input.current)
	if err := current.Validate(); err != nil {
		return scallop.Snapshot{}, snapshotInvalid("assignment_invalid", err)
	}
	activePartitions, activePartitionSet, err := sortedUniquePartitions(input.activePartitions)
	if err != nil {
		return scallop.Snapshot{}, err
	}
	activeReplicas, activeReplicaSet, err := sortedUniqueReplicas(input.activeReplicas)
	if err != nil {
		return scallop.Snapshot{}, err
	}
	for _, entry := range current.Entries {
		if _, ok := activePartitionSet[entry.PartitionID]; !ok {
			return scallop.Snapshot{}, snapshotInvalid(
				"assignment_inactive_partition",
				fmt.Errorf("tenant %q range [%d,%d] uses inactive partition %d", entry.TenantID, entry.Range.Lo, entry.Range.Hi, entry.PartitionID),
			)
		}
	}

	owners, err := activeScallopOwners(input.now, activePartitions, activeReplicaSet, input.readcacheLog, input.unavailableReplicas)
	if err != nil {
		return scallop.Snapshot{}, err
	}
	loads, err := authoritativeScallopLoads(current, input.rates, input.unreadyPartitions)
	if err != nil {
		return scallop.Snapshot{}, err
	}

	return scallop.Snapshot{
		At:               input.now,
		Assignment:       current,
		RangeLoads:       loads,
		ActivePartitions: activePartitions,
		PartitionOwners:  owners,
		ActiveReplicas:   activeReplicas,
		LastHostedAt: deriveScallopLocality(
			input.now,
			policy.LocalityWindow,
			input.hashLog,
			input.readcacheLog,
		),
	}, nil
}

// authoritativeScallopLoads requires one trustworthy observation per assigned range.
func authoritativeScallopLoads(current *assignment.Assignment, rates []rangeRate, unreadyPartitions map[int32]bool) (map[scallop.RangeKey]float64, error) {
	assigned := make(map[scallop.RangeKey]int32, len(current.Entries))
	for _, entry := range current.Entries {
		key := scallop.RangeKey{TenantID: entry.TenantID, Range: entry.Range}
		if _, duplicate := assigned[key]; duplicate {
			return nil, snapshotInvalid("assignment_duplicate_range", fmt.Errorf("duplicate range %+v", key))
		}
		assigned[key] = entry.PartitionID
	}

	loads := make(map[scallop.RangeKey]float64, len(assigned))
	for _, rate := range rates {
		if math.IsNaN(rate.sampleRate) || math.IsInf(rate.sampleRate, 0) || rate.sampleRate < 0 {
			return nil, snapshotInvalid("load_invalid", fmt.Errorf("tenant %q range [%d,%d] has load %v", rate.tenantID, rate.hr.Lo, rate.hr.Hi, rate.sampleRate))
		}
		key := scallop.RangeKey{TenantID: rate.tenantID, Range: rate.hr}
		partitionID, ok := assigned[key]
		if !ok || partitionID != rate.partitionID {
			return nil, snapshotInvalid("load_not_owned", fmt.Errorf("tenant %q range [%d,%d] on partition %d is not currently assigned there", rate.tenantID, rate.hr.Lo, rate.hr.Hi, rate.partitionID))
		}
		if _, duplicate := loads[key]; duplicate {
			return nil, snapshotInvalid("load_duplicate", fmt.Errorf("tenant %q range [%d,%d] has multiple observations", rate.tenantID, rate.hr.Lo, rate.hr.Hi))
		}
		loads[key] = rate.sampleRate
	}
	for key, partitionID := range assigned {
		if _, ok := loads[key]; ok {
			continue
		}
		if unreadyPartitions[partitionID] {
			return nil, snapshotNotReady("load_unknown", fmt.Errorf("partition %d is not authoritatively warm for tenant %q range [%d,%d]", partitionID, key.TenantID, key.Range.Lo, key.Range.Hi))
		}
		loads[key] = 0
	}
	return loads, nil
}

// activeScallopOwners chooses the one logical readcache owner Scallop should
// use for each partition. During a partition move, the assignment log briefly
// keeps both the old and new leases active so traffic can hand off safely.
// Scallop models one owner, so the entry with the latest start time is the
// destination that should receive future placement decisions.
func activeScallopOwners(
	now time.Time,
	activePartitions []int32,
	activeReplicas map[string]struct{},
	entries []readcacheassignment.LogEntry,
	unavailableReplicas map[string]struct{},
) (map[int32]string, error) {
	type selectedOwner struct {
		id   string
		from time.Time
	}
	selected := make(map[int32]selectedOwner, len(activePartitions))
	activeSet := make(map[int32]struct{}, len(activePartitions))
	for _, partitionID := range activePartitions {
		activeSet[partitionID] = struct{}{}
	}
	for _, entry := range entries {
		if !entry.ActiveAt(now) {
			continue
		}
		if _, ok := activeSet[entry.PartitionID]; !ok {
			continue
		}
		if entry.InstanceID == "" {
			return nil, snapshotInvalid("owner_empty", fmt.Errorf("partition %d has an empty active owner", entry.PartitionID))
		}
		previous, ok := selected[entry.PartitionID]
		switch {
		case !ok || entry.From.After(previous.from):
			selected[entry.PartitionID] = selectedOwner{id: entry.InstanceID, from: entry.From}
		case entry.From.Equal(previous.from) && entry.InstanceID != previous.id:
			return nil, snapshotInvalid("owner_conflict", fmt.Errorf("partition %d has conflicting owners %q and %q from %s", entry.PartitionID, previous.id, entry.InstanceID, entry.From))
		}
	}

	owners := make(map[int32]string, len(activePartitions))
	for _, partitionID := range activePartitions {
		owner, ok := selected[partitionID]
		if !ok {
			return nil, snapshotNotReady("owner_missing", fmt.Errorf("active partition %d has no active readcache owner", partitionID))
		}
		if _, ok := activeReplicas[owner.id]; !ok {
			return nil, snapshotNotReady("owner_inactive", fmt.Errorf("active partition %d owner %q is not an active logical replica", partitionID, owner.id))
		}
		if _, unavailable := unavailableReplicas[owner.id]; unavailable {
			return nil, snapshotNotReady("owner_unready", fmt.Errorf("active partition %d owner %q is unavailable", partitionID, owner.id))
		}
		owners[partitionID] = owner.id
	}
	return owners, nil
}

// deriveScallopLocality records the last time each tenant was actually hosted
// by each logical readcache. A tenant counts as hosted only while one of its
// ranges was assigned to a partition at the same time that readcache owned the
// partition. Scallop uses the resulting timestamps to avoid moving tenant load
// to a readcache whose tenant-local state is likely cold.
func deriveScallopLocality(
	now time.Time,
	window time.Duration,
	hashEntries []assignment.LogEntry,
	readcacheEntries []readcacheassignment.LogEntry,
) map[string]map[string]time.Time {
	if window <= 0 {
		return nil
	}
	cutoff := now.Add(-window)
	byPartition := make(map[int32][]readcacheassignment.LogEntry)
	for _, entry := range readcacheEntries {
		if entry.InstanceID == "" || !entry.To.After(entry.From) || !entry.To.After(cutoff) || !entry.From.Before(now) {
			continue
		}
		byPartition[entry.PartitionID] = append(byPartition[entry.PartitionID], entry)
	}

	result := make(map[string]map[string]time.Time)
	for _, hashEntry := range hashEntries {
		if !intervalEndsAfter(hashEntry.To, cutoff) || !hashEntry.From.Before(now) {
			continue
		}
		for _, replicaEntry := range byPartition[hashEntry.PartitionID] {
			start := laterTime(hashEntry.From, replicaEntry.From)
			end := earlierIntervalEnd(hashEntry.To, replicaEntry.To, now)
			if start.Before(cutoff) {
				start = cutoff
			}
			if !start.Before(end) {
				continue
			}
			lastHosted := end
			if hashEntry.ActiveAt(now) && replicaEntry.ActiveAt(now) {
				lastHosted = now
			}
			byReplica := result[hashEntry.TenantID]
			if byReplica == nil {
				byReplica = make(map[string]time.Time)
				result[hashEntry.TenantID] = byReplica
			}
			if previous, ok := byReplica[replicaEntry.InstanceID]; !ok || lastHosted.After(previous) {
				byReplica[replicaEntry.InstanceID] = lastHosted
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// scallopPlanTranslation is the mutation-free bridge from a plan to production logs and pushes.
type scallopPlanTranslation struct {
	hashAssignment      *assignment.Assignment
	readcacheAssignment *readcacheassignment.Assignment
	actions             []scallop.Action
	pushIntents         []scallopRangePushIntent
}

// scallopRangePushIntent binds one projected partition's ranges to its logical owner.
type scallopRangePushIntent struct {
	partitionID int32
	logicalID   string
	ranges      []ingester_client.HashRangeEntry
}

// translateScallopPlan validates a plan result and projects both assignment logs without mutation.
func translateScallopPlan(snapshot scallop.Snapshot, result scallop.PlanResult, activePartitions []int32) (scallopPlanTranslation, error) {
	if result.Assignment == nil {
		return scallopPlanTranslation{}, errors.New("scallop plan has no assignment")
	}
	hashAssignment := cloneAndSortAssignment(result.Assignment)
	if err := hashAssignment.Validate(); err != nil {
		return scallopPlanTranslation{}, fmt.Errorf("invalid scallop hash assignment: %w", err)
	}
	if err := validateScallopTenantIsolation(snapshot.Assignment, hashAssignment, result.Actions); err != nil {
		return scallopPlanTranslation{}, err
	}
	partitions, activeSet, err := sortedUniquePartitions(activePartitions)
	if err != nil {
		return scallopPlanTranslation{}, err
	}
	readcacheEntries := make([]readcacheassignment.AssignmentEntry, 0, len(partitions))
	for _, partitionID := range partitions {
		owner := result.PartitionOwners[partitionID]
		if owner == "" {
			return scallopPlanTranslation{}, fmt.Errorf("scallop plan has no owner for active partition %d", partitionID)
		}
		readcacheEntries = append(readcacheEntries, readcacheassignment.AssignmentEntry{PartitionID: partitionID, InstanceID: owner})
	}
	for partitionID := range result.PartitionOwners {
		if _, ok := activeSet[partitionID]; !ok {
			return scallopPlanTranslation{}, fmt.Errorf("scallop plan includes owner for inactive partition %d", partitionID)
		}
	}

	rangesByPartition := make(map[int32][]ingester_client.HashRangeEntry)
	for _, entry := range hashAssignment.Entries {
		if _, ok := activeSet[entry.PartitionID]; !ok {
			return scallopPlanTranslation{}, fmt.Errorf("scallop assignment uses inactive partition %d", entry.PartitionID)
		}
		rangesByPartition[entry.PartitionID] = append(rangesByPartition[entry.PartitionID], ingester_client.HashRangeEntry{
			TenantId:    entry.TenantID,
			Lo:          entry.Range.Lo,
			Hi:          entry.Range.Hi,
			PartitionId: entry.PartitionID,
		})
	}
	intents := make([]scallopRangePushIntent, 0, len(partitions))
	for _, entry := range readcacheEntries {
		intents = append(intents, scallopRangePushIntent{
			partitionID: entry.PartitionID,
			logicalID:   entry.InstanceID,
			ranges:      append([]ingester_client.HashRangeEntry(nil), rangesByPartition[entry.PartitionID]...),
		})
	}
	return scallopPlanTranslation{
		hashAssignment:      hashAssignment,
		readcacheAssignment: &readcacheassignment.Assignment{Entries: readcacheEntries},
		actions:             append([]scallop.Action(nil), result.Actions...),
		pushIntents:         intents,
	}, nil
}

// validateScallopTenantIsolation ensures partition-only moves and untouched tenants keep their ranges.
func validateScallopTenantIsolation(before, after *assignment.Assignment, actions []scallop.Action) error {
	if before == nil {
		return errors.New("Scallop snapshot has no source assignment")
	}
	touched := make(map[string]struct{})
	for _, action := range actions {
		switch action.Kind {
		case scallop.ActionMove, scallop.ActionSplit, scallop.ActionMerge:
			touched[action.TenantID] = struct{}{}
		case scallop.ActionMovePartition:
		default:
			return fmt.Errorf("Scallop plan contains unknown action kind %q", action.Kind)
		}
	}
	beforeByTenant := assignmentEntriesByTenant(before)
	afterByTenant := assignmentEntriesByTenant(after)
	for tenantID, entries := range beforeByTenant {
		if _, changed := touched[tenantID]; changed {
			continue
		}
		if !assignmentEntriesEqual(entries, afterByTenant[tenantID]) {
			return fmt.Errorf("Scallop plan changed untouched tenant %q", tenantID)
		}
		delete(afterByTenant, tenantID)
	}
	for tenantID := range afterByTenant {
		if _, changed := touched[tenantID]; !changed {
			return fmt.Errorf("Scallop plan introduced untouched tenant %q", tenantID)
		}
	}
	return nil
}

func assignmentEntriesByTenant(input *assignment.Assignment) map[string][]assignment.Entry {
	out := make(map[string][]assignment.Entry)
	for _, entry := range input.Entries {
		out[entry.TenantID] = append(out[entry.TenantID], entry)
	}
	for tenantID := range out {
		sort.Slice(out[tenantID], func(i, j int) bool {
			return out[tenantID][i].Range.Lo < out[tenantID][j].Range.Lo
		})
	}
	return out
}

func assignmentEntriesEqual(left, right []assignment.Entry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// resolveScallopPushIntents expands logical owners to concrete readcache RPC targets.
func resolveScallopPushIntents(intents []scallopRangePushIntent, replicaMap readcacheassignment.ReplicaMap) map[string][]ingester_client.HashRangeEntry {
	resolved := make(map[string][]ingester_client.HashRangeEntry)
	for _, intent := range intents {
		for _, concreteID := range replicaMap.ConcreteIDs(intent.logicalID) {
			resolved[concreteID] = append(resolved[concreteID], intent.ranges...)
		}
	}
	return resolved
}

// The production stores expose caller-owned, independently ordered values,
// while Scallop requires an immutable canonical snapshot for deterministic
// planning and replay. Keep that normalization at this adapter boundary rather
// than teaching either the stores or the planner about the other's internals.
func sortedUniquePartitions(input []int32) ([]int32, map[int32]struct{}, error) {
	out := append([]int32(nil), input...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	set := make(map[int32]struct{}, len(out))
	for _, partitionID := range out {
		if _, duplicate := set[partitionID]; duplicate {
			return nil, nil, snapshotInvalid("partition_duplicate", fmt.Errorf("active partition %d is duplicated", partitionID))
		}
		set[partitionID] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil, snapshotInvalid("partitions_missing", errors.New("active partitions are required"))
	}
	return out, set, nil
}

func sortedUniqueReplicas(input []string) ([]string, map[string]struct{}, error) {
	out := append([]string(nil), input...)
	sort.Strings(out)
	set := make(map[string]struct{}, len(out))
	for _, replicaID := range out {
		if replicaID == "" {
			return nil, nil, snapshotInvalid("replica_empty", errors.New("active logical replica ID is empty"))
		}
		if _, duplicate := set[replicaID]; duplicate {
			return nil, nil, snapshotInvalid("replica_duplicate", fmt.Errorf("active logical replica %q is duplicated", replicaID))
		}
		set[replicaID] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil, snapshotNotReady("replicas_missing", errors.New("active logical replicas are unavailable"))
	}
	return out, set, nil
}

func cloneAndSortAssignment(input *assignment.Assignment) *assignment.Assignment {
	out := &assignment.Assignment{Entries: append([]assignment.Entry(nil), input.Entries...)}
	sort.Slice(out.Entries, func(i, j int) bool {
		if out.Entries[i].TenantID != out.Entries[j].TenantID {
			return out.Entries[i].TenantID < out.Entries[j].TenantID
		}
		if out.Entries[i].Range.Lo != out.Entries[j].Range.Lo {
			return out.Entries[i].Range.Lo < out.Entries[j].Range.Lo
		}
		if out.Entries[i].Range.Hi != out.Entries[j].Range.Hi {
			return out.Entries[i].Range.Hi < out.Entries[j].Range.Hi
		}
		return out.Entries[i].PartitionID < out.Entries[j].PartitionID
	})
	return out
}

func snapshotInvalid(reason string, err error) error {
	return &scallopSnapshotError{kind: scallopSnapshotInvalid, reason: reason, err: err}
}

func snapshotNotReady(reason string, err error) error {
	return &scallopSnapshotError{kind: scallopSnapshotNotReady, reason: reason, err: err}
}

func intervalEndsAfter(end, at time.Time) bool {
	return end.IsZero() || end.After(at)
}

func laterTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func earlierIntervalEnd(a, b, cap time.Time) time.Time {
	out := cap
	if !a.IsZero() && a.Before(out) {
		out = a
	}
	if !b.IsZero() && b.Before(out) {
		out = b
	}
	return out
}
