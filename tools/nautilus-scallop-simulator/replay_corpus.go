// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

//go:embed replay-fixtures/*.json
var replayCorpusFiles embed.FS

type ReplayCorpusCase struct {
	Version           int                       `json:"version"`
	Name              string                    `json:"name"`
	At                time.Time                 `json:"at"`
	Partitions        int                       `json:"partitions"`
	PartitionOwners   []string                  `json:"partition_owners"`
	Tenants           []ReplayCorpusTenant      `json:"tenants"`
	LastHostedOffsets map[string]map[string]int `json:"last_hosted_minutes_ago,omitempty"`
	Gate              ReplayCorpusGate          `json:"gate"`
}

type ReplayCorpusTenant struct {
	ID              string  `json:"id"`
	Ranges          int     `json:"ranges"`
	TotalLoad       float64 `json:"total_load"`
	HotRange        int     `json:"hot_range,omitempty"`
	HotFraction     float64 `json:"hot_fraction,omitempty"`
	PlacementOffset int     `json:"placement_offset,omitempty"`
	SinglePartition *int    `json:"single_partition,omitempty"`
}

type ReplayCorpusGate struct {
	NoActions              bool               `json:"no_actions,omitempty"`
	RequireAction          scallop.ActionKind `json:"require_action,omitempty"`
	RequireNetMerging      bool               `json:"require_net_merging,omitempty"`
	MaxCompleteProjections int                `json:"max_complete_projections,omitempty"`
}

type ReplayEvaluation struct {
	Name                string                             `json:"name"`
	InputID             string                             `json:"input_identity"`
	Passed              bool                               `json:"passed"`
	GateErrors          []string                           `json:"gate_errors,omitempty"`
	Actions             []scallop.Action                   `json:"actions"`
	ProjectedAssignment *assignment.Assignment             `json:"projected_assignment"`
	ProjectedOwners     map[int32]string                   `json:"projected_partition_owners"`
	InitialCost         scallop.CostBreakdown              `json:"initial_cost"`
	FinalCost           scallop.CostBreakdown              `json:"final_cost"`
	CandidateSearch     scallop.CandidateSearchDiagnostics `json:"candidate_search"`
}

func loadReplayCorpus() ([]ReplayCorpusCase, error) {
	names, err := fs.Glob(replayCorpusFiles, "replay-fixtures/*.json")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var cases []ReplayCorpusCase
	seen := map[string]struct{}{}
	for _, path := range names {
		data, err := replayCorpusFiles.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var fileCases []ReplayCorpusCase
		if err := json.Unmarshal(data, &fileCases); err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		for _, replayCase := range fileCases {
			if err := replayCase.validate(); err != nil {
				return nil, fmt.Errorf("%s case %q: %w", path, replayCase.Name, err)
			}
			if _, exists := seen[replayCase.Name]; exists {
				return nil, fmt.Errorf("duplicate replay corpus case %q", replayCase.Name)
			}
			seen[replayCase.Name] = struct{}{}
			cases = append(cases, replayCase)
		}
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].Name < cases[j].Name })
	return cases, nil
}

func (c ReplayCorpusCase) validate() error {
	if c.Version != scallop.ReplaySnapshotVersion {
		return fmt.Errorf("unsupported version %d", c.Version)
	}
	if c.Name == "" || c.At.IsZero() || c.Partitions <= 1 || len(c.PartitionOwners) != c.Partitions {
		return fmt.Errorf("name, timestamp, and complete topology are required")
	}
	if len(c.Tenants) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	for _, tenant := range c.Tenants {
		if tenant.ID == "" || tenant.Ranges <= 0 || tenant.TotalLoad < 0 {
			return fmt.Errorf("invalid tenant definition")
		}
		if tenant.HotRange < 0 || tenant.HotRange >= tenant.Ranges ||
			tenant.HotFraction < 0 || tenant.HotFraction > 1 {
			return fmt.Errorf("tenant %q has invalid hotspot", tenant.ID)
		}
		if tenant.SinglePartition != nil && (*tenant.SinglePartition < 0 || *tenant.SinglePartition >= c.Partitions) {
			return fmt.Errorf("tenant %q has invalid single partition", tenant.ID)
		}
	}
	return nil
}

func (c ReplayCorpusCase) snapshot() scallop.Snapshot {
	partitionOwners := make(map[int32]string, c.Partitions)
	activePartitions := make([]int32, c.Partitions)
	replicaSet := map[string]struct{}{}
	for partition := range c.Partitions {
		partitionID := int32(partition)
		activePartitions[partition] = partitionID
		partitionOwners[partitionID] = c.PartitionOwners[partition]
		replicaSet[c.PartitionOwners[partition]] = struct{}{}
	}
	activeReplicas := make([]string, 0, len(replicaSet))
	for replica := range replicaSet {
		activeReplicas = append(activeReplicas, replica)
	}
	sort.Strings(activeReplicas)

	entries := make([]assignment.Entry, 0)
	loads := make(map[scallop.RangeKey]float64)
	for _, tenant := range c.Tenants {
		destinations := make([]int32, tenant.Ranges)
		for i := range destinations {
			destinations[i] = int32((i + tenant.PlacementOffset) % c.Partitions)
			if tenant.SinglePartition != nil {
				destinations[i] = int32(*tenant.SinglePartition)
			}
		}
		tenantAssignment := assignment.EvenSplitForTenant(tenant.ID, destinations)
		ordinaryLoad := tenant.TotalLoad / float64(tenant.Ranges)
		hotLoad := ordinaryLoad
		if tenant.HotFraction > 0 {
			hotLoad = tenant.TotalLoad * tenant.HotFraction
			if tenant.Ranges > 1 {
				ordinaryLoad = tenant.TotalLoad * (1 - tenant.HotFraction) / float64(tenant.Ranges-1)
			}
		}
		for i, entry := range tenantAssignment.Entries {
			entries = append(entries, entry)
			load := ordinaryLoad
			if i == tenant.HotRange {
				load = hotLoad
			}
			loads[scallop.RangeKey{TenantID: tenant.ID, Range: entry.Range}] = load
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TenantID != entries[j].TenantID {
			return entries[i].TenantID < entries[j].TenantID
		}
		return entries[i].Range.Lo < entries[j].Range.Lo
	})
	lastHosted := make(map[string]map[string]time.Time, len(c.LastHostedOffsets))
	for tenantID, replicas := range c.LastHostedOffsets {
		lastHosted[tenantID] = make(map[string]time.Time, len(replicas))
		for replica, minutesAgo := range replicas {
			lastHosted[tenantID][replica] = c.At.Add(-time.Duration(minutesAgo) * time.Minute)
		}
	}
	return scallop.Snapshot{
		At:               c.At,
		Assignment:       &assignment.Assignment{Entries: entries},
		RangeLoads:       loads,
		ActivePartitions: activePartitions,
		PartitionOwners:  partitionOwners,
		ActiveReplicas:   activeReplicas,
		LastHostedAt:     lastHosted,
	}
}

func evaluateReplayCorpus(policy scallop.Policy) ([]ReplayEvaluation, error) {
	cases, err := loadReplayCorpus()
	if err != nil {
		return nil, err
	}
	out := make([]ReplayEvaluation, 0, len(cases))
	for _, replayCase := range cases {
		snapshot := replayCase.snapshot()
		envelope := scallop.NewReplaySnapshotEnvelope(snapshot, policy)
		canonical, err := json.Marshal(envelope)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(canonical)
		result, err := scallop.Plan(snapshot, policy)
		if err != nil {
			return nil, fmt.Errorf("replay corpus %s: %w", replayCase.Name, err)
		}
		repeated, err := scallop.Plan(snapshot, policy)
		if err != nil {
			return nil, fmt.Errorf("repeat replay corpus %s: %w", replayCase.Name, err)
		}
		resultBytes, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		repeatedBytes, err := json.Marshal(repeated)
		if err != nil {
			return nil, err
		}
		evaluation := ReplayEvaluation{
			Name:                replayCase.Name,
			InputID:             hex.EncodeToString(sum[:]),
			Passed:              true,
			Actions:             result.Actions,
			ProjectedAssignment: result.Assignment,
			ProjectedOwners:     result.PartitionOwners,
			InitialCost:         result.InitialCost,
			FinalCost:           result.FinalCost,
			CandidateSearch:     result.CandidateSearch,
		}
		evaluation.GateErrors = replayGateErrors(replayCase, result)
		if string(resultBytes) != string(repeatedBytes) {
			evaluation.GateErrors = append(evaluation.GateErrors, "repeated planning output was not byte-identical")
		}
		evaluation.Passed = len(evaluation.GateErrors) == 0
		out = append(out, evaluation)
	}
	return out, nil
}

func replayGateErrors(replayCase ReplayCorpusCase, result scallop.PlanResult) []string {
	var errors []string
	if replayCase.Gate.NoActions && len(result.Actions) != 0 {
		errors = append(errors, fmt.Sprintf("selected %d actions, want no-op", len(result.Actions)))
	}
	if replayCase.Gate.RequireAction != "" {
		found := false
		for _, action := range result.Actions {
			found = found || action.Kind == replayCase.Gate.RequireAction
		}
		if !found {
			errors = append(errors, fmt.Sprintf("missing required %s action", replayCase.Gate.RequireAction))
		}
	}
	if replayCase.Gate.RequireNetMerging {
		merges, splits := 0, 0
		for _, action := range result.Actions {
			switch action.Kind {
			case scallop.ActionMerge:
				merges++
			case scallop.ActionSplit:
				splits++
			}
		}
		if merges <= splits || len(result.Assignment.Entries) >= len(replayCase.snapshot().Assignment.Entries) {
			errors = append(errors, fmt.Sprintf("not net-merging: merges=%d splits=%d", merges, splits))
		}
	}
	if replayCase.Gate.MaxCompleteProjections > 0 &&
		result.CandidateSearch.Work.CompleteProjections > replayCase.Gate.MaxCompleteProjections {
		errors = append(errors, fmt.Sprintf(
			"complete projections=%d exceeds %d",
			result.CandidateSearch.Work.CompleteProjections,
			replayCase.Gate.MaxCompleteProjections,
		))
	}
	if math.IsNaN(result.FinalCost.WeightedTotal) || math.IsInf(result.FinalCost.WeightedTotal, 0) {
		errors = append(errors, "final cost is not finite")
	}
	return errors
}
