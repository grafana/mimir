// SPDX-License-Identifier: AGPL-3.0-only

package main

// This file implements simulator commands without duplicating simulation or evaluation logic.

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

// TickActionCounts reports the primitive actions selected during one simulation tick.
type TickActionCounts struct {
	Moves          int `json:"moves"`
	Splits         int `json:"splits"`
	Merges         int `json:"merges"`
	PartitionMoves int `json:"partition_moves"`
	Total          int `json:"total"`
}

// TickEffectiveness relates one tick's immediate imbalance change to its rebalancing work.
type TickEffectiveness struct {
	ImbalanceReduction    float64 `json:"imbalance_reduction"`
	ReductionPerAction    float64 `json:"reduction_per_action"`
	ReductionPerMovedLoad float64 `json:"reduction_per_moved_load"`
}

// TickTracking compares post-plan imbalance with the no-rebalancing baseline at one tick.
type TickTracking struct {
	PartitionCorrectedFraction *float64 `json:"partition_corrected_fraction"`
	ReplicaCorrectedFraction   *float64 `json:"replica_corrected_fraction"`
	PartitionAboveThreshold    bool     `json:"partition_above_threshold"`
	ReplicaAboveThreshold      bool     `json:"replica_above_threshold"`
	WorkloadChanged            bool     `json:"workload_changed"`
}

// TickEvaluationRecord is the stable machine-readable output for one simulated tick.
type TickEvaluationRecord struct {
	RecordType string         `json:"record_type"`
	Fixture    string         `json:"fixture"`
	Policy     scallop.Policy `json:"policy"`
	Tick       int            `json:"tick"`
	Time       time.Time      `json:"time"`

	Static   ImbalancePoint `json:"static"`
	PrePlan  ImbalancePoint `json:"pre_plan"`
	PostPlan ImbalancePoint `json:"post_plan"`

	TotalLoad         float64          `json:"total_load"`
	RangeCount        int              `json:"range_count"`
	TenantPartitions  int              `json:"tenant_partitions"`
	TenantRangeCounts map[string]int   `json:"tenant_range_counts"`
	UnsettledTenants  int              `json:"unsettled_tenants"`
	PartitionOwners   map[int32]string `json:"partition_owners"`

	Actions                    TickActionCounts                   `json:"actions"`
	MovedLoad                  float64                            `json:"moved_load"`
	MovedLoadFraction          float64                            `json:"moved_load_fraction"`
	PartitionMovedLoad         float64                            `json:"partition_moved_load"`
	PartitionMovedLoadFraction float64                            `json:"partition_moved_load_fraction"`
	MovedHashFraction          float64                            `json:"moved_hash_fraction"`
	ColdMovedLoad              float64                            `json:"cold_moved_load"`
	Effectiveness              TickEffectiveness                  `json:"action_effectiveness"`
	Tracking                   TickTracking                       `json:"imbalance_tracking"`
	CandidateSearch            scallop.CandidateSearchDiagnostics `json:"candidate_search"`
}

// FixtureBaselineSummary captures tenant-level consolidation progress over a complete run.
type FixtureBaselineSummary struct {
	InitialRangeCounts map[string]int `json:"initial_range_counts"`
	FinalRangeCounts   map[string]int `json:"final_range_counts"`
	MergesPerTenant    map[string]int `json:"merges_per_tenant"`
	UnsettledTenants   int            `json:"unsettled_tenants"`
}

// FixtureSummaryRecord carries trajectory-wide metrics that cannot be represented per tick.
type FixtureSummaryRecord struct {
	RecordType      string                 `json:"record_type"`
	Fixture         string                 `json:"fixture"`
	Policy          scallop.Policy         `json:"policy"`
	Evaluation      EvaluationReport       `json:"evaluation"`
	CandidateSearch CandidateSearchSummary `json:"candidate_search"`
	Baseline        FixtureBaselineSummary `json:"consolidation_baseline"`
}

// runCLI dispatches the default beam search or the standalone fixture runner.
func runCLI(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "run-fixture" {
		return runFixtureCommand(args[1:], stdout, stderr)
	}
	return runSearchCommand(args, stdout, stderr)
}

// runSearchCommand preserves the original complete beam-search command.
func runSearchCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("nautilus-scallop-simulator", flag.ContinueOnError)
	flags.SetOutput(stderr)
	outputDirectory := flags.String("output-dir", "scallop-results", "Directory for JSON and CSV search reports.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	fixtures, err := loadEmbeddedFixtures()
	if err != nil {
		return err
	}
	policy := scallop.DefaultPolicy()
	result, err := runWeightSearch(fixtures, policy, defaultSearchConfig())
	if err != nil {
		return err
	}
	if err := writeReports(*outputDirectory, result); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, recommendationSummary(result))
	return err
}

// runFixtureCommand executes one fixture with caller-selected weights and emits tick records plus a summary.
func runFixtureCommand(args []string, stdout, stderr io.Writer) error {
	policy := scallop.DefaultPolicy()

	flags := flag.NewFlagSet("run-fixture", flag.ContinueOnError)
	flags.SetOutput(stderr)
	fixtureName := flags.String("fixture", "", "Name of one checked-in fixture to run.")
	jsonOutput := flags.String("json-output", "", "Optional path for JSON Lines tick and summary records.")
	csvOutput := flags.String("csv-output", "", "Optional path for CSV tick and summary records.")
	flags.Float64Var(&policy.Weights.ReplicaBalance, "replica-balance", policy.Weights.ReplicaBalance, "Replica balance weight.")
	flags.Float64Var(&policy.Weights.TransitionEvents, "transition-events", policy.Weights.TransitionEvents, "Transition event weight.")
	flags.Float64Var(&policy.Weights.TransitionLoad, "transition-load", policy.Weights.TransitionLoad, "Relocated load weight.")
	flags.Float64Var(&policy.Weights.TransitionHashSpace, "transition-hash-space", policy.Weights.TransitionHashSpace, "Relocated hash-space weight.")
	flags.Float64Var(&policy.Weights.LocalityMiss, "locality-miss", policy.Weights.LocalityMiss, "Cold destination weight.")
	flags.Float64Var(&policy.Weights.Fragmentation, "fragmentation", policy.Weights.Fragmentation, "Range fragmentation weight.")
	flags.Float64Var(&policy.Weights.Resolution, "resolution", policy.Weights.Resolution, "Coarse hot-range weight.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *fixtureName == "" {
		return errors.New("-fixture is required")
	}
	if err := validateCommandWeights(policy.Weights); err != nil {
		return err
	}

	fixtures, err := loadEmbeddedFixtures()
	if err != nil {
		return err
	}
	fixture, ok := findFixture(fixtures, *fixtureName)
	if !ok {
		return fmt.Errorf("unknown fixture %q", *fixtureName)
	}
	result, err := simulateFixture(fixture, policy)
	if err != nil {
		return err
	}
	ticks, summary := buildFixtureRecords(fixture, result)
	if err := writeFixtureJSONLines(stdout, ticks, summary); err != nil {
		return err
	}
	if *jsonOutput != "" {
		if err := writeFixtureJSONLinesFile(*jsonOutput, ticks, summary); err != nil {
			return err
		}
	}
	if *csvOutput != "" {
		if err := writeFixtureCSV(*csvOutput, ticks, summary); err != nil {
			return err
		}
	}
	return nil
}

// validateCommandWeights rejects values that would make planner costs invalid or non-deterministic.
func validateCommandWeights(weights scallop.Weights) error {
	values := []struct {
		name  string
		value float64
	}{
		{"replica-balance", weights.ReplicaBalance},
		{"transition-events", weights.TransitionEvents},
		{"transition-load", weights.TransitionLoad},
		{"transition-hash-space", weights.TransitionHashSpace},
		{"locality-miss", weights.LocalityMiss},
		{"fragmentation", weights.Fragmentation},
		{"resolution", weights.Resolution},
	}
	for _, weight := range values {
		if math.IsNaN(weight.value) || math.IsInf(weight.value, 0) || weight.value < 0 {
			return fmt.Errorf("-%s must be finite and non-negative, got %v", weight.name, weight.value)
		}
	}
	return nil
}

// findFixture resolves a checked-in fixture by its stable configured name.
func findFixture(fixtures []Fixture, name string) (Fixture, bool) {
	for _, fixture := range fixtures {
		if fixture.Name == name {
			return fixture, true
		}
	}
	return Fixture{}, false
}

// buildFixtureRecords derives instantaneous metrics without replacing trajectory-wide evaluation.
func buildFixtureRecords(fixture Fixture, result SimulationResult) ([]TickEvaluationRecord, FixtureSummaryRecord) {
	ticks := make([]TickEvaluationRecord, 0, len(result.Rounds))
	mergesPerTenant := make(map[string]int, len(fixture.Tenants))
	for _, tenant := range fixture.Tenants {
		mergesPerTenant[tenant.ID] = 0
	}
	for _, round := range result.Rounds {
		actions := TickActionCounts{Total: len(round.Actions)}
		movedLoad := 0.0
		movedLoadFraction := 0.0
		partitionMovedLoad := 0.0
		partitionMovedLoadFraction := 0.0
		movedHashFraction := 0.0
		coldMovedLoad := 0.0
		for _, action := range round.Actions {
			switch action.Kind {
			case scallop.ActionMove:
				actions.Moves++
			case scallop.ActionSplit:
				actions.Splits++
			case scallop.ActionMerge:
				actions.Merges++
				mergesPerTenant[action.TenantID]++
			case scallop.ActionMovePartition:
				actions.PartitionMoves++
				partitionMovedLoad += action.MovedLoad
				partitionMovedLoadFraction += action.Transition.TransitionLoad
			}
			movedLoad += action.MovedLoad
			movedLoadFraction += action.Transition.TransitionLoad
			movedHashFraction += action.MovedHashFraction
			coldMovedLoad += actionColdMovedLoad(action)
		}
		reduction := round.PrePlan.Partition + round.PrePlan.Replica -
			round.PostPlan.Partition - round.PostPlan.Replica
		effectiveness := TickEffectiveness{ImbalanceReduction: reduction}
		if actions.Total > 0 {
			effectiveness.ReductionPerAction = reduction / float64(actions.Total)
		}
		if movedLoad > 0 {
			effectiveness.ReductionPerMovedLoad = reduction / movedLoad
		}
		ticks = append(ticks, TickEvaluationRecord{
			RecordType:                 "tick",
			Fixture:                    fixture.Name,
			Policy:                     result.Policy,
			Tick:                       round.Tick,
			Time:                       round.Time,
			Static:                     round.Static,
			PrePlan:                    round.PrePlan,
			PostPlan:                   round.PostPlan,
			TotalLoad:                  round.TotalLoad,
			RangeCount:                 round.RangeCount,
			TenantPartitions:           round.TenantPartitions,
			TenantRangeCounts:          cloneIntMap(round.TenantRangeCounts),
			UnsettledTenants:           countUnsettled(round.TenantRangeCounts, fixture.settledRangeTarget()),
			PartitionOwners:            cloneOwners(round.PartitionOwners),
			Actions:                    actions,
			MovedLoad:                  movedLoad,
			MovedLoadFraction:          movedLoadFraction,
			PartitionMovedLoad:         partitionMovedLoad,
			PartitionMovedLoadFraction: partitionMovedLoadFraction,
			MovedHashFraction:          movedHashFraction,
			ColdMovedLoad:              coldMovedLoad,
			Effectiveness:              effectiveness,
			Tracking: TickTracking{
				PartitionCorrectedFraction: correctedFraction(round.Static.Partition, round.PostPlan.Partition),
				ReplicaCorrectedFraction:   correctedFraction(round.Static.Replica, round.PostPlan.Replica),
				PartitionAboveThreshold:    round.PostPlan.Partition > fixture.ImbalanceThreshold,
				ReplicaAboveThreshold:      round.PostPlan.Replica > fixture.ImbalanceThreshold,
				WorkloadChanged:            fixture.workloadChangedAt(round.Tick),
			},
			CandidateSearch: round.CandidateSearch,
		})
	}

	initial := make(map[string]int, len(fixture.Tenants))
	for _, tenant := range fixture.Tenants {
		initial[tenant.ID] = fixture.InitialRanges
	}
	final := map[string]int{}
	if len(result.Rounds) > 0 {
		final = cloneIntMap(result.Rounds[len(result.Rounds)-1].TenantRangeCounts)
	}
	summary := FixtureSummaryRecord{
		RecordType:      "summary",
		Fixture:         fixture.Name,
		Policy:          result.Policy,
		Evaluation:      result.Evaluation,
		CandidateSearch: result.CandidateSearch,
		Baseline: FixtureBaselineSummary{
			InitialRangeCounts: initial,
			FinalRangeCounts:   final,
			MergesPerTenant:    mergesPerTenant,
			UnsettledTenants:   countUnsettled(final, fixture.settledRangeTarget()),
		},
	}
	return ticks, summary
}

// correctedFraction reports the fraction of static imbalance removed, or nil for a zero baseline.
func correctedFraction(static, actual float64) *float64 {
	if static <= 0 {
		return nil
	}
	value := (static - actual) / static
	return &value
}

// countUnsettled counts tenants that have not yet reached the fixture's structural target.
func countUnsettled(counts map[string]int, settledTarget int) int {
	count := 0
	for _, ranges := range counts {
		if ranges > settledTarget {
			count++
		}
	}
	return count
}

// cloneIntMap prevents output records from retaining mutable simulator maps.
func cloneIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// writeFixtureJSONLines emits tick records followed by the trajectory-wide summary.
func writeFixtureJSONLines(writer io.Writer, ticks []TickEvaluationRecord, summary FixtureSummaryRecord) error {
	encoder := json.NewEncoder(writer)
	for _, tick := range ticks {
		if err := encoder.Encode(tick); err != nil {
			return err
		}
	}
	return encoder.Encode(summary)
}

// writeFixtureJSONLinesFile writes the stdout-equivalent JSONL stream to a caller-selected path.
func writeFixtureJSONLinesFile(path string, ticks []TickEvaluationRecord, summary FixtureSummaryRecord) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := writeFixtureJSONLines(file, ticks, summary); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// writeFixtureCSV writes the same tick and summary records in a flat, script-friendly form.
func writeFixtureCSV(path string, ticks []TickEvaluationRecord, summary FixtureSummaryRecord) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	header := []string{
		"record_type", "fixture", "tick", "time", "policy_json",
		"static_partition", "static_replica", "pre_partition", "pre_replica", "post_partition", "post_replica",
		"total_load", "range_count", "tenant_partitions", "unsettled_tenants",
		"moves", "splits", "merges", "partition_moves", "moved_load", "moved_load_fraction",
		"partition_moved_load", "partition_moved_load_fraction", "moved_hash_fraction", "cold_moved_load",
		"imbalance_reduction", "reduction_per_action", "reduction_per_moved_load",
		"partition_corrected_fraction", "replica_corrected_fraction",
		"partition_above_threshold", "replica_above_threshold", "workload_changed",
		"tenant_range_counts_json", "partition_owners_json", "candidate_search_json", "summary_json",
	}
	if err := writer.Write(header); err != nil {
		_ = file.Close()
		return err
	}
	for _, tick := range ticks {
		row := []string{
			tick.RecordType, tick.Fixture, strconv.Itoa(tick.Tick), tick.Time.Format(time.RFC3339Nano), mustJSON(tick.Policy),
			formatFloat(tick.Static.Partition), formatFloat(tick.Static.Replica),
			formatFloat(tick.PrePlan.Partition), formatFloat(tick.PrePlan.Replica),
			formatFloat(tick.PostPlan.Partition), formatFloat(tick.PostPlan.Replica),
			formatFloat(tick.TotalLoad), strconv.Itoa(tick.RangeCount), strconv.Itoa(tick.TenantPartitions), strconv.Itoa(tick.UnsettledTenants),
			strconv.Itoa(tick.Actions.Moves), strconv.Itoa(tick.Actions.Splits), strconv.Itoa(tick.Actions.Merges), strconv.Itoa(tick.Actions.PartitionMoves),
			formatFloat(tick.MovedLoad), formatFloat(tick.MovedLoadFraction),
			formatFloat(tick.PartitionMovedLoad), formatFloat(tick.PartitionMovedLoadFraction),
			formatFloat(tick.MovedHashFraction), formatFloat(tick.ColdMovedLoad),
			formatFloat(tick.Effectiveness.ImbalanceReduction), formatFloat(tick.Effectiveness.ReductionPerAction), formatFloat(tick.Effectiveness.ReductionPerMovedLoad),
			formatOptionalFloat(tick.Tracking.PartitionCorrectedFraction), formatOptionalFloat(tick.Tracking.ReplicaCorrectedFraction),
			strconv.FormatBool(tick.Tracking.PartitionAboveThreshold), strconv.FormatBool(tick.Tracking.ReplicaAboveThreshold), strconv.FormatBool(tick.Tracking.WorkloadChanged),
			mustJSON(tick.TenantRangeCounts), mustJSON(tick.PartitionOwners), mustJSON(tick.CandidateSearch), "",
		}
		if err := writer.Write(row); err != nil {
			_ = file.Close()
			return err
		}
	}
	summaryRow := make([]string, len(header))
	summaryRow[0] = summary.RecordType
	summaryRow[1] = summary.Fixture
	summaryRow[4] = mustJSON(summary.Policy)
	summaryRow[len(summaryRow)-1] = mustJSON(summary)
	if err := writer.Write(summaryRow); err != nil {
		_ = file.Close()
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// mustJSON serializes internal report values whose concrete types are all JSON-safe.
func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
