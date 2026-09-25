// SPDX-License-Identifier: AGPL-3.0-only

package main

// This file implements one-shot replay of a production-shaped Scallop input.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

const replaySnapshotVersion = scallop.ReplaySnapshotVersion

// ReplaySnapshotEnvelope keeps command tests source-compatible with the shared replay contract.
type ReplaySnapshotEnvelope = scallop.ReplaySnapshotEnvelope

// ReplaySnapshotRecord is the deterministic output of one direct Scallop plan.
type ReplaySnapshotRecord struct {
	RecordType string `json:"record_type"`
	Version    int    `json:"version"`
	InputID    string `json:"input_identity"`

	Actions             []scallop.Action                   `json:"actions"`
	ProjectedAssignment *assignment.Assignment             `json:"projected_assignment"`
	ProjectedOwners     map[int32]string                   `json:"projected_partition_owners"`
	InitialCost         scallop.CostBreakdown              `json:"initial_cost"`
	FinalCost           scallop.CostBreakdown              `json:"final_cost"`
	CandidateSearch     scallop.CandidateSearchDiagnostics `json:"candidate_search"`
}

// runReplaySnapshotCommand reads one envelope, invokes Plan once, and emits one JSON record.
func runReplaySnapshotCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("replay-snapshot", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "", "Path to one versioned Scallop snapshot and policy JSON envelope.")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *inputPath == "" {
		return errors.New("-input is required")
	}

	file, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("open replay input: %w", err)
	}
	defer file.Close()

	envelope, err := decodeReplaySnapshot(file)
	if err != nil {
		return err
	}
	normalizeReplaySnapshot(&envelope.Snapshot)
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal canonical replay input: %w", err)
	}
	sum := sha256.Sum256(canonical)

	result, err := scallop.Plan(envelope.Snapshot, envelope.Policy)
	if err != nil {
		return fmt.Errorf("plan replay snapshot: %w", err)
	}
	record := ReplaySnapshotRecord{
		RecordType:          "scallop_snapshot_replay",
		Version:             replaySnapshotVersion,
		InputID:             hex.EncodeToString(sum[:]),
		Actions:             result.Actions,
		ProjectedAssignment: result.Assignment,
		ProjectedOwners:     result.PartitionOwners,
		InitialCost:         result.InitialCost,
		FinalCost:           result.FinalCost,
		CandidateSearch:     result.CandidateSearch,
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		return fmt.Errorf("encode replay result: %w", err)
	}
	return nil
}

// decodeReplaySnapshot strictly decodes exactly one supported envelope.
func decodeReplaySnapshot(r io.Reader) (ReplaySnapshotEnvelope, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ReplaySnapshotEnvelope{}, fmt.Errorf("read replay input: %w", err)
	}
	var envelope ReplaySnapshotEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return ReplaySnapshotEnvelope{}, fmt.Errorf("decode replay input: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ReplaySnapshotEnvelope{}, errors.New("decode replay input: multiple JSON values")
		}
		return ReplaySnapshotEnvelope{}, fmt.Errorf("decode replay input trailer: %w", err)
	}
	if envelope.Version != replaySnapshotVersion {
		return ReplaySnapshotEnvelope{}, fmt.Errorf("unsupported replay input version %d", envelope.Version)
	}
	if err := validateReplayInputFields(data); err != nil {
		return ReplaySnapshotEnvelope{}, err
	}
	return envelope, nil
}

// validateReplayInputFields distinguishes omitted valid-zero policy values from explicit zeroes.
func validateReplayInputFields(data []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return fmt.Errorf("decode replay input fields: %w", err)
	}
	if err := requireJSONFields("replay input", top, "version", "snapshot", "policy"); err != nil {
		return err
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(top["snapshot"], &snapshot); err != nil {
		return fmt.Errorf("snapshot must be an object: %w", err)
	}
	if err := requireJSONFields("snapshot", snapshot, "at", "assignment", "range_loads", "active_partitions", "partition_owners", "active_replicas"); err != nil {
		return err
	}
	var policy map[string]json.RawMessage
	if err := json.Unmarshal(top["policy"], &policy); err != nil {
		return fmt.Errorf("policy must be an object: %w", err)
	}
	if err := requireJSONFields("policy", policy, "weights", "action_multipliers", "candidate_search", "action_limits", "locality_window"); err != nil {
		return err
	}
	nested := []struct {
		name   string
		fields []string
	}{
		{"weights", []string{"replica_balance", "transition_events", "transition_load", "transition_hash_space", "locality_miss", "fragmentation", "resolution"}},
		{"action_multipliers", []string{"move", "split", "merge", "move_partition"}},
		{"candidate_search", []string{"max_move_sources", "max_destinations_per_range", "max_split_candidates", "max_merge_candidates", "max_partition_move_sources", "max_destinations_per_partition", "max_partition_move_candidates", "max_fully_scored"}},
		{"action_limits", []string{"total", "move", "split", "merge", "merge_per_tenant", "move_partition"}},
	}
	for _, object := range nested {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(policy[object.name], &fields); err != nil {
			return fmt.Errorf("policy.%s must be an object: %w", object.name, err)
		}
		if err := requireJSONFields("policy."+object.name, fields, object.fields...); err != nil {
			return err
		}
	}
	return nil
}

func requireJSONFields(name string, object map[string]json.RawMessage, fields ...string) error {
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("%s is incomplete: missing %q", name, field)
		}
	}
	return nil
}

// normalizeReplaySnapshot removes irrelevant input ordering before identity and planning.
func normalizeReplaySnapshot(snapshot *scallop.Snapshot) {
	sort.Slice(snapshot.ActivePartitions, func(i, j int) bool {
		return snapshot.ActivePartitions[i] < snapshot.ActivePartitions[j]
	})
	sort.Strings(snapshot.ActiveReplicas)
	if snapshot.Assignment == nil {
		return
	}
	sort.Slice(snapshot.Assignment.Entries, func(i, j int) bool {
		left, right := snapshot.Assignment.Entries[i], snapshot.Assignment.Entries[j]
		if left.TenantID != right.TenantID {
			return left.TenantID < right.TenantID
		}
		if left.Range.Lo != right.Range.Lo {
			return left.Range.Lo < right.Range.Lo
		}
		if left.Range.Hi != right.Range.Hi {
			return left.Range.Hi < right.Range.Hi
		}
		return left.PartitionID < right.PartitionID
	})
}
