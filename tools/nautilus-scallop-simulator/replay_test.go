// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestReplaySnapshotMatchesDirectPlanAndIsDeterministic(t *testing.T) {
	envelope := replayTestEnvelope()
	first := runReplayForTest(t, envelope)

	normalizeReplaySnapshot(&envelope.Snapshot)
	expected, err := scallop.Plan(envelope.Snapshot, envelope.Policy)
	require.NoError(t, err)
	assert.Equal(t, expected.Actions, first.Actions)
	assert.Equal(t, expected.Assignment, first.ProjectedAssignment)
	assert.Equal(t, expected.PartitionOwners, first.ProjectedOwners)
	assert.Equal(t, expected.InitialCost, first.InitialCost)
	assert.Equal(t, expected.FinalCost, first.FinalCost)
	assert.Equal(t, expected.CandidateSearch, first.CandidateSearch)
	assert.Len(t, first.InputID, 64)

	second := runReplayForTest(t, replayTestEnvelope())
	assert.Equal(t, first, second)
}

func TestReplaySnapshotNormalizesTopologyOrdering(t *testing.T) {
	ordered := replayTestEnvelope()
	reordered := replayTestEnvelope()
	reordered.Snapshot.ActivePartitions[0], reordered.Snapshot.ActivePartitions[1] =
		reordered.Snapshot.ActivePartitions[1], reordered.Snapshot.ActivePartitions[0]
	reordered.Snapshot.ActiveReplicas[0], reordered.Snapshot.ActiveReplicas[1] =
		reordered.Snapshot.ActiveReplicas[1], reordered.Snapshot.ActiveReplicas[0]
	reordered.Snapshot.Assignment.Entries[0], reordered.Snapshot.Assignment.Entries[1] =
		reordered.Snapshot.Assignment.Entries[1], reordered.Snapshot.Assignment.Entries[0]

	assert.Equal(t, runReplayForTest(t, ordered), runReplayForTest(t, reordered))
}

func TestReplaySnapshotRejectsMalformedIncompleteAndUnknownVersion(t *testing.T) {
	temp := t.TempDir()
	for name, testCase := range map[string]struct {
		body    string
		message string
	}{
		"malformed": {
			body:    `{`,
			message: "decode replay input",
		},
		"unknown field": {
			body:    `{"version":1,"snapshot":{},"policy":{},"extra":true}`,
			message: `unknown field "extra"`,
		},
		"unknown version": {
			body:    `{"version":99,"snapshot":{},"policy":{}}`,
			message: "unsupported replay input version 99",
		},
		"incomplete": {
			body:    `{"version":1,"snapshot":{},"policy":{}}`,
			message: "snapshot is incomplete",
		},
		"partial policy": {
			body: `{
				"version": 1,
				"snapshot": {"at":"2026-01-01T00:00:00Z","assignment":null,"range_loads":{},"active_partitions":[],"partition_owners":{},"active_replicas":[]},
				"policy": {"weights":{},"action_multipliers":{},"candidate_search":{},"action_limits":{},"locality_window":0}
			}`,
			message: "policy.weights is incomplete",
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(temp, name+".json")
			require.NoError(t, os.WriteFile(path, []byte(testCase.body), 0o600))
			err := runCLI([]string{"replay-snapshot", "-input", path}, &bytes.Buffer{}, &bytes.Buffer{})
			require.ErrorContains(t, err, testCase.message)
		})
	}
}

func runReplayForTest(t *testing.T, envelope ReplaySnapshotEnvelope) ReplaySnapshotRecord {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	var output bytes.Buffer
	require.NoError(t, runCLI([]string{"replay-snapshot", "-input", path}, &output, &bytes.Buffer{}))
	var record ReplaySnapshotRecord
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	return record
}

func replayTestEnvelope() ReplaySnapshotEnvelope {
	now := time.Unix(50_000, 0).UTC()
	mid := uint32(math.MaxUint32 / 2)
	left := assignment.HashRange{Lo: 0, Hi: mid}
	right := assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}
	return ReplaySnapshotEnvelope{
		Version: replaySnapshotVersion,
		Snapshot: scallop.Snapshot{
			At: now,
			Assignment: &assignment.Assignment{Entries: []assignment.Entry{
				{TenantID: "tenant-a", Range: left, PartitionID: 0},
				{TenantID: "tenant-a", Range: right, PartitionID: 1},
			}},
			RangeLoads: map[scallop.RangeKey]float64{
				{TenantID: "tenant-a", Range: left}:  100,
				{TenantID: "tenant-a", Range: right}: 1,
			},
			ActivePartitions: []int32{0, 1},
			PartitionOwners:  map[int32]string{0: "rc-a", 1: "rc-b"},
			ActiveReplicas:   []string{"rc-a", "rc-b"},
			LastHostedAt:     map[string]map[string]time.Time{"tenant-a": {"rc-b": now}},
		},
		Policy: scallop.DefaultPolicy(),
	}
}
