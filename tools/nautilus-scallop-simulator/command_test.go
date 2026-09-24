// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

// TestRunFixtureCommandEmitsDeterministicTickRecordsAndSummary protects the command's scriptable output contract.
func TestRunFixtureCommandEmitsDeterministicTickRecordsAndSummary(t *testing.T) {
	args := []string{
		"run-fixture",
		"-fixture", "single-tenant-growing",
		"-replica-balance", "0.75",
		"-transition-events", "0.11",
		"-transition-load", "0.12",
		"-transition-hash-space", "0.13",
		"-locality-miss", "0.14",
		"-fragmentation", "0.2",
		"-resolution", "0.003",
	}
	var first, second bytes.Buffer
	require.NoError(t, runCLI(args, &first, &bytes.Buffer{}))
	require.NoError(t, runCLI(args, &second, &bytes.Buffer{}))
	require.Equal(t, first.String(), second.String())

	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture, ok := findFixture(fixtures, "single-tenant-growing")
	require.True(t, ok)

	lines := nonEmptyLines(first.String())
	require.Len(t, lines, fixture.Ticks+1)
	for tick, line := range lines[:fixture.Ticks] {
		var record TickEvaluationRecord
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		require.Equal(t, "tick", record.RecordType)
		require.Equal(t, tick, record.Tick)
		require.Equal(t, 0.75, record.Policy.Weights.ReplicaBalance)
		require.Equal(t, 0.11, record.Policy.Weights.TransitionEvents)
		require.Equal(t, 0.12, record.Policy.Weights.TransitionLoad)
		require.Equal(t, 0.13, record.Policy.Weights.TransitionHashSpace)
		require.Equal(t, 0.14, record.Policy.Weights.LocalityMiss)
		require.Equal(t, 0.2, record.Policy.Weights.Fragmentation)
		require.Equal(t, 0.003, record.Policy.Weights.Resolution)
	}
	var summary FixtureSummaryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.Equal(t, "summary", summary.RecordType)
	requireSevenEvaluationGroups(t, summary.Evaluation)
}

// TestRunFixtureCommandMatchesBeamSearchSimulation proves the command adds no alternate simulation behavior.
func TestRunFixtureCommandMatchesBeamSearchSimulation(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture, ok := findFixture(fixtures, "single-tenant-static")
	require.True(t, ok)
	policy := scallop.DefaultPolicy()
	policy.MaxActions = 4
	evaluation, err := evaluatePolicy([]Fixture{fixture}, policy, 0)
	require.NoError(t, err)
	require.Len(t, evaluation.Fixtures, 1)
	expected := evaluation.Fixtures[0]

	var output bytes.Buffer
	require.NoError(t, runCLI(
		[]string{"run-fixture", "-fixture", fixture.Name},
		&output,
		&bytes.Buffer{},
	))
	lines := nonEmptyLines(output.String())
	var firstTick TickEvaluationRecord
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &firstTick))
	require.Equal(t, expected.Rounds[0].PostPlan, firstTick.PostPlan)
	var summary FixtureSummaryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.Equal(t, policy, summary.Policy)
	require.Equal(t, expected.Evaluation, summary.Evaluation)
	require.Equal(t, expected.CandidateSearch, summary.CandidateSearch)
}

// TestRunFixtureCommandWritesJSONAndCSV verifies optional files carry the same tick and summary records.
func TestRunFixtureCommandWritesJSONAndCSV(t *testing.T) {
	directory := t.TempDir()
	jsonPath := filepath.Join(directory, "fixture.jsonl")
	csvPath := filepath.Join(directory, "fixture.csv")
	var output bytes.Buffer
	require.NoError(t, runCLI([]string{
		"run-fixture",
		"-fixture", "single-tenant-static",
		"-json-output", jsonPath,
		"-csv-output", csvPath,
	}, &output, &bytes.Buffer{}))

	jsonFile, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	require.Equal(t, output.String(), string(jsonFile))
	csvFile, err := os.ReadFile(csvPath)
	require.NoError(t, err)
	require.Contains(t, string(csvFile), "record_type,fixture,tick")
	require.Contains(t, string(csvFile), "summary,single-tenant-static")
	rows, err := csv.NewReader(bytes.NewReader(csvFile)).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, len(nonEmptyLines(output.String()))+1)
	for _, row := range rows {
		require.Len(t, row, len(rows[0]))
	}
}

// TestRunFixtureCommandRejectsInvalidInput keeps CLI failures explicit before simulation starts.
func TestRunFixtureCommandRejectsInvalidInput(t *testing.T) {
	for name, testCase := range map[string]struct {
		args    []string
		message string
	}{
		"missing fixture": {
			args:    []string{"run-fixture"},
			message: "-fixture is required",
		},
		"unknown fixture": {
			args:    []string{"run-fixture", "-fixture", "missing"},
			message: `unknown fixture "missing"`,
		},
		"negative weight": {
			args:    []string{"run-fixture", "-fixture", "single-tenant-static", "-resolution", "-1"},
			message: "-resolution must be finite and non-negative",
		},
		"non-finite weight": {
			args:    []string{"run-fixture", "-fixture", "single-tenant-static", "-resolution", "NaN"},
			message: "-resolution must be finite and non-negative",
		},
		"unexpected argument": {
			args:    []string{"run-fixture", "-fixture", "single-tenant-static", "extra"},
			message: "unexpected positional arguments",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := runCLI(testCase.args, &bytes.Buffer{}, &bytes.Buffer{})
			require.ErrorContains(t, err, testCase.message)
		})
	}
}

// TestTinyTenantFixtureCapturesMergeStarvationBaseline freezes the failure Phase 4 will improve.
func TestTinyTenantFixtureCapturesMergeStarvationBaseline(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture, ok := findFixture(fixtures, "many-tiny-tenants-consolidating")
	require.True(t, ok)
	require.Equal(t, 500, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Equal(t, 64, fixture.InitialRanges)
	require.Len(t, fixture.Tenants, 50)
	for _, tenant := range fixture.Tenants {
		require.NotEmpty(t, tenant.Components)
		for _, component := range tenant.Components {
			require.Equal(t, "constant", component.Amplitude.Kind)
		}
	}

	var output bytes.Buffer
	require.NoError(t, runCLI([]string{
		"run-fixture",
		"-fixture", fixture.Name,
		"-replica-balance", "0",
		"-transition-events", "0",
		"-transition-load", "0",
		"-transition-hash-space", "0",
		"-locality-miss", "0",
		"-fragmentation", "100",
		"-resolution", "0",
	}, &output, &bytes.Buffer{}))
	lines := nonEmptyLines(output.String())
	require.Len(t, lines, fixture.Ticks+1)
	ticks := make([]TickEvaluationRecord, fixture.Ticks)
	for i := range ticks {
		require.NoError(t, json.Unmarshal([]byte(lines[i]), &ticks[i]))
		require.Equal(t, 4, ticks[i].Actions.Merges)
		require.Equal(t, 3200-4*(i+1), ticks[i].RangeCount)
		require.Len(t, ticks[i].TenantRangeCounts, len(fixture.Tenants))
		require.False(t, ticks[i].Tracking.WorkloadChanged)
	}
	var summary FixtureSummaryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.Len(t, summary.Baseline.InitialRangeCounts, len(fixture.Tenants))
	require.Len(t, summary.Baseline.MergesPerTenant, len(fixture.Tenants))
	require.Equal(t, 32, summary.Baseline.FinalRangeCounts["tiny-00"])
	require.Equal(t, 32, summary.Baseline.MergesPerTenant["tiny-00"])
	for tenant, merges := range summary.Baseline.MergesPerTenant {
		if tenant != "tiny-00" {
			require.Zero(t, merges)
		}
	}
	require.Equal(t, len(fixture.Tenants), summary.Baseline.UnsettledTenants)
	require.Equal(t, 32, summary.Evaluation.RebalancingWork.Merges)
}

func nonEmptyLines(output string) []string {
	var lines []string
	for _, line := range strings.Split(output, "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
