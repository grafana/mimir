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
		"-move-partition-multiplier", "64",
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
		require.Equal(t, 64.0, record.Policy.ActionMultipliers.MovePartition)
		require.Equal(t, defaultTrafficNoiseCV, record.TrafficNoise.CoefficientOfVariation)
	}
	var summary FixtureSummaryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.Equal(t, "summary", summary.RecordType)
	require.Equal(t, defaultTrafficNoiseCV, summary.TrafficNoise.CoefficientOfVariation)
	requireSevenEvaluationGroups(t, summary.Evaluation)
}

// TestRunFixtureCommandMatchesBeamSearchSimulation proves the command adds no alternate simulation behavior.
func TestRunFixtureCommandMatchesBeamSearchSimulation(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	policy := scallop.DefaultPolicy()
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			expected, err := simulateFixture(fixture, policy)
			require.NoError(t, err)

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
		})
	}
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
		"negative partition move multiplier": {
			args:    []string{"run-fixture", "-fixture", "single-tenant-static", "-move-partition-multiplier", "-1"},
			message: "-move-partition-multiplier must be finite and non-negative",
		},
		"negative range partition share": {
			args:    []string{"run-fixture", "-fixture", "single-tenant-static", "-max-range-partition-share", "-1"},
			message: "-max-range-partition-share must be finite and non-negative",
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

// TestTinyTenantFixtureConvergesFairly protects Phase 4's bounded compaction behavior.
func TestTinyTenantFixtureConvergesFairly(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture, ok := findFixture(fixtures, "many-tiny-tenants-consolidating")
	require.True(t, ok)
	require.Equal(t, 500, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Equal(t, 64, fixture.InitialRanges)
	require.Equal(t, 4, fixture.SettledRanges)
	require.Len(t, fixture.Tenants, 51)
	background := fixture.Tenants[0]
	require.Equal(t, "background", background.ID)
	require.Empty(t, background.Components)
	require.Equal(t, background.InitialRanges, background.SettledRanges)
	for _, tenant := range fixture.Tenants[1:] {
		require.NotEmpty(t, tenant.Components)
		for _, component := range tenant.Components {
			require.Equal(t, "constant", component.Amplitude.Kind)
		}
	}

	var output bytes.Buffer
	require.NoError(t, runCLI([]string{
		"run-fixture",
		"-fixture", fixture.Name,
	}, &output, &bytes.Buffer{}))
	lines := nonEmptyLines(output.String())
	require.Len(t, lines, fixture.Ticks+1)
	ticks := make([]TickEvaluationRecord, fixture.Ticks)
	for i := range ticks {
		require.NoError(t, json.Unmarshal([]byte(lines[i]), &ticks[i]))
		require.Len(t, ticks[i].TenantRangeCounts, len(fixture.Tenants))
		require.Equal(t, i > 0, ticks[i].Tracking.WorkloadChanged)
		require.LessOrEqual(t, ticks[i].Actions.Merges, ticks[i].Policy.ActionLimits.Merge)
		require.LessOrEqual(t, ticks[i].Actions.Total, ticks[i].Policy.ActionLimits.Total)
	}
	require.Equal(t, ticks[0].Policy.ActionLimits.Merge, ticks[0].Actions.Merges)
	require.Equal(t, background.InitialRanges, ticks[len(ticks)-1].TenantRangeCounts[background.ID])
	minFirstCount, maxFirstCount := fixture.InitialRanges, 0
	for tenant, count := range ticks[0].TenantRangeCounts {
		if tenant == background.ID {
			continue
		}
		require.Less(t, count, fixture.InitialRanges, "every tenant must share the first merge wave")
		minFirstCount = min(minFirstCount, count)
		maxFirstCount = max(maxFirstCount, count)
	}
	require.LessOrEqual(t, maxFirstCount-minFirstCount, 4, "fair merge allocation must keep first-round progress close")
	require.Zero(t, ticks[15].UnsettledTenants)
	var summary FixtureSummaryRecord
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.Len(t, summary.Baseline.InitialRangeCounts, len(fixture.Tenants))
	require.Len(t, summary.Baseline.MergesPerTenant, len(fixture.Tenants))
	for tenant, merges := range summary.Baseline.MergesPerTenant {
		if tenant == background.ID {
			require.Zero(t, merges, "background ranges sit above the merge ceiling")
			continue
		}
		require.GreaterOrEqual(t, merges, fixture.InitialRanges-fixture.SettledRanges, "tenant %s must settle", tenant)
		require.LessOrEqual(t, summary.Baseline.FinalRangeCounts[tenant], fixture.SettledRanges)
	}
	require.Zero(t, summary.Baseline.UnsettledTenants)
	require.Zero(t, summary.Evaluation.StructuralFootprint.UnsettledTenants)
	require.Less(t, summary.Evaluation.StructuralFootprint.MaxSettleTick, fixture.Ticks)
	require.Less(t, summary.Evaluation.StructuralFootprint.P95SettleTick, fixture.Ticks)
	require.LessOrEqual(t, summary.Evaluation.PeakImbalance.Partition.Worst, fixture.ImbalanceThreshold)
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
