// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTQuantile(t *testing.T) {
	for df, expected := range map[float64]float64{1: 12.7062, 2: 4.3027, 4: 2.7764, 10: 2.2281, 30: 2.0423, 1e6: 1.9600} {
		require.InDelta(t, expected, tQuantile(0.975, df), 1e-4, "df %v", df)
	}
	require.InDelta(t, -2.7764, tQuantile(0.025, 4), 1e-4)
	require.InDelta(t, 0, tQuantile(0.5, 3), 1e-6)
}

func TestPairedEstimate(t *testing.T) {
	t.Run("stratified by order with two runs of each", func(t *testing.T) {
		e := pairedEstimate([2][]float64{{0.01, 0.03}, {0.05, 0.07}})
		require.True(t, e.Stratified)
		require.Equal(t, [2]int{2, 2}, e.Orders)
		require.InDelta(t, 0.04, e.Mean, 1e-12)
		// Each stratum's variance is 2e-4, so the mean's is (2e-4/2 + 2e-4/2)/4 = 5e-5, on 2 degrees of freedom.
		half := 4.3027 * math.Sqrt(5e-5)
		require.InDelta(t, 0.04-half, e.Low, 1e-5)
		require.InDelta(t, 0.04+half, e.High, 1e-5)
	})
	t.Run("over all runs with fewer", func(t *testing.T) {
		e := pairedEstimate([2][]float64{{0.01}, {0.03}})
		require.False(t, e.Stratified)
		require.InDelta(t, 0.02, e.Mean, 1e-12)
		half := 12.7062 * math.Sqrt(2e-4/2)
		require.InDelta(t, 0.02-half, e.Low, 1e-4)
		require.InDelta(t, 0.02+half, e.High, 1e-4)
	})
	t.Run("no interval of one run", func(t *testing.T) {
		e := pairedEstimate([2][]float64{{0.01}, nil})
		require.InDelta(t, 0.01, e.Mean, 1e-12)
		require.True(t, math.IsNaN(e.Low))
	})
	t.Run("nothing of no runs", func(t *testing.T) {
		require.True(t, math.IsNaN(pairedEstimate([2][]float64{}).Mean))
	})
}

// validReport is a run of the two ingesters that counts, with the replay CPU per sample of each.
func validReport(started int64, a, b string, replayA, replayB float64) Report {
	ready := map[string]float64{"cortex_ingester_tsdb_compactions_total": 1, readRejections: 0}
	return Report{
		Ingesters:   [2]string{"A " + a, "B " + b},
		Started:     started,
		SamplesPerS: 1000,
		ReplayCPU:   [2]ReplayCPU{{CPUSecondsPerSample: replayA, Coverage: 1}, {CPUSecondsPerSample: replayB, Coverage: 1}},
		Resources:   [2]Resources{{CPUCores: 1, HeapLiveAvg: 100}, {CPUCores: 1.1, HeapLiveAvg: 110}},
		Counters:    [2]Counters{{Ready: ready, WindowEnd: ready}, {Ready: ready, WindowEnd: ready}},
	}
}

func TestAnalyzeRunDiscardsWhatBreaksTheRunHygiene(t *testing.T) {
	require.Empty(t, analyzeRun("run", validReport(0, "port-prometheus", "shipped-prometheus", 1, 1)).Discard)

	steal := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	steal.Steal.Window = 0.02
	coverage := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	coverage.ReplayCPU[1].Coverage = 0.5
	rejected := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	rejected.Counters[0].WindowEnd = map[string]float64{"cortex_ingester_tsdb_compactions_total": 1, readRejections: 3}
	compacted := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	compacted.Counters[1].WindowEnd = map[string]float64{"cortex_ingester_tsdb_compactions_total": 2}
	different := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	different.Counters[1] = Counters{Ready: map[string]float64{"cortex_ingester_tsdb_compactions_total": 2}, WindowEnd: map[string]float64{"cortex_ingester_tsdb_compactions_total": 2}}
	// Stand-ins per record may be a quarter off the profile's, the others a tenth.
	target := &Composition{SeriesPerRecord: 30, SeriesPerValue: 2, StandInsPerRecord: 3}
	composed := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	composed.Composition, composed.CompositionTarget = Composition{Records: 100, SeriesPerRecord: 32, SeriesPerValue: 1.9, StandInsPerRecord: 3.6}, target
	require.Empty(t, analyzeRun("run", composed).Discard)
	miscomposed := composed
	miscomposed.Composition.SeriesPerValue = 2.3
	// The same engine's compactions may differ by 2% at ready and its checkpoints by one, but not its head truncations.
	counters := func(compactions, truncations, checkpoints float64) map[string]float64 {
		return map[string]float64{"cortex_ingester_tsdb_compactions_total": compactions, "cortex_ingester_tsdb_head_truncations_total": truncations,
			"cortex_ingester_tsdb_checkpoint_creations_total": checkpoints, "cortex_ingester_tsdb_wal_truncations_total": checkpoints}
	}
	nearly := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	nearly.Counters = [2]Counters{{Ready: counters(300, 100, 1), WindowEnd: counters(300, 100, 1)}, {Ready: counters(297, 100, 2), WindowEnd: counters(297, 100, 2)}}
	require.Empty(t, analyzeRun("run", nearly).Discard)
	farther := nearly
	farther.Counters[1] = Counters{Ready: counters(290, 100, 1), WindowEnd: counters(290, 100, 1)}
	truncated := nearly
	truncated.Counters[1] = Counters{Ready: counters(300, 99, 1), WindowEnd: counters(300, 99, 1)}
	checkpointed := nearly
	checkpointed.Counters[1] = Counters{Ready: counters(300, 100, 3), WindowEnd: counters(300, 100, 3)}
	for name, report := range map[string]Report{"steal": steal, "coverage": coverage, "read rejections": rejected, "compaction after ready": compacted, "same engine's counters differ": different, "composition": miscomposed, "compactions 3% apart": farther, "head truncations differ": truncated} {
		require.Len(t, analyzeRun("run", report).Discard, 1, name)
	}
	// Checkpoints two apart make two discards: their creations and the WAL's truncations.
	require.Len(t, analyzeRun("run", checkpointed).Discard, 2)

	// Engines count differently, and triggers of compactions are counted whether or not anything compacts.
	engines := validReport(0, "x-prometheus", "x-seriesstore", 1, 1)
	engines.Counters[1] = Counters{Ready: map[string]float64{"cortex_ingester_tsdb_compactions_triggered_total": 0}, WindowEnd: map[string]float64{"cortex_ingester_tsdb_compactions_triggered_total": 100}}
	require.Empty(t, analyzeRun("run", engines).Discard)
}

func TestAnalyzeResults(t *testing.T) {
	results := t.TempDir()
	reports := map[string]Report{
		// The target costs 2% and 4% more in one order, 4% and 6% more in the other.
		"1": validReport(1, "port-prometheus", "shipped-prometheus", 1, 1.02),
		"2": validReport(2, "port-prometheus", "shipped-prometheus", 1, 1.04),
		"3": validReport(3, "shipped-prometheus", "port-prometheus", 1.04, 1),
		"4": validReport(4, "shipped-prometheus", "port-prometheus", 1.06, 1),
		"5": validReport(5, "shipped-prometheus", "shipped-prometheus", 1, 1.01),
		"6": validReport(6, "shipped-prometheus", "shipped-prometheus", 1, 0.99),
	}
	discarded := validReport(7, "port-prometheus", "shipped-prometheus", 1, 2)
	discarded.Steal.Replay = 0.05
	reports["7"] = discarded
	for dir, report := range reports {
		require.NoError(t, os.MkdirAll(filepath.Join(results, dir), 0o755))
		require.NoError(t, writeJSON(filepath.Join(results, dir, "report.json"), report))
	}

	comparisons, err := parseComparisons("port-prometheus:shipped-prometheus")
	require.NoError(t, err)
	a, err := analyzeResults(results, comparisons)
	require.NoError(t, err)
	require.Len(t, a.Runs, 7)
	require.Len(t, a.Comparisons, 2)

	shipped := a.Comparisons[0].Estimates["replay CPU per sample"]
	require.Equal(t, [2]int{2, 2}, shipped.Orders)
	require.True(t, shipped.Stratified)
	require.InDelta(t, 0.04, shipped.Mean, 1e-12)
	require.Equal(t, comparison{"shipped-prometheus", "shipped-prometheus"}, a.Comparisons[1].comparison)
	noise := a.Comparisons[1].Estimates["replay CPU per sample"]
	require.Equal(t, [2]int{2, 0}, noise.Orders)
	require.InDelta(t, 0, noise.Mean, 1e-12)
	// The window's CPU per sample is 10% more of whichever build is B: the A/A sees it, and the orders mostly cancel
	// it out of the comparison.
	require.InDelta(t, 0.1, a.Comparisons[1].Estimates["window CPU per sample"].Mean, 1e-12)
	require.InDelta(t, (0.1-0.1/1.1)/2, a.Comparisons[0].Estimates["window CPU per sample"].Mean, 1e-12)

	// The analysis is written as JSON, NaNs as nulls.
	require.NoError(t, writeJSON(filepath.Join(results, "analysis.json"), a))
}

func TestParseComparisons(t *testing.T) {
	c, err := parseComparisons("a:b,c:d")
	require.NoError(t, err)
	require.Equal(t, []comparison{{"a", "b"}, {"c", "d"}}, c)
	_, err = parseComparisons("a")
	require.Error(t, err)
}
