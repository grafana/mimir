// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFiellerInterval(t *testing.T) {
	numerator := estimate{Mean: 1, se: 0.1, df: 1e6}
	denominator := estimate{Mean: 4, se: 0.1, df: 1e6}
	r := fieller(numerator, denominator, 0)
	require.True(t, r.Bounded)
	// About the delta method's interval, 0.25 ± 1.96 √(0.1² + 0.25²·0.1²) / 4.
	half := 1.96 * math.Sqrt(0.01+0.0625*0.01) / 4
	require.InDelta(t, 0.25-half, r.Low, 0.005)
	require.InDelta(t, 0.25+half, r.High, 0.005)
	require.Equal(t, "pass", r.verdict(passRatio))
	// A denominator that may be zero leaves the ratio unbounded.
	require.False(t, fieller(numerator, estimate{Mean: 0.1, se: 0.1, df: 1e6}, 0).Bounded)
}

// phase2Report is a run of two ingesters, with their replay CPU per sample and their memory at the window's end.
func phase2Report(started int64, names [2]string, cpu [2]float64, heap [2]float64, withMetadata, capacity [2]float64) Report {
	r := validReport(started, names[0], names[1], cpu[0], cpu[1])
	for i := range 2 {
		r.EndMemory[i] = map[string]float64{liveHeapMetric: heap[i], seriesWithMetadataMetric: withMetadata[i], entryCapacityMetric: capacity[i], memorySeriesMetric: 1e6}
	}
	return r
}

func TestDecidePhase2(t *testing.T) {
	results := t.TempDir()
	n := 0
	write := func(r Report) {
		n++
		dir := filepath.Join(results, fmt.Sprint(n))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, writeJSON(filepath.Join(dir, "report.json"), r))
	}
	// Each pair in both orders, twice, with a little noise: the TSDB's extra is 1e-7 CPU seconds per sample and 100
	// bytes per series; seriesstore's 2e-8 and 10 bytes, plus 8 bytes for each of 1.2 entries per series.
	for repeat := range 2 {
		jitter := float64(repeat) * 1e-9
		for _, order := range [][2]int{{0, 1}, {1, 0}} {
			pair := func(off, on string, cpuOff, cpuOn, heapOff, heapOn, capacity float64) {
				names := [2]string{off, on}
				cpu := [2]float64{cpuOff, cpuOn + jitter}
				heap := [2]float64{heapOff, heapOn + jitter*1e15}
				series := [2]float64{0, 1e6}
				capacities := [2]float64{capacity, capacity}
				write(phase2Report(int64(n), [2]string{names[order[0]], names[order[1]]}, [2]float64{cpu[order[0]], cpu[order[1]]}, [2]float64{heap[order[0]], heap[order[1]]}, [2]float64{series[order[0]], series[order[1]]}, [2]float64{capacities[order[0]], capacities[order[1]]}))
			}
			pair("shipped-prometheus", "shipped-prometheus-metadata", 1e-6, 1.1e-6, 1e9, 1.1e9, 0)
			pair("shipped-seriesstore", "shipped-seriesstore-metadata", 5e-7, 5.2e-7, 2e8, 2.1e8, 1.2e6)
			pair("shipped-prometheus", "shipped-seriesstore", 1e-6, 5e-7, 1e9, 2e8, 1.2e6)
		}
	}
	pairs, err := parsePhase2Pairs("tsdb=shipped-prometheus:shipped-prometheus-metadata,seriesstore=shipped-seriesstore:shipped-seriesstore-metadata,parity=shipped-prometheus:shipped-seriesstore")
	require.NoError(t, err)
	d, err := decidePhase2(results, pairs, true)
	require.NoError(t, err)

	require.InDelta(t, 1e-7, d.CPU.TSDB.Mean, 2e-9)
	require.InDelta(t, 2e-8, d.CPU.Seriesstore.Mean, 2e-9)
	require.InDelta(t, 0.2, d.CPU.Ratio.Mean, 0.03)
	require.Equal(t, "pass", d.CPU.Verdict)
	require.InDelta(t, 100, d.Bytes.TSDB.Mean, 2)
	require.InDelta(t, 10+8*1.2, d.Bytes.Seriesstore.Mean, 2)
	require.Equal(t, "pass", d.Bytes.Verdict)
	require.Equal(t, "pass", d.Decision)
	// Without an ablation, there's no credit to give.
	require.Equal(t, "not evaluated", d.CPU.Credit)
	require.Equal(t, "port seriesstore's choices to #19563; seriesstore keeps its own design", d.Outcome)
	require.Equal(t, "met", d.ExitGate)
	require.NoError(t, writeDecision(results, d))
}

// The ablation's extra against seriesstore's says whether #19563's choices would cost seriesstore less than its own,
// which decides nothing.
func TestDecideReportsTheLessonsForSeriesstore(t *testing.T) {
	at := func(mean float64) estimate { return estimate{Mean: mean, se: 1e-9, df: 10} }
	tsdb, seriesstore := at(1e-7), at(2e-8)
	for ablation, lesson := range map[float64]string{
		1e-8:   "port #19563's choices to seriesstore",
		4e-8:   "keep seriesstore's",
		2.1e-8: "inconclusive",
	} {
		d := decideMetric(tsdb, seriesstore, at(ablation), 0)
		require.Equal(t, lesson, d.SeriesstoreLesson, "ablation %g", ablation)
		require.InDelta(t, ablation-2e-8, d.ChoicesOnSeriesstore.Mean, 1e-12)
	}
	without := decideMetric(tsdb, seriesstore, pairedEstimate([2][]float64{}), 0)
	require.Equal(t, "not evaluated", without.SeriesstoreLesson)
	require.True(t, math.IsNaN(without.ChoicesOnSeriesstore.Mean))
}

func TestParseLabelledPairs(t *testing.T) {
	pairs, err := parseLabelledPairs("noskip=a-seriesstore:a-seriesstore-metadata,nocap=b-seriesstore:b-seriesstore-metadata")
	require.NoError(t, err)
	require.Equal(t, []labelledPair{
		{label: "noskip", comparison: comparison{Baseline: "a-seriesstore", Target: "a-seriesstore-metadata"}},
		{label: "nocap", comparison: comparison{Baseline: "b-seriesstore", Target: "b-seriesstore-metadata"}},
	}, pairs)
	_, err = parseLabelledPairs("noskip")
	require.Error(t, err)
}
