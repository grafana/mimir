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

// aaResults writes fast A/A runs of the engine to a results directory, with the deltas, B over A, of CPU per replayed
// sample and heap per series, and A's peak RSS.
func aaResults(t *testing.T, engine string, cpu, heap []float64, rss float64) string {
	dir := t.TempDir()
	for n := range cpu {
		r := validReport(int64(n), "p3-"+engine, "p3-"+engine, 1e-6, 1e-6*(1+cpu[n]))
		r.Config = map[string]string{"method": methodFast}
		clean := map[string]float64{compactions: 0}
		r.Counters = [2]Counters{{Ready: clean, WindowEnd: clean}, {Ready: clean, WindowEnd: clean}}
		for i, perSeries := range []float64{1000, 1000 * (1 + heap[n])} {
			r.EndMemory[i] = map[string]float64{liveHeapMetric: perSeries * 1e6, memorySeriesMetric: 1e6}
		}
		r.Resources[0].RSSMax = rss
		run := filepath.Join(dir, fmt.Sprint(n))
		require.NoError(t, os.MkdirAll(run, 0o755))
		require.NoError(t, writeJSON(filepath.Join(run, "report.json"), r))
	}
	return dir
}

func TestMethodologyCheck(t *testing.T) {
	unbiased := []float64{-0.002, 0.004, 0.0, -0.001}
	c, err := checkMethodology([]string{aaResults(t, "prometheus", unbiased, unbiased, 20<<30), aaResults(t, "seriesstore", unbiased, []float64{0.01, 0.015, -0.004, 0.006}, 20<<30)})
	require.NoError(t, err)
	require.Empty(t, c.Hygiene)
	require.Equal(t, "pass", c.Engines["prometheus"].Verdict)
	require.Equal(t, "pass", c.Engines["seriesstore"].Verdict)
	require.Equal(t, 4, c.Engines["prometheus"].Runs)
	require.InDelta(t, 0.00025, c.Engines["prometheus"].Tests[0].Mean, 1e-12)

	// A bias of 3% asks for 4 more runs, and on 8 runs, falls back.
	biased := []float64{0.03, 0.031, 0.029, 0.032}
	c, err = checkMethodology([]string{aaResults(t, "seriesstore", biased, unbiased, 20<<30)})
	require.NoError(t, err)
	require.Less(t, c.Engines["seriesstore"].Tests[0].P, methodologyConfirm)
	require.Equal(t, "confirm with 4 more A/A runs", c.Engines["seriesstore"].Verdict)
	c, err = checkMethodology([]string{aaResults(t, "seriesstore", biased, unbiased, 20<<30), aaResults(t, "seriesstore", biased, unbiased, 20<<30)})
	require.NoError(t, err)
	require.Equal(t, 8, c.Engines["seriesstore"].Runs)
	require.Equal(t, "fall back to Phase 2's methodology", c.Engines["seriesstore"].Verdict)

	// An ingester close to its memory limit breaks the hygiene, and its run isn't tested.
	c, err = checkMethodology([]string{aaResults(t, "prometheus", unbiased, unbiased, 20<<30), aaResults(t, "prometheus", unbiased, unbiased, 30<<30)})
	require.NoError(t, err)
	require.Len(t, c.Hygiene, 4)
	require.Equal(t, 4, c.Engines["prometheus"].Runs)
	require.Equal(t, "the hygiene isn't met", c.Engines["prometheus"].Verdict)
}

// Welch's test of two samples of 4 and 3, 1.5 apart: t is 1.5 over √(5/12 + 1/3), its degrees of freedom are
// Welch–Satterthwaite's, and its two-sided p is the t density's tails, integrated numerically.
func TestWelch(t *testing.T) {
	w := welch([]float64{1, 2, 3, 4}, []float64{0, 1, 2})
	require.InDelta(t, math.Sqrt(3), w.T, 1e-9)
	require.InDelta(t, 4.959184, w.DF, 1e-6)
	require.InDelta(t, 0.144293, w.P, 1e-5)
}
