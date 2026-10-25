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
			// What the phase cost seriesstore with metadata off: 2%, within the gate's 5%.
			pair("earlier-seriesstore", "shipped-seriesstore", 4.9e-7, 5e-7, 1.96e8, 2e8, 1.2e6)
		}
	}
	pairs, err := parsePhase2Pairs("tsdb=shipped-prometheus:shipped-prometheus-metadata,seriesstore=shipped-seriesstore:shipped-seriesstore-metadata,parity=shipped-prometheus:shipped-seriesstore,previous=earlier-seriesstore:shipped-seriesstore")
	require.NoError(t, err)
	d, err := decidePhase2(results, pairs, true, "")
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
	// The budget is 5% of the TSDB's 1e-6 CPU seconds per sample and 1000 bytes per series, with metadata off: the
	// TSDB's extras are 10% of them, seriesstore's 2%.
	require.InDelta(t, 5e-8, d.Budget.CPU.Budget, 1e-12)
	require.InDelta(t, 50, d.Budget.Bytes.Budget, 1e-6)
	require.False(t, d.Budget.CPU.TSDBWithin)
	require.True(t, d.Budget.CPU.SeriesstoreWithin)
	require.False(t, d.Budget.Bytes.TSDBWithin)
	require.True(t, d.Budget.Bytes.SeriesstoreWithin)
	require.Nil(t, d.SensitivityBudget)
	require.Equal(t, "not triggered", d.Phase6)
	require.InDelta(t, 0.02, d.PreviousCPU.Mean, 0.005)
	require.Equal(t, "met", d.ExitGate)

	// The same runs as sensitivity runs are checked against the budget too, and a previous build that cost seriesstore
	// less fails the gate.
	d, err = decidePhase2(results, pairs, true, results)
	require.NoError(t, err)
	require.NotNil(t, d.SensitivityBudget)
	require.True(t, d.SensitivityBudget.CPU.SeriesstoreWithin)
	pairs.Previous = comparison{Baseline: "shipped-seriesstore", Target: "earlier-seriesstore"}
	d, err = decidePhase2(results, pairs, true, "")
	require.NoError(t, err)
	require.Equal(t, "met", d.ExitGate, "a previous build that cost more isn't a regression")
	pairs.Previous = comparison{Baseline: "shipped-prometheus", Target: "shipped-prometheus-metadata"}
	d, err = decidePhase2(results, pairs, true, "")
	require.NoError(t, err)
	require.Equal(t, "not met", d.ExitGate, "a 10% regression")
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

// Phase 6 starts on the outcome towards columnar, or when neither engine is within the cost budget on a metric.
func TestPhase6Triggers(t *testing.T) {
	oneWithin := budgetCheck{SeriesstoreWithin: true}
	noneWithin := budgetCheck{}
	within := budgetChecks{CPU: oneWithin, Bytes: oneWithin}
	for _, c := range []struct {
		decision, credit string
		budget           budgetChecks
		sensitivity      *budgetChecks
		want             string
	}{
		{"pass", "no credit", within, nil, "not triggered"},
		{"pass", "credit", within, nil, "triggered: the outcome is towards columnar"},
		{"fail", "inconclusive", budgetChecks{CPU: oneWithin, Bytes: noneWithin}, nil, "triggered: neither engine's bytes extra is within the cost budget"},
		{"pass", "credit", budgetChecks{CPU: noneWithin, Bytes: oneWithin}, nil, "triggered: the outcome is towards columnar; neither engine's CPU extra is within the cost budget"},
		{"pass", "no credit", within, &budgetChecks{CPU: noneWithin, Bytes: oneWithin}, "triggered: neither engine's CPU extra is within the cost budget on the sensitivity runs"},
	} {
		d := phase2Decision{Decision: c.decision, Credit: c.credit, Budget: c.budget, SensitivityBudget: c.sensitivity}
		require.Equal(t, c.want, phase6Trigger(d), "%+v", c)
	}
}

// An extra is within the budget at its upper bound, and one without an interval isn't.
func TestCheckBudgets(t *testing.T) {
	results := t.TempDir()
	for i, names := range [][2]string{{"t", "t-metadata"}, {"s", "s-metadata"}} {
		dir := filepath.Join(results, fmt.Sprint(i))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, writeJSON(filepath.Join(dir, "report.json"), phase2Report(int64(i), names, [2]float64{1e-6, 1.01e-6}, [2]float64{1e9, 1.01e9}, [2]float64{0, 1e6}, [2]float64{0, 0})))
	}
	runs, err := phase2Runs(results, true)
	require.NoError(t, err)
	b := checkBudgets(runs, phase2Pairs{TSDB: comparison{"t", "t-metadata"}, Seriesstore: comparison{"s", "s-metadata"}})
	require.InDelta(t, 5e-8, b.CPU.Budget, 1e-12)
	// One run per pair gives an extra but no interval.
	require.InDelta(t, 1e-8, b.CPU.TSDB.Mean, 1e-12)
	require.False(t, b.CPU.TSDBWithin)
	require.False(t, b.CPU.SeriesstoreWithin)
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
