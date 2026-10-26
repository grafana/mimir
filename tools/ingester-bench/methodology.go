// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// The native metadata prototype's check of Phase 3's methodology, agreed before its measurements. Its first runs, 4 A/A
// runs per engine, 2 on each droplet and one in each order, are held to the fast runs' hygiene, and their deltas are
// tested for bias against Phase 2's A/A runs': a Welch two-sample test for each engine and statistic.
const (
	// A test below this calls for 4 more A/A runs of the engine; on all 8 of them, a test below the second falls back
	// to Phase 2's methodology. The second is the first over the four tests, Bonferroni's.
	methodologyConfirm  = 0.05
	methodologyFallBack = 0.0125
	// An ingester whose peak RSS is above this share of its container's 32 GiB limit comes too close to it.
	memoryLimitBytes   = 32 << 30
	closeToMemoryLimit = 0.9
)

// phase2AADeltas are Phase 2's A/A runs' deltas, B over A, by engine: of CPU per replayed sample, and of live heap per
// series.
var phase2AADeltas = map[string][2][]float64{
	"prometheus":  {{-0.00188, 0.00451, -0.00031}, {0.00006, 0.00152, -0.00279}},
	"seriesstore": {{0.00271, -0.00609, -0.00529}, {0.01326, 0.01880, -0.00402}},
}

// methodologyStatistics are the statistics the A/A runs are tested on.
var methodologyStatistics = [2]string{"CPU per replayed sample", "heap per series"}

// methodologyCheck is the check of the A/A runs of some results directories, one for each droplet.
type methodologyCheck struct {
	// The runs that break the hygiene, and why: none may.
	Hygiene []string
	Engines map[string]*engineCheck
}

// engineCheck is the check of an engine's A/A runs: each statistic's test, and what follows from them.
type engineCheck struct {
	Runs    int
	Tests   [2]welchTest
	Verdict string
}

// welchTest is a Welch two-sample test of the runs' deltas against Phase 2's, with both deltas' standard deviations.
type welchTest struct {
	Deltas                []float64
	Mean, SD, Phase2SD, T float64
	DF, P                 float64
}

// checkMethodology checks the A/A runs of the results directories.
func checkMethodology(dirs []string) (methodologyCheck, error) {
	c := methodologyCheck{Engines: map[string]*engineCheck{}}
	deltas := map[string]*[2][]float64{}
	for _, dir := range dirs {
		a, err := analyzeResults(dir, nil)
		if err != nil {
			return c, err
		}
		for _, run := range a.Runs {
			if run.Names[0] != run.Names[1] {
				continue
			}
			name := filepath.Join(dir, filepath.Base(run.Dir))
			for _, reason := range run.Discard {
				c.Hygiene = append(c.Hygiene, name+": "+reason)
			}
			for side, stats := range run.Stats {
				if share := stats.RSSMax / memoryLimitBytes; share > closeToMemoryLimit {
					c.Hygiene = append(c.Hygiene, fmt.Sprintf("%s: %c's peak RSS is %.0f%% of its memory limit", name, 'A'+side, share*100))
				}
			}
			if len(run.Discard) > 0 {
				continue
			}
			engine := engineOf(run.Names[0])
			d, ok := deltas[engine]
			if !ok {
				d = &[2][]float64{}
				deltas[engine] = d
			}
			d[0] = append(d[0], run.Stats[1].ReplayCPUPerSample/run.Stats[0].ReplayCPUPerSample-1)
			d[1] = append(d[1], liveHeapPerSeries(run, 1)/liveHeapPerSeries(run, 0)-1)
		}
	}
	for engine, d := range deltas {
		e := &engineCheck{Runs: len(d[0])}
		for i := range e.Tests {
			e.Tests[i] = welch(d[i], phase2AADeltas[engine][i])
		}
		e.Verdict = methodologyVerdict(e, len(c.Hygiene) > 0)
		c.Engines[engine] = e
	}
	return c, nil
}

// methodologyVerdict is what follows from an engine's tests: with 4 runs, more runs if a test is below the first
// level; with 8, falling back if one is below the second.
func methodologyVerdict(e *engineCheck, unhygienic bool) string {
	p := math.Min(e.Tests[0].P, e.Tests[1].P)
	switch {
	case unhygienic:
		return "the hygiene isn't met"
	case e.Runs < 4 || math.IsNaN(p):
		return "too few runs"
	case e.Runs < 8 && p < methodologyConfirm:
		return "confirm with 4 more A/A runs"
	case e.Runs >= 8 && p < methodologyFallBack:
		return "fall back to Phase 2's methodology"
	default:
		return "pass"
	}
}

// welch tests whether deltas and phase2 have the same mean, without assuming the same variance.
func welch(deltas, phase2 []float64) welchTest {
	w := welchTest{Deltas: deltas, T: math.NaN(), DF: math.NaN(), P: math.NaN()}
	var phase2Mean float64
	w.Mean, w.SD = meanSD(deltas)
	phase2Mean, w.Phase2SD = meanSD(phase2)
	if len(deltas) < 2 || len(phase2) < 2 {
		return w
	}
	a, b := w.SD*w.SD/float64(len(deltas)), w.Phase2SD*w.Phase2SD/float64(len(phase2))
	w.T = (w.Mean - phase2Mean) / math.Sqrt(a+b)
	w.DF = (a + b) * (a + b) / (a*a/float64(len(deltas)-1) + b*b/float64(len(phase2)-1))
	w.P = 2 * (1 - tCDF(math.Abs(w.T), w.DF))
	return w
}

// meanSD is the values' mean and sample standard deviation.
func meanSD(values []float64) (mean, sd float64) {
	if len(values) == 0 {
		return math.NaN(), math.NaN()
	}
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))
	if len(values) < 2 {
		return mean, math.NaN()
	}
	for _, v := range values {
		sd += (v - mean) * (v - mean)
	}
	return mean, math.Sqrt(sd / float64(len(values)-1))
}

func (c methodologyCheck) print(w io.Writer) {
	if len(c.Hygiene) > 0 {
		fmt.Fprintf(w, "hygiene not met:\n  %s\n", strings.Join(c.Hygiene, "\n  "))
	}
	for _, engine := range slices.Sorted(maps.Keys(c.Engines)) {
		e := c.Engines[engine]
		fmt.Fprintf(w, "%s, %d A/A runs: %s\n", engine, e.Runs, e.Verdict)
		for i, t := range e.Tests {
			fmt.Fprintf(w, "  %s: mean %+.3f%%, SD %.3f%% against Phase 2's %.3f%%; t %.2f, df %.1f, p %.4f\n",
				methodologyStatistics[i], t.Mean*100, t.SD*100, t.Phase2SD*100, t.T, t.DF, t.P)
		}
	}
}

func writeMethodology(dir string, c methodologyCheck) error {
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "methodology.json"), data, 0o644)
}
