// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
)

// The run hygiene's limits, outside which a run doesn't count.
const (
	maxStealShare = 0.01
	// A replay measured over less of the backfill says too little of it.
	minReplayCoverage = 0.9
	readRejections    = "cortex_ingester_utilization_limited_read_requests_total"
)

// analyzedRun is what the analysis takes from a run's report: each ingester's name without its A or B, its
// statistics, and the reasons the run doesn't count, if any.
type analyzedRun struct {
	Dir     string      `json:"dir"`
	Started int64       `json:"started"`
	Names   [2]string   `json:"names"`
	Stats   [2]runStats `json:"stats"`
	// Each ingester's memory at the window's end, and the reads that differed after their retries.
	Memory     [2]map[string]float64 `json:"memory"`
	Mismatches int                   `json:"mismatches"`
	Discard    []string              `json:"discard,omitempty"`
}

type runStats struct {
	ReplayCPUPerSample float64 `json:"replay_cpu_seconds_per_sample"`
	WindowCPUPerSample float64 `json:"window_cpu_seconds_per_sample"`
	LiveHeap           float64 `json:"heap_live_avg_bytes"`
	// After two collections at the window's end, with reads paused.
	LiveHeapEnd float64 `json:"heap_live_end_bytes"`
}

// statistics are what comparisons compare, the primary first.
var statistics = []struct {
	name string
	of   func(runStats) float64
}{
	{"replay CPU per sample", func(s runStats) float64 { return s.ReplayCPUPerSample }},
	{"window CPU per sample", func(s runStats) float64 { return s.WindowCPUPerSample }},
	{"heap live avg", func(s runStats) float64 { return s.LiveHeap }},
	{"heap live at end", func(s runStats) float64 { return s.LiveHeapEnd }},
}

// comparison is a target build and engine against a baseline, by ingester name; an A/A has the same name twice.
type comparison struct {
	Baseline string `json:"baseline"`
	Target   string `json:"target"`
}

// estimate is a comparison's paired difference of one statistic, relative to the baseline: one delta per run.
type estimate struct {
	// Runs with the baseline as A, and as B.
	Orders [2]int  `json:"orders"`
	Mean   float64 `json:"mean"`
	// The 95% interval, NaN with fewer than two runs.
	Low  float64 `json:"low"`
	High float64 `json:"high"`
	// Whether the interval is stratified by order, which takes two runs of each.
	Stratified bool `json:"stratified"`
	// The mean's standard error and degrees of freedom, NaN with fewer than two runs.
	se, df float64
}

func (e estimate) MarshalJSON() ([]byte, error) {
	type plain estimate
	finite := func(v float64) any {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return v
	}
	return json.Marshal(struct {
		plain
		Mean any `json:"mean"`
		Low  any `json:"low"`
		High any `json:"high"`
	}{plain(e), finite(e.Mean), finite(e.Low), finite(e.High)})
}

type comparisonResult struct {
	comparison
	Estimates map[string]estimate `json:"estimates"`
}

type analysis struct {
	Runs        []analyzedRun      `json:"runs"`
	Comparisons []comparisonResult `json:"comparisons"`
}

// analyzeResults compares the runs of the results directory: the comparisons given, and every A/A.
func analyzeResults(results string, comparisons []comparison) (analysis, error) {
	entries, err := os.ReadDir(results)
	if err != nil {
		return analysis{}, err
	}
	var a analysis
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(results, entry.Name(), "report.json"))
		if !entry.IsDir() || err != nil {
			continue
		}
		var report Report
		if err := json.Unmarshal(data, &report); err != nil {
			return analysis{}, fmt.Errorf("%s: %w", entry.Name(), err)
		}
		a.Runs = append(a.Runs, analyzeRun(entry.Name(), report))
	}
	sort.Slice(a.Runs, func(i, j int) bool { return a.Runs[i].Started < a.Runs[j].Started })

	seen := map[comparison]bool{}
	for _, c := range comparisons {
		seen[c] = true
	}
	for _, run := range a.Runs {
		if c := (comparison{run.Names[0], run.Names[0]}); run.Names[0] == run.Names[1] && !seen[c] {
			seen[c] = true
			comparisons = append(comparisons, c)
		}
	}
	for _, c := range comparisons {
		a.Comparisons = append(a.Comparisons, compareRuns(a.Runs, c))
	}
	return a, nil
}

// analyzeRun takes a run's statistics from its report, and checks it against the run hygiene.
func analyzeRun(dir string, report Report) analyzedRun {
	run := analyzedRun{Dir: dir, Started: report.Started, Memory: report.EndMemory}
	for _, api := range report.APIs {
		run.Mismatches += api.Mismatch
	}
	for i := range 2 {
		run.Names[i] = strings.TrimPrefix(report.Ingesters[i], string(rune('A'+i))+" ")
		run.Stats[i] = runStats{ReplayCPUPerSample: report.ReplayCPU[i].CPUSecondsPerSample, LiveHeap: report.Resources[i].HeapLiveAvg, LiveHeapEnd: report.EndMemory[i][liveHeapMetric]}
		if report.SamplesPerS > 0 {
			run.Stats[i].WindowCPUPerSample = report.Resources[i].CPUCores / report.SamplesPerS
		}
	}
	discard := func(format string, args ...any) { run.Discard = append(run.Discard, fmt.Sprintf(format, args...)) }
	if report.Steal.Replay > maxStealShare || report.Steal.Window > maxStealShare {
		discard("steal %s in the replay, %s in the window", share(report.Steal.Replay), share(report.Steal.Window))
	}
	for i, c := range report.Counters {
		id := 'A' + i
		if report.ReplayCPU[i].Coverage < minReplayCoverage {
			discard("%c's replay was measured over %.0f%% of it", id, report.ReplayCPU[i].Coverage*100)
		}
		if rejected := c.WindowEnd[readRejections]; rejected > 0 {
			discard("%c's read path rejected %.0f reads", id, rejected)
		}
		for _, name := range changedCounters(c.Ready, c.WindowEnd) {
			discard("%c's %s changed after ready", id, name)
		}
	}
	if engineOf(run.Names[0]) == engineOf(run.Names[1]) {
		for _, name := range changedCounters(report.Counters[0].Ready, report.Counters[1].Ready) {
			discard("A's and B's %s differ at ready", name)
		}
	}
	return run
}

// changedCounters are the counters that differ between before and after, but for the read path's rejections, which
// have a rule of their own, and the triggers of compactions, which an ingester counts whether or not it compacts.
func changedCounters(before, after map[string]float64) []string {
	var changed []string
	for name, v := range after {
		if name != readRejections && !strings.Contains(name, "triggered") && before[name] != v {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	return changed
}

// engineOf is the engine of an ingester named <build>-<engine>[-metadata], as run-batch.sh names them.
func engineOf(name string) string {
	for _, part := range strings.Split(name, "-") {
		if part == "prometheus" || part == "seriesstore" {
			return part
		}
	}
	return name
}

// compareRuns estimates the comparison's differences over the runs that count.
func compareRuns(runs []analyzedRun, c comparison) comparisonResult {
	result := comparisonResult{comparison: c, Estimates: map[string]estimate{}}
	for _, statistic := range statistics {
		var deltas [2][]float64
		for _, run := range runs {
			if len(run.Discard) > 0 {
				continue
			}
			var baseline, target, order int
			switch {
			case run.Names == [2]string{c.Baseline, c.Target}:
				baseline, target, order = 0, 1, 0
			case run.Names == [2]string{c.Target, c.Baseline}:
				baseline, target, order = 1, 0, 1
			default:
				continue
			}
			base := statistic.of(run.Stats[baseline])
			if base <= 0 {
				continue
			}
			deltas[order] = append(deltas[order], (statistic.of(run.Stats[target])-base)/base)
		}
		result.Estimates[statistic.name] = pairedEstimate(deltas)
	}
	return result
}

// pairedEstimate is the mean of the per-run deltas, by order, with its 95% t-interval: stratified by order, as the
// mean of the two orders' means, when both have two runs or more, and over all runs otherwise.
func pairedEstimate(deltas [2][]float64) estimate {
	e := estimate{Orders: [2]int{len(deltas[0]), len(deltas[1])}, Low: math.NaN(), High: math.NaN(), se: math.NaN(), df: math.NaN()}
	all := append(append([]float64(nil), deltas[0]...), deltas[1]...)
	if len(all) == 0 {
		e.Mean = math.NaN()
		return e
	}
	if len(deltas[0]) >= 2 && len(deltas[1]) >= 2 {
		// Welch–Satterthwaite over the two strata.
		var variance, df float64
		for _, stratum := range deltas {
			m, v := meanVariance(stratum)
			e.Mean += m / 2
			part := v / float64(len(stratum)) / 4
			variance += part
			df += part * part / float64(len(stratum)-1)
		}
		e.Stratified = true
		if variance == 0 {
			e.Low, e.High, e.se, e.df = e.Mean, e.Mean, 0, math.Inf(1)
			return e
		}
		e.se, e.df = math.Sqrt(variance), variance*variance/df
		half := tQuantile(0.975, e.df) * e.se
		e.Low, e.High = e.Mean-half, e.Mean+half
		return e
	}
	m, v := meanVariance(all)
	e.Mean = m
	if len(all) >= 2 {
		e.se, e.df = math.Sqrt(v/float64(len(all))), float64(len(all)-1)
		half := tQuantile(0.975, e.df) * e.se
		e.Low, e.High = m-half, m+half
	}
	return e
}

// meanVariance is the mean and sample variance of values.
func meanVariance(values []float64) (mean, variance float64) {
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))
	if len(values) < 2 {
		return mean, 0
	}
	for _, v := range values {
		variance += (v - mean) * (v - mean)
	}
	return mean, variance / float64(len(values)-1)
}

// tQuantile is the p quantile of Student's t distribution with df degrees of freedom, by bisection of its CDF.
func tQuantile(p, df float64) float64 {
	lo, hi := -1e4, 1e4
	for range 200 {
		mid := (lo + hi) / 2
		if tCDF(mid, df) < p {
			lo = mid
		} else {
			hi = mid
		}
	}
	return (lo + hi) / 2
}

func tCDF(t, df float64) float64 {
	tail := 0.5 * regularizedIncompleteBeta(df/2, 0.5, df/(df+t*t))
	if t > 0 {
		return 1 - tail
	}
	return tail
}

// regularizedIncompleteBeta is I_x(a, b), from its continued fraction (Numerical Recipes, 6.4).
func regularizedIncompleteBeta(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	lab, _ := math.Lgamma(a + b)
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	front := math.Exp(lab - la - lb + a*math.Log(x) + b*math.Log(1-x))
	if x < (a+1)/(a+b+2) {
		return front * betaContinuedFraction(a, b, x) / a
	}
	return 1 - front*betaContinuedFraction(b, a, 1-x)/b
}

func betaContinuedFraction(a, b, x float64) float64 {
	const tiny = 1e-300
	c, d := 1.0, 1-(a+b)*x/(a+1)
	if math.Abs(d) < tiny {
		d = tiny
	}
	d = 1 / d
	h := d
	for m := 1.0; m <= 300; m++ {
		for _, aa := range []float64{m * (b - m) * x / ((a + 2*m - 1) * (a + 2*m)), -(a + m) * (a + b + m) * x / ((a + 2*m) * (a + 2*m + 1))} {
			d = 1 + aa*d
			if math.Abs(d) < tiny {
				d = tiny
			}
			c = 1 + aa/c
			if math.Abs(c) < tiny {
				c = tiny
			}
			d = 1 / d
			h *= d * c
		}
		if math.Abs(d*c-1) < 1e-15 {
			break
		}
	}
	return h
}

// print writes the analysis: each comparison's estimates, then the runs that don't count and why.
func (a analysis) print(w io.Writer) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "comparison\truns A/B"
	for _, statistic := range statistics {
		header += "\t" + statistic.name
	}
	fmt.Fprintln(tw, header+"\t")
	for _, c := range a.Comparisons {
		name := c.Target + " against " + c.Baseline
		if c.Baseline == c.Target {
			name = "A/A " + c.Baseline
		}
		first := c.Estimates[statistics[0].name]
		line := fmt.Sprintf("%s\t%d/%d", name, first.Orders[0], first.Orders[1])
		for _, statistic := range statistics {
			line += "\t" + c.Estimates[statistic.name].String()
		}
		fmt.Fprintln(tw, line+"\t")
	}
	tw.Flush()
	for _, run := range a.Runs {
		if len(run.Discard) > 0 {
			fmt.Fprintf(w, "\n%s (%s, %s) doesn't count: %s", run.Dir, run.Names[0], run.Names[1], strings.Join(run.Discard, "; "))
		}
	}
	fmt.Fprintln(w)
}

func (e estimate) String() string {
	switch {
	case math.IsNaN(e.Mean):
		return "-"
	case math.IsNaN(e.Low):
		return fmt.Sprintf("%+.2f%%", e.Mean*100)
	default:
		suffix := ""
		if !e.Stratified {
			suffix = " (unstratified)"
		}
		return fmt.Sprintf("%+.2f%% [%+.2f%%, %+.2f%%]%s", e.Mean*100, e.Low*100, e.High*100, suffix)
	}
}

// parseComparisons parses baseline:target pairs, comma separated.
func parseComparisons(s string) ([]comparison, error) {
	var out []comparison
	for _, item := range strings.Split(s, ",") {
		if item == "" {
			continue
		}
		baseline, target, ok := strings.Cut(item, ":")
		if !ok || baseline == "" || target == "" {
			return nil, fmt.Errorf("%q is not a baseline:target pair", item)
		}
		out = append(out, comparison{Baseline: baseline, Target: target})
	}
	return out, nil
}
