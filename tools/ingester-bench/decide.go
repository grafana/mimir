// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// The native metadata prototype's Phase 2 decision rules, agreed before measuring.
const (
	// A ratio of seriesstore's extra to the TSDB's at most this passes.
	passRatio = 0.5
	// CPU extras below this share of the TSDB arm's CPU per sample are immaterial.
	materialityShare = 0.01
	// The ablation is credited when at least this share of the TSDB's extra over seriesstore's is the architecture's.
	creditShare = 0.5
	// Seriesstore with metadata off may cost at most this much more than the TSDB, at the upper bound.
	exitGateMargin = 0.10
	// An engine's extra at most this share of the TSDB arm's cost with metadata off, at the upper bound, is within the
	// cost budget, which says whether metadata is practical at all.
	budgetShare = 0.05
	// The bytes of the inline metadata of each series entry seriesstore has room for, which on minus off cancels.
	structuralBytesPerEntry = 8
)

const (
	seriesWithMetadataMetric = "cortex_ingester_tsdb_head_native_metric_metadata_series"
	entryCapacityMetric      = "cortex_ingester_seriesstore_series_entry_capacity"
	clampedMetric            = "cortex_ingester_seriesstore_native_metric_metadata_clamped_total"
	memorySeriesMetric       = "cortex_ingester_memory_series"
)

// phase2Pairs are the arms' on/off pairs, by ingester names, and the engines' pair with metadata off.
type phase2Pairs struct {
	TSDB, Seriesstore, Ablation, Parity comparison
}

// extras are an on/off pair's absolute extras: CPU seconds per replayed sample, and bytes per series with metadata.
type extras struct {
	CPU, Bytes estimate
}

type ratio struct {
	Mean, Low, High float64
	// Whether the interval is bounded: an unbounded one is inconclusive.
	Bounded bool
}

type metricDecision struct {
	TSDB, Seriesstore, Ablation estimate
	Ratio                       ratio
	// pass, fail, inconclusive, or immaterial for CPU.
	Verdict string
	// credit, no credit, inconclusive, or not evaluated.
	Ablated ratio
	Credit  string
	// Reported, deciding nothing: the ablation's extra minus seriesstore's, whether #19563's choices cost seriesstore
	// less than its own do, for if Mimir moves onto seriesstore. "port #19563's choices to seriesstore" when surely
	// below 0, "keep seriesstore's" when surely above, or inconclusive, or not evaluated.
	ChoicesOnSeriesstore estimate
	SeriesstoreLesson    string
	// The cost budget, in the metric's terms, and whether each engine's extra is within it.
	Budget                                    float64
	TSDBWithinBudget, SeriesstoreWithinBudget bool
}

// applyBudget checks both engines' extras against the budget, at their upper bounds: an extra without an interval
// isn't within it.
func (m *metricDecision) applyBudget(budget float64) {
	m.Budget = budget
	m.TSDBWithinBudget, m.SeriesstoreWithinBudget = m.TSDB.High <= budget, m.Seriesstore.High <= budget
}

type phase2Decision struct {
	CPU, Bytes metricDecision
	Decision   string
	Credit     string
	Outcome    string
	// Whether Phase 6, the columnar layout, starts: "triggered: " and why, or "not triggered".
	Phase6 string
	// The exit gate: seriesstore off against the TSDB off, relative, and the persistent mismatches.
	ParityCPU, ParityHeap estimate
	Mismatches            int
	ExitGate              string
	Runs                  []analyzedRun
}

// pairExtras are the pair's extras over its runs that count: on minus off in each, in absolute terms.
func pairExtras(runs []analyzedRun, c comparison) extras {
	var cpu, bytes [2][]float64
	for _, run := range runs {
		if len(run.Discard) > 0 {
			continue
		}
		var off, on, order int
		switch {
		case run.Names == [2]string{c.Baseline, c.Target}:
			off, on, order = 0, 1, 0
		case run.Names == [2]string{c.Target, c.Baseline}:
			off, on, order = 1, 0, 1
		default:
			continue
		}
		cpu[order] = append(cpu[order], run.Stats[on].ReplayCPUPerSample-run.Stats[off].ReplayCPUPerSample)
		if series := run.Memory[on][seriesWithMetadataMetric]; series > 0 {
			extra := run.Stats[on].LiveHeapEnd - run.Stats[off].LiveHeapEnd
			if engineOf(c.Target) == "seriesstore" {
				extra += structuralBytesPerEntry * run.Memory[on][entryCapacityMetric]
			}
			bytes[order] = append(bytes[order], extra/series)
		}
	}
	return extras{CPU: pairedEstimate(cpu), Bytes: pairedEstimate(bytes)}
}

// fieller is the 95% interval of the ratio of two estimates, with the covariance of their errors.
func fieller(numerator, denominator estimate, covariance float64) ratio {
	r := ratio{Mean: numerator.Mean / denominator.Mean, Low: math.NaN(), High: math.NaN()}
	df := math.Min(numerator.df, denominator.df)
	if math.IsNaN(df) || df < 1 {
		return r
	}
	t := tQuantile(0.975, df)
	a, b := numerator.Mean, denominator.Mean
	va, vb := numerator.se*numerator.se, denominator.se*denominator.se
	qa := b*b - t*t*vb
	qb := a*b - t*t*covariance
	qc := a*a - t*t*va
	disc := qb*qb - qa*qc
	if qa <= 0 || disc < 0 {
		return r
	}
	root := math.Sqrt(disc)
	r.Low, r.High, r.Bounded = (qb-root)/qa, (qb+root)/qa, true
	return r
}

func (r ratio) MarshalJSON() ([]byte, error) {
	finite := func(v float64) any {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return v
	}
	return json.Marshal(map[string]any{"mean": finite(r.Mean), "low": finite(r.Low), "high": finite(r.High), "bounded": r.Bounded})
}

func (r ratio) verdict(threshold float64) string {
	switch {
	case !r.Bounded:
		return "inconclusive"
	case r.High <= threshold:
		return "pass"
	case r.Low > threshold:
		return "fail"
	default:
		return "inconclusive"
	}
}

// decideMetric decides a metric on its extras: the ratio rule, with for CPU the materiality floor first, and the
// ablation's credit, evaluated only when the TSDB's extra is surely above seriesstore's.
func decideMetric(tsdb, seriesstore, ablation estimate, floor float64) metricDecision {
	d := metricDecision{TSDB: tsdb, Seriesstore: seriesstore, Ablation: ablation, Credit: "not evaluated", SeriesstoreLesson: "not evaluated",
		ChoicesOnSeriesstore: estimate{Mean: math.NaN(), Low: math.NaN(), High: math.NaN()}}
	d.Ratio = fieller(seriesstore, tsdb, 0)
	if !math.IsNaN(ablation.Mean) {
		d.ChoicesOnSeriesstore = estimateDifference(ablation, seriesstore)
		switch c := d.ChoicesOnSeriesstore; {
		case !math.IsNaN(c.High) && c.High < 0:
			d.SeriesstoreLesson = "port #19563's choices to seriesstore"
		case !math.IsNaN(c.Low) && c.Low > 0:
			d.SeriesstoreLesson = "keep seriesstore's"
		default:
			d.SeriesstoreLesson = "inconclusive"
		}
	}
	switch {
	case floor > 0 && !math.IsNaN(tsdb.High) && tsdb.High < floor && !math.IsNaN(seriesstore.High) && seriesstore.High < floor:
		d.Verdict = "immaterial"
	case floor > 0 && !math.IsNaN(tsdb.High) && tsdb.High < floor && !math.IsNaN(seriesstore.Low) && seriesstore.Low > floor:
		d.Verdict = "fail"
	case floor > 0 && !math.IsNaN(tsdb.High) && tsdb.High < floor:
		d.Verdict = "inconclusive"
	default:
		d.Verdict = d.Ratio.verdict(passRatio)
	}
	gap := estimateDifference(tsdb, seriesstore)
	if math.IsNaN(ablation.Mean) || math.IsNaN(gap.Low) || gap.Low <= 0 {
		return d
	}
	// The TSDB's extra is in both the numerator and the denominator.
	d.Ablated = fieller(estimateDifference(tsdb, ablation), gap, tsdb.se*tsdb.se)
	switch v := d.Ablated.verdict(creditShare); {
	case !d.Ablated.Bounded:
		d.Credit = "inconclusive"
	case d.Ablated.Low >= creditShare:
		d.Credit = "credit"
	case v == "pass":
		d.Credit = "no credit"
	default:
		d.Credit = "inconclusive"
	}
	return d
}

// estimateDifference is a minus b, of independent estimates.
func estimateDifference(a, b estimate) estimate {
	e := estimate{Mean: a.Mean - b.Mean, se: math.Hypot(a.se, b.se), df: math.Min(a.df, b.df), Low: math.NaN(), High: math.NaN()}
	if !math.IsNaN(e.df) && e.df >= 1 {
		half := tQuantile(0.975, e.df) * e.se
		e.Low, e.High = e.Mean-half, e.Mean+half
	}
	return e
}

// decidePhase2 applies the decision rules to the runs of the results directory.
func decidePhase2(results string, pairs phase2Pairs, fullMetadata bool) (phase2Decision, error) {
	a, err := analyzeResults(results, nil)
	if err != nil {
		return phase2Decision{}, err
	}
	for i := range a.Runs {
		run := &a.Runs[i]
		for side, name := range run.Names {
			if !strings.HasSuffix(name, "-metadata") {
				continue
			}
			memory := run.Memory[side]
			if memory[clampedMetric] > 0 {
				run.Discard = append(run.Discard, fmt.Sprintf("%c's run starts clamped %.0f times", 'A'+side, memory[clampedMetric]))
			}
			if fullMetadata && memory[seriesWithMetadataMetric] != memory[memorySeriesMetric] {
				run.Discard = append(run.Discard, fmt.Sprintf("%c has metadata of %.0f of %.0f series", 'A'+side, memory[seriesWithMetadataMetric], memory[memorySeriesMetric]))
			}
		}
	}
	d := phase2Decision{Runs: a.Runs}
	tsdb, seriesstore := pairExtras(a.Runs, pairs.TSDB), pairExtras(a.Runs, pairs.Seriesstore)
	ablation := extras{CPU: pairedEstimate([2][]float64{}), Bytes: pairedEstimate([2][]float64{})}
	if pairs.Ablation.Baseline != "" {
		ablation = pairExtras(a.Runs, pairs.Ablation)
	}
	cpuBaseline := baselineMean(a.Runs, pairs.TSDB.Baseline, func(run analyzedRun, side int) float64 { return run.Stats[side].ReplayCPUPerSample })
	heapBaseline := baselineMean(a.Runs, pairs.TSDB.Baseline, func(run analyzedRun, side int) float64 {
		if series := run.Memory[side][memorySeriesMetric]; series > 0 {
			return run.Stats[side].LiveHeapEnd / series
		}
		return math.NaN()
	})
	d.CPU = decideMetric(tsdb.CPU, seriesstore.CPU, ablation.CPU, materialityShare*cpuBaseline)
	d.Bytes = decideMetric(tsdb.Bytes, seriesstore.Bytes, ablation.Bytes, 0)
	d.CPU.applyBudget(budgetShare * cpuBaseline)
	d.Bytes.applyBudget(budgetShare * heapBaseline)

	switch {
	case d.CPU.Verdict == "fail" || d.Bytes.Verdict == "fail":
		d.Decision = "fail"
	case d.Bytes.Verdict == "pass" && (d.CPU.Verdict == "pass" || d.CPU.Verdict == "immaterial"):
		d.Decision = "pass"
	default:
		d.Decision = "inconclusive"
	}
	// Credit on every metric that passed and is material, none on any of them, or else inconclusive.
	var credits []string
	for _, m := range []metricDecision{d.CPU, d.Bytes} {
		if m.Verdict == "pass" {
			credits = append(credits, m.Credit)
		}
	}
	switch {
	case len(credits) > 0 && allOf(credits, "credit"):
		d.Credit = "credit"
	case len(credits) > 0 && allOf(credits, "no credit"):
		d.Credit = "no credit"
	default:
		d.Credit = "inconclusive"
	}
	switch {
	case d.Decision == "fail":
		d.Outcome = "metadata is no argument for seriesstore: metric metadata stays on the vendored #19563 path, which seriesstore takes if Mimir moves onto it"
	case d.Decision == "pass" && d.Credit == "credit":
		d.Outcome = "metadata is a reason for an engine Mimir owns: keep building native metadata on seriesstore, towards columnar"
	case d.Decision == "pass":
		d.Outcome = "port seriesstore's choices to #19563; seriesstore keeps its own design"
	default:
		d.Outcome = "port seriesstore's choices to #19563, recorded as inconclusive; seriesstore keeps its own design"
	}
	d.Phase6 = phase6Trigger(d)

	parity := compareRuns(a.Runs, pairs.Parity)
	d.ParityCPU, d.ParityHeap = parity.Estimates["replay CPU per sample"], parity.Estimates["heap live at end"]
	for _, run := range a.Runs {
		d.Mismatches += run.Mismatches
	}
	d.ExitGate = "met"
	if math.IsNaN(d.ParityCPU.High) || d.ParityCPU.High > exitGateMargin || math.IsNaN(d.ParityHeap.High) || d.ParityHeap.High > exitGateMargin || d.Mismatches > 0 {
		d.ExitGate = "not met"
	}
	return d, nil
}

// phase6Trigger says whether Phase 6, the columnar layout, starts: on the outcome towards columnar, or when neither
// engine's extra is within the cost budget on a metric.
func phase6Trigger(d phase2Decision) string {
	var reasons []string
	if d.Decision == "pass" && d.Credit == "credit" {
		reasons = append(reasons, "the outcome is towards columnar")
	}
	for _, m := range []struct {
		name string
		d    metricDecision
	}{{"CPU", d.CPU}, {"bytes", d.Bytes}} {
		if !m.d.TSDBWithinBudget && !m.d.SeriesstoreWithinBudget {
			reasons = append(reasons, fmt.Sprintf("neither engine's %s extra is within the cost budget", m.name))
		}
	}
	if len(reasons) == 0 {
		return "not triggered"
	}
	return "triggered: " + strings.Join(reasons, "; ")
}

// baselineMean is the mean of a statistic of the ingesters named name over the runs that count, of those it's
// known for.
func baselineMean(runs []analyzedRun, name string, of func(run analyzedRun, side int) float64) float64 {
	sum, n := 0.0, 0
	for _, run := range runs {
		for side, other := range run.Names {
			if v := of(run, side); other == name && len(run.Discard) == 0 && !math.IsNaN(v) {
				sum += v
				n++
			}
		}
	}
	if n == 0 {
		return math.NaN()
	}
	return sum / float64(n)
}

func allOf(values []string, want string) bool {
	for _, v := range values {
		if v != want {
			return false
		}
	}
	return true
}

func (d phase2Decision) print(w io.Writer) {
	for _, m := range []struct {
		name string
		d    metricDecision
	}{{"CPU per replayed sample", d.CPU}, {"bytes per series", d.Bytes}} {
		fmt.Fprintf(w, "%s: TSDB %s, seriesstore %s, ablation %s; ratio %.3f [%.3f, %.3f]: %s; ablation's share %.3f [%.3f, %.3f]: %s\n",
			m.name, m.d.TSDB.absolute(), m.d.Seriesstore.absolute(), m.d.Ablation.absolute(), m.d.Ratio.Mean, m.d.Ratio.Low, m.d.Ratio.High, m.d.Verdict,
			m.d.Ablated.Mean, m.d.Ablated.Low, m.d.Ablated.High, m.d.Credit)
	}
	fmt.Fprintf(w, "decision %s, %s: %s\n", d.Decision, d.Credit, d.Outcome)
	fmt.Fprintf(w, "on seriesstore, #19563's choices' extra minus its own: CPU %s: %s; bytes %s: %s\n",
		d.CPU.ChoicesOnSeriesstore.absolute(), d.CPU.SeriesstoreLesson, d.Bytes.ChoicesOnSeriesstore.absolute(), d.Bytes.SeriesstoreLesson)
	within := func(ok bool) string {
		if ok {
			return "within"
		}
		return "not within"
	}
	fmt.Fprintf(w, "cost budget, %.0f%% of the TSDB's with metadata off: CPU %.4g, TSDB %s, seriesstore %s; bytes %.4g, TSDB %s, seriesstore %s; Phase 6 %s\n",
		budgetShare*100, d.CPU.Budget, within(d.CPU.TSDBWithinBudget), within(d.CPU.SeriesstoreWithinBudget),
		d.Bytes.Budget, within(d.Bytes.TSDBWithinBudget), within(d.Bytes.SeriesstoreWithinBudget), d.Phase6)
	fmt.Fprintf(w, "exit gate %s: seriesstore off against the TSDB off, CPU %s, heap %s, %d persistent mismatches\n", d.ExitGate, d.ParityCPU, d.ParityHeap, d.Mismatches)
}

func (e estimate) absolute() string {
	if math.IsNaN(e.Low) {
		return fmt.Sprintf("%.4g", e.Mean)
	}
	return fmt.Sprintf("%.4g [%.4g, %.4g]", e.Mean, e.Low, e.High)
}

// parsePhase2Pairs parses tsdb=off:on,seriesstore=off:on,ablation=off:on,parity=tsdb:seriesstore.
func parsePhase2Pairs(s string) (phase2Pairs, error) {
	var p phase2Pairs
	for _, item := range strings.Split(s, ",") {
		key, pair, ok := strings.Cut(item, "=")
		if !ok {
			return p, fmt.Errorf("%q is not arm=baseline:target", item)
		}
		c, err := parseComparisons(pair)
		if err != nil || len(c) != 1 {
			return p, fmt.Errorf("%q is not arm=baseline:target", item)
		}
		switch key {
		case "tsdb":
			p.TSDB = c[0]
		case "seriesstore":
			p.Seriesstore = c[0]
		case "ablation":
			p.Ablation = c[0]
		case "parity":
			p.Parity = c[0]
		default:
			return p, fmt.Errorf("unknown arm %q", key)
		}
	}
	if p.TSDB.Baseline == "" || p.Seriesstore.Baseline == "" || p.Parity.Baseline == "" {
		return p, fmt.Errorf("the tsdb, seriesstore and parity pairs are needed")
	}
	return p, nil
}

// labelledPair is an on/off pair of ingester names with a label of its own.
type labelledPair struct {
	label string
	comparison
}

// parseLabelledPairs parses label=off:on pairs, comma separated.
func parseLabelledPairs(s string) ([]labelledPair, error) {
	var out []labelledPair
	for _, item := range strings.Split(s, ",") {
		label, pair, ok := strings.Cut(item, "=")
		c, err := parseComparisons(pair)
		if !ok || err != nil || len(c) != 1 {
			return nil, fmt.Errorf("%q is not label=off:on", item)
		}
		out = append(out, labelledPair{label: label, comparison: c[0]})
	}
	return out, nil
}

func writeDecision(results string, d phase2Decision) error {
	data, err := json.MarshalIndent(d, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(results, "decision.json"), data, 0o644)
}
