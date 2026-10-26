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

// The native metadata prototype's Phase 2 decision rules, agreed before measuring, which Phase 3 decides resource
// attributes by too, with the target_info baseline as its cost budget.
const (
	// A ratio of seriesstore's extra to the TSDB's at most this passes.
	passRatio = 0.5
	// CPU extras below this share of the TSDB arm's CPU per sample are immaterial.
	materialityShare = 0.01
	// The ablation is credited when at least this share of the TSDB's extra over seriesstore's is the architecture's.
	creditShare = 0.5
	// Seriesstore with metadata off may cost at most this much more than the TSDB, at the upper bound.
	exitGateMargin = 0.10
	// And at most this much more than its previous phase's build, at the upper bound: what the phase cost it.
	regressionMargin = 0.05
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

// decisionSubject is what a phase decides on: metric metadata in Phase 2, resource attributes in Phase 3.
type decisionSubject struct {
	// The suffix of the names of the ingesters that keep it, and the pull request whose store the TSDB keeps it in.
	suffix, pr string
	// How the outcomes name it.
	noun, kept, native string
	// The metric of each engine's series that have it, and of seriesstore's runs whose starts were clamped.
	seriesWith map[string]string
	clamped    string
	// The bytes per series entry that seriesstore, and its ablation, have inline for it with it off too.
	seriesstoreBytes, ablationBytes float64
}

var (
	metadataSubject = decisionSubject{
		suffix: "-metadata", pr: "#19563", noun: "metadata is", kept: "metric metadata stays", native: "native metadata",
		seriesWith: map[string]string{"prometheus": seriesWithMetadataMetric, "seriesstore": seriesWithMetadataMetric},
		clamped:    clampedMetric, seriesstoreBytes: structuralBytesPerEntry, ablationBytes: structuralBytesPerEntry,
	}
	// The ablation's series keep their marker of a resource in padding.
	resourcesSubject = decisionSubject{
		suffix: "-resources", pr: "#17740", noun: "resources are", kept: "resource attributes stay", native: "native resources",
		seriesWith: map[string]string{"prometheus": "cortex_ingester_tsdb_head_seriesmetadata_resource_entries", "seriesstore": "cortex_ingester_seriesstore_native_resources_series"},
		clamped:    "cortex_ingester_seriesstore_native_resources_clamped_total", seriesstoreBytes: structuralBytesPerEntry,
	}
)

// subjects are the decision subjects by -subject's name.
var subjects = map[string]decisionSubject{"metadata": metadataSubject, "resources": resourcesSubject}

// seriesMetric is the metric of the series with the subject of the ingesters named name: the TSDB's, of a name
// without an engine.
func (s decisionSubject) seriesMetric(name string) string {
	if m, ok := s.seriesWith[engineOf(name)]; ok {
		return m
	}
	return s.seriesWith["prometheus"]
}

// structuralBytes is the bytes per series entry that the comparison's target has inline with the subject off too,
// which on minus off cancels: seriesstore's, or its ablation's.
func (s decisionSubject) structuralBytes(c comparison, ablation bool) float64 {
	switch {
	case engineOf(c.Target) != "seriesstore":
		return 0
	case ablation:
		return s.ablationBytes
	default:
		return s.seriesstoreBytes
	}
}

// phase2Pairs are the arms' on/off pairs, by ingester names, the engines' pair with the subject off, seriesstore's
// previous build against this one, with it off, and for resources, the TSDB without target_info against with it.
type phase2Pairs struct {
	TSDB, Seriesstore, Ablation, Parity, Previous, TargetInfo comparison
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
}

// budgetCheck is a metric's cost budget, in the metric's terms, and whether each engine's extra is within it, at its
// upper bound: an extra without an interval isn't.
type budgetCheck struct {
	Budget                        float64
	TSDB, Seriesstore             estimate
	TSDBWithin, SeriesstoreWithin bool
}

// budgetChecks are the cost budget's checks of one results directory's runs, and what the budget is.
type budgetChecks struct {
	CPU, Bytes budgetCheck
	Basis      string
	// For the target_info baseline, what target_info costs the TSDB, which the budget is the mean of.
	TargetInfo *extras `json:",omitempty"`
}

// checkBudgets checks the TSDB's and seriesstore's extras over the runs against the cost budget: what target_info
// costs the TSDB, with a target_info pair, or else a share of the TSDB's CPU per sample and live heap per series with
// the subject off, over the same runs.
func checkBudgets(runs []analyzedRun, pairs phase2Pairs, s decisionSubject) budgetChecks {
	tsdb := pairExtras(runs, pairs.TSDB, s, 0)
	seriesstore := pairExtras(runs, pairs.Seriesstore, s, s.structuralBytes(pairs.Seriesstore, false))
	check := func(budget float64, tsdb, seriesstore estimate) budgetCheck {
		return budgetCheck{Budget: budget, TSDB: tsdb, Seriesstore: seriesstore, TSDBWithin: tsdb.High <= budget, SeriesstoreWithin: seriesstore.High <= budget}
	}
	if pairs.TargetInfo.Baseline != "" {
		// Per series with the subject, as the TSDB arm keeps it, of the same series.
		series := baselineMean(runs, pairs.TSDB.Target, func(run analyzedRun, side int) float64 { return run.Memory[side][s.seriesMetric(pairs.TSDB.Target)] })
		e := targetInfoExtras(runs, pairs.TargetInfo, series)
		return budgetChecks{CPU: check(e.CPU.Mean, tsdb.CPU, seriesstore.CPU), Bytes: check(e.Bytes.Mean, tsdb.Bytes, seriesstore.Bytes), Basis: "the target_info baseline", TargetInfo: &e}
	}
	return budgetChecks{
		CPU:   check(budgetShare*baselineMean(runs, pairs.TSDB.Baseline, replayCPUPerSample), tsdb.CPU, seriesstore.CPU),
		Bytes: check(budgetShare*baselineMean(runs, pairs.TSDB.Baseline, liveHeapPerSeries), tsdb.Bytes, seriesstore.Bytes),
		Basis: fmt.Sprintf("%.0f%% of the TSDB's with it off", budgetShare*100),
	}
}

// targetInfoExtras are what target_info costs the TSDB over the pair's runs that count, without it against with it:
// CPU per sample of the ingester with it, since the one without ingests fewer, and live heap per series that would
// hold a resource instead.
func targetInfoExtras(runs []analyzedRun, c comparison, series float64) extras {
	var cpu, bytes [2][]float64
	for _, run := range runs {
		if len(run.Discard) > 0 {
			continue
		}
		var without, with, order int
		switch run.Names {
		case [2]string{c.Baseline, c.Target}:
			without, with, order = 0, 1, 0
		case [2]string{c.Target, c.Baseline}:
			without, with, order = 1, 0, 1
		default:
			continue
		}
		if samples := run.Stats[with].ReplaySamples; samples > 0 {
			cpu[order] = append(cpu[order], (run.Stats[with].ReplayCPUSeconds-run.Stats[without].ReplayCPUSeconds)/samples)
		}
		if series > 0 {
			bytes[order] = append(bytes[order], (run.Stats[with].LiveHeapEnd-run.Stats[without].LiveHeapEnd)/series)
		}
	}
	return extras{CPU: pairedEstimate(cpu), Bytes: pairedEstimate(bytes)}
}

func replayCPUPerSample(run analyzedRun, side int) float64 { return run.Stats[side].ReplayCPUPerSample }

// liveHeapPerSeries is the ingester's live heap at the window's end per series in memory, unknown without series.
func liveHeapPerSeries(run analyzedRun, side int) float64 {
	if series := run.Memory[side][memorySeriesMetric]; series > 0 {
		return run.Stats[side].LiveHeapEnd / series
	}
	return math.NaN()
}

type phase2Decision struct {
	CPU, Bytes metricDecision
	Decision   string
	Credit     string
	Outcome    string
	// The cost budget's checks of the runs, and of the sensitivity runs', if any.
	Budget            budgetChecks
	SensitivityBudget *budgetChecks
	// The decision on the runs of another scenario, reported, deciding nothing: whether it holds under it.
	Scenario *scenarioDecision `json:",omitempty"`
	// Whether Phase 6, the columnar layout, starts: "triggered: " and why, or "not triggered".
	Phase6 string
	// The exit gate: seriesstore off against the TSDB off, and against its previous build, relative, and the
	// persistent mismatches.
	ParityCPU, ParityHeap     estimate
	PreviousCPU, PreviousHeap estimate
	Mismatches                int
	ExitGate                  string
	Runs                      []analyzedRun
}

// scenarioDecision is the decision on another scenario's runs, and whether it's the same as on the runs'.
type scenarioDecision struct {
	Decision, Credit, Outcome string
	Holds                     bool
}

// pairExtras are the pair's extras over its runs that count: on minus off in each, in absolute terms, with the bytes
// per series with the subject, plus the target's structural bytes per series entry.
func pairExtras(runs []analyzedRun, c comparison, s decisionSubject, structural float64) extras {
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
		if series := run.Memory[on][s.seriesMetric(c.Target)]; series > 0 {
			extra := run.Stats[on].LiveHeapEnd - run.Stats[off].LiveHeapEnd + structural*run.Memory[on][entryCapacityMetric]
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
func decideMetric(tsdb, seriesstore, ablation estimate, floor float64, pr string) metricDecision {
	d := metricDecision{TSDB: tsdb, Seriesstore: seriesstore, Ablation: ablation, Credit: "not evaluated", SeriesstoreLesson: "not evaluated",
		ChoicesOnSeriesstore: estimate{Mean: math.NaN(), Low: math.NaN(), High: math.NaN()}}
	d.Ratio = fieller(seriesstore, tsdb, 0)
	if !math.IsNaN(ablation.Mean) {
		d.ChoicesOnSeriesstore = estimateDifference(ablation, seriesstore)
		switch c := d.ChoicesOnSeriesstore; {
		case !math.IsNaN(c.High) && c.High < 0:
			d.SeriesstoreLesson = "port " + pr + "'s choices to seriesstore"
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

// decidePhase2 applies the decision rules on the subject to the runs of the results directory, the cost budget also to
// the sensitivity runs' directory, and the decision rules to the scenario runs' directory, if any.
func decidePhase2(results string, pairs phase2Pairs, s decisionSubject, fullMetadata bool, sensitivity, scenario string) (phase2Decision, error) {
	runs, err := phase2Runs(results, s, fullMetadata)
	if err != nil {
		return phase2Decision{}, err
	}
	d := phase2Decision{Runs: runs}
	if sensitivity != "" {
		sensitivityRuns, err := phase2Runs(sensitivity, s, fullMetadata)
		if err != nil {
			return phase2Decision{}, err
		}
		b := checkBudgets(sensitivityRuns, pairs, s)
		d.SensitivityBudget = &b
	}
	tsdb := pairExtras(runs, pairs.TSDB, s, 0)
	seriesstore := pairExtras(runs, pairs.Seriesstore, s, s.structuralBytes(pairs.Seriesstore, false))
	ablation := extras{CPU: pairedEstimate([2][]float64{}), Bytes: pairedEstimate([2][]float64{})}
	if pairs.Ablation.Baseline != "" {
		ablation = pairExtras(runs, pairs.Ablation, s, s.structuralBytes(pairs.Ablation, true))
	}
	floor := materialityShare * baselineMean(runs, pairs.TSDB.Baseline, replayCPUPerSample)
	d.CPU = decideMetric(tsdb.CPU, seriesstore.CPU, ablation.CPU, floor, s.pr)
	d.Bytes = decideMetric(tsdb.Bytes, seriesstore.Bytes, ablation.Bytes, 0, s.pr)
	d.Budget = checkBudgets(runs, pairs, s)

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
		d.Outcome = fmt.Sprintf("%s no argument for seriesstore: %s on the vendored %s path, which seriesstore takes if Mimir moves onto it", s.noun, s.kept, s.pr)
	case d.Decision == "pass" && d.Credit == "credit":
		d.Outcome = fmt.Sprintf("%s a reason for an engine Mimir owns: keep building %s on seriesstore, towards columnar", s.noun, s.native)
	case d.Decision == "pass":
		d.Outcome = fmt.Sprintf("port seriesstore's choices to %s; seriesstore keeps its own design", s.pr)
	default:
		d.Outcome = fmt.Sprintf("port seriesstore's choices to %s, recorded as inconclusive; seriesstore keeps its own design", s.pr)
	}
	d.Phase6 = phase6Trigger(d)
	if scenario != "" {
		other, err := decidePhase2(scenario, pairs, s, fullMetadata, "", "")
		if err != nil {
			return phase2Decision{}, err
		}
		d.Scenario = &scenarioDecision{Decision: other.Decision, Credit: other.Credit, Outcome: other.Outcome, Holds: other.Decision == d.Decision && other.Credit == d.Credit}
	}

	parity, previous := compareRuns(runs, pairs.Parity), compareRuns(runs, pairs.Previous)
	d.ParityCPU, d.ParityHeap = parity.Estimates["replay CPU per sample"], parity.Estimates["heap live at end"]
	d.PreviousCPU, d.PreviousHeap = previous.Estimates["replay CPU per sample"], previous.Estimates["heap live at end"]
	for _, run := range runs {
		d.Mismatches += run.Mismatches
	}
	// An estimate without an interval doesn't meet it.
	within := func(e estimate, margin float64) bool { return e.High <= margin }
	d.ExitGate = "not met"
	if within(d.ParityCPU, exitGateMargin) && within(d.ParityHeap, exitGateMargin) && within(d.PreviousCPU, regressionMargin) && within(d.PreviousHeap, regressionMargin) && d.Mismatches == 0 {
		d.ExitGate = "met"
	}
	return d, nil
}

// phase6Trigger says whether Phase 6, the columnar layout, starts: on the outcome towards columnar, or when neither
// engine's extra is within the cost budget on a metric, of the runs or of the sensitivity runs.
func phase6Trigger(d phase2Decision) string {
	var reasons []string
	if d.Decision == "pass" && d.Credit == "credit" {
		reasons = append(reasons, "the outcome is towards columnar")
	}
	checks := []struct {
		runs string
		b    budgetChecks
	}{{"", d.Budget}}
	if d.SensitivityBudget != nil {
		checks = append(checks, struct {
			runs string
			b    budgetChecks
		}{" on the sensitivity runs", *d.SensitivityBudget})
	}
	for _, c := range checks {
		for _, m := range []struct {
			name string
			b    budgetCheck
		}{{"CPU", c.b.CPU}, {"bytes", c.b.Bytes}} {
			if !m.b.TSDBWithin && !m.b.SeriesstoreWithin {
				reasons = append(reasons, fmt.Sprintf("neither engine's %s extra is within the cost budget%s", m.name, c.runs))
			}
		}
	}
	if len(reasons) == 0 {
		return "not triggered"
	}
	return "triggered: " + strings.Join(reasons, "; ")
}

// phase2Runs are the results directory's runs, with the decision's discards on top of the analysis': runs whose
// subject's starts were clamped, and with fullMetadata, runs whose ingesters with it don't have it for every series.
func phase2Runs(results string, s decisionSubject, fullMetadata bool) ([]analyzedRun, error) {
	a, err := analyzeResults(results, nil)
	if err != nil {
		return nil, err
	}
	for i := range a.Runs {
		run := &a.Runs[i]
		for side, name := range run.Names {
			if !strings.Contains(name+"-", s.suffix+"-") {
				continue
			}
			memory := run.Memory[side]
			if memory[s.clamped] > 0 {
				run.Discard = append(run.Discard, fmt.Sprintf("%c's run starts clamped %.0f times", 'A'+side, memory[s.clamped]))
			}
			if with := memory[s.seriesMetric(name)]; fullMetadata && with != memory[memorySeriesMetric] {
				run.Discard = append(run.Discard, fmt.Sprintf("%c has %s of %.0f of %.0f series", 'A'+side, strings.TrimPrefix(s.suffix, "-"), with, memory[memorySeriesMetric]))
			}
		}
	}
	return a.Runs, nil
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
	if o := d.Scenario; o != nil {
		fmt.Fprintf(w, "on the scenario's runs, decision %s, %s, which holds: %t\n", o.Decision, o.Credit, o.Holds)
	}
	fmt.Fprintf(w, "on seriesstore, the TSDB's choices' extra minus its own: CPU %s: %s; bytes %s: %s\n",
		d.CPU.ChoicesOnSeriesstore.absolute(), d.CPU.SeriesstoreLesson, d.Bytes.ChoicesOnSeriesstore.absolute(), d.Bytes.SeriesstoreLesson)
	within := func(ok bool) string {
		if ok {
			return "within"
		}
		return "not within"
	}
	budgets := []struct {
		runs string
		b    *budgetChecks
	}{{"", &d.Budget}, {" of the sensitivity runs", d.SensitivityBudget}}
	for _, c := range budgets {
		if c.b == nil {
			continue
		}
		fmt.Fprintf(w, "cost budget%s, %s: CPU %.4g, TSDB %s, seriesstore %s; bytes %.4g, TSDB %s, seriesstore %s\n",
			c.runs, c.b.Basis, c.b.CPU.Budget, within(c.b.CPU.TSDBWithin), within(c.b.CPU.SeriesstoreWithin),
			c.b.Bytes.Budget, within(c.b.Bytes.TSDBWithin), within(c.b.Bytes.SeriesstoreWithin))
		if e := c.b.TargetInfo; e != nil {
			fmt.Fprintf(w, "  target_info costs the TSDB CPU %s, bytes %s\n", e.CPU.absolute(), e.Bytes.absolute())
		}
	}
	fmt.Fprintf(w, "Phase 6 %s\n", d.Phase6)
	fmt.Fprintf(w, "exit gate %s: seriesstore off against the TSDB off, CPU %s, heap %s; against its previous build, CPU %s, heap %s; %d persistent mismatches\n",
		d.ExitGate, d.ParityCPU, d.ParityHeap, d.PreviousCPU, d.PreviousHeap, d.Mismatches)
}

func (e estimate) absolute() string {
	if math.IsNaN(e.Low) {
		return fmt.Sprintf("%.4g", e.Mean)
	}
	return fmt.Sprintf("%.4g [%.4g, %.4g]", e.Mean, e.Low, e.High)
}

// parsePhase2Pairs parses tsdb=off:on,seriesstore=off:on,ablation=off:on,parity=tsdb:seriesstore,previous=before:after,
// target-info=without:with.
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
		case "previous":
			p.Previous = c[0]
		case "target-info":
			p.TargetInfo = c[0]
		default:
			return p, fmt.Errorf("unknown arm %q", key)
		}
	}
	if p.TSDB.Baseline == "" || p.Seriesstore.Baseline == "" || p.Parity.Baseline == "" || p.Previous.Baseline == "" {
		return p, fmt.Errorf("the tsdb, seriesstore, parity and previous pairs are needed")
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
