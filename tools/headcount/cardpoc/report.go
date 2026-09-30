// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"fmt"
	"strings"

	"github.com/grafana/mimir/tools/headcount/model"
)

// E1Summary aggregates RunE1's per-block results into totals.
type E1Summary struct {
	Blocks, ValuesChecked, Mismatches int
}

func summarizeE1(results []E1Result) E1Summary {
	s := E1Summary{Blocks: len(results)}
	for _, r := range results {
		s.ValuesChecked += r.ValuesChecked
		if !r.Exact() {
			s.Mismatches++
		}
	}
	return s
}

func (s E1Summary) Pass() bool { return s.Mismatches == 0 }

// Report is every core experiment's outcome against one
// tenant's l1, handover and compacted snapshots.
type Report struct {
	E1L1, E1Handover, E1Compacted E1Summary
	E2                            E2Result
	E3                            E3Result
	E4                            E4Result
	E6                            E6Result
	E9K3, E9K12                   E9Result
}

// Pass reports whether every experiment in the report passed.
func (r Report) Pass() bool {
	return r.E1L1.Pass() && r.E1Handover.Pass() && r.E1Compacted.Pass() &&
		r.E2.Pass() && r.E3.Pass() && r.E4.Pass() && r.E6.Pass() &&
		r.E9K3.Pass() && r.E9K12.Pass()
}

// RunAll runs E1 (on all three snapshots), E2, E3, E4, E6 and E9 (at
// K=3 and K=12 simulated store-gateways), and reports each experiment's
// pass/fail alongside the report as a whole.
func RunAll(l1Dir, handoverDir, compactedDir string, pop *model.Model, e6Threshold, e9TopN int) (Report, error) {
	var r Report

	e1l1, err := RunE1(l1Dir)
	if err != nil {
		return r, fmt.Errorf("E1 on %s: %w", l1Dir, err)
	}
	r.E1L1 = summarizeE1(e1l1)

	e1handover, err := RunE1(handoverDir)
	if err != nil {
		return r, fmt.Errorf("E1 on %s: %w", handoverDir, err)
	}
	r.E1Handover = summarizeE1(e1handover)

	e1compacted, err := RunE1(compactedDir)
	if err != nil {
		return r, fmt.Errorf("E1 on %s: %w", compactedDir, err)
	}
	r.E1Compacted = summarizeE1(e1compacted)

	if r.E2, err = RunE2(compactedDir, pop); err != nil {
		return r, fmt.Errorf("E2: %w", err)
	}
	if r.E3, err = RunE3(l1Dir, pop); err != nil {
		return r, fmt.Errorf("E3: %w", err)
	}
	if r.E4, err = RunE4(handoverDir, pop); err != nil {
		return r, fmt.Errorf("E4: %w", err)
	}
	if r.E6, err = RunE6(compactedDir, pop, e6Threshold); err != nil {
		return r, fmt.Errorf("E6: %w", err)
	}
	if r.E9K3, err = RunE9(compactedDir, pop, 3, e9TopN); err != nil {
		return r, fmt.Errorf("E9 (K=3): %w", err)
	}
	if r.E9K12, err = RunE9(compactedDir, pop, 12, e9TopN); err != nil {
		return r, fmt.Errorf("E9 (K=12): %w", err)
	}
	return r, nil
}

// String renders a one-line-per-experiment summary, suitable for a CLI.
func (r Report) String() string {
	var b strings.Builder
	line := func(name string, pass bool, detail string) {
		status := "FAIL"
		if pass {
			status = "PASS"
		}
		fmt.Fprintf(&b, "%-4s %-22s %s\n", status, name, detail)
	}

	line("E1", r.E1L1.Pass(), fmt.Sprintf("l1: %d blocks, %d values, %d mismatches", r.E1L1.Blocks, r.E1L1.ValuesChecked, r.E1L1.Mismatches))
	line("E1", r.E1Handover.Pass(), fmt.Sprintf("handover: %d blocks, %d values, %d mismatches", r.E1Handover.Blocks, r.E1Handover.ValuesChecked, r.E1Handover.Mismatches))
	line("E1", r.E1Compacted.Pass(), fmt.Sprintf("compacted: %d blocks, %d values, %d mismatches", r.E1Compacted.Blocks, r.E1Compacted.ValuesChecked, r.E1Compacted.Mismatches))
	line("E2", r.E2.Pass(), fmt.Sprintf("truth=%d sum_all=%d(%+.1f%%) hash_union=%d", r.E2.Truth, r.E2.SumAll, 100*r.E2.RelativeError(r.E2.SumAll), r.E2.HashUnion))
	line("E3", r.E3.Pass(), fmt.Sprintf("truth=%d sum_all=%d(%+.1f%%) sources_skip=%d(%+.1f%%) min_level2=%d(%+.1f%%) hash_union=%d",
		r.E3.Truth, r.E3.SumAll, 100*r.E3.RelativeError(r.E3.SumAll), r.E3.SumSkipSources, 100*r.E3.RelativeError(r.E3.SumSkipSources), r.E3.SumMinLevel2, 100*r.E3.RelativeError(r.E3.SumMinLevel2), r.E3.HashUnion))
	line("E4", r.E4.Pass(), fmt.Sprintf("truth=%d sum_all=%d(%+.1f%%) sources_skip=%d(%+.1f%%) hash_union=%d",
		r.E4.Truth, r.E4.SumAll, 100*r.E4.RelativeError(r.E4.SumAll), r.E4.SumSkipSources, 100*r.E4.RelativeError(r.E4.SumSkipSources), r.E4.HashUnion))

	var usedHLL int
	for _, g := range r.E6.Groups {
		if g.UsedHLL {
			usedHLL++
		}
	}
	p95Err, maxErr := r.E6.HLLErrorStats()
	line("E6", r.E6.Pass(), fmt.Sprintf("%d names, %d used HLL (threshold %d), HLL |error| p95=%.2f%% max=%.2f%%",
		len(r.E6.Groups), usedHLL, r.E6.Threshold, 100*p95Err, 100*maxErr))
	line("E9", r.E9K3.Pass(), fmt.Sprintf("K=3: threshold=%d %d above, %d naive misses", r.E9K3.Threshold, len(r.E9K3.Above), len(r.E9K3.NaiveMisses)))
	line("E9", r.E9K12.Pass(), fmt.Sprintf("K=12: threshold=%d %d above, %d naive misses", r.E9K12.Threshold, len(r.E9K12.Above), len(r.E9K12.NaiveMisses)))

	return b.String()
}

// Details lists the per-name numbers behind the E6 and E9 rows: every
// name that went through HLL, and for each K the names above the E9
// threshold and the nearest names below it.
func (r Report) Details() string {
	return r.E6.Details() + r.E9K3.Details() + r.E9K12.Details()
}

// Details lists every name that used HLL with its truth, estimate, error
// and sketch size.
func (r E6Result) Details() string {
	var b strings.Builder
	groups := r.HLLGroups()
	fmt.Fprintf(&b, "E6 names above threshold %d: %d\n", r.Threshold, len(groups))
	for _, g := range groups {
		fmt.Fprintf(&b, "  %-14s truth=%-7d estimate=%-7d error=%+.2f%% sketch=%dB\n",
			shortName(g.Name), g.Truth, g.Estimate, 100*g.RelativeError(), g.PayloadBytes)
	}
	return b.String()
}

// Details lists the names found above the threshold, with their true
// counts, the nearest names below it, and any naive misses.
func (r E9Result) Details() string {
	var b strings.Builder
	fmt.Fprintf(&b, "E9 K=%d top-%d threshold=%d\n", r.K, r.TopN, r.Threshold)
	truth := make(map[string]int, len(r.Truth))
	for _, e := range r.Truth {
		truth[e.Name] = e.Count
	}
	for _, e := range r.Above {
		fmt.Fprintf(&b, "  above       %-14s count=%-7d truth=%d\n", shortName(e.Name), e.Count, truth[e.Name])
	}
	for _, e := range r.NearBelow {
		fmt.Fprintf(&b, "  near below  %-14s truth=%-7d margin=%d\n", shortName(e.Name), e.Count, r.Threshold-e.Count)
	}
	for _, name := range r.NaiveMisses {
		fmt.Fprintf(&b, "  naive miss  %s\n", shortName(name))
	}
	return b.String()
}

// shortName trims a generated metric name to its "metric_NNNNNN" prefix;
// the random suffix only pads the name to a realistic length.
func shortName(name string) string {
	const prefix = len("metric_000000")
	if len(name) > prefix {
		return name[:prefix]
	}
	return name
}
