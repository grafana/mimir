// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"io"
	"math"
	"sort"
)

// schemeStats accumulates per-query partition counts for a single scheme so
// that we can compute distribution statistics at the end of the run.
type schemeStats struct {
	name           string
	samples        []int // partitions touched, one entry per query
	totalPartCount int64 // sum of partitions touched
	fullShardHits  int   // queries that ended up touching all partitions
}

func newSchemeStats(name string, expected int) *schemeStats {
	return &schemeStats{
		name:    name,
		samples: make([]int, 0, expected),
	}
}

func (s *schemeStats) record(partitions, totalPartitions int) {
	s.samples = append(s.samples, partitions)
	s.totalPartCount += int64(partitions)
	if partitions >= totalPartitions {
		s.fullShardHits++
	}
}

func (s *schemeStats) percentile(p float64) int {
	if len(s.samples) == 0 {
		return 0
	}
	sorted := append([]int(nil), s.samples...)
	sort.Ints(sorted)
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (s *schemeStats) mean() float64 {
	if len(s.samples) == 0 {
		return 0
	}
	return float64(s.totalPartCount) / float64(len(s.samples))
}

// fractionLE returns the fraction of recorded queries that touched <= n
// partitions.
func (s *schemeStats) fractionLE(n int) float64 {
	if len(s.samples) == 0 {
		return 0
	}
	var hits int
	for _, v := range s.samples {
		if v <= n {
			hits++
		}
	}
	return float64(hits) / float64(len(s.samples))
}

// metricCountBucket is one band of the "Query complexity" histogram in the
// report. The buckets are shared between analyseAll (for sample collection)
// and report.render (for display) so both stay aligned. label is what gets
// shown in the leftmost column; [lo, hi] is the inclusive distinct-name range
// that lands in this bucket. The fullShard bucket is the only one that uses
// 0 — every other bucket starts at 1 since "no name resolved" maps to 0 only
// when the query couldn't be bucketed under any name-based scheme.
type metricCountBucket struct {
	label string
	lo    int
	hi    int
}

var metricCountBuckets = []metricCountBucket{
	{"fullShard (0)", 0, 0},
	{"1", 1, 1},
	{"2-5", 2, 5},
	{"6-20", 6, 20},
	{"21-100", 21, 100},
	{"101+", 101, 1 << 30},
}

// bucketIndexForCount returns the index into metricCountBuckets that contains
// the given distinct-metric-name count. It panics if the count doesn't fit any
// bucket, which would be a programming error since the last bucket extends to
// 1<<30.
func bucketIndexForCount(count int) int {
	for i, b := range metricCountBuckets {
		if count >= b.lo && count <= b.hi {
			return i
		}
	}
	panic(fmt.Sprintf("no metricCountBucket for count=%d", count))
}

// report renders all aggregate statistics to w.
type report struct {
	totalQueries    int
	parseFailures   int
	fullShardOnly   int // queries with no usable __name__ constraint
	totalPartitions int
	universeSize    int

	// minNames is the active -min-names threshold (0 = filter disabled).
	// filteredOut counts queries that parsed successfully but were excluded
	// from all stats because their distinct-metric-name count fell below
	// minNames (fullShard queries are always excluded when minNames > 0).
	minNames    int
	filteredOut int

	// samplesPerBucket caps the number of example queries collected per
	// metricCountBuckets entry (0 = no samples collected). bucketSamples is
	// parallel to metricCountBuckets and holds the first N raw query strings
	// that landed in each bucket. We deliberately keep the first N rather
	// than reservoir-sampling so the report is reproducible for a given
	// input.
	samplesPerBucket int
	bucketSamples    [][]string

	// cdf toggles renderNameDistance between the default per-bucket
	// histogram and a CDF view: P(score <= 256^-k) at each cutoff. The
	// CDF view is equivalent to "fraction of queries whose lex-min/max
	// metric names share at least k bytes" and is usually the easier
	// framing for the design-doc question "how many queries would name-
	// based sharding help?".
	cdf bool

	// metricCountHist[i] = number of queries that touched exactly i distinct
	// metric names. Index 0 is the "fullShard" bucket.
	metricCountHist map[int]int

	// nameDistances are per-query 0..1 scores describing how far apart the most
	// distant pair of resolved metric names sits under lexDistance. 0 means
	// a single name (or all names share a lex position); 1 means the names
	// span the lex-encoding range (e.g. `aaaaaa{} / zzzzzz{}`). fullShard
	// queries and queries where no name could be resolved are excluded;
	// nameDistanceSkipped counts those.
	nameDistances       []float64
	nameDistanceSkipped int

	schemes []*schemeStats
}

func newReport(totalPartitions, universeSize, expectedQueries int, schemes []*schemeStats) *report {
	return &report{
		totalPartitions: totalPartitions,
		universeSize:    universeSize,
		metricCountHist: map[int]int{},
		nameDistances:   make([]float64, 0, expectedQueries),
		schemes:         schemes,
		bucketSamples:   make([][]string, len(metricCountBuckets)),
	}
}

// recordSample stores q under the bucket that holds count, up to
// samplesPerBucket entries. Caller is responsible for not invoking this when
// samplesPerBucket == 0 (cheap check that avoids the bucket lookup).
func (r *report) recordSample(count int, q string) {
	idx := bucketIndexForCount(count)
	if len(r.bucketSamples[idx]) >= r.samplesPerBucket {
		return
	}
	r.bucketSamples[idx] = append(r.bucketSamples[idx], q)
}

// analysedQueries returns the number of parsed queries that survived the
// -min-names filter (i.e. the ones whose stats actually populate the report).
func (r *report) analysedQueries() int {
	return r.totalQueries - r.filteredOut
}

func (r *report) render(w io.Writer) {
	fmt.Fprintf(w, "Inputs\n")
	fmt.Fprintf(w, "  total log lines read:    %d\n", r.totalQueries+r.parseFailures)
	fmt.Fprintf(w, "  queries parsed:          %d\n", r.totalQueries)
	fmt.Fprintf(w, "  promql parse failures:   %d\n", r.parseFailures)
	if r.minNames > 0 {
		fmt.Fprintf(w, "  excluded (-min-names=%d): %d  (parsed queries with fewer distinct metric names)\n", r.minNames, r.filteredOut)
		fmt.Fprintf(w, "  queries analysed:        %d\n", r.analysedQueries())
	}
	fmt.Fprintf(w, "  fullShard queries:       %d  (no __name__ constraint, negative name matcher, or unresolved regex)\n", r.fullShardOnly)
	if r.universeSize > 0 {
		fmt.Fprintf(w, "  metric-name universe:    %d distinct names (for regex resolution only)\n", r.universeSize)
	} else {
		fmt.Fprintf(w, "  metric-name universe:    not provided (regex-on-__name__ queries fall back to fullShard)\n")
	}
	fmt.Fprintf(w, "  simulated partitions:    %d\n\n", r.totalPartitions)

	if r.analysedQueries() == 0 {
		return
	}

	fmt.Fprintf(w, "Failure domain per scheme (partitions touched per query)\n")
	fmt.Fprintf(w, "  %-26s %8s %8s %8s %8s %8s %12s\n",
		"scheme", "mean", "p50", "p90", "p99", "max", "full-shard%")
	for _, s := range r.schemes {
		max := 0
		for _, v := range s.samples {
			if v > max {
				max = v
			}
		}
		fmt.Fprintf(w, "  %-26s %8.2f %8d %8d %8d %8d %11.2f%%\n",
			s.name,
			s.mean(),
			s.percentile(0.50),
			s.percentile(0.90),
			s.percentile(0.99),
			max,
			100*float64(s.fullShardHits)/float64(len(s.samples)),
		)
	}

	fmt.Fprintf(w, "\nFraction of queries with failure domain <= K partitions\n")
	thresholds := failureDomainThresholds(r.totalPartitions)
	header := "  %-26s"
	args := []any{"scheme"}
	for _, k := range thresholds {
		header += " %10s"
		args = append(args, fmt.Sprintf("<=%d", k))
	}
	fmt.Fprintf(w, header+"\n", args...)
	for _, s := range r.schemes {
		row := "  %-26s"
		rowArgs := []any{s.name}
		for _, k := range thresholds {
			row += " %9.2f%%"
			rowArgs = append(rowArgs, 100*s.fractionLE(k))
		}
		fmt.Fprintf(w, row+"\n", rowArgs...)
	}

	r.renderNameDistance(w)

	fmt.Fprintf(w, "\nQuery complexity (distinct metric names per query)\n")
	fmt.Fprintf(w, "  %-20s %10s\n", "buckets", "queries")
	for i, b := range metricCountBuckets {
		var count int
		for k, v := range r.metricCountHist {
			if k >= b.lo && k <= b.hi {
				count += v
			}
		}
		fmt.Fprintf(w, "  %-20s %10d\n", b.label, count)
		if r.samplesPerBucket > 0 && len(r.bucketSamples[i]) > 0 {
			for _, q := range r.bucketSamples[i] {
				fmt.Fprintf(w, "      | %s\n", q)
			}
		}
	}
}

// renderNameDistance prints the [0, 1) metric-name distance distribution:
// lexFraction(maxName) − lexFraction(minName) per query, where lexFraction
// places each name on the byte-positional lex spectrum.
//
// Because the encoding decays geometrically per byte, prefix-sharing names
// produce very small scores (e.g. on the order of 256⁻⁵ ≈ 9×10⁻¹³ for names
// that share the first 5 bytes). We print stats in scientific notation and
// use log-spaced histogram buckets so the orders of magnitude are visible.
func (r *report) renderNameDistance(w io.Writer) {
	fmt.Fprintf(w, "\nName distance score (lexFraction(max) - lexFraction(min) over each query's matched names)\n")
	if len(r.nameDistances) == 0 {
		fmt.Fprintf(w, "  (no scored queries)\n")
		fmt.Fprintf(w, "  skipped (fullShard / unresolved): %d\n", r.nameDistanceSkipped)
		return
	}

	sorted := append([]float64(nil), r.nameDistances...)
	sort.Float64s(sorted)
	var sum float64
	var singleName int
	for _, v := range sorted {
		sum += v
		if v == 0 {
			singleName++
		}
	}
	mean := sum / float64(len(sorted))
	pf := func(p float64) float64 {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	fmt.Fprintf(w, "  scored queries:                   %d  (skipped %d fullShard / unresolved)\n", len(sorted), r.nameDistanceSkipped)
	fmt.Fprintf(w, "  single-name queries (score==0):   %d  (%.2f%%)\n",
		singleName, 100*float64(singleName)/float64(len(sorted)))
	fmt.Fprintf(w, "  %11s %11s %11s %11s %11s %11s\n", "mean", "p50", "p75", "p90", "p99", "max")
	fmt.Fprintf(w, "  %11.3e %11.3e %11.3e %11.3e %11.3e %11.3e\n",
		mean, pf(0.50), pf(0.75), pf(0.90), pf(0.99), sorted[len(sorted)-1])

	if r.cdf {
		renderNameDistanceCDF(w, sorted)
	} else {
		renderNameDistanceHistogram(w, sorted)
	}
}

// nameDistanceRow is one row of either the histogram or CDF view: the
// label (always identical across runs for a given mode), how many queries
// fall into the row, and that count as a percentage of the scored total.
// nameDistanceHistogram and nameDistanceCDF both return a slice of these
// in display order, which is what both the text renderer and the CSV
// emitter consume.
//
// Bytes is the integer bytes-shared value the row corresponds to: for the
// histogram it's "exactly k bytes shared" (with the top bucket being open-
// ended at maxPrefixDepth+), and for the CDF it's "at least k bytes
// shared". Rows that don't sit on the bytes-shared axis (the "single
// name" rows) have Bytes == nameDistanceRowSpecial and are kept in the
// text view but dropped from the CSV view.
type nameDistanceRow struct {
	Label      string
	Bytes      int
	Count      int
	Percentage float64 // 0..100
}

// nameDistanceRowSpecial marks rows that aren't on the bytes-shared
// integer axis (e.g. "single-name queries"). The CSV emitter skips them
// so the chart x-axis stays purely numeric.
const nameDistanceRowSpecial = -1

// nameDistanceHistogram returns the per-bucket counts: how many queries
// landed in (256^-(k+1), 256^-k]. sorted must be ascending; an empty
// input returns rows with zero counts so callers don't have to special-
// case it.
func nameDistanceHistogram(sorted []float64) []nameDistanceRow {
	total := len(sorted)
	buckets := nameDistanceBuckets()
	out := make([]nameDistanceRow, 0, len(buckets))
	for _, b := range buckets {
		var count int
		for _, v := range sorted {
			if b.lo == b.hi {
				if v == b.lo {
					count++
				}
				continue
			}
			if v > b.lo && v <= b.hi {
				count++
			}
		}
		out = append(out, nameDistanceRow{
			Label:      b.label,
			Bytes:      b.bytes,
			Count:      count,
			Percentage: pctOf(count, total),
		})
	}
	return out
}

// nameDistanceCDF returns cumulative counts at each cutoff: at row k the
// value is P(score <= 256^-k). The first row (score == 0) collapses to
// "single-name queries only"; the last row anchors at "score <= 1" which
// is always 100%. sorted must be ascending.
func nameDistanceCDF(sorted []float64) []nameDistanceRow {
	total := len(sorted)
	// sort.SearchFloat64s returns the index of the first element >= x;
	// to count values <= cutoff we search for the smallest float that's
	// strictly greater than cutoff.
	cumLE := func(cutoff float64) int {
		if total == 0 {
			return 0
		}
		return sort.SearchFloat64s(sorted, math.Nextafter(cutoff, math.Inf(1)))
	}
	pow := func(k int) float64 {
		v := 1.0
		for i := 0; i < k; i++ {
			v /= 256
		}
		return v
	}

	out := make([]nameDistanceRow, 0, maxPrefixDepth+2)

	zeroCount := cumLE(0)
	out = append(out, nameDistanceRow{
		Label:      "score == 0 (single-name queries only)",
		Bytes:      nameDistanceRowSpecial,
		Count:      zeroCount,
		Percentage: pctOf(zeroCount, total),
	})
	for k := maxPrefixDepth; k >= 1; k-- {
		word := "bytes"
		if k == 1 {
			word = "byte"
		}
		count := cumLE(pow(k))
		out = append(out, nameDistanceRow{
			Label:      fmt.Sprintf("score <= 256^-%-2d (>= %d %s shared)", k, k, word),
			Bytes:      k,
			Count:      count,
			Percentage: pctOf(count, total),
		})
	}
	out = append(out, nameDistanceRow{
		Label: "score <= 1 (all scored queries)",
		// The "score <= 1" anchor maps to "fraction with >= 0 bytes
		// shared", which is tautologically 100%. Marking it special
		// drops it from the CSV view so the x-axis stays unambiguous
		// (Bytes=0 in histogram mode means "first byte differs", a
		// different concept).
		Bytes:      nameDistanceRowSpecial,
		Count:      total,
		Percentage: pctOf(total, total),
	})
	return out
}

func pctOf(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return 100 * float64(num) / float64(denom)
}

// renderNameDistanceHistogram prints the per-bucket count.
func renderNameDistanceHistogram(w io.Writer, sorted []float64) {
	fmt.Fprintf(w, "  distribution (log-spaced; cutoffs at 256^-k mean 'first k bytes shared'):\n")
	for _, r := range nameDistanceHistogram(sorted) {
		fmt.Fprintf(w, "    %-45s %8d  %6.2f%%\n", r.Label, r.Count, r.Percentage)
	}
}

// renderNameDistanceCDF prints the cumulative distribution.
func renderNameDistanceCDF(w io.Writer, sorted []float64) {
	fmt.Fprintf(w, "  distribution (CDF; fraction with score <= 256^-k = fraction with >= k bytes shared):\n")
	for _, r := range nameDistanceCDF(sorted) {
		fmt.Fprintf(w, "    %-45s %8d  %6.2f%%\n", r.Label, r.Count, r.Percentage)
	}
}

// maxPrefixDepth is the deepest "k bytes shared" bucket we surface in the
// name-distance histogram. Bumping it adds finer-grained buckets at the
// prefix-sharing end of the spectrum; the score for k shared bytes is on
// the order of 256^-(k+1) which stays well above float64's underflow even
// for k around 30+.
const maxPrefixDepth = 16

// nameDistanceBucket is one entry in the histogram printed by
// renderNameDistance. (lo, hi] is the half-open score range that falls into
// this bucket; the only exception is the "score == 0" bucket where lo == hi
// and the test is exact equality.
//
// bytes is the integer "bytes-shared" value the bucket represents. The
// special "single-name" bucket uses nameDistanceRowSpecial because it
// doesn't sit on the bytes-shared axis; the open-ended top bucket uses
// maxPrefixDepth (i.e. "16 or more bytes" all collapse to 16); everything
// else uses the exact k.
type nameDistanceBucket struct {
	label string
	bytes int
	lo    float64
	hi    float64
}

// nameDistanceBuckets builds the histogram bucket list, generated from
// maxPrefixDepth so that adjusting the depth is a one-line change. The
// returned slice is ordered from "names are identical / share the most
// bytes" (top) to "names share no leading bytes" (bottom), which matches
// how the previous hand-rolled list read top-to-bottom.
func nameDistanceBuckets() []nameDistanceBucket {
	pow := func(k int) float64 {
		v := 1.0
		for i := 0; i < k; i++ {
			v /= 256
		}
		return v
	}
	rangeLabel := func(rng string, descr string) string {
		// 22 chars is just wide enough for "(256^-16, 256^-15]"; the
		// description trails after a single space.
		return fmt.Sprintf("%-22s %s", rng, descr)
	}
	out := make([]nameDistanceBucket, 0, maxPrefixDepth+2)
	out = append(out, nameDistanceBucket{
		label: "score == 0 (single name)",
		bytes: nameDistanceRowSpecial,
		lo:    0, hi: 0,
	})
	out = append(out, nameDistanceBucket{
		label: rangeLabel(fmt.Sprintf("(0, 256^-%d]", maxPrefixDepth),
			fmt.Sprintf("first %d+ bytes shared", maxPrefixDepth)),
		bytes: maxPrefixDepth,
		lo:    0, hi: pow(maxPrefixDepth),
	})
	for k := maxPrefixDepth; k >= 2; k-- {
		word := "bytes"
		if k-1 == 1 {
			word = "byte"
		}
		out = append(out, nameDistanceBucket{
			label: rangeLabel(fmt.Sprintf("(256^-%d, 256^-%d]", k, k-1),
				fmt.Sprintf("first %d %s shared", k-1, word)),
			bytes: k - 1,
			lo:    pow(k), hi: pow(k - 1),
		})
	}
	out = append(out, nameDistanceBucket{
		label: rangeLabel("(256^-1, 1)", "first byte differs"),
		bytes: 0,
		lo:    pow(1), hi: 1.0,
	})
	return out
}

// failureDomainThresholds returns interesting cutoff points for grouping
// queries by failure-domain size given the total number of partitions. The
// returned slice is sorted and deduplicated so columns in the report are
// stable even when `total` is small.
func failureDomainThresholds(total int) []int {
	raw := []int{1, 4, total / 10, total / 4, total / 2, total}
	seen := map[int]struct{}{}
	out := make([]int, 0, len(raw))
	for _, v := range raw {
		if v < 1 {
			v = 1
		}
		if v > total {
			v = total
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}
