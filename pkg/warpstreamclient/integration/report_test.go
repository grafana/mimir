// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// reportBuilder accumulates the scenario report so the test emits it via a
// single t.Log call at the end. Methods are not safe for concurrent use;
// the scenario writes serially after all schedules complete.
type reportBuilder struct {
	sb strings.Builder
}

func (r *reportBuilder) writef(format string, args ...interface{}) {
	fmt.Fprintf(&r.sb, format, args...)
}

func (r *reportBuilder) writeln(s string) {
	r.sb.WriteString(s)
	r.sb.WriteByte('\n')
}

func (r *reportBuilder) string() string {
	return r.sb.String()
}

// formatDuration renders a duration with exactly one decimal in seconds so
// every cell in the report is the same width and easy to scan.
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// formatPartitionSummary renders the one-line per-partition summary used as
// a side-channel in scenario reports.
func formatPartitionSummary(s summary) string {
	rate := "n/a"
	if s.total > 0 {
		rate = fmt.Sprintf("%d/%d (%.1f%%)", s.successes, s.total, 100*float64(s.successes)/float64(s.total))
	}
	return fmt.Sprintf("success=%s mean=%s p50=%s p99=%s",
		rate, formatDuration(s.meanLatency), formatDuration(s.p50Latency), formatDuration(s.p99Latency))
}

// writeSummary renders the side-by-side WS vs kgo summary block.
func (r *reportBuilder) writeSummary(name string, ws, kg summary) {
	successRate := func(s summary) string {
		if s.total == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%d/%d (%.1f%%)", s.successes, s.total, 100*float64(s.successes)/float64(s.total))
	}
	r.writef(`
=== scenario: %s ===
                          warpstream             kgo (baseline)
app requests:             %-22d %-22d
success rate:             %-22s %-22s
mean latency:             %-22s %-22s
p50 latency:              %-22s %-22s
p99 latency:              %-22s %-22s
`,
		name,
		ws.total, kg.total,
		successRate(ws), successRate(kg),
		formatDuration(ws.meanLatency), formatDuration(kg.meanLatency),
		formatDuration(ws.p50Latency), formatDuration(kg.p50Latency),
		formatDuration(ws.p99Latency), formatDuration(kg.p99Latency),
	)
}

// writeTimeBucketedTable renders the 10s-window side-by-side comparison.
// produceDeltas[i] holds the (primary, hedge) wire-request totals for the
// i-th bucket, captured by the counter sampler at exact bucket boundaries.
// produceDeltas may have an extra trailing entry for the drain window
// between the last bucket boundary and the actual end of the test; it is
// rendered on its own row when present.
func (r *reportBuilder) writeTimeBucketedTable(wsApp []observation, produceDeltas []counterBucketDelta, kgoApp []observation, windowSize time.Duration) {
	wsBuckets, _ := timeBucketStats(wsApp, windowSize)
	kgoBuckets, _ := timeBucketStats(kgoApp, windowSize)
	n := len(wsBuckets)
	if len(kgoBuckets) > n {
		n = len(kgoBuckets)
	}
	if n == 0 && len(produceDeltas) == 0 {
		r.writeln("(no observations)")
		return
	}

	rate := func(s bucketStats) string {
		if s.total == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%3d/%3d (%3.0f%%)", s.success, s.total, 100*float64(s.success)/float64(s.total))
	}
	surge := func(d counterBucketDelta) string {
		if d.primary <= 0 {
			return "n/a"
		}
		return fmt.Sprintf("%d/%d (+%.0f%%)", d.hedge, d.primary, 100*float64(d.hedge)/float64(d.primary))
	}
	r.writef("\n  %-9s | %-18s %-12s %-12s %-22s | %-18s %-12s %-12s\n",
		"window",
		"ws success", "ws mean", "ws p99", "ws surge (hedge/prim)",
		"kgo success", "kgo mean", "kgo p99")
	r.writef("  %s\n", strings.Repeat("-", 124))
	secs := int(windowSize / time.Second)
	for i := 0; i < n; i++ {
		var ws, kg bucketStats
		if i < len(wsBuckets) {
			ws = wsBuckets[i]
		}
		if i < len(kgoBuckets) {
			kg = kgoBuckets[i]
		}
		var surgeStr string
		if i < len(produceDeltas) {
			surgeStr = surge(produceDeltas[i])
		} else {
			surgeStr = "n/a"
		}
		r.writef("  %2ds-%-4ds| %-18s %-12s %-12s %-22s | %-18s %-12s %-12s\n",
			i*secs, (i+1)*secs,
			rate(ws), formatDuration(ws.meanLatency), formatDuration(ws.p99Latency),
			surgeStr,
			rate(kg), formatDuration(kg.meanLatency), formatDuration(kg.p99Latency))
	}
	// Drain row: any wire requests fired between the last bucket boundary
	// and the actual stop time. Often non-zero in the timeout / failure
	// scenarios because in-flight produces complete after the last event
	// was scheduled.
	if drain := len(produceDeltas) - n; drain > 0 {
		var d counterBucketDelta
		for _, x := range produceDeltas[n:] {
			d.primary += x.primary
			d.hedge += x.hedge
		}
		r.writef("  %2ds+%-5s| %-18s %-12s %-12s %-22s |\n",
			n*secs, "drain", "—", "—", "—", surge(d))
	}
}

// errorBreakdown groups failure observations by error string, returning a
// sorted "count × err-string" listing for debug logging.
func errorBreakdown(obs []observation) []string {
	counts := map[string]int{}
	for _, o := range obs {
		if o.err == nil {
			continue
		}
		counts[o.err.Error()]++
	}
	out := make([]string, 0, len(counts))
	for k, v := range counts {
		out = append(out, fmt.Sprintf("%5d × %s", v, k))
	}
	sort.Strings(out)
	return out
}

func TestFormatDuration_OneDecimal(t *testing.T) {
	t.Parallel()
	cases := map[string]time.Duration{
		"0.0s": 0,
		"0.5s": 500 * time.Millisecond,
		"1.2s": 1234 * time.Millisecond,
		"4.6s": 4600 * time.Millisecond,
	}
	for want, in := range cases {
		assert.Equal(t, want, formatDuration(in), "input %v", in)
	}
}

func TestFormatPartitionSummary_NaWhenEmpty(t *testing.T) {
	t.Parallel()
	assert.Contains(t, formatPartitionSummary(summary{}), "success=n/a")
}

func TestReportBuilder_AccumulatesAcrossCalls(t *testing.T) {
	t.Parallel()
	var r reportBuilder
	r.writef("a=%d\n", 1)
	r.writeln("b=2")
	r.writef("c=%s\n", "three")
	assert.Equal(t, "a=1\nb=2\nc=three\n", r.string())
}

func TestErrorBreakdown_GroupsAndSorts(t *testing.T) {
	t.Parallel()
	obs := []observation{
		{err: errStub("a")},
		{err: errStub("b")},
		{err: errStub("a")},
		{err: nil},
		{err: errStub("a")},
	}
	got := errorBreakdown(obs)
	// The format is "%5d × <err>": "    1" sorts before "    3", so the
	// 1-count "b" line precedes the 3-count "a" line.
	assert.Len(t, got, 2)
	assert.Contains(t, got[0], "× b")
	assert.Contains(t, got[1], "× a")
}

type errStub string

func (e errStub) Error() string { return string(e) }
