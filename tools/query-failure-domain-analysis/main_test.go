// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadInputParsesGcxLogfmt(t *testing.T) {
	// Real-shaped lines from `gcx logs query` against query-frontend
	// "query stats" logs. Each line is prefixed with a UI timestamp + level
	// that must be skipped before logfmt parsing.
	input := strings.Join([]string{
		// Line 1: valid query, tenant 174711, no namespace.
		`2026-05-11 18:08:41.943 info ts=2026-05-11T18:08:41Z caller=handler.go:412 ` +
			`level=info user=174711 msg="query stats" component=query-frontend ` +
			`method=POST path=/prometheus/api/v1/query_range ` +
			`param_query="sum(rate(flowable_deadletter_jobs_total{kubernetes_namespace=~\"prod\"}[2592000s])) by (activity_id)" ` +
			`status=success`,
		// Line 2: tenant 9 with a namespace injected via LogQL line_format.
		`2026-05-11 18:08:42.001 info ts=2026-05-11T18:08:42Z caller=handler.go:412 ` +
			`level=info user=9 namespace=mimir-dedicated-48 msg="query stats" ` +
			`param_query="cpu_usage / cpu_limit" status=success`,
		// Line 3: a line without param_query - should be skipped silently
		// (this can happen if the LogQL filter is too broad).
		`2026-05-11 18:08:43.001 info ts=2026-05-11T18:08:43Z level=info msg="something else"`,
		// Line 4: completely junk - counts as a parse failure.
		`not logfmt at all just some prose without an equals sign`,
		``,
	}, "\n")

	records, parseFailures, err := readInput(strings.NewReader(input), 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(records))
	assert.Equal(t, "174711", records[0].OrgID)
	assert.Empty(t, records[0].Namespace)
	assert.Equal(t, "9", records[1].OrgID)
	assert.Equal(t, "mimir-dedicated-48", records[1].Namespace)
	assert.Equal(t, 1, parseFailures)
}

func TestFindLogfmtStart(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", -1},
		{"no equals", "just some prose", -1},
		{"leading ident", "key=val", 0},
		{"gcx prefix", "2026-05-11 18:08:41.943 info ts=2026Z", len("2026-05-11 18:08:41.943 info ")},
		{"identifier without =, then proper kv", "info ts=2026Z", len("info ")},
		{"dash in date", "2026-05-11 ts=2026Z", len("2026-05-11 ")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, findLogfmtStart([]byte(c.in)))
		})
	}
}

func TestParseLogfmtLine_EscapedQuotes(t *testing.T) {
	// Embedded escaped quotes inside param_query must round-trip correctly.
	line := []byte(`ts=2026Z level=info user=42 msg="query stats" ` +
		`param_query="sum(rate(metric{label=~\"a|b\"}[5m]))"`)
	q, org, ns, ok := parseLogfmtLine(line)
	require.True(t, ok)
	assert.Equal(t, "42", org)
	assert.Empty(t, ns)
	assert.Equal(t, `sum(rate(metric{label=~"a|b"}[5m]))`, q)
}

func TestParseLogfmtLine_Namespace(t *testing.T) {
	// Namespace can be injected by LogQL `| line_format` so each line
	// carries `namespace=...`. The parser should pick it up cleanly even
	// alongside the usual fields.
	line := []byte(`ts=2026Z level=info user=42 namespace=mimir-dedicated-48 ` +
		`msg="query stats" param_query="up"`)
	q, org, ns, ok := parseLogfmtLine(line)
	require.True(t, ok)
	assert.Equal(t, "42", org)
	assert.Equal(t, "mimir-dedicated-48", ns)
	assert.Equal(t, "up", q)
}

func TestEndToEnd_StatusQuoVsLexSort_NoUniverse(t *testing.T) {
	schemes := []scheme{
		allLabelsScheme{},
		hashByNameScheme{},
		lexSortScheme{},
	}

	queries := []queryRecord{
		mustParse(t, "1", "cpu_usage / cpu_limit"),       // shared prefix
		mustParse(t, "1", "memory_usage / memory_limit"), // shared prefix
		mustParse(t, "2", `count({namespace="prod"})`),   // fullShard
		mustParse(t, "3", "aaaaaa / zzzzzz"),             // extreme pair
	}

	const N = 10
	rep := analyseAll(queries, 0, N, nil, schemes, 0, 0)

	assert.InDelta(t, float64(N), rep.schemes[0].mean(), 0.001, "status quo always full shard")
	assert.Less(t, rep.schemes[2].mean(), float64(N), "lex-sort reduces mean failure domain")

	// Spot-check the per-query name distance scores produced.
	require.Equal(t, 3, len(rep.nameDistances), "fullShard query should be skipped from scoring")
	// cpu_usage / cpu_limit: shared "cpu_" then divergence at byte 4. On the
	// order of 256^-5 ~= 9e-13.
	assert.Equal(t, lexFractionDiff("cpu_limit", "cpu_usage"), rep.nameDistances[0])
	assert.Positive(t, rep.nameDistances[0])
	// memory_usage / memory_limit: shared "memory_" then divergence at byte 7.
	// Distinct from the cpu_* pair (different divergence depth).
	assert.Equal(t, lexFractionDiff("memory_limit", "memory_usage"), rep.nameDistances[1])
	assert.NotEqual(t, rep.nameDistances[0], rep.nameDistances[1])
	// aaaaaa / zzzzzz: ~0.098 under base-256 byte-positional encoding.
	assert.Equal(t, lexFractionDiff("aaaaaa", "zzzzzz"), rep.nameDistances[2])
}

func TestEndToEnd_RegexNeedsUniverse(t *testing.T) {
	schemes := []scheme{lexSortScheme{}}

	queries := []queryRecord{mustParse(t, "1", `{__name__=~"cpu_.*"}`)}

	t.Run("without universe -> treated as fullShard", func(t *testing.T) {
		rep := analyseAll(queries, 0, 8, nil, schemes, 0, 0)
		assert.Equal(t, 1, rep.fullShardOnly)
		assert.InDelta(t, 8, rep.schemes[0].mean(), 0.001)
	})

	t.Run("with universe -> regex resolved and bucketed", func(t *testing.T) {
		uni := newUniverse([]string{"cpu_usage", "cpu_limit", "disk_used_bytes"})
		rep := analyseAll(queries, 0, 8, uni, schemes, 0, 0)
		assert.Equal(t, 0, rep.fullShardOnly)
		assert.Less(t, rep.schemes[0].mean(), 8.0)
	})
}

func mustParse(t *testing.T, org, expr string) queryRecord {
	t.Helper()
	pq, err := extractMetricSelectors(expr)
	require.NoError(t, err, "expr=%s", expr)
	return queryRecord{OrgID: org, Query: expr, PQ: pq}
}

func TestAnalyseAll_MinNamesFilter(t *testing.T) {
	schemes := []scheme{
		allLabelsScheme{},
		hashByNameScheme{},
		lexSortScheme{},
	}
	queries := []queryRecord{
		mustParse(t, "1", "cpu_usage"),                              // 1 name -> excluded
		mustParse(t, "1", "cpu_usage / cpu_limit"),                  // 2 names -> kept
		mustParse(t, "1", "(http_total - http_errors) / http_total"), // 2 names -> kept
		mustParse(t, "2", `count({namespace="prod"})`),              // fullShard -> excluded
		mustParse(t, "2", `{__name__=~"cpu_.*"}`),                   // unresolved regex -> excluded (no universe)
	}

	t.Run("filter disabled by default", func(t *testing.T) {
		rep := analyseAll(queries, 0, 16, nil, schemes, 0, 0)
		assert.Equal(t, 5, rep.totalQueries)
		assert.Equal(t, 0, rep.filteredOut)
		assert.Equal(t, 5, rep.analysedQueries())
	})

	t.Run("min-names=2 excludes single-name and fullShard queries", func(t *testing.T) {
		rep := analyseAll(queries, 0, 16, nil, schemes, 2, 0)
		assert.Equal(t, 5, rep.totalQueries, "all parsed queries still counted in input")
		assert.Equal(t, 3, rep.filteredOut, "1 single-name + 1 fullShard + 1 unresolved-regex excluded")
		assert.Equal(t, 2, rep.analysedQueries())

		// fullShardOnly is only incremented for queries that survive the filter.
		// All would-be fullShard queries are filtered out at min-names>=1.
		assert.Equal(t, 0, rep.fullShardOnly)

		// Each scheme should have recorded exactly the 2 kept queries.
		for _, s := range rep.schemes {
			assert.Equal(t, 2, len(s.samples))
		}
		// Only the kept multi-metric queries produce distance scores.
		assert.Equal(t, 2, len(rep.nameDistances))
	})

	t.Run("min-names=3 excludes everything in this set", func(t *testing.T) {
		rep := analyseAll(queries, 0, 16, nil, schemes, 3, 0)
		assert.Equal(t, 0, rep.analysedQueries())
		assert.Equal(t, 5, rep.filteredOut)
	})

	t.Run("regex resolved by universe satisfies the filter", func(t *testing.T) {
		uni := newUniverse([]string{"cpu_usage", "cpu_limit", "cpu_iowait", "disk_used"})
		regexOnly := []queryRecord{mustParse(t, "1", `{__name__=~"cpu_.*"}`)}
		rep := analyseAll(regexOnly, 0, 16, uni, schemes, 2, 0)
		assert.Equal(t, 1, rep.analysedQueries(),
			"regex expands to 3 cpu_* names via universe, satisfies min-names=2")
		assert.Equal(t, 0, rep.filteredOut)
	})
}

func TestAnalyseAll_BucketSamples(t *testing.T) {
	schemes := []scheme{allLabelsScheme{}}
	queries := []queryRecord{
		mustParse(t, "1", `count({namespace="prod"})`),     // fullShard bucket
		mustParse(t, "1", `cpu_usage`),                     // 1-name bucket
		mustParse(t, "1", `mem_usage`),                     // 1-name bucket
		mustParse(t, "1", `node_uptime_seconds`),           // 1-name bucket
		mustParse(t, "1", `cpu_usage / cpu_limit`),         // 2-5 bucket
		mustParse(t, "1", `mem_usage / mem_total`),         // 2-5 bucket
	}

	t.Run("samples disabled by default", func(t *testing.T) {
		rep := analyseAll(queries, 0, 8, nil, schemes, 0, 0)
		for _, s := range rep.bucketSamples {
			assert.Empty(t, s)
		}
	})

	t.Run("samples=2 keeps first two queries per bucket", func(t *testing.T) {
		rep := analyseAll(queries, 0, 8, nil, schemes, 0, 2)

		fullShardIdx := bucketIndexForCount(0)
		oneIdx := bucketIndexForCount(1)
		twoToFiveIdx := bucketIndexForCount(2)

		assert.Equal(t, []string{`count({namespace="prod"})`}, rep.bucketSamples[fullShardIdx])
		assert.Equal(t, []string{`cpu_usage`, `mem_usage`}, rep.bucketSamples[oneIdx],
			"only the first 2 single-name queries should be retained")
		assert.Equal(t, []string{`cpu_usage / cpu_limit`, `mem_usage / mem_total`}, rep.bucketSamples[twoToFiveIdx])

		// Empty buckets stay empty.
		for _, idx := range []int{
			bucketIndexForCount(6),
			bucketIndexForCount(21),
			bucketIndexForCount(101),
		} {
			assert.Empty(t, rep.bucketSamples[idx])
		}
	})

	t.Run("samples respect the -min-names filter", func(t *testing.T) {
		rep := analyseAll(queries, 0, 8, nil, schemes, 2, 5)
		// fullShard and 1-name queries are filtered out, so their sample
		// slots stay empty.
		assert.Empty(t, rep.bucketSamples[bucketIndexForCount(0)])
		assert.Empty(t, rep.bucketSamples[bucketIndexForCount(1)])
		assert.Equal(t, []string{`cpu_usage / cpu_limit`, `mem_usage / mem_total`},
			rep.bucketSamples[bucketIndexForCount(2)])
	})
}

func TestRenderNameDistance_CDF(t *testing.T) {
	// Construct scores corresponding to known prefix-sharing depths.
	// lexFractionDiff(min, max) for names that share the first k bytes lands
	// in [256^-(k+1), 256^-k), so a value just below 256^-k is a safe
	// representative for "first k bytes shared".
	scoreFor := func(k int) float64 {
		// 256^-(k+1) * 1.5 — well inside the (256^-(k+1), 256^-k] band.
		v := 1.0
		for i := 0; i < k+1; i++ {
			v /= 256
		}
		return v * 1.5
	}

	rep := &report{
		nameDistances: []float64{
			0,            // single-name query
			0,            // single-name query
			scoreFor(16), // 16 bytes shared
			scoreFor(5),  // 5 bytes shared
			scoreFor(4),  // 4 bytes shared
			scoreFor(0),  // first byte differs (0 bytes shared)
		},
		schemes: []*schemeStats{newSchemeStats("noop", 0)},
		cdf:     true,
	}
	for range rep.nameDistances {
		rep.schemes[0].record(1, 16)
	}
	rep.totalQueries = len(rep.nameDistances)

	var buf strings.Builder
	rep.renderNameDistance(&buf)
	out := buf.String()

	// Header reflects CDF mode.
	require.Contains(t, out, "distribution (CDF;")
	// Total row at the top: exact-zero == 2 of 6 queries == 33.33%.
	require.Contains(t, out, "score == 0 (single-name queries only)")
	require.Regexp(t, `score == 0[^\n]*2\s+33\.33%`, out)
	// score <= 256^-16 should include the two zeros + the depth-16 score => 3 of 6 == 50%.
	require.Regexp(t, `score <= 256\^-16[^\n]*3\s+50\.00%`, out)
	// score <= 256^-5 should include zeros + depth-16 + depth-5 => 4 of 6 == 66.67%.
	require.Regexp(t, `score <= 256\^-5\b[^\n]*4\s+66\.67%`, out)
	// score <= 256^-1 should include everything except the first-byte-differs query
	// => 5 of 6 == 83.33%.
	require.Regexp(t, `score <= 256\^-1\b[^\n]*5\s+83\.33%`, out)
	// Anchor row: all queries.
	require.Regexp(t, `score <= 1 \(all scored queries\)[^\n]*6\s+100\.00%`, out)

	// Plural-ness in labels: "1 byte shared" (singular), ">= 16 bytes shared" (plural).
	require.Contains(t, out, ">= 1 byte shared")
	require.Contains(t, out, ">= 16 bytes shared")
}

func TestRenderNameDistance_HistogramVsCDF_RowCount(t *testing.T) {
	// Both modes should produce the same number of distribution rows so
	// per-tenant reports line up visually.
	rep := &report{
		nameDistances: []float64{0.0, 0.5, 1e-10, 1e-20},
		schemes:       []*schemeStats{newSchemeStats("noop", 0)},
	}
	rep.totalQueries = len(rep.nameDistances)
	rep.schemes[0].samples = []int{1, 1, 1, 1}

	countRows := func(out string) int {
		// Distribution rows start with 4 spaces.
		n := 0
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, "    ") && !strings.HasPrefix(line, "      ") {
				n++
			}
		}
		return n
	}

	var histBuf strings.Builder
	rep.cdf = false
	rep.renderNameDistance(&histBuf)
	hist := histBuf.String()

	var cdfBuf strings.Builder
	rep.cdf = true
	rep.renderNameDistance(&cdfBuf)
	cdf := cdfBuf.String()

	assert.Equal(t, countRows(hist), countRows(cdf),
		"histogram and CDF should print the same number of distribution rows")
}

func TestEmitCSV_PerNamespace(t *testing.T) {
	mk := func(ns, expr string) queryRecord {
		r := mustParse(t, "1", expr)
		r.Namespace = ns
		return r
	}
	records := []queryRecord{
		mk("mimir-dedicated-48", "cpu_usage / cpu_limit"),                 // ns 48: 4 bytes shared
		mk("mimir-dedicated-48", "memory_usage / memory_limit"),           // ns 48: 7 bytes shared
		mk("mimir-dedicated-49", "aaaaaa{} / zzzzzz{}"),                   // ns 49: 0 bytes shared
		mk("", "cpu_usage / cpu_throttled"),                               // unspecified: 4 bytes shared
	}

	t.Run("histogram csv", func(t *testing.T) {
		var buf strings.Builder
		require.NoError(t, emitCSV(&buf, records, 64, nil, "lex-sort", 0, false))

		rows := mustParseCSV(t, buf.String())
		require.NotEmpty(t, rows)

		// Header: bytes_shared + one column per namespace, sorted by
		// descending query count with alphabetical tiebreak.
		assert.Equal(t,
			[]string{"bytes_shared", "mimir-dedicated-48", "(unspecified)", "mimir-dedicated-49"},
			rows[0])

		// Data rows are integers 0..maxPrefixDepth sorted ascending; the
		// "single name" row (Bytes == -1) is dropped.
		require.Equal(t, maxPrefixDepth+1, len(rows)-1, "row count: 0..maxPrefixDepth inclusive")
		for i := 0; i <= maxPrefixDepth; i++ {
			assert.Equal(t, strconv.Itoa(i), rows[i+1][0], "row %d first column", i)
		}

		// k=4: cpu_usage/cpu_limit in ns-48 (1 of 2 → 50%);
		// cpu_usage/cpu_throttled in (unspecified) (1 of 1 → 100%);
		// nothing in ns-49.
		row4 := findCSVRowByBytes(t, rows, 4)
		assert.Equal(t, "50.0000", row4[1])
		assert.Equal(t, "100.0000", row4[2])
		assert.Equal(t, "0.0000", row4[3])

		// k=0 in histogram mode means "first byte differs": only aaaaaa/zzzzzz
		// in ns-49 (1 of 1 → 100%).
		row0 := findCSVRowByBytes(t, rows, 0)
		assert.Equal(t, "0.0000", row0[1])
		assert.Equal(t, "0.0000", row0[2])
		assert.Equal(t, "100.0000", row0[3])
	})

	t.Run("cdf csv", func(t *testing.T) {
		var buf strings.Builder
		require.NoError(t, emitCSV(&buf, records, 64, nil, "lex-sort", 0, true))

		rows := mustParseCSV(t, buf.String())
		require.Equal(t, []string{"bytes_shared", "mimir-dedicated-48", "(unspecified)", "mimir-dedicated-49"},
			rows[0])
		// CDF in CSV emits k=1..maxPrefixDepth (the tautological k=0
		// anchor row is filtered out as special).
		require.Equal(t, maxPrefixDepth, len(rows)-1)
		for i := 1; i <= maxPrefixDepth; i++ {
			assert.Equal(t, strconv.Itoa(i), rows[i][0], "row %d first column", i-1)
		}

		// k=1: P(score <= 256^-1) = fraction with >=1 byte shared.
		// ns-48: cpu_usage/cpu_limit and memory_usage/memory_limit both
		// qualify → 2/2 = 100%.
		// (unspecified): cpu_usage/cpu_throttled → 1/1 = 100%.
		// ns-49: aaaaaa/zzzzzz shares no bytes → 0/1 = 0%.
		row1 := findCSVRowByBytes(t, rows, 1)
		assert.Equal(t, "100.0000", row1[1])
		assert.Equal(t, "100.0000", row1[2])
		assert.Equal(t, "0.0000", row1[3])
	})

	t.Run("no records still produces a header", func(t *testing.T) {
		var buf strings.Builder
		require.NoError(t, emitCSV(&buf, nil, 64, nil, "lex-sort", 0, false))
		rows := mustParseCSV(t, buf.String())
		require.GreaterOrEqual(t, len(rows), 1)
		assert.Equal(t, []string{"bytes_shared"}, rows[0])
	})
}

func mustParseCSV(t *testing.T, s string) [][]string {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(s)).ReadAll()
	require.NoError(t, err, "csv:\n%s", s)
	return rows
}

func findCSVRowByBytes(t *testing.T, rows [][]string, bytes int) []string {
	t.Helper()
	want := strconv.Itoa(bytes)
	for _, r := range rows {
		if len(r) > 0 && r[0] == want {
			return r
		}
	}
	t.Fatalf("no CSV row with bytes_shared=%d in column 0; rows=%v", bytes, rows)
	return nil
}

func TestBucketIndexForCount(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 0},   // fullShard
		{1, 1},   // 1
		{2, 2},   // 2-5
		{5, 2},   // 2-5 upper edge
		{6, 3},   // 6-20
		{20, 3},  // 6-20 upper edge
		{21, 4},  // 21-100
		{100, 4}, // 21-100 upper edge
		{101, 5}, // 101+
		{10_000, 5},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, bucketIndexForCount(c.in), "count=%d", c.in)
	}
}

func TestBuildSchemes(t *testing.T) {
	t.Run("default includes all three", func(t *testing.T) {
		s, err := buildSchemes("all-labels,hash-by-name,lex-sort")
		require.NoError(t, err)
		assert.Equal(t, 3, len(s))
	})

	t.Run("rejects unknown scheme", func(t *testing.T) {
		_, err := buildSchemes("magic-scheme")
		require.Error(t, err)
	})

	t.Run("dedupes duplicates", func(t *testing.T) {
		s, err := buildSchemes("hash-by-name,hash-by-name")
		require.NoError(t, err)
		assert.Equal(t, 1, len(s))
	})

	t.Run("rejects empty", func(t *testing.T) {
		_, err := buildSchemes("")
		require.Error(t, err)
	})
}
