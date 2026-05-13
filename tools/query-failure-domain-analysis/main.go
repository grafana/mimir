// SPDX-License-Identifier: AGPL-3.0-only

/*
query-failure-domain-analysis estimates how much the failure domain (number of
partitions a query has to fan out to) would shrink if Mimir routed series to
partitions by metric name rather than by all labels.

It is intended to be fed Mimir query-frontend "query stats" lines via gcx,
e.g.:

	gcx logs query -d <loki-uid> --from now-24h --to now --limit 0 \
	  '{namespace=~"cortex-prod-04",name=~"query-frontend.*"} |= "query stats" != "/api/v1/read"' \
	  | go run ./tools/query-failure-domain-analysis -partitions=385

The tool reads logfmt-formatted log lines from stdin (one record per line)
and extracts the param_query field (the PromQL expression) and the user
field (the Mimir tenant / org_id). Any leading non-key=value prefix that
gcx prepends (e.g. UI timestamp + level) is skipped automatically.

For each parsed query the tool walks the AST to extract __name__ matchers,
then simulates how many partitions the query would touch under three
routing schemes:

  - all-labels (status quo): all partitions, always
  - hash-by-name:            partition = xxhash(name) % N, per metric
  - lex-sort:                partition = floor(lexDistance(name) * N), where
    lexDistance is a static base-26 positional encoding of the name into
    [0, 1]. Universe-independent.

Per query, the tool also computes a 0..1 "name distance" score: how far apart
in lex space the query's most distant pair of metric names sits. 0 means a
single name; 1 means the names span the lex extremes (e.g. aaaaaa{} / zzzzzz{}).

Queries with no __name__ constraint (e.g. count({namespace="prod"})) and
queries whose only __name__ constraint is a regex are reported as "fullShard"
unless a metric-name universe is supplied via -metric-names; the universe is
the only thing in this tool that's universe-dependent and is only used to
enumerate which names a regex matcher could match. See the design doc
"Mimir: query locality optimized ingestion" for context.
*/
package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logfmt/logfmt"
)

type config struct {
	totalPartitions  int
	metricNamesFile  string
	byTenant         bool
	byNamespace      bool
	includeSchemes   string
	progressInterval int
	minNames         int
	samplesPerBucket int
	cdf              bool
	csv              bool
}

func (c *config) registerFlags(fs *flag.FlagSet) {
	fs.IntVar(&c.totalPartitions, "partitions", 256, "Number of partitions to simulate. Should approximate the target cell's ingestion shuffle shard size for the tenants of interest.")
	fs.StringVar(&c.metricNamesFile, "metric-names", "", "Optional path to a file containing one metric name per line. Used only to resolve __name__ regex matchers. Without it, queries whose __name__ constraint is a regex are treated as fullShard. Equality-matched queries do not need this file.")
	fs.BoolVar(&c.byTenant, "by-tenant", false, "When set, emit a separate report per tenant (org_id) in addition to the global report.")
	fs.BoolVar(&c.byNamespace, "by-namespace", false, "When set, emit a separate report per Kubernetes namespace in addition to the global report. Requires that each log line carries a `namespace=` field; see the README for how to inject it via LogQL `line_format`.")
	fs.StringVar(&c.includeSchemes, "schemes", "all-labels,hash-by-name,lex-sort", "Comma-separated list of schemes to simulate. Valid values: all-labels, hash-by-name, lex-sort.")
	fs.IntVar(&c.progressInterval, "progress", 50_000, "Print parse progress to stderr every N input lines. 0 disables.")
	fs.IntVar(&c.minNames, "min-names", 0, "If > 0, only analyse queries that target at least this many distinct metric names (after regex resolution). Set to 2 to focus on multi-metric queries like `cpu_usage / cpu_limit`. fullShard queries (no usable __name__ constraint) are excluded by any value > 0.")
	fs.IntVar(&c.samplesPerBucket, "samples", 0, "If > 0, print up to this many example PromQL queries under each distinct-metric-name bucket in the \"Query complexity\" section of the report. Useful for spot-checking what kinds of queries land in each class.")
	fs.BoolVar(&c.cdf, "cdf", false, "Render the name-distance distribution as a CDF (fraction of queries with score <= each 256^-k cutoff, equivalent to \"fraction of queries with at least k bytes shared\") instead of the default per-bucket histogram.")
	fs.BoolVar(&c.csv, "csv", false, "Emit the name-distance distribution as CSV with one column per namespace (plus a \"bucket\" column), suitable for charting in Google Sheets. Suppresses the human-readable report. Combine with -cdf to emit cumulative percentages instead of per-bucket percentages.")
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("query-failure-domain-analysis: %v", err)
	}
}

func run() error {
	var cfg config
	cfg.registerFlags(flag.CommandLine)
	flag.Parse()

	if cfg.totalPartitions <= 0 {
		return fmt.Errorf("-partitions must be > 0")
	}
	if cfg.minNames < 0 {
		return fmt.Errorf("-min-names must be >= 0")
	}
	if cfg.samplesPerBucket < 0 {
		return fmt.Errorf("-samples must be >= 0")
	}

	queries, parseFailures, err := readInput(os.Stdin, cfg.progressInterval)
	if err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	var uni *universe
	if cfg.metricNamesFile != "" {
		names, err := loadUniverse(cfg.metricNamesFile)
		if err != nil {
			return fmt.Errorf("loading metric-name universe: %w", err)
		}
		uni = newUniverse(names)
	}

	schemes, err := buildSchemes(cfg.includeSchemes)
	if err != nil {
		return err
	}

	if cfg.csv {
		return emitCSV(os.Stdout, queries, cfg.totalPartitions, uni, cfg.includeSchemes, cfg.minNames, cfg.cdf)
	}

	globalRep := analyseAll(queries, parseFailures, cfg.totalPartitions, uni, schemes, cfg.minNames, cfg.samplesPerBucket)
	globalRep.cdf = cfg.cdf
	globalRep.render(os.Stdout)

	if cfg.byNamespace {
		emitPerNamespace(os.Stdout, queries, cfg.totalPartitions, uni, cfg.includeSchemes, cfg.minNames, cfg.samplesPerBucket, cfg.cdf)
	}
	if cfg.byTenant {
		emitPerTenant(os.Stdout, queries, cfg.totalPartitions, uni, cfg.includeSchemes, cfg.minNames, cfg.samplesPerBucket, cfg.cdf)
	}
	return nil
}

// queryRecord pairs a parsed query with its tenant and (optionally) the
// Kubernetes namespace the log line came from. Query is the raw PromQL
// string as it appeared in the input log line; we keep it around so the
// report can surface concrete examples for each metric-count bucket when
// -samples is set. Namespace is sourced from a `namespace=` field on the
// logfmt line; it will be empty when the user didn't inject the namespace
// via a LogQL `line_format` (see the README for the recipe).
type queryRecord struct {
	OrgID     string
	Namespace string
	Query     string
	PQ        parsedQuery
}

func readInput(r io.Reader, progressEvery int) (records []queryRecord, parseFailures int, err error) {
	scanner := bufio.NewScanner(r)
	// Some PromQL queries can be very large. Allow up to 8 MiB per line.
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)

	var lineNum int
	for scanner.Scan() {
		lineNum++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		query, orgID, namespace, ok := parseLogfmtLine(raw)
		if !ok {
			parseFailures++
			continue
		}
		if query == "" {
			continue
		}
		pq, perr := extractMetricSelectors(query)
		if perr != nil {
			parseFailures++
			continue
		}
		records = append(records, queryRecord{
			OrgID:     orgID,
			Namespace: namespace,
			Query:     query,
			PQ:        pq,
		})
		if progressEvery > 0 && lineNum%progressEvery == 0 {
			fmt.Fprintf(os.Stderr, "... read %d lines, %d parsed queries, %d failures\n",
				lineNum, len(records), parseFailures)
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, parseFailures, err
	}
	return records, parseFailures, nil
}

// parseLogfmtLine extracts the param_query, user (tenant / org_id) and
// namespace fields from a single Mimir query-frontend "query stats" log
// line. The line is expected to be in logfmt format, optionally prefixed
// by some non-key=value content that gcx adds (e.g. a UI timestamp and
// level). The prefix is skipped by scanning forward to the first
// `<ident>=` token.
//
// namespace is not part of the natural query-frontend log line; it can be
// injected via a LogQL `| logfmt | line_format` so that breakdowns by
// namespace become possible. An empty namespace just means "no per-line
// namespace was provided" and the record will land in the "(unspecified)"
// group.
//
// ok is false only when the line is so malformed that no logfmt records
// could be scanned at all; an empty query (with ok == true) just means the
// line happened not to be a query stats line and is skipped silently.
func parseLogfmtLine(line []byte) (query, orgID, namespace string, ok bool) {
	start := findLogfmtStart(line)
	if start < 0 {
		return "", "", "", false
	}
	dec := logfmt.NewDecoder(bytes.NewReader(line[start:]))
	for dec.ScanRecord() {
		for dec.ScanKeyval() {
			switch string(dec.Key()) {
			case "param_query":
				// Take a deep copy: the decoder reuses its internal buffer
				// between calls and `dec.Value()` is only valid until the
				// next ScanKeyval/ScanRecord.
				query = string(dec.Value())
			case "user":
				orgID = string(dec.Value())
			case "namespace":
				namespace = string(dec.Value())
			}
		}
	}
	if dec.Err() != nil && query == "" {
		return "", "", "", false
	}
	return query, orgID, namespace, true
}

// findLogfmtStart returns the index of the first `<ident>=` token in line,
// where <ident> is one or more bytes in [A-Za-z0-9_]. Returns -1 if no such
// token exists. This skips arbitrary non-logfmt prefix content like
// `2026-05-11 18:08:41.943 info ` before the actual logfmt body.
func findLogfmtStart(line []byte) int {
	const sentinel = -1
	identStart := sentinel
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case isIdentByte(c):
			if identStart == sentinel {
				identStart = i
			}
		case c == '=':
			if identStart != sentinel {
				return identStart
			}
		default:
			identStart = sentinel
		}
	}
	return -1
}

func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func loadUniverse(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var names []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		n := strings.TrimSpace(scanner.Text())
		if n == "" || strings.HasPrefix(n, "#") {
			continue
		}
		names = append(names, n)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func buildSchemes(spec string) ([]scheme, error) {
	var out []scheme
	seen := map[string]struct{}{}
	for _, name := range strings.Split(spec, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		switch name {
		case "all-labels":
			out = append(out, allLabelsScheme{})
		case "hash-by-name":
			out = append(out, hashByNameScheme{})
		case "lex-sort":
			out = append(out, lexSortScheme{})
		default:
			return nil, fmt.Errorf("unknown scheme %q (valid: all-labels, hash-by-name, lex-sort)", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one scheme must be selected via -schemes")
	}
	return out, nil
}

func analyseAll(records []queryRecord, parseFailures, totalPartitions int, uni *universe, schemes []scheme, minNames, samplesPerBucket int) *report {
	stats := make([]*schemeStats, len(schemes))
	for i, s := range schemes {
		stats[i] = newSchemeStats(s.Name(), len(records))
	}
	universeSize := 0
	if uni != nil {
		universeSize = len(uni.Names)
	}
	rep := newReport(totalPartitions, universeSize, len(records), stats)
	rep.minNames = minNames
	rep.samplesPerBucket = samplesPerBucket

	for _, r := range records {
		rep.totalQueries++
		full := r.PQ.fullShard()
		var matched map[string]struct{}
		if !full {
			var unresolved bool
			matched, unresolved = resolve(r.PQ, uni)
			if unresolved {
				// Regex matcher on __name__ with no universe — fall back to
				// fullShard so we don't over-credit any localising scheme.
				full = true
				matched = nil
			}
		}

		// Apply the -min-names filter. fullShard queries have an effective
		// name count of 0 (we couldn't even enumerate which names they
		// touch), so any minNames > 0 will exclude them.
		if minNames > 0 {
			effective := len(matched)
			if full {
				effective = 0
			}
			if effective < minNames {
				rep.filteredOut++
				continue
			}
		}

		bucketCount := len(matched)
		if full {
			rep.fullShardOnly++
			bucketCount = 0
		}
		rep.metricCountHist[bucketCount]++
		if samplesPerBucket > 0 {
			rep.recordSample(bucketCount, r.Query)
		}
		if full {
			rep.nameDistanceSkipped++
		} else if score, ok := nameDistanceScore(matched); ok {
			rep.nameDistances = append(rep.nameDistances, score)
		} else {
			rep.nameDistanceSkipped++
		}
		for i, s := range schemes {
			stats[i].record(s.PartitionsFor(matched, full, totalPartitions), totalPartitions)
		}
	}
	rep.parseFailures = parseFailures
	return rep
}

func emitPerTenant(w io.Writer, records []queryRecord, totalPartitions int, uni *universe, schemesSpec string, minNames, samplesPerBucket int, cdf bool) {
	emitGrouped(w, "tenant", records, func(r queryRecord) string { return r.OrgID },
		totalPartitions, uni, schemesSpec, minNames, samplesPerBucket, cdf)
}

func emitPerNamespace(w io.Writer, records []queryRecord, totalPartitions int, uni *universe, schemesSpec string, minNames, samplesPerBucket int, cdf bool) {
	emitGrouped(w, "namespace", records, func(r queryRecord) string { return r.Namespace },
		totalPartitions, uni, schemesSpec, minNames, samplesPerBucket, cdf)
}

// emitGrouped is the shared text-mode rendering for any per-(org|namespace)
// breakdown. It groups records by the key extracted from keyFn, orders
// groups by descending query count (largest first), and renders one report
// per group. An empty key is normalised to "(unspecified)" so the heading
// is readable.
func emitGrouped(
	w io.Writer,
	label string,
	records []queryRecord,
	keyFn func(queryRecord) string,
	totalPartitions int,
	uni *universe,
	schemesSpec string,
	minNames, samplesPerBucket int,
	cdf bool,
) {
	groups := groupRecords(records, keyFn)
	keys := sortKeysByCountDesc(groups)
	for _, k := range keys {
		schemes, err := buildSchemes(schemesSpec)
		if err != nil {
			fmt.Fprintf(w, "\n=== %s %q: scheme build error: %v\n", label, k, err)
			continue
		}
		rep := analyseAll(groups[k], 0, totalPartitions, uni, schemes, minNames, samplesPerBucket)
		rep.cdf = cdf
		fmt.Fprintf(w, "\n=== %s %q (%d queries)\n", label, displayKey(k), len(groups[k]))
		rep.render(w)
	}
}

// groupRecords buckets records by the string returned by keyFn.
func groupRecords(records []queryRecord, keyFn func(queryRecord) string) map[string][]queryRecord {
	out := map[string][]queryRecord{}
	for _, r := range records {
		out[keyFn(r)] = append(out[keyFn(r)], r)
	}
	return out
}

// sortKeysByCountDesc returns the keys of groups sorted by descending
// member count. Ties break alphabetically so the ordering is deterministic.
func sortKeysByCountDesc(groups map[string][]queryRecord) []string {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ni, nj := len(groups[keys[i]]), len(groups[keys[j]])
		if ni != nj {
			return ni > nj
		}
		return keys[i] < keys[j]
	})
	return keys
}

// displayKey replaces empty group keys with a sentinel that reads well in
// reports and CSV column headers.
func displayKey(k string) string {
	if k == "" {
		return "(unspecified)"
	}
	return k
}

// emitCSV writes the name-distance distribution as a wide-format CSV with
// one column per namespace and one row per distribution bucket, ready for
// pasting into Google Sheets / Excel and drawing a chart from. The first
// column is the bucket label; subsequent columns are the per-namespace
// percentages (0..100). cdf controls whether each row is the per-bucket
// share (default) or the cumulative share. When the input has no namespace
// information, the data lands in a single "(unspecified)" column.
//
// The output is the only thing written to w in CSV mode: callers should
// suppress the text report.
func emitCSV(w io.Writer, records []queryRecord, totalPartitions int, uni *universe, schemesSpec string, minNames int, cdf bool) error {
	groups := groupRecords(records, func(r queryRecord) string { return r.Namespace })
	keys := sortKeysByCountDesc(groups)

	// Compute one row-slice per namespace. We don't need the schemes here
	// at all - the CSV is purely about the name-distance distribution -
	// but analyseAll is the canonical entry point that respects minNames
	// and resolves regex against the universe, so we go through it with a
	// minimal scheme set.
	cheapSchemes, err := buildSchemes(schemesSpec)
	if err != nil {
		return fmt.Errorf("building schemes for CSV: %w", err)
	}
	perGroup := make(map[string][]nameDistanceRow, len(keys))
	for _, k := range keys {
		rep := analyseAll(groups[k], 0, totalPartitions, uni, cheapSchemes, minNames, 0)
		perGroup[k] = distributionRows(rep, cdf)
	}

	// All groups produce rows in the same order with the same Bytes
	// values, so the first group's row metadata defines the row layout.
	// We sort by ascending Bytes so the chart x-axis reads 0..16 left-
	// to-right, and we skip rows whose Bytes is nameDistanceRowSpecial
	// (e.g. "single-name queries") because they don't sit on the bytes-
	// shared axis.
	var sample []nameDistanceRow
	if len(keys) > 0 {
		sample = perGroup[keys[0]]
	} else {
		sample = distributionRows(&report{}, cdf)
	}
	rowOrder := orderedAxisRows(sample)

	cw := csv.NewWriter(w)
	defer cw.Flush()

	header := []string{"bytes_shared"}
	for _, k := range keys {
		header = append(header, displayKey(k))
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	for _, idx := range rowOrder {
		row := []string{strconv.Itoa(sample[idx].Bytes)}
		for _, k := range keys {
			row = append(row, fmt.Sprintf("%.4f", perGroup[k][idx].Percentage))
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// orderedAxisRows returns the indices of rows whose Bytes value sits on
// the bytes-shared integer axis, sorted by ascending Bytes. Special rows
// (single-name etc.) are dropped so the chart x-axis stays purely numeric.
func orderedAxisRows(rows []nameDistanceRow) []int {
	out := make([]int, 0, len(rows))
	for i, r := range rows {
		if r.Bytes == nameDistanceRowSpecial {
			continue
		}
		out = append(out, i)
	}
	sort.Slice(out, func(i, j int) bool {
		return rows[out[i]].Bytes < rows[out[j]].Bytes
	})
	return out
}

// distributionRows picks the histogram or CDF rows for a single report.
// It exists so emitCSV can ask "give me the rows in display order" without
// depending on the renderer's exact output format.
func distributionRows(rep *report, cdf bool) []nameDistanceRow {
	sorted := append([]float64(nil), rep.nameDistances...)
	sort.Float64s(sorted)
	if cdf {
		return nameDistanceCDF(sorted)
	}
	return nameDistanceHistogram(sorted)
}
