// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bufio"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/chunks"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
)

// rustResults are the query results the Rust store returned for the same data and queries, from
// testdata/rust-query-results.txt.gz: `case <name> <series>` lines, each followed by one
// `<labels> <chunk>,<chunk>...` line per series, in hex. They are written by the storegolden
// program next to the Go port's scratch tools, from the Rust crate's public store API.
type queryResult struct {
	labels []byte
	chunks [][]byte
}

func rustResults(t testing.TB) map[string][]queryResult {
	t.Helper()
	file, err := os.Open(filepath.Join("testdata", "rust-query-results.txt.gz"))
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	results := map[string][]queryResult{}
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		require.Equal(t, "case", fields[0])
		count, err := strconv.Atoi(fields[2])
		require.NoError(t, err)
		series := []queryResult{}
		for range count {
			require.True(t, scanner.Scan())
			parts := strings.SplitN(scanner.Text(), " ", 2)
			labels, err := hex.DecodeString(parts[0])
			require.NoError(t, err)
			result := queryResult{labels: labels}
			if len(parts) > 1 && parts[1] != "" {
				for _, wire := range strings.Split(parts[1], ",") {
					decoded, err := hex.DecodeString(wire)
					require.NoError(t, err)
					result.chunks = append(result.chunks, decoded)
				}
			}
			series = append(series, result)
		}
		results[fields[1]] = series
	}
	require.NoError(t, scanner.Err())
	return results
}

func resultsOf(views []QuerySeriesView) []queryResult {
	results := make([]queryResult, len(views))
	for index, view := range views {
		result := queryResult{labels: view.EncodedLabels}
		for _, chunk := range view.Chunks {
			result.chunks = append(result.chunks, chunk.Wire)
		}
		results[index] = result
	}
	return results
}

// presenceStore holds a sparse label (agg, on one series in 12), a common one (job) and a unique
// one (n), on series that move to cold blocks and series that stay in the head. The old ones come
// first: the head rejects samples an hour behind its newest.
func presenceStore(t testing.TB, directory string) *Store {
	t.Helper()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	start := 10 * hour
	for _, group := range []struct {
		name     string
		from, to int64
	}{{"old", 0, 100}, {"hot", 150, 300}} {
		// Unrelated cold series, which a name regex for another metric must not visit.
		unrelated := 0
		if group.name == "old" {
			unrelated = 300
		}
		minutes := func(value func(int64) float64) []sample {
			return samplesRange(group.from, group.to, func(m int64) sample { return sample{start + m*60_000, value(m)} })
		}
		request := seriesRequest("keep", minutes(func(int64) float64 { return 1 })...)
		for n := range int64(120) {
			series := seriesOf(group.name, minutes(func(int64) float64 { return float64(n) })...)
			series.Labels = append(series.Labels, [2]string{"job", fmt.Sprintf("job-%d", n%5)}, [2]string{"n", strconv.FormatInt(n, 10)})
			if n%12 == 0 {
				series.Labels = append(series.Labels, [2]string{"agg", fmt.Sprintf("sum-%d", n%24)})
			}
			slices.SortFunc(series.Labels, comparePairs)
			request.Series = append(request.Series, series)
		}
		for n := range unrelated {
			series := seriesOf("gone", minutes(func(int64) float64 { return 1 })...)
			series.Labels = append(series.Labels, [2]string{"n", strconv.Itoa(n)})
			request.Series = append(request.Series, series)
		}
		require.NoError(t, s.Ingest("tenant", request))
	}
	s.HeadTick(true, false)
	s.HeadTick(false, false)
	return s
}

func presenceShapes() [][]LabelMatcher {
	m := matcher
	return [][]LabelMatcher{
		{m(2, "agg", "sum-.*")},
		{m(2, "agg", ".+")},
		{m(2, "job", "job-[12]")},
		{m(0, "__name__", "hot"), m(3, "agg", ".+")},
		{m(0, "__name__", "old"), m(1, "agg", "sum-0")},
		{m(2, "__name__", "hot|old"), m(3, "agg", ".+")},
		{m(2, "__name__", "hot|old"), m(0, "agg", "")},
		{m(0, "__name__", "hot"), m(2, "agg", ".*")},
		{m(0, "__name__", "hot"), m(3, "agg", "")},
		{m(3, "job", "job-1"), m(2, "agg", ".+")},
		{m(2, "n", "1.*"), m(3, "agg", "sum-12")},
		{m(2, "missing", ".+")},
		{m(3, "missing", ".+"), m(0, "__name__", "old")},
		{m(0, "job", "job-2"), m(3, "agg", ".+")},
		{m(2, "__name__", ".+"), m(3, "n", "[0-5]?[0-9]")},
		{m(1, "agg", ""), m(0, "__name__", "hot")},
		// Alternations of literals, whose values are looked up.
		{m(2, "job", "job-1|job-3")},
		{m(2, "__name__", "hot|gone"), m(2, "job", "job-(0|4)")},
		{m(2, "agg", "sum-0|sum-12|missing")},
		{m(0, "__name__", "old"), m(2, "n", "1|2|3|200")},
		{m(3, "job", "job-1|job-2"), m(2, "__name__", "old|keep")},
		{m(2, "job", "|job-1")},
		{m(2, "n", "(?i)1[0-2]")},
		{m(2, "__name__", "(?:ho|ol)[td]"), m(2, "job", "job-[0-2]")},
		{m(2, "agg", "nothing|none")},
		{m(3, "__name__", "hot|keep"), m(2, "n", "4[0-9]")},
	}
}

// checkPresenceResults checks every shape against the Rust store's results and, unsharded over
// the whole range, against a brute-force reference.
func checkPresenceResults(t *testing.T, s *Store, golden map[string][]queryResult) {
	start := 10 * hour
	everything := everySeries(t, s, "tenant")
	// Prometheus's semantics: a missing label has the empty value; regexes are anchored.
	expected := func(matchers []LabelMatcher) [][][2]string {
		compiled, err := compileMatchers(matchers)
		require.NoError(t, err)
		var selected [][][2]string
		for _, pairs := range everything {
			ok := true
			for index := range compiled {
				matcher := &compiled[index]
				value := ""
				for _, pair := range pairs {
					if pair[0] == matcher.name {
						value = pair[1]
					}
				}
				// By the regex itself, not the values looked up for it.
				var matched bool
				switch matcher.kind {
				case kindRegex:
					matched = matcher.re.matcher.MatchString(value)
				case kindNotRegex:
					matched = !matcher.re.matcher.MatchString(value)
				default:
					matched = matcher.matchesValue(value)
				}
				ok = ok && matched
			}
			if ok {
				selected = append(selected, pairs)
			}
		}
		slices.SortFunc(selected, comparePairLists)
		return selected
	}
	for index, base := range presenceShapes() {
		for _, shard := range []string{"none", "1_of_3", "3_of_3"} {
			matchers := slices.Clone(base)
			if shard != "none" {
				matchers = append(matchers, matcher(0, "__query_shard__", shard))
			}
			for rangeIndex, bounds := range [][2]int64{{math.MinInt64, math.MaxInt64}, {start + 20*60_000, start + 40*60_000}} {
				views, err := s.SelectChunks("tenant", bounds[0], bounds[1], matchers)
				require.NoError(t, err)
				name := fmt.Sprintf("presence/%d/%s/%d", index, shard, rangeIndex)
				require.Equal(t, golden[name], resultsOf(views), name)
				if shard == "none" && rangeIndex == 0 {
					selected := make([][][2]string, len(views))
					for i, view := range views {
						selected[i] = decodeLabels(t, view.EncodedLabels)
					}
					slices.SortFunc(selected, comparePairLists)
					want := expected(base)
					if len(want) == 0 {
						want = [][][2]string{}
					}
					require.Equal(t, want, selected, "%v", base)
				}
			}
		}
	}
}

func TestLabelMatchersSelectTheSameWhateverSeriesHaveTheirLabel(t *testing.T) {
	s := presenceStore(t, t.TempDir())
	defer s.Close()
	require.Less(t, s.NumSeries("tenant"), uint64(241), "old series moved to cold blocks")
	golden := rustResults(t)
	checkPresenceResults(t, s, golden)
	everything := everySeries(t, s, "tenant")
	// Only series with the label are checked, head and cold, rather than every one.
	withAgg := uint64(0)
	for _, pairs := range everything {
		for _, pair := range pairs {
			if pair[0] == "agg" {
				withAgg++
			}
		}
	}
	reads := func(matchers ...LabelMatcher) uint64 {
		before := labelValueReads.Load()
		_, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, matchers)
		require.NoError(t, err)
		return labelValueReads.Load() - before
	}
	// A nameless regex only checks series with its label.
	nameless := reads(matcher(2, "agg", "sum-.*"))
	require.LessOrEqual(t, nameless, withAgg)
	// A negation only checks series with its label; cold series still check their name regex from
	// their labels. Every series checked each matcher before.
	negated := reads(matcher(2, "__name__", "hot|old"), matcher(3, "agg", ".+"))
	require.Less(t, negated, uint64(len(everything)))
	// A cold name regex visits the series of the names it accepts, not the other 300.
	coldName := reads(matcher(2, "__name__", "old"), matcher(3, "agg", ".+"))
	require.Less(t, coldName, uint64(200))
	// Repeated, a query matches no names again, and cold blocks list no label's series again.
	counters := func() [3]uint64 {
		return [3]uint64{nameMatchCount.Load(), coldLabelLists.Load(), coldNameMatches.Load()}
	}
	repeated := []LabelMatcher{matcher(2, "__name__", "hot|old"), matcher(3, "job", "job-1")}
	reads(repeated...)
	before := counters()
	reads(repeated...)
	require.Equal(t, before, counters())
	// A label regex is evaluated once per distinct value of the label, not per series, in memory
	// and in cold blocks.
	for _, name := range []string{"hot", "old"} {
		before := regexEvaluations.Load()
		reads(matcher(0, "__name__", name), matcher(2, "job", "job-[12]"))
		evaluations := regexEvaluations.Load() - before
		require.LessOrEqual(t, evaluations, uint64(5*16), "regex evaluations of %s for 5 values in 16 shards", name)
	}
}

// The Rust store's head snapshot and cold blocks of the same data restore to the same reads.
func TestRestoresTheSnapshotAndColdBlocksTheRustStoreWrote(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "store")
	copyFixture(t, filepath.Join("testdata", "rust-presence-snapshot"), directory)
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.NotNil(t, restored)
	defer restored.Store.Close()
	require.Equal(t, []SnapshotOffset{{Offset: 7, HasOffset: true, TimestampMs: 3}}, restored.Offsets)
	checkPresenceResults(t, restored.Store, rustResults(t))
}

// copyFixture copies a store directory, extending chunk files, of which fixtures keep the written
// part, to their mapped size.
func copyFixture(t testing.TB, from, to string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(from, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(target, bytes, 0o644); err != nil {
			return err
		}
		parent := filepath.Base(filepath.Dir(path))
		if strings.HasPrefix(parent, "shard-") && !strings.Contains(path, string(filepath.Separator)+"cold"+string(filepath.Separator)) && isDigits(entry.Name()) {
			return os.Truncate(target, chunks.FileSize)
		}
		return nil
	}))
}

func isDigits(name string) bool {
	return strings.IndexFunc(name, func(r rune) bool { return !unicode.IsDigit(r) }) < 0
}

func TestRegexesAcceptExactlyTheValuesTheyExpandTo(t *testing.T) {
	for _, c := range []struct {
		pattern  string
		expected []string
		finite   bool
	}{
		{"a|b", []string{"a", "b"}, true},
		{"job-(0|4)", []string{"job-0", "job-4"}, true},
		{"x[0-2]", []string{"x0", "x1", "x2"}, true},
		{"(?i)ab", []string{"AB", "Ab", "aB", "ab"}, true},
		{"|a", []string{"", "a"}, true},
		{"a?b{2}", []string{"abb", "bb"}, true},
		{"", []string{""}, true},
		{"é|ü", []string{"é", "ü"}, true},
		{".*", nil, false},
		{"a+", nil, false},
		{"^a", nil, false},
		{`a\b`, nil, false},
		{"[a-z]{3}", nil, false},
		{"a)|(b", nil, false},
		{`(?-u:\xff)`, nil, false},
	} {
		values, finite := acceptedValues(c.pattern)
		require.Equal(t, c.finite, finite, c.pattern)
		if c.finite {
			require.Equal(t, c.expected, values, c.pattern)
		}
	}
	// Whatever the value, looking it up agrees with the regex.
	for _, pattern := range []string{
		"a|b", "job-(0|4)|job-1[0-9]", "(?i)abc|d", "|a|aa", "a?b?c?", "(a|b)(c|d)(e|f)", "x{1,3}",
		"api_(requests|errors)_total", "[0-9]", `\.|\*`,
	} {
		re, err := anchoredRegex(pattern)
		require.NoError(t, err)
		values, finite := re.acceptedValues()
		require.True(t, finite, pattern)
		probes := []string{"", "a", "ab", "job-"}
		for _, value := range values {
			runes := []rune(value)
			reversed := slices.Clone(runes)
			slices.Reverse(reversed)
			skipped := ""
			if len(runes) > 0 {
				skipped = string(runes[1:])
			}
			probes = append(probes, value+"x", "x"+value, strings.ToUpper(value), skipped, string(reversed), value)
		}
		for _, probe := range probes {
			require.Equal(t, re.matcher.MatchString(probe), re.isMatch(probe), "%s on %q", pattern, probe)
		}
	}
}

func TestAlternationsLookTheirValuesUp(t *testing.T) {
	s, err := NewWithShards(20*60*1000, Retention{}, "", 1, 1)
	require.NoError(t, err)
	request := record.DecodedRequest{}
	for _, name := range []string{"api_requests", "api_errors", "db_queries"} {
		for n := range 50 {
			series := seriesOf(name, sample{nowMs(), 1})
			series.Labels = append(series.Labels, [2]string{"job", fmt.Sprintf("job-%d", n%10)}, [2]string{"n", strconv.Itoa(n)})
			slices.SortFunc(series.Labels, comparePairs)
			request.Series = append(request.Series, series)
		}
	}
	require.NoError(t, s.Ingest("tenant", request))
	counters := func() [2]uint64 { return [2]uint64{nameMatchCount.Load(), labelValueReads.Load()} }
	before := counters()
	views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(2, "__name__", "api_errors|db_queries|missing")})
	require.NoError(t, err)
	require.Len(t, views, 100)
	require.Equal(t, before, counters(), "no metric name or label checked")
	before = counters()
	views, err = s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(2, "job", "job-1|job-2")})
	require.NoError(t, err)
	require.Len(t, views, 30)
	after := counters()
	require.Equal(t, before[0], after[0])
	// Only the series with either value are candidates, each checked once.
	require.Equal(t, uint64(30), after[1]-before[1])
}

func TestNameRegexesOnlyCheckTheNamesThatAppearedSince(t *testing.T) {
	s, err := NewWithShards(20*60*1000, Retention{}, "", 1, 1)
	require.NoError(t, err)
	ingest := func(name string) {
		require.NoError(t, s.Ingest("tenant", seriesRequest(name, sample{nowMs(), 1})))
	}
	for _, name := range []string{"api_requests", "api_errors", "db_queries"} {
		ingest(name)
	}
	regex := []LabelMatcher{matcher(2, "__name__", "api_.*")}
	// Every series of an api_ metric, by their labels.
	names := func() int {
		count := 0
		for _, pairs := range everySeries(t, s, "tenant") {
			for _, pair := range pairs {
				if pair[0] == "__name__" && strings.HasPrefix(pair[1], "api_") {
					count++
				}
			}
		}
		return count
	}
	selected := func() int {
		views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, regex)
		require.NoError(t, err)
		return len(views)
	}
	require.Equal(t, 2, selected())
	before := nameMatchCount.Load()
	require.Equal(t, 2, selected(), "cached")
	require.Equal(t, before, nameMatchCount.Load())
	ingest("api_latency")
	ingest("db_errors")
	before = nameMatchCount.Load()
	require.Equal(t, 3, selected(), "a new metric the regex accepts")
	require.Equal(t, uint64(2), nameMatchCount.Load()-before, "only the new names are checked")
	require.Equal(t, names(), selected())
}
