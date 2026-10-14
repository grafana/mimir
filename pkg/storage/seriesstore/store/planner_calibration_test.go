// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

// plannerEngine opens an engine with total series of skewed metrics and labels of several cardinalities.
func plannerEngine(t *testing.T, total int) *Engine {
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	app := engine.Appender(context.Background())
	for n := range total {
		// Metrics have skewed series counts: a few big ones and many small.
		metric := fmt.Sprintf("metric_%d", (n*2654435761)%1000)
		if n%3 == 0 {
			metric = "metric_big"
		}
		lset := promlabels.FromStrings(
			"__name__", metric,
			"cluster", fmt.Sprintf("c%d", n%5),
			"namespace", fmt.Sprintf("ns%d", (n/7)%200),
			"job", fmt.Sprintf("job%d", (n/11)%50),
			"env", fmt.Sprintf("e%d", n%3),
			"pod", fmt.Sprintf("pod-%d", n),
		)
		_, err := app.Append(0, lset, 1000, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return engine
}

type plannerShape struct {
	name     string
	matchers []*promlabels.Matcher
}

func plannerShapes() []plannerShape {
	eq := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchEqual, name, value)
	}
	ne := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchNotEqual, name, value)
	}
	re := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchRegexp, name, value)
	}
	nre := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchNotRegexp, name, value)
	}
	return []plannerShape{
		{"cluster=", []*promlabels.Matcher{eq("cluster", "c1")}},
		{"cluster= namespace=", []*promlabels.Matcher{eq("cluster", "c1"), eq("namespace", "ns7")}},
		{"namespace=~ns1.*", []*promlabels.Matcher{re("namespace", "ns1.*")}},
		{"namespace!~ns1.*", []*promlabels.Matcher{nre("namespace", "ns1.*")}},
		{"name=~metric_1.*", []*promlabels.Matcher{re("__name__", "metric_1.*")}},
		{"name=~metric_1.* cluster=", []*promlabels.Matcher{re("__name__", "metric_1.*"), eq("cluster", "c1")}},
		{"big cluster!=", []*promlabels.Matcher{eq("__name__", "metric_big"), ne("cluster", "c1")}},
		{"big namespace=~.+", []*promlabels.Matcher{eq("__name__", "metric_big"), re("namespace", ".+")}},
		{"big job=", []*promlabels.Matcher{eq("__name__", "metric_big"), eq("job", "job3")}},
		{"big pod=~pod-1.*", []*promlabels.Matcher{eq("__name__", "metric_big"), re("pod", "pod-1.*")}},
		{"job= namespace!=\"\"", []*promlabels.Matcher{eq("job", "job1"), ne("namespace", "")}},
		{"name=~.+ env=", []*promlabels.Matcher{re("__name__", ".+"), eq("env", "e1")}},
		{"small name= cluster!=", []*promlabels.Matcher{eq("__name__", "metric_17"), ne("cluster", "c1")}},
		{"missing label=", []*promlabels.Matcher{eq("nothing", "x")}},
		{"name=~.+ missing!=", []*promlabels.Matcher{re("__name__", ".+"), ne("nothing", "x")}},
	}
}

// matchedBy returns the series a lookup finds, across the shards.
func matchedBy(t *testing.T, engine *Engine, shape plannerShape) []uint64 {
	compiled, err := compileMatchers(storeMatchers(shape.matchers))
	require.NoError(t, err)
	var found []uint64
	for _, shard := range engine.store.shards {
		shard.RLock()
		shard.tenants["tenant"].series.matching(compiled, func(entry *seriesEntry) bool {
			found = append(found, entry.hash)
			return true
		})
		shard.RUnlock()
	}
	slices.Sort(found)
	return found
}

// What the lookup finds doesn't depend on whether it goes by the postings or by a scan.
func TestLookupPlansFindTheSameSeries(t *testing.T) {
	engine := plannerEngine(t, 20_000)
	defer func(old float64) { refLookupCost = old }(refLookupCost)
	for _, shape := range plannerShapes() {
		refLookupCost = 1e9
		scanned := matchedBy(t, engine, shape)
		for _, cost := range []float64{0.01, 1, 4, 16} {
			refLookupCost = cost
			require.Equal(t, scanned, matchedBy(t, engine, shape), "%s at %g", shape.name, cost)
		}
	}
}

// Times a lookup of several shapes by the cost it assumes for finding a series by its ref, to see where a scan beats
// the postings, as the Go head's planner decides with its own costs. Run with GO_INGESTER_BENCH=1.
func TestLookupPlannerCalibration(t *testing.T) {
	requireBench(t)
	engine := plannerEngine(t, 400_000)
	costs := []float64{1, 2, 4, 8, 16, 1e9}
	defer func(old float64) { refLookupCost = old }(refLookupCost)
	fmt.Printf("%-28s %9s", "shape", "matched")
	for _, c := range costs {
		fmt.Printf(" %9s", fmt.Sprintf("x%g", c))
	}
	fmt.Println()
	for _, shape := range plannerShapes() {
		compiled, err := compileMatchers(storeMatchers(shape.matchers))
		require.NoError(t, err)
		matched := -1
		var row string
		for _, cost := range costs {
			refLookupCost = cost
			best := time.Duration(1 << 62)
			for range 5 {
				count := 0
				start := time.Now()
				for _, shard := range engine.store.shards {
					shard.RLock()
					shard.tenants["tenant"].series.matching(compiled, func(*seriesEntry) bool { count++; return true })
					shard.RUnlock()
				}
				if d := time.Since(start); d < best {
					best = d
				}
				if matched >= 0 && count != matched {
					t.Fatalf("%s: cost %g matched %d, not %d", shape.name, cost, count, matched)
				}
				matched = count
			}
			row += fmt.Sprintf(" %9s", best.Round(time.Microsecond))
		}
		fmt.Printf("%-28s %9d%s\n", shape.name, matched, row)
	}
}

// A series that was removed and came back with the same labels has a new id, and the ref the postings still hold of the old
// one doesn't find it a second time.
func TestLookupByPostingsFindsReturningSeriesOnce(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const hourMs = int64(3_600_000)
	app := engine.Appender(ctx)
	back := promlabels.FromStrings("__name__", "metric", "job", "returning", "pod", "a")
	_, err = app.Append(0, back, 1000, 1)
	require.NoError(t, err)
	for n := range 20 {
		_, err = app.Append(0, promlabels.FromStrings("__name__", "metric", "job", "returning", "pod", fmt.Sprintf("other-%d", n)), 5*hourMs, 1)
		require.NoError(t, err)
	}
	// Enough series without the job that the postings of the job are the cheaper way to its series.
	for n := range 300 {
		_, err = app.Append(0, promlabels.FromStrings("__name__", "metric", "job", "filler", "pod", fmt.Sprintf("filler-%d", n)), 5*hourMs, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	// Its only sample is old: the head's compaction moves it out, and a prune removes it from what remains.
	require.NoError(t, engine.Compact(ctx))
	require.NoError(t, engine.prune(2*hourMs))
	app = engine.Appender(ctx)
	_, err = app.Append(0, back, 5*hourMs+1000, 2)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	for _, shape := range []plannerShape{
		{"job=", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "returning")}},
		{"job=~", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", "return.*")}},
		{"pod!=", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "returning"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "pod", "other-1")}},
	} {
		found := matchedBy(t, engine, shape)
		require.Equal(t, len(slices.Compact(slices.Clone(found))), len(found), "%s found a series twice", shape.name)
	}
}

// A selector without a metric name that comes in again is answered from the series it matched, plus the ones that
// came after, whatever happened to the series meanwhile.
func TestRepeatedNamelessSelectorsStayRight(t *testing.T) {
	ctx := context.Background()
	defer func(old time.Duration) { minGeneralBuild = old }(minGeneralBuild)
	minGeneralBuild = 0
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 2, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const hourMs = int64(3_600_000)
	alive := map[string]bool{}
	add := func(job, pod string, at int64) {
		lset := promlabels.FromStrings("__name__", "metric", "job", job, "pod", pod)
		app := engine.Appender(ctx)
		_, err := app.Append(0, lset, at, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		alive[lset.String()] = true
	}
	selectors := []struct {
		name     string
		matchers []*promlabels.Matcher
		wants    func(job, pod string) bool
	}{
		{"job=a", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "a")}, func(job, _ string) bool { return job == "a" }},
		{"pod=~p-1.*", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "p-1.*")}, func(_, pod string) bool { return strings.HasPrefix(pod, "p-1") }},
		{"job!=a", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "job", "a"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", ".+")}, func(job, _ string) bool { return job != "a" }},
	}
	models := map[string]map[string]bool{}
	podsOf := map[string][2]string{}
	record := func(job, pod string) {
		podsOf[promlabels.FromStrings("__name__", "metric", "job", job, "pod", pod).String()] = [2]string{job, pod}
	}
	check := func(step string) {
		for _, selector := range selectors {
			q, err := engine.ChunkQuerier(0, 9*hourMs)
			require.NoError(t, err)
			set := q.Select(ctx, false, nil, selector.matchers...)
			got := map[string]int{}
			for set.Next() {
				got[set.At().Labels().String()]++
			}
			require.NoError(t, set.Err())
			require.NoError(t, q.Close())
			want := map[string]int{}
			for lset := range alive {
				if jp := podsOf[lset]; selector.wants(jp[0], jp[1]) {
					want[lset] = 1
				}
			}
			require.Equal(t, want, got, "%s: %s", step, selector.name)
		}
		_ = models
	}
	addAll := func(job string, from, to int, at int64) {
		for n := from; n < to; n++ {
			pod := fmt.Sprintf("p-%d", n)
			record(job, pod)
			add(job, pod, at)
		}
	}
	addAll("a", 0, 150, 1000)
	addAll("a", 150, 300, 5*hourMs)
	addAll("b", 0, 200, 5*hourMs)
	check("first")
	check("repeated")
	addAll("a", 300, 650, 5*hourMs)
	check("with a lot of new series")
	addAll("a", 650, 660, 5*hourMs)
	check("with a few")
	require.NoError(t, engine.prune(2*hourMs))
	for n := range 150 {
		delete(alive, promlabels.FromStrings("__name__", "metric", "job", "a", "pod", fmt.Sprintf("p-%d", n)).String())
	}
	check("after the old series were removed")
	add("a", "p-3", 5*hourMs+1000)
	check("with a removed series back")
}

// Times a repeated selector through the engine: the first lookup, then the ones that the cache answers. Run with
// GO_INGESTER_BENCH=1.
func TestRepeatedSelectorTimes(t *testing.T) {
	requireBench(t)
	engine := plannerEngine(t, 400_000)
	fmt.Printf("%-28s %10s %10s %8s\n", "shape", "first", "repeated", "matched")
	for _, shape := range plannerShapes() {
		compiled, err := compileMatchers(storeMatchers(shape.matchers))
		require.NoError(t, err)
		var first, repeated time.Duration
		matched := 0
		for round := range 6 {
			start := time.Now()
			series, buffers, err := engine.selectRaw(0, 2000, compiled)
			require.NoError(t, err)
			d := time.Since(start)
			giveQueryBuffers(buffers)
			matched = len(series)
			if round == 0 {
				first = d
			} else if repeated == 0 || d < repeated {
				repeated = d
			}
		}
		fmt.Printf("%-28s %10s %10s %8d\n", shape.name, first.Round(time.Microsecond), repeated.Round(time.Microsecond), matched)
	}
}

func postingRefs(t *testing.T, postings index.Postings) []storage.SeriesRef {
	var refs []storage.SeriesRef
	for postings.Next() {
		refs = append(refs, postings.At())
	}
	require.NoError(t, postings.Err())
	return refs
}

// Selecting by matchers natively gives the series that assembling the postings from each matcher's does.
func TestNativePostingsMatchTheAssembledOnes(t *testing.T) {
	engine := plannerEngine(t, 20_000)
	ix := &engineIndex{e: engine}
	for _, shape := range plannerShapes() {
		assembled, err := tsdb.PostingsForMatchers(context.Background(), ix, shape.matchers...)
		require.NoError(t, err)
		native, err := ix.PostingsForMatchersNative(context.Background(), shape.matchers...)
		require.NoError(t, err)
		want := postingRefs(t, assembled)
		slices.Sort(want)
		require.Equal(t, want, postingRefs(t, native), shape.name)
	}
}

// Times both ways of selecting series by matchers over 400k series. Run with GO_INGESTER_BENCH=1.
func TestNativePostingsTimes(t *testing.T) {
	requireBench(t)
	engine := plannerEngine(t, 400_000)
	ix := &engineIndex{e: engine}
	fmt.Printf("%-28s %12s %12s\n", "shape", "assembled", "native")
	for _, shape := range plannerShapes() {
		timeIt := func(run func()) time.Duration {
			best := time.Duration(1 << 62)
			for range 3 {
				start := time.Now()
				run()
				best = min(best, time.Since(start))
			}
			return best
		}
		assembled := timeIt(func() {
			p, err := tsdb.PostingsForMatchers(context.Background(), ix, shape.matchers...)
			require.NoError(t, err)
			postingRefs(t, p)
		})
		native := timeIt(func() {
			p, err := ix.PostingsForMatchersNative(context.Background(), shape.matchers...)
			require.NoError(t, err)
			postingRefs(t, p)
		})
		fmt.Printf("%-28s %12s %12s\n", shape.name, assembled.Round(time.Microsecond), native.Round(time.Microsecond))
	}
}

// A name selector remembers a small group by its name, and notices when the group grows past what is scanned.
func TestRepeatedNameSelectorsTrackTheirGroup(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	selected := func() []string {
		q, err := engine.ChunkQuerier(0, 9_000_000)
		require.NoError(t, err)
		defer q.Close()
		set := q.Select(ctx, false, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "grows"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "p-.*"))
		var pods []string
		for set.Next() {
			pods = append(pods, set.At().Labels().Get("pod"))
		}
		require.NoError(t, set.Err())
		slices.Sort(pods)
		return pods
	}
	var want []string
	// A name the tenant has no series of yet, asked for twice, then one that appears.
	require.Empty(t, selected())
	require.Empty(t, selected())
	for n := range 30 {
		pod := fmt.Sprintf("p-%02d", n)
		app := engine.Appender(ctx)
		_, err := app.Append(0, promlabels.FromStrings("__name__", "grows", "pod", pod), 1000, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		want = append(want, pod)
		// A group of one series, a few, eight and more, each asked for twice so the second answer comes from what the first kept.
		require.Equal(t, want, selected(), "after %d series", n+1)
		require.Equal(t, want, selected(), "repeated after %d series", n+1)
	}
}

// A cold block's remembered candidates are what finding them again gives, including none, and the bounds drop them.
func TestColdCandidatesCacheMatchesComputingThem(t *testing.T) {
	engine := plannerEngine(t, 20_000)
	var refs []storage.SeriesRef
	for _, shard := range engine.store.shards {
		shard.RLock()
		shard.tenants["tenant"].series.forEach(func(entry *seriesEntry) {
			if entry.series.ref%3 == 0 {
				refs = append(refs, storage.SeriesRef(entry.series.ref))
			}
		})
		shard.RUnlock()
	}
	require.NoError(t, engine.CompactSelectedSeries(refs))
	blocks := 0
	for _, shape := range plannerShapes() {
		compiled, err := compileMatchers(storeMatchers(shape.matchers))
		require.NoError(t, err)
		key := selectorKey(compiled)
		for _, shard := range engine.store.shards {
			for _, block := range shard.cold.blocks {
				blocks++
				want := block.candidates("tenant", compiled)
				for range 2 {
					require.Equal(t, want, block.candidatesCached("tenant", key, compiled), shape.name)
				}
			}
		}
	}
	require.Positive(t, blocks, "series moved to cold blocks")
	// More selectors than a block keeps are all still right.
	for n := range 3 * maxCachedSelectorsPerBlock {
		compiled, err := compileMatchers(storeMatchers([]*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", fmt.Sprintf("pod-%d", n))}))
		require.NoError(t, err)
		for _, shard := range engine.store.shards {
			for _, block := range shard.cold.blocks {
				require.Equal(t, block.candidates("tenant", compiled), block.candidatesCached("tenant", selectorKey(compiled), compiled))
			}
		}
	}
}
