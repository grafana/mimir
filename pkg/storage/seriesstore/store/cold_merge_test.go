// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// manyColdBlocks opens an engine whose series left the head over rounds, a block a round in each shard, as idle
// series leave it in production, and some came back to the head after.
func manyColdBlocks(t testing.TB, series, rounds, shards int) *Engine {
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: shards, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	refs := make([]storage.SeriesRef, series)
	appendAt := func(ts int64, picks func(n int) bool) {
		app := engine.Appender(context.Background())
		for n := range series {
			if picks(n) {
				ref, err := app.Append(0, rw2Labels(n), 0, ts, float64(n), nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
				refs[n] = ref
			}
		}
		require.NoError(t, app.Commit())
	}
	appendAt(1000, func(int) bool { return true })
	for round := range rounds {
		var leaving []storage.SeriesRef
		for n := range series {
			// This round's, and those that came back last round, which then have chunks in two blocks.
			if n%rounds == round || round > 0 && n%rounds == round-1 && n%7 == 0 {
				leaving = append(leaving, refs[n])
			}
		}
		require.NoError(t, engine.CompactSelectedSeries(leaving))
		// Some come back, and leave again with a later round.
		appendAt(int64(2000+round), func(n int) bool { return n%rounds == round && n%7 == 0 })
	}
	return engine
}

func coldBlocks(engine *Engine) (min, max int) {
	min = 1 << 30
	for _, shard := range engine.store.shards {
		shard.RLock()
		count := len(shard.cold.blocks)
		shard.RUnlock()
		min, max = minInt(min, count), maxInt(max, count)
	}
	return min, max
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// readEverything is every series a few selectors find, with their samples, and label values and names.
func readEverything(t testing.TB, engine *Engine) string {
	var out strings.Builder
	q, err := engine.ChunkQuerier(0, 1<<40)
	require.NoError(t, err)
	defer q.Close()
	for _, matchers := range [][]*promlabels.Matcher{
		{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "container_metric_7")},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "namespace", "namespace-3"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "pod-1.*")},
	} {
		set := q.Select(context.Background(), true, nil, matchers...)
		for set.Next() {
			out.WriteString(set.At().Labels().String())
			it := set.At().Iterator(nil)
			for it.Next() {
				samples := it.At().Chunk.Iterator(nil)
				for samples.Next() != 0 {
					ts, v := samples.At()
					fmt.Fprintf(&out, " %d=%g", ts, v)
				}
			}
			out.WriteString("\n")
		}
		require.NoError(t, set.Err())
	}
	plain, err := engine.Querier(0, 1<<40)
	require.NoError(t, err)
	defer plain.Close()
	values, _, err := plain.LabelValues(context.Background(), "namespace", nil)
	require.NoError(t, err)
	names, _, err := plain.LabelNames(context.Background(), nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "container_metric_7"))
	require.NoError(t, err)
	fmt.Fprintln(&out, values, names)
	return out.String()
}

// Merging a shard's cold blocks down to its target answers every query as the blocks did.
func TestMergingColdBlocksAnswersTheSame(t *testing.T) {
	engine := manyColdBlocks(t, 6000, 12, 16)
	_, before := coldBlocks(engine)
	require.Greater(t, before, coldTargetBlocks)
	want := readEverything(t, engine)
	engine.compactionLock.Lock()
	for range 10 {
		engine.mergeColdBlocks()
	}
	engine.compactionLock.Unlock()
	low, high := coldBlocks(engine)
	require.LessOrEqual(t, high, coldTargetBlocks)
	require.Positive(t, low)
	require.Equal(t, want, readEverything(t, engine))
	// And after the engine opens them again.
	dir := engine.Dir()
	require.NoError(t, engine.Close())
	reopened, err := OpenEngine(dir, "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, want, readEverything(t, reopened))
}

// BenchmarkColdBlocksQuery reads a tenant whose series left the head over a dozen rounds, with a block a round in each
// shard, or with them merged.
func BenchmarkColdBlocksQuery(b *testing.B) {
	// As production's blocks, which each have more series than are merged as they leave the head.
	defer func(small int) { coldSmallSeries = small }(coldSmallSeries)
	coldSmallSeries = 0
	for _, merged := range []bool{false, true} {
		engine := manyColdBlocks(b, 200_000, 15, 64)
		if merged {
			started := time.Now()
			engine.compactionLock.Lock()
			for range 10 {
				engine.mergeColdBlocks()
			}
			engine.compactionLock.Unlock()
			b.Logf("merged in %v", time.Since(started))
		}
		_, blocks := coldBlocks(engine)
		for _, lookup := range []struct {
			name string
			run  func(b *testing.B)
		}{
			{"select one metric", func(b *testing.B) {
				q, err := engine.ChunkQuerier(0, 1<<40)
				require.NoError(b, err)
				set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", fmt.Sprintf("container_metric_%d", b.N%300)))
				for set.Next() {
				}
				_ = q.Close()
			}},
			{"select absent metric", func(b *testing.B) {
				q, err := engine.ChunkQuerier(0, 1<<40)
				require.NoError(b, err)
				set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "absent"), promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "x"))
				for set.Next() {
				}
				_ = q.Close()
			}},
			{"select by namespace", func(b *testing.B) {
				q, err := engine.ChunkQuerier(0, 1<<40)
				require.NoError(b, err)
				set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "namespace", "namespace-3"), promlabels.MustNewMatcher(promlabels.MatchEqual, "container", "container-3"))
				for set.Next() {
				}
				_ = q.Close()
			}},
			{"label values", func(b *testing.B) {
				q, err := engine.Querier(0, 1<<40)
				require.NoError(b, err)
				_, _, err = q.LabelValues(context.Background(), "container", nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "container_metric_7"))
				require.NoError(b, err)
				_ = q.Close()
			}},
		} {
			b.Run(fmt.Sprintf("%s/blocks=%d", lookup.name, blocks), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					lookup.run(b)
				}
			})
		}
	}
}

// A block keeps each series' hashes, which are those of the series' labels.
func TestColdBlockHashesAreTheSeriesHashes(t *testing.T) {
	var frozen []frozenSeries
	for n := range 500 {
		var pairs [][2]string
		rw2Labels(n).Range(func(l promlabels.Label) { pairs = append(pairs, [2]string{l.Name, l.Value}) })
		frozen = append(frozen, frozenSeries{tenant: fmt.Sprintf("tenant-%d", n%2), labels: labels.FromSorted(pairs)})
	}
	block, err := buildColdBlock(t.TempDir(), 1, frozen)
	require.NoError(t, err)
	for _, tenant := range []string{"tenant-0", "tenant-1"} {
		table := block.tenant(tenant)
		require.Equal(t, 250, table.seriesCount)
		var pairs [][2]string
		for index := range table.seriesCount {
			series := block.seriesIn(table, index)
			require.Equal(t, series.labelsInto(&pairs).Hash(), block.labelsHash(table, index))
			require.Equal(t, labels.StableHash(&series), block.shardHash(table, index))
		}
	}
}

// BenchmarkBuildColdBlock builds the block of what a busy head leaves at once, a few thousand series of scrape targets.
func BenchmarkBuildColdBlock(b *testing.B) {
	series := make([]frozenSeries, 2000)
	for n := range series {
		lset := scrapeLabels(n)
		var pairs [][2]string
		lset.Range(func(l promlabels.Label) { pairs = append(pairs, [2]string{l.Name, l.Value}) })
		series[n] = frozenSeries{tenant: "tenant", labels: labels.FromSorted(pairs)}
	}
	b.ReportAllocs()
	for b.Loop() {
		block, err := buildColdBlock("", 1, series)
		require.NoError(b, err)
		discardColdBlock(block)
	}
}

// A block keeps the candidates of the selectors in use through bursts of new ones: a selector that comes in again
// between them stays kept, and what is kept is what the postings give.
func TestColdCandidatesStayKeptThroughNewSelectors(t *testing.T) {
	series := make([]frozenSeries, 500)
	for n := range series {
		series[n] = frozenSeries{tenant: "tenant", labels: labels.FromStrings("__name__", "metric", "pod", fmt.Sprint(n))}
	}
	block, err := buildColdBlock("", 1, series)
	require.NoError(t, err)
	t.Cleanup(func() { discardColdBlock(block) })
	compile := func(pod string) []compiledMatcher {
		compiled, err := compileMatchers([]LabelMatcher{matcher(0, "__name__", "metric"), matcher(0, "pod", pod)})
		require.NoError(t, err)
		return compiled
	}
	hot := compile("7")
	lookup := func(compiled []compiledMatcher) (list []uint32, hit bool) {
		hits, _ := ColdCandidatesCacheCounts()
		list = block.candidatesCached("tenant", selectorID(compiled), compiled, compiled)
		after, _ := ColdCandidatesCacheCounts()
		return list, after > hits
	}
	want := block.candidates("tenant", hot)
	require.Len(t, want, 1)
	_, hit := lookup(hot)
	require.False(t, hit, "the first lookup reads the postings")
	for burst := range 4 {
		// More new selectors than a generation holds.
		for n := range maxCachedSelectorsPerBlock {
			lookup(compile(fmt.Sprint(100 + burst*maxCachedSelectorsPerBlock + n)))
			if n == maxCachedSelectorsPerBlock/2 {
				list, hit := lookup(hot)
				require.True(t, hit, "burst %d", burst)
				require.Equal(t, want, list)
			}
		}
		list, hit := lookup(hot)
		require.True(t, hit, "burst %d", burst)
		require.Equal(t, want, list)
	}
}

// A value's posting is found in its name's range of the postings, for every name and value, and none for the ones the
// block doesn't have, as every entry read in order finds.
func TestColdPostingsByNameRange(t *testing.T) {
	series := make([]frozenSeries, 3000)
	for n := range series {
		series[n] = frozenSeries{tenant: "tenant", labels: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%7), "job", fmt.Sprintf("job-%d", n%13), "pod", fmt.Sprint(n), "zone", fmt.Sprintf("z%d", n%3))}
	}
	block, err := buildColdBlock("", 1, series)
	require.NoError(t, err)
	t.Cleanup(func() { discardColdBlock(block) })
	table := block.tenant("tenant")
	scan := func(name, value string) (uint32, int) {
		local, ok := block.localIDs[name]
		if !ok {
			return 0, 0
		}
		hash := labels.ValueHash(value)
		for index := range table.postingsCount {
			entryName, entryHash, offset := block.entry(table, index)
			if entryName == local && entryHash == hash {
				return offset, block.listLen(table, offset)
			}
		}
		return 0, 0
	}
	for _, name := range []string{"__name__", "job", "pod", "zone", "absent"} {
		for n := range 40 {
			for _, value := range []string{fmt.Sprintf("metric_%d", n), fmt.Sprintf("job-%d", n), fmt.Sprint(n * 77), fmt.Sprintf("z%d", n), "nothing"} {
				// Twice: the first finds the name's range, the second reads it.
				for range 2 {
					offset, count := block.postingRef(table, name, value)
					wantOffset, wantCount := scan(name, value)
					require.Equal(t, wantCount, count, "%s=%s", name, value)
					if wantCount > 0 {
						require.Equal(t, wantOffset, offset, "%s=%s", name, value)
					}
				}
			}
		}
		require.Equal(t, len(block.withLabelUncached(table, name)), func() int {
			local, ok := block.localIDs[name]
			if !ok {
				return 0
			}
			count := 0
			for index := range table.postingsCount {
				if entryName, _, offset := block.entry(table, index); entryName == local {
					count += block.listLen(table, offset)
				}
			}
			return count
		}(), name)
	}
}

// A shard over its block target still writes a block with chunks retention no longer keeps again, before merging: new
// blocks keep a shard over its target, and the expired chunks of its others stayed on disk.
func TestColdRewritesExpiredBlocksBeforeMerging(t *testing.T) {
	var c coldState
	for i := range coldTargetBlocks + 2 {
		c.blocks = append(c.blocks, &coldBlock{minTime: int64(1000 + i), maxTime: 5000, retainedTo: 1000})
	}
	c.blocks[3].minTime, c.blocks[3].retainedTo = 100, 0
	from, to, ok := c.pickMerge(1000)
	require.True(t, ok)
	require.Equal(t, [2]int{3, 4}, [2]int{from, to}, "the block with chunks before the cutoff")
	c.blocks[3].retainedTo = 1000
	from, to, ok = c.pickMerge(1000)
	require.True(t, ok)
	require.Greater(t, to-from, 1, "a merge, once it was written with the cutoff")
}
