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
				ref, err := app.Append(0, rw2Labels(n), ts, float64(n))
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
