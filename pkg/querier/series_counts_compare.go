// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var unregisteredDeduplicatorMetrics = limiter.NewSeriesDeduplicatorMetrics(nil)

// chunksPathCost is what the chunks path read, from the querier's stats.
type chunksPathCost struct {
	FetchedSeries     uint64 `json:"fetched_series"`
	FetchedChunks     uint64 `json:"fetched_chunks"`
	FetchedChunkBytes uint64 `json:"fetched_chunk_bytes"`
	FetchedIndexBytes uint64 `json:"fetched_index_bytes"`
}

// SeriesCountsFromChunks answers req the way a PromQL count does: it selects
// the matching series with their chunks through the store-gateways' Series
// call and counts a series in a bucket if it has a sample there. compare=true
// on the cardinality/estimate route checks the estimate against it.
func (q *BlocksStoreQueryable) SeriesCountsFromChunks(ctx context.Context, req CardinalityEstimateRequest) (map[string][]int64, chunksPathCost, error) {
	st, ctx := querier_stats.ContextWithEmptyStats(ctx)
	// Select needs these in the context; the PromQL query path adds them.
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, unregisteredDeduplicatorMetrics)
	qr, err := q.Querier(req.MinT, req.MaxT-1)
	if err != nil {
		return nil, chunksPathCost{}, err
	}
	defer qr.Close()

	matchers := req.Matchers
	if len(matchers) == 0 {
		matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")}
	}
	set := qr.Select(ctx, false, &storage.SelectHints{Start: req.MinT, End: req.MaxT - 1}, matchers...)
	counts, err := countSeriesSamples(set, req)
	cost := chunksPathCost{
		FetchedSeries:     st.LoadFetchedSeries(),
		FetchedChunks:     st.LoadFetchedChunks(),
		FetchedChunkBytes: st.LoadFetchedChunkBytes(),
		FetchedIndexBytes: st.LoadFetchedIndexBytes(),
	}
	return counts, cost, err
}

// countSeriesSamples counts each series of set once per bucket of req in
// which it has at least one sample.
func countSeriesSamples(set storage.SeriesSet, req CardinalityEstimateRequest) (map[string][]int64, error) {
	buckets := 1
	if req.Step > 0 {
		buckets = int((req.MaxT - req.MinT) / req.Step)
	}
	groupBy := req.GroupBy
	if groupBy == "" {
		groupBy = labels.MetricName
	}
	out := map[string][]int64{}
	seen := make([]bool, buckets)
	var it chunkenc.Iterator
	for set.Next() {
		s := set.At()
		clear(seen)
		found := false
		it = s.Iterator(it)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			t := it.AtT()
			if t < req.MinT || t >= req.MaxT {
				continue
			}
			i := 0
			if req.Step > 0 {
				i = int((t - req.MinT) / req.Step)
			}
			seen[i], found = true, true
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		v := s.Labels().Get(groupBy)
		counts := out[v]
		if counts == nil {
			counts = make([]int64, buckets)
			out[v] = counts
		}
		for i, hit := range seen {
			if hit {
				counts[i]++
			}
		}
	}
	return out, set.Err()
}

// countsDiff lists the groups whose counts differ between the two paths,
// at most limit of them, and how many differ in total.
func countsDiff(index, chunks map[string][]int64, limit int) ([]string, int) {
	var keys []string
	for k := range index {
		keys = append(keys, k)
	}
	for k := range chunks {
		if _, ok := index[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	var out []string
	n := 0
	for _, k := range keys {
		if slices.Equal(index[k], chunks[k]) {
			continue
		}
		n++
		if len(out) < limit {
			out = append(out, fmt.Sprintf("%s: index %v, chunks %v", k, index[k], chunks[k]))
		}
	}
	return out, n
}
