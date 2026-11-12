// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/shardingtest"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/testdatagen"
	storagesharding "github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

func TestQuerySharding_SubsetLabelShardingCorrectness(t *testing.T) {
	dataStart := time.Unix(0, 0)
	start := dataStart.Add(5 * time.Minute)
	end := start.Add(time.Minute)
	step := 30 * time.Second
	expr := `histogram_quantile(0.5, sum by (job, zone, le) (rate(request_duration_seconds_bucket[2m])))`

	groups := []struct {
		job, zone string
		quantile  float64
	}{
		{"api", "east", 1.5},
		{"api", "west", 4.0 / 3},
		{"worker", "east", 7.0 / 6},
		{"worker", "west", 1},
	}
	var series []storage.Series
	for groupIndex, group := range groups {
		for instance := range 2 {
			for bucketIndex, le := range []string{"1", "2", "+Inf"} {
				// Each histogram needs buckets from separate series and contributions
				// from both instances. Hashing le or instance would split the histogram.
				// Give instances different distributions, while their sum remains
				// proportional to [1+groupIndex, 4+groupIndex, 5+groupIndex].
				increment := float64(([]int{1, 4, 5}[bucketIndex]+groupIndex)*(instance+1) + (1-2*instance)*(1-bucketIndex))
				series = append(series, testdatagen.NewSeries(labels.FromStrings(
					"__name__", "request_duration_seconds_bucket",
					"job", group.job, "zone", group.zone,
					"instance", fmt.Sprint(instance), "le", le,
				), dataStart, end, step, testdatagen.Factor(increment)))
			}
		}
	}

	for _, rangeQuery := range []bool{false, true} {
		t.Run(fmt.Sprintf("range=%t", rangeQuery), func(t *testing.T) {
			ctx := user.InjectOrgID(t.Context(), "test-user")
			newQuery := func(ctx context.Context, engine promql.QueryEngine, queryable storage.Queryable) promql.Query {
				var query promql.Query
				var err error
				if rangeQuery {
					query, err = engine.NewRangeQuery(ctx, queryable, nil, expr, start, end, step)
				} else {
					query, err = engine.NewInstantQuery(ctx, queryable, nil, expr, end)
				}
				require.NoError(t, err)
				t.Cleanup(query.Close)
				return query
			}

			unshardedEngine, _ := createEngine(t, 0)
			unshardedQuery := newQuery(ctx, unshardedEngine, testdatagen.StorageSeriesQueryable(series))
			unshardedResult := unshardedQuery.Exec(ctx)
			require.NoError(t, unshardedResult.Err)
			require.Empty(t, unshardedResult.Warnings)

			// Check concrete labels, timestamps and quantiles, not just equivalence
			// between two executions that could both return incomplete results.
			var expected promql.Matrix
			for _, group := range groups {
				resultSeries := promql.Series{Metric: labels.FromStrings("job", group.job, "zone", group.zone)}
				first := end
				if rangeQuery {
					first = start
				}
				for ts := first; !ts.After(end); ts = ts.Add(step) {
					resultSeries.Floats = append(resultSeries.Floats, promql.FPoint{T: ts.UnixMilli(), F: group.quantile})
				}
				expected = append(expected, resultSeries)
			}
			var expectedResult *promql.Result
			if rangeQuery {
				expectedResult = &promql.Result{Value: expected}
			} else {
				var vector promql.Vector
				for _, s := range expected {
					vector = append(vector, promql.Sample{Metric: s.Metric, T: s.Floats[0].T, F: s.Floats[0].F})
				}
				expectedResult = &promql.Result{Value: vector}
			}
			testutils.RequireEqualResults(t, expr, expectedResult, unshardedResult, false)

			for _, numShards := range []int{2, 4, 8} {
				t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
					// Ensure the data would expose accidental whole-series sharding:
					// the buckets of each output group span multiple ordinary shards.
					for _, group := range groups {
						ordinaryShards := map[uint64]bool{}
						for _, s := range series {
							if s.Labels().Get("job") == group.job && s.Labels().Get("zone") == group.zone {
								ordinaryShards[labels.StableHash(s.Labels())%uint64(numShards)] = true
							}
						}
						require.Greater(t, len(ordinaryShards), 1)
					}

					querier := &subsetLabelExecutionQuerier{QuerierMock: testdatagen.QuerierMock{Series: series}}
					queryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) { return querier, nil })
					shardedEngine, reg := createEngine(t, numShards)
					shardedCtx := requestoptions.ContextWithOptions(ctx, requestoptions.Options{
						PropagatedHeaders: map[string][]string{"X-Mimir-Subset-Label-Sharding": {"true"}},
					})
					query := newQuery(shardedCtx, shardedEngine, queryable)
					result := query.Exec(shardedCtx)
					require.NoError(t, result.Err)
					require.Empty(t, result.Warnings)
					testutils.RequireEqualResults(t, expr, unshardedResult, result, false)
					shardingtest.AssertShardingMetrics(t, reg, 1, numShards)

					querier.mu.Lock()
					defer querier.mu.Unlock()
					require.Len(t, querier.shards, numShards)
					seen := map[uint64]bool{}
					for _, shard := range querier.shards {
						require.Equal(t, uint64(numShards), shard.ShardCount)
						require.Equal(t, []string{"job", "zone"}, shard.ByLabels)
						seen[shard.ShardIndex] = true
					}
					require.Len(t, seen, numShards)
				})
			}
		})
	}
}

// subsetLabelExecutionQuerier applies the shard's label projection before delegating
// ordinary matcher filtering and sorting to QuerierMock, which only supports full-label sharding.
type subsetLabelExecutionQuerier struct {
	testdatagen.QuerierMock
	mu     sync.Mutex
	shards []storagesharding.ShardSelector
}

func (q *subsetLabelExecutionQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shard, matchers, err := storagesharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if shard == nil {
		return q.QuerierMock.Select(ctx, sorted, hints, matchers...)
	}
	q.mu.Lock()
	q.shards = append(q.shards, *shard)
	q.mu.Unlock()

	var selected []storage.Series
	var hashBuffer []byte
	for _, s := range q.Series {
		hash := labels.StableHash(s.Labels())
		if len(shard.ByLabels) > 0 {
			hash, hashBuffer = s.Labels().HashForLabels(hashBuffer, shard.ByLabels...)
		}
		if hash%shard.ShardCount == shard.ShardIndex {
			selected = append(selected, s)
		}
	}
	delegate := testdatagen.QuerierMock{Series: selected}
	return delegate.Select(ctx, sorted, hints, matchers...)
}
