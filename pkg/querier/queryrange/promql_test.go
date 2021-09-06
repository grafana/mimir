// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/querysharding"
	"github.com/grafana/mimir/pkg/util"
)

var (
	start         = time.Now()
	end           = start.Add(30 * time.Minute)
	step          = 30 * time.Second
	lookbackDelta = 5 * time.Minute
)

func Test_FunctionParallelism(t *testing.T) {
	mkQueries := func(tpl, fn string, testMatrix bool, fArgs []string) []string {
		if tpl == "" {
			tpl = `(<fn>(bar1{}<fArgs>))`
		}
		result := strings.Replace(tpl, "<fn>", fn, -1)

		if testMatrix {
			// turn selectors into ranges
			result = strings.Replace(result, "}", "}[1m]", -1)
		}

		if len(fArgs) > 0 {
			args := "," + strings.Join(fArgs, ",")
			result = strings.Replace(result, "<fArgs>", args, -1)
		} else {
			result = strings.Replace(result, "<fArgs>", "", -1)
		}

		return []string{
			result,
			"sum" + result,
			"sum by (bar)" + result,
			"count" + result,
			"count by (bar)" + result,
		}
	}

	for _, tc := range []struct {
		fn           string
		fArgs        []string
		isTestMatrix bool
		tpl          string
	}{
		{
			fn: "abs",
		},
		{
			fn:           "avg_over_time",
			isTestMatrix: true,
		},
		{
			fn: "ceil",
		},
		{
			fn:           "changes",
			isTestMatrix: true,
		},
		{
			fn:           "count_over_time",
			isTestMatrix: true,
		},
		{
			fn: "days_in_month",
		},
		{
			fn: "day_of_month",
		},
		{
			fn: "day_of_week",
		},
		{
			fn:           "delta",
			isTestMatrix: true,
		},
		{
			fn:           "deriv",
			isTestMatrix: true,
		},
		{
			fn: "exp",
		},
		{
			fn: "floor",
		},
		{
			fn: "hour",
		},
		{
			fn:           "idelta",
			isTestMatrix: true,
		},
		{
			fn:           "increase",
			isTestMatrix: true,
		},
		{
			fn:           "irate",
			isTestMatrix: true,
		},
		{
			fn: "ln",
		},
		{
			fn: "log10",
		},
		{
			fn: "log2",
		},
		{
			fn:           "max_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "min_over_time",
			isTestMatrix: true,
		},
		{
			fn: "minute",
		},
		{
			fn: "month",
		},
		{
			fn:           "rate",
			isTestMatrix: true,
		},
		{
			fn:           "resets",
			isTestMatrix: true,
		},
		{
			fn: "sort",
		},
		{
			fn: "sort_desc",
		},
		{
			fn: "sqrt",
		},
		{
			fn:           "stddev_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "stdvar_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "sum_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "last_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "present_over_time",
			isTestMatrix: true,
		},
		{
			fn:           "quantile_over_time",
			isTestMatrix: true,
			tpl:          `(<fn>(0.5,bar1{}))`,
		},
		{
			fn:           "quantile_over_time",
			isTestMatrix: true,
			tpl:          `(<fn>(0.99,bar1{}))`,
		},
		{
			fn: "timestamp",
		},
		{
			fn: "year",
		},
		{
			fn: "sgn",
		},
		{
			fn:    "clamp",
			fArgs: []string{"5", "10"},
		},
		{
			fn:    "clamp_max",
			fArgs: []string{"5"},
		},
		{
			fn:    "clamp_min",
			fArgs: []string{"5"},
		},
		{
			fn:           "predict_linear",
			isTestMatrix: true,
			fArgs:        []string{"1"},
		},
		{
			fn:    "round",
			fArgs: []string{"20"},
		},
		{
			fn:           "holt_winters",
			isTestMatrix: true,
			fArgs:        []string{"0.5", "0.7"},
		},
	} {

		const numShards = 4
		for _, query := range mkQueries(tc.tpl, tc.fn, tc.isTestMatrix, tc.fArgs) {
			t.Run(query, func(t *testing.T) {
				req := &PrometheusRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
					Query: query,
				}

				reg := prometheus.NewPedanticRegistry()
				engine := newEngine()
				shardingware := NewQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					numShards,
					reg,
				)
				downstream := &downstreamHandler{
					engine:    engine,
					queryable: shardAwareQueryable,
				}

				// Run the query without sharding.
				expectedRes, err := downstream.Do(context.Background(), req)
				require.Nil(t, err)

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedRes.(*PrometheusResponse).Data.Result)

				// Run the query with sharding.
				shardedRes, err := shardingware.Wrap(downstream).Do(context.Background(), req)
				require.Nil(t, err)

				// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
				// if you rerun the same query twice).
				approximatelyEquals(t, expectedRes.(*PrometheusResponse), shardedRes.(*PrometheusResponse))
			})
		}
	}
}

var shardAwareQueryable = storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &querierMock{
		series: []*promql.StorageSeries{
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "barr"}}, start.Add(-lookbackDelta), end, factor(5)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "bazz"}}, start.Add(-lookbackDelta), end, factor(7)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "buzz"}}, start.Add(-lookbackDelta), end, factor(12)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bozz"}}, start.Add(-lookbackDelta), end, factor(11)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "buzz"}}, start.Add(-lookbackDelta), end, factor(8)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bazz"}}, start.Add(-lookbackDelta), end, arithmeticSequence(10)),
		},
	}, nil
})

type querierMock struct {
	series []*promql.StorageSeries
}

func (m *querierMock) Select(sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shard, matchers, err := querysharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Filter series by label matchers.
	var filtered []*promql.StorageSeries

	for _, series := range m.series {
		if seriesMatches(series, matchers...) {
			filtered = append(filtered, series)
		}
	}

	// Filter series by shard (if any)
	filtered = filterSeriesByShard(filtered, shard)

	// Honor the sorting.
	if sorted {
		sort.Slice(filtered, func(i, j int) bool {
			return labels.Compare(filtered[i].Labels(), filtered[j].Labels()) < 0
		})
	}

	return newSeriesIteratorMock(filtered)
}

func (m *querierMock) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) Close() error { return nil }

func seriesMatches(series *promql.StorageSeries, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(series.Labels().Get(m.Name)) {
			return false
		}
	}

	return true
}

func filterSeriesByShard(series []*promql.StorageSeries, shard *querysharding.ShardSelector) []*promql.StorageSeries {
	if shard == nil {
		return series
	}

	var filtered []*promql.StorageSeries

	for _, s := range series {
		if s.Labels().Hash()%shard.ShardCount == shard.ShardIndex {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

func newSeries(metric labels.Labels, from, to time.Time, gen generator) *promql.StorageSeries {
	var (
		points    []promql.Point
		prevValue *float64
	)

	for ts := from; ts.Unix() <= to.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		v := gen(t)

		// If both the previous and current values are the stale marker, then we omit the
		// point completely (we just keep the 1st one in a consecutive series of stale markers).
		shouldSkip := prevValue != nil && value.IsStaleNaN(*prevValue) && value.IsStaleNaN(v)
		prevValue = &v
		if shouldSkip {
			continue
		}

		points = append(points, promql.Point{
			T: t,
			V: v,
		})
	}

	// Ensure series labels are sorted.
	sort.Sort(metric)

	return promql.NewStorageSeries(promql.Series{
		Metric: metric,
		Points: points,
	})
}

// newTestCounterLabels generates series labels for a counter metric used in tests.
func newTestCounterLabels(id int) labels.Labels {
	return labels.Labels{
		{Name: "__name__", Value: "metric_counter"},
		{Name: "const", Value: "fixed"},                 // A constant label.
		{Name: "unique", Value: strconv.Itoa(id)},       // A unique label.
		{Name: "group_1", Value: strconv.Itoa(id % 10)}, // A first grouping label.
		{Name: "group_2", Value: strconv.Itoa(id % 5)},  // A second grouping label.
	}
}

// newTestCounterLabels generates series labels for an histogram metric used in tests.
func newTestHistogramLabels(id int, bucketLe float64) labels.Labels {
	return labels.Labels{
		{Name: "__name__", Value: "metric_histogram_bucket"},
		{Name: "le", Value: fmt.Sprintf("%f", bucketLe)},
		{Name: "const", Value: "fixed"},                 // A constant label.
		{Name: "unique", Value: strconv.Itoa(id)},       // A unique label.
		{Name: "group_1", Value: strconv.Itoa(id % 10)}, // A first grouping label.
		{Name: "group_2", Value: strconv.Itoa(id % 5)},  // A second grouping label.
	}
}

// generator defined a function used to generate sample values in tests.
type generator func(ts int64) float64

func factor(f float64) generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i * f
		return res
	}
}

func arithmeticSequence(f float64) generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i + f
		return res
	}
}

// stale wraps the input generator and injects stale marker between from and to.
func stale(from, to time.Time, wrap generator) generator {
	return func(ts int64) float64 {
		// Always get the next value from the wrapped generator.
		v := wrap(ts)

		// Inject the stale marker if we're at the right time.
		if ts >= util.TimeToMillis(from) && ts <= util.TimeToMillis(to) {
			return math.Float64frombits(value.StaleNaN)
		}

		return v
	}
}

type seriesIteratorMock struct {
	idx    int
	series []*promql.StorageSeries
}

func newSeriesIteratorMock(series []*promql.StorageSeries) *seriesIteratorMock {
	return &seriesIteratorMock{
		idx:    -1,
		series: series,
	}
}

func (i *seriesIteratorMock) Next() bool {
	i.idx++
	return i.idx < len(i.series)
}

func (i *seriesIteratorMock) At() storage.Series {
	if i.idx >= len(i.series) {
		return nil
	}

	return i.series[i.idx]
}

func (i *seriesIteratorMock) Err() error {
	return nil
}

func (i *seriesIteratorMock) Warnings() storage.Warnings {
	return nil
}

// newEngine creates and return a new promql.Engine used for testing.
func newEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		Reg:                nil,
		MaxSamples:         10e6,
		Timeout:            1 * time.Hour,
		ActiveQueryTracker: nil,
		LookbackDelta:      lookbackDelta,
		EnableAtModifier:   true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
		},
	})
}
