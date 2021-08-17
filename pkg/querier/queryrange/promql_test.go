// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/querysharding"
)

var (
	start  = time.Unix(1000, 0)
	end    = start.Add(3 * time.Minute)
	step   = 30 * time.Second
	ctx    = context.Background()
	engine = promql.NewEngine(promql.EngineOpts{
		Reg:                prometheus.DefaultRegisterer,
		Logger:             log.NewNopLogger(),
		Timeout:            1 * time.Hour,
		MaxSamples:         10e6,
		ActiveQueryTracker: nil,
	})
)

func Test_FunctionParallelism(t *testing.T) {
	tpl := `sum(<fn>(bar1{}<fArgs>))`
	shardTpl := `sum(
				sum without(__query_shard__) (<fn>(bar1{__query_shard__="0_of_3"}<fArgs>)) or
				sum without(__query_shard__) (<fn>(bar1{__query_shard__="1_of_3"}<fArgs>)) or
				sum without(__query_shard__) (<fn>(bar1{__query_shard__="2_of_3"}<fArgs>))
			  )`

	mkQuery := func(tpl, fn string, testMatrix bool, fArgs []string) (result string) {
		result = strings.Replace(tpl, "<fn>", fn, -1)

		if testMatrix {
			// turn selectors into ranges
			result = strings.Replace(result, "}<fArgs>", "}[1m]<fArgs>", -1)
		}

		if len(fArgs) > 0 {
			args := "," + strings.Join(fArgs, ",")
			result = strings.Replace(result, "<fArgs>", args, -1)
		} else {
			result = strings.Replace(result, "<fArgs>", "", -1)
		}

		return result
	}

	for _, tc := range []struct {
		fn           string
		fArgs        []string
		isTestMatrix bool
		approximate  bool
	}{
		{
			fn: "abs",
		},
		{
			fn:           "avg_over_time",
			isTestMatrix: true,
			approximate:  true,
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
			approximate:  true,
		},
		{
			fn:           "deriv",
			isTestMatrix: true,
			approximate:  true,
		},
		{
			fn:          "exp",
			approximate: true,
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
			approximate:  true,
		},
		{
			fn:           "increase",
			isTestMatrix: true,
			approximate:  true,
		},
		{
			fn:           "irate",
			isTestMatrix: true,
			approximate:  true,
		},
		{
			fn:          "ln",
			approximate: true,
		},
		{
			fn:          "log10",
			approximate: true,
		},
		{
			fn:          "log2",
			approximate: true,
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
			approximate:  true,
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
			fn:          "sqrt",
			approximate: true,
		},
		{
			fn:           "stddev_over_time",
			isTestMatrix: true,
			approximate:  true,
		},
		{
			fn:           "stdvar_over_time",
			isTestMatrix: true,
			approximate:  true,
		},
		{
			fn:           "sum_over_time",
			isTestMatrix: true,
		},
		{
			fn: "timestamp",
		},
		{
			fn: "year",
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
			approximate:  true,
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
			approximate:  true,
		},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			baseQuery, err := engine.NewRangeQuery(
				shardAwareQueryable,
				mkQuery(tpl, tc.fn, tc.isTestMatrix, tc.fArgs),
				start,
				end,
				step,
			)
			require.Nil(t, err)
			shardQuery, err := engine.NewRangeQuery(
				shardAwareQueryable,
				mkQuery(shardTpl, tc.fn, tc.isTestMatrix, tc.fArgs),
				start,
				end,
				step,
			)
			require.Nil(t, err)
			baseResult := baseQuery.Exec(ctx)
			shardResult := shardQuery.Exec(ctx)
			t.Logf("base: %+v\n", baseResult)
			t.Logf("shard: %+v\n", shardResult)
			if !tc.approximate {
				require.Equal(t, baseResult, shardResult)
			} else {
				// Some functions yield tiny differences when sharded due to combining floating point calculations.
				baseSeries := baseResult.Value.(promql.Matrix)[0]
				shardSeries := shardResult.Value.(promql.Matrix)[0]

				require.Equal(t, len(baseSeries.Points), len(shardSeries.Points))
				for i, basePt := range baseSeries.Points {
					shardPt := shardSeries.Points[i]
					require.Equal(t, basePt.T, shardPt.T)
					require.Equal(
						t,
						math.Round(basePt.V*1e6)/1e6,
						math.Round(shardPt.V*1e6)/1e6,
					)
				}

			}
		})
	}
}

var shardAwareQueryable = storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &testMatrix{
		series: []*promql.StorageSeries{
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "barr"}}, factor(5)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "bazz"}}, factor(7)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "buzz"}}, factor(12)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bozz"}}, factor(11)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "buzz"}}, factor(8)),
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bazz"}}, identity),
		},
	}, nil
})

type testMatrix struct {
	series []*promql.StorageSeries
}

func (m *testMatrix) Select(sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
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

func (m *testMatrix) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *testMatrix) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *testMatrix) Close() error { return nil }

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

func newSeries(metric labels.Labels, generator func(float64) float64) *promql.StorageSeries {
	sort.Sort(metric)
	var points []promql.Point

	for ts := start.Add(-step); ts.Unix() <= end.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		points = append(points, promql.Point{
			T: t,
			V: generator(float64(t)),
		})
	}

	return promql.NewStorageSeries(promql.Series{
		Metric: metric,
		Points: points,
	})
}

func identity(t float64) float64 {
	return float64(t)
}

func factor(f float64) func(float64) float64 {
	i := 0.
	return func(float64) float64 {
		i++
		res := i * f
		return res
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
