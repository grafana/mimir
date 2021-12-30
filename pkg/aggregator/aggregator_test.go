package aggregator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/util"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestHorizontalAggregation(t *testing.T) {
	// Aggregating to an output interval of 1min
	from := time.Minute * 5
	to := time.Minute * 6

	inputSeries1 := promql.Series{
		Metric: labels.Labels([]labels.Label{
			{Name: "__name__", Value: "test_series"},
			{Name: "other", Value: "foo"},
		}),
		Points: []promql.Point{
			{V: 3, T: time.Duration(from + 10*time.Second).Milliseconds()},
			{V: 3.5, T: time.Duration(from + 20*time.Second).Milliseconds()},
			{V: 3.2, T: time.Duration(from + 30*time.Second).Milliseconds()},
			{V: 2.8, T: time.Duration(from + 40*time.Second).Milliseconds()},
			{V: 2.7, T: time.Duration(from + 50*time.Second).Milliseconds()},
			{V: 3.4, T: time.Duration(from + 60*time.Second).Milliseconds()},
		},
	}
	inputSeries2 := promql.Series{
		Metric: labels.Labels([]labels.Label{
			{Name: "__name__", Value: "test_series"},
			{Name: "other", Value: "bar"},
		}),
		Points: []promql.Point{
			{V: 7.3, T: time.Duration(from + 10*time.Second).Milliseconds()},
			{V: 7.9, T: time.Duration(from + 20*time.Second).Milliseconds()},
			{V: 7.3, T: time.Duration(from + 30*time.Second).Milliseconds()},
			{V: 6.7, T: time.Duration(from + 40*time.Second).Milliseconds()},
			{V: 6.8, T: time.Duration(from + 50*time.Second).Milliseconds()},
			{V: 7.95, T: time.Duration(from + 60*time.Second).Milliseconds()},
		},
	}

	query, err := getPromqlEngine().NewInstantQuery(
		storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return &querierMock{
				series: []*promql.StorageSeries{
					promql.NewStorageSeries(inputSeries1),
					promql.NewStorageSeries(inputSeries2),
				},
			}, nil
		}),
		"max_over_time(test_series1[60s])",
		util.TimeFromMillis(to.Milliseconds()),
	)
	require.NoError(t, err)

	result := query.Exec(context.Background())
	require.NoError(t, result.Err)

	vector, err := result.Vector()
	require.NoError(t, err)
	require.Equal(
		t,
		promql.Vector{
			promql.Sample{
				Point: promql.Point{
					T: to.Milliseconds(),
					V: float64(3.5),
				},
				Metric: labels.Labels{
					{
						Name:  "other",
						Value: "foo",
					},
				},
			},

			promql.Sample{
				Point: promql.Point{
					T: to.Milliseconds(),
					V: float64(7.95),
				},
				Metric: labels.Labels{
					{
						Name:  "other",
						Value: "bar",
					},
				},
			},
		},
		vector,
	)
}

func TestVerticalAggregation(t *testing.T) {
	// Vertically aggregating multiple series at ts=5min
	ts := time.Minute * 5

	inputValues := []float64{3, 18, 2, -1.5, 22, 22, 22}
	inputSeries := make([]*promql.StorageSeries, 0, len(inputValues))
	for inputValueIdx, inputValue := range inputValues {
		inputSeries = append(inputSeries, promql.NewStorageSeries(
			promql.Series{
				Metric: labels.Labels([]labels.Label{
					{Name: "__name__", Value: "test_series"},
					{Name: "enum", Value: fmt.Sprintf("series%d", inputValueIdx)},
				}),
				Points: []promql.Point{
					{V: inputValue, T: time.Duration(ts).Milliseconds()},
				},
			},
		))
	}

	query, err := getPromqlEngine().NewInstantQuery(
		storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return &querierMock{series: inputSeries}, nil
		}),
		"quantile(0.5, test_series)",
		util.TimeFromMillis(ts.Milliseconds()),
	)
	require.NoError(t, err)

	result := query.Exec(context.Background())
	require.NoError(t, result.Err)

	vector, err := result.Vector()
	require.NoError(t, err)
	require.Equal(
		t,
		promql.Vector{
			promql.Sample{
				Point: promql.Point{
					T: ts.Milliseconds(),
					V: float64(18),
				},
				Metric: labels.Labels{},
			},
		},
		vector,
	)
}

func TestHorizontalAndVerticalAggregationCombined(t *testing.T) {
	// Aggregating to an output interval of 1min
	from := time.Minute * 5
	to := time.Minute * 6

	// 4 series where each has 6 points within the 1min aggregation bucket
	inputValues := [][]float64{
		{3, 18, 2, -1.5, 22, 22},     // avg = 10.91666
		{4, 32.1, 4, 32, 35, 37},     // avg = 24.01666
		{7, 112, 234, 222, 199, 150}, // avg = 154
		{2, 32, 14, -1.5, -103, 12},  // avg = -7.41666
	}
	// sum(10.91666, 24.01666, 154, -7.41666) = 181.51666

	inputSeries := make([]*promql.StorageSeries, 0, len(inputValues))
	for inputSeriesIdx := 0; inputSeriesIdx < len(inputValues); inputSeriesIdx++ {
		points := make([]promql.Point, 0, len(inputValues))

		for inputValueIdx, inputValue := range inputValues[inputSeriesIdx] {
			points = append(points, promql.Point{
				T: from.Milliseconds() + int64(inputValueIdx+1)*10*time.Second.Milliseconds(),
				V: inputValue,
			})
		}

		inputSeries = append(inputSeries, promql.NewStorageSeries(
			promql.Series{
				Metric: labels.Labels([]labels.Label{
					{Name: "__name__", Value: "test_series"},
					{Name: "enum", Value: fmt.Sprintf("series%d", inputSeriesIdx)},
				}),
				Points: points,
			},
		))
	}

	query, err := getPromqlEngine().NewInstantQuery(
		storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return &querierMock{series: inputSeries}, nil
		}),
		"sum(avg_over_time(test_series[1m]))",
		util.TimeFromMillis(to.Milliseconds()),
	)
	require.NoError(t, err)

	result := query.Exec(context.Background())
	require.NoError(t, result.Err)

	vector, err := result.Vector()
	require.NoError(t, err)
	require.Equal(
		t,
		promql.Vector{
			promql.Sample{
				Point: promql.Point{
					T: to.Milliseconds(),
					V: float64(181.51666666666668),
				},
				Metric: labels.Labels{},
			},
		},
		vector,
	)
}

func getPromqlEngine() *promql.Engine {
	engineOpts := promql.EngineOpts{
		Logger:             nil,
		Reg:                nil,
		MaxSamples:         10e6,
		Timeout:            1 * time.Hour,
		ActiveQueryTracker: nil,
		LookbackDelta:      5 * time.Minute,
		EnableAtModifier:   true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
		},
	}
	engine := promql.NewEngine(engineOpts)
	return engine
}
