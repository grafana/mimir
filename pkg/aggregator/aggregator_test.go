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

// TestAggregation tests a scenario where 7 series get aggregated together at one single time stamp,
// from each input series the last sample is taken into account.
// The aggregation function used is median/p50.
func TestVerticalAggregation(t *testing.T) {
	// Aggregating multiple series at ts=5min
	ts := time.Minute * 5

	inputValues := []float64{3, 18, 2, -1.5, 22, 22, 22}

	// inputValuesBySeries is the datastructure based on which the Aggregator would generate a sample.
	inputValuesBySeries := make(map[uint64]float64, len(inputValues))
	for inputValueIdx, inputValue := range inputValues {
		labelHash := labels.Labels([]labels.Label{
			{Name: "__name__", Value: "test_series"},
			{Name: "enum", Value: fmt.Sprintf("series%d", inputValueIdx)},
		}).Hash()

		inputValuesBySeries[labelHash] = inputValue
	}

	// Generate anonymous series from the inputValuesBySeries to pass to the mock querier.
	inputSeries := make([]*promql.StorageSeries, 0, len(inputValuesBySeries))
	for _, inputValue := range inputValuesBySeries {
		inputSeries = append(inputSeries, promql.NewStorageSeries(
			promql.Series{
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
