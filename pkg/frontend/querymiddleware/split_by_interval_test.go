package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/util"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestQuerySplittingCorrectness(t *testing.T) {
	var (
		numSeries          = 1000
		numStaleSeries     = 100
		numHistograms      = 1000
		numStaleHistograms = 100
		histogramBuckets   = []float64{1.0, 2.0, 4.0, 10.0, 100.0, math.Inf(1)}
	)

	tests := map[string]struct {
		query string

		// Expected number of sharded queries per shard (the final expected
		// number will be multiplied for the number of shards).
		expectedSplitQueries int
	}{
		// TODO: Fixme
		//"count_over_time": {
		//	query:                `count_over_time(metric_counter[3m])`,
		//	expectedSplitQueries: 3,
		//},
		"sum(count_over_time)": {
			query:                `sum(count_over_time(metric_counter[3m]))`,
			expectedSplitQueries: 3,
		},
		"max(sum_over_time)": {
			query:                `max(sum_over_time(metric_counter[3m]))`,
			expectedSplitQueries: 3,
		},
		"min(sum_over_time)": {
			query:                `min(sum_over_time(metric_counter[3m]))`,
			expectedSplitQueries: 3,
		},
	}

	series := make([]*promql.StorageSeries, 0, numSeries+(numHistograms*len(histogramBuckets)))
	seriesID := 0

	// Add counter series.
	for i := 0; i < numSeries; i++ {
		gen := factor(float64(i) * 0.1)
		if i >= numSeries-numStaleSeries {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
		}

		series = append(series, newSeries(newTestCounterLabels(seriesID), start.Add(-lookbackDelta), end, step, gen))
		seriesID++
	}

	// Add a special series whose data points end earlier than the end of the queried time range
	// and has NO stale marker.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(-lookbackDelta), end.Add(-5*time.Minute), step, factor(2)))
	seriesID++

	// Add a special series whose data points end earlier than the end of the queried time range
	// and HAS a stale marker at the end.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(-lookbackDelta), end.Add(-5*time.Minute), step, stale(end.Add(-6*time.Minute), end.Add(-4*time.Minute), factor(2))))
	seriesID++

	// Add a special series whose data points start later than the start of the queried time range.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(5*time.Minute), end, step, factor(2)))
	seriesID++

	// Add histogram series.
	for i := 0; i < numHistograms; i++ {
		for bucketIdx, bucketLe := range histogramBuckets {
			// We expect each bucket to have a value higher than the previous one.
			gen := factor(float64(i) * float64(bucketIdx) * 0.1)
			if i >= numHistograms-numStaleHistograms {
				// Wrap the generator to inject the staleness marker between minute 10 and 20.
				gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
			}

			series = append(series, newSeries(newTestHistogramLabels(seriesID, bucketLe),
				start.Add(-lookbackDelta), end, step, gen))
		}

		// Increase the series ID after all per-bucket series have been created.
		seriesID++
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	for testName, testData := range tests {
		// Change scope to ensure it work fine when test cases are executed concurrently.
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			reqs := []Request{
				&PrometheusInstantQueryRequest{
					Path:  "/query",
					Time:  util.TimeToMillis(end),
					Query: testData.query,
				},
			}

			for _, req := range reqs {
				t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
					engine := newEngine()
					downstream := &downstreamHandler{
						engine:    engine,
						queryable: queryable,
					}

					// Run the query without sharding.
					expectedRes, err := downstream.Do(context.Background(), req)
					require.Nil(t, err)
					expectedPrometheusRes := expectedRes.(*PrometheusResponse)

					// Ensure the query produces some results.
					require.NotEmpty(t, expectedPrometheusRes.Data.Result)
					requireValidSamples(t, expectedPrometheusRes.Data.Result)

					splittingware := newSplitByIntervalMiddleware(true, 1*time.Minute, mockLimits{}, log.NewNopLogger(), engine)

					// Run the query with sharding.
					splitRes, err := splittingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
					require.Nil(t, err)

					splitPrometheusRes := splitRes.(*PrometheusResponse)
					approximatelyEquals(t, expectedPrometheusRes, splitPrometheusRes)
				})
			}
		})
	}
}
