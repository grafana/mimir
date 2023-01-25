package aggregator

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/stretchr/testify/require"
)

func TestPerformingAggregations(t *testing.T) {
	user := "test_user"

	offsetToMs := func(offset int64) int64 {
		return offset * time.Second.Milliseconds()
	}

	type ingestCall struct {
		user              string
		aggregated        string
		raw               string
		timestamp         int64
		value             float64
		expectedTimestamp int64
		expectedValue     float64
	}

	type testCase struct {
		name        string
		aggInterval time.Duration
		aggDelay    time.Duration
		ingestCalls []ingestCall
	}

	testCases := []testCase{
		{
			name:        "single raw series to single aggregated series",
			aggInterval: time.Minute,
			aggDelay:    30 * time.Second,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(105), 3, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(165), 5, offsetToMs(120) - 1, 3},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(225), 10, offsetToMs(180) - 1, 5},
			},
		}, {
			name:        "two raw series to single aggregated series",
			aggInterval: time.Minute,
			aggDelay:    30 * time.Second,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(105), 3, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(110), 7, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(165), 5, offsetToMs(120) - 1, 10},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(170), 11, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(225), 10, offsetToMs(180) - 1, 16},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(230), 18, offsetToMs(180) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(285), 12, offsetToMs(240) - 1, 28},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(290), 25, offsetToMs(240) - 1, math.NaN()},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			aggs := newUserAggregations(tc.aggInterval, tc.aggDelay)

			for callIdx, call := range tc.ingestCalls {
				t.Run("call "+strconv.Itoa(callIdx), func(t *testing.T) {
					gotPoint := aggs.ingest(call.user, call.aggregated, call.raw, mimirpb.Sample{TimestampMs: call.timestamp, Value: call.value})
					require.Equal(t, call.expectedTimestamp, gotPoint.TimestampMs, "unexpected timestamp")
					if math.IsNaN(call.expectedValue) {
						require.True(t, math.IsNaN(gotPoint.Value), fmt.Sprintf("expected value to be NaN, but was %f", gotPoint.Value))
					} else {
						require.Equal(t, call.expectedValue, gotPoint.Value, "unexpected value")
					}
				})
			}
		})
	}
}
