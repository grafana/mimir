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
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(165), 5, offsetToMs(120) - 1, 10},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(170), 11, offsetToMs(120) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(225), 10, offsetToMs(180) - 1, 16},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(230), 18, offsetToMs(180) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(285), 12, offsetToMs(240) - 1, 28},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(290), 25, offsetToMs(240) - 1, math.NaN()},
			},
		}, {
			name:        "four raw series to two aggregated series",
			aggInterval: time.Minute,
			aggDelay:    30 * time.Second,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(105), 3, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(106), 22, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value1\"}", offsetToMs(103), 1, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value2\"}", offsetToMs(100), 4, offsetToMs(60) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(165), 6, offsetToMs(120) - 1, 25},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(166), 42, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value1\"}", offsetToMs(163), 3, offsetToMs(120) - 1, 5},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value2\"}", offsetToMs(160), 5, offsetToMs(120) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(225), 7, offsetToMs(180) - 1, 48},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value2\"}", offsetToMs(226), 75, offsetToMs(180) - 1, math.NaN()},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value1\"}", offsetToMs(223), 3, offsetToMs(180) - 1, 8},
				{user, "test_metric{label1=\"value2\"}", "test_metric{label1=\"value2\",label2=\"value2\"}", offsetToMs(220), 6, offsetToMs(180) - 1, math.NaN()},
			},
		}, {
			name:        "long aggregation delay",
			aggInterval: time.Minute,
			aggDelay:    4 * time.Minute,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1000), 3, offsetToMs(720) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1060), 5, offsetToMs(780) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1120), 10, offsetToMs(840) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1180), 14, offsetToMs(900) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1240), 18, offsetToMs(960) - 1, math.NaN()},
				// Aggregation delay for first ingested sample is up.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1300), 21, offsetToMs(1020) - 1, 3},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(1360), 24, offsetToMs(1080) - 1, 5},
			},
		}, {
			name:        "no aggregation delay",
			aggInterval: time.Minute,
			aggDelay:    0,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(141), 3, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(202), 4, offsetToMs(180) - 1, 3},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(263), 5, offsetToMs(240) - 1, 4},
			},
		}, {
			name:        "ingested samples at the bucket boundaries",
			aggInterval: time.Minute,
			aggDelay:    0,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(180) - 1, 3, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(180), 4, offsetToMs(180) - 1, 3},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(300) - 1, 5, offsetToMs(240) - 1, 4},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(300), 6, offsetToMs(300) - 1, 5},
			},
		}, {
			name:        "ingesting multiple samples per time bucket per series",
			aggInterval: time.Minute,
			aggDelay:    time.Minute,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(135), 3, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(150), 4, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(165), 5, offsetToMs(60) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(179), 6, offsetToMs(60) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(205), 7, offsetToMs(120) - 1, math.NaN()},
				// Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(240), 8, offsetToMs(180) - 1, 6},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(241), 9, offsetToMs(180) - 1, math.NaN()},
			},
		}, {
			name:        "skipping time buckets",
			aggInterval: time.Minute,
			aggDelay:    time.Minute,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(135), 3, offsetToMs(60) - 1, math.NaN()},
				// 5x Next time bucket.
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(435), 4, offsetToMs(180) - 1, 3},
			},
		}, {
			name:        "out of order data",
			aggInterval: time.Minute,
			aggDelay:    0,
			ingestCalls: []ingestCall{
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(135), 3, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(195), 4, offsetToMs(180) - 1, 3},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(136), 3, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(255), 8, offsetToMs(240) - 1, 4},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(137), 4, offsetToMs(120) - 1, math.NaN()},
				{user, "test_metric{label1=\"value1\"}", "test_metric{label1=\"value1\",label2=\"value1\"}", offsetToMs(315), 9, offsetToMs(300) - 1, 8},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			aggs := newUserAggregations(tc.aggInterval, tc.aggDelay, newAggregationMetrics(nil))

			for callIdx, call := range tc.ingestCalls {
				t.Run("call "+strconv.Itoa(callIdx), func(t *testing.T) {
					gotAggregate := aggs.ingest(call.user, call.aggregated, call.raw, mimirpb.Sample{TimestampMs: call.timestamp, Value: call.value})
					require.Equal(t, call.expectedTimestamp, gotAggregate.TimestampMs, "unexpected timestamp")
					if math.IsNaN(call.expectedValue) {
						require.True(t, math.IsNaN(gotAggregate.Value), fmt.Sprintf("expected value to be NaN, but was %f", gotAggregate.Value))
					} else {
						require.Equal(t, call.expectedValue, gotAggregate.Value, "unexpected value")
					}
				})
			}
		})
	}
}

func TestTimeBucketsIncreaseToTs(t *testing.T) {
	interval := int64(60)
	type testCase struct {
		name           string
		bucketCount    int
		ingestSamples  []mimirpb.Sample
		toTs           int64
		expectTs       int64
		expectIncrease float64
	}
	testCases := []testCase{
		{
			name:        "ingest one sample, get its value as increase",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 100, Value: 3},
			},
			toTs:           200,
			expectTs:       100,
			expectIncrease: 3,
		}, {
			name:        "ingest two samples into consecutive buckets, get increase between them",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 3},
				{TimestampMs: 170, Value: 5},
			},
			toTs:           180,
			expectTs:       170,
			expectIncrease: 2,
		}, {
			name:        "ingest two samples into same bucket, get higher as increase",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 3},
				{TimestampMs: 111, Value: 5},
			},
			toTs:           180,
			expectTs:       111,
			expectIncrease: 5,
		}, {
			name:        "ingest two samples into two buckets with gap between, get increase between them",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 3},
				{TimestampMs: 240, Value: 7},
			},
			toTs:           250,
			expectTs:       240,
			expectIncrease: 4,
		}, {
			name:        "ingest two samples into two buckets, get up to timestamp of latter",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 2},
				{TimestampMs: 120, Value: 5},
			},
			toTs:           120,
			expectTs:       120,
			expectIncrease: 3,
		}, {
			name:        "ingest two samples into two buckets with reset, get second value as increase",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 5},
				{TimestampMs: 120, Value: 2},
			},
			toTs:           120,
			expectTs:       120,
			expectIncrease: 2,
		}, {
			name:        "ingest four samples into four buckets, get increase between last two",
			bucketCount: 4,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 5},
				{TimestampMs: 170, Value: 6},
				{TimestampMs: 230, Value: 8},
				{TimestampMs: 290, Value: 11},
			},
			toTs:           300,
			expectTs:       290,
			expectIncrease: 3,
		}, {
			name:        "ingest four samples into two buckets, get increase between last two",
			bucketCount: 2,
			ingestSamples: []mimirpb.Sample{
				{TimestampMs: 110, Value: 5},
				{TimestampMs: 170, Value: 6},
				{TimestampMs: 230, Value: 8},
				{TimestampMs: 290, Value: 11},
			},
			toTs:           300,
			expectTs:       290,
			expectIncrease: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTimeBuckets(tc.bucketCount)
			for _, ingest := range tc.ingestSamples {
				tb.ingest(ingest, interval)
			}
			gotTs, gotIncrease := tb.increaseToTs(tc.toTs)
			require.Equal(t, tc.expectTs, gotTs)
			require.Equal(t, tc.expectIncrease, gotIncrease)
		})
	}
}

func offsetToMs(offset int64) int64 {
	return offset * time.Second.Milliseconds()
}
