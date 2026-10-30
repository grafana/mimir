// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestDifferenceDiagnosticFloatAndMissingSample(t *testing.T) {
	primary := streamFrames(t, floatChunk(t, []int64{100, 200}, []float64{1, 2}))
	shadow := streamFrames(t, floatChunk(t, []int64{100, 200}, []float64{1, 9}))
	query := &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 200}
	a, err := querySamples(primary, query, testConfig(), time.Now())
	require.NoError(t, err)
	b, err := querySamples(shadow, query, testConfig(), time.Now())
	require.NoError(t, err)
	d := differenceDiagnostic(firstSampleDifference(a, b), primary, shadow)
	require.Empty(t, d.Error)
	require.Equal(t, "sample_value", d.Reason)
	require.Equal(t, int64(200), d.Timestamp)
	require.Equal(t, "2", d.Primary.Float.Value)
	require.Equal(t, "9", d.Shadow.Float.Value)
	require.Equal(t, "0x4000000000000000", d.Primary.Float.Bits)
	require.Equal(t, "0x4022000000000000", d.Shadow.Float.Bits)
	d = differenceDiagnostic(firstSampleDifference(a, nil), primary, nil)
	require.Equal(t, "missing_from_shadow", d.Reason)
	require.Equal(t, int64(100), d.Timestamp)
	require.Nil(t, d.Shadow)
}

func TestDifferenceDiagnosticHistogram(t *testing.T) {
	frames := func(value float64) []frame {
		c := chunkenc.NewFloatHistogramChunk()
		a, err := c.Appender()
		require.NoError(t, err)
		h := &histogram.FloatHistogram{Schema: 0, Count: value, Sum: value, ZeroCount: value, ZeroThreshold: 0.001}
		next, _, _, err := a.AppendFloatHistogram(nil, 0, 100, h, false)
		require.NoError(t, err)
		require.Nil(t, next)
		return streamFrames(t, client.Chunk{Encoding: 6, Data: c.Bytes(), StartTimestampMs: 100, EndTimestampMs: 100})
	}
	primary, shadow := frames(1), frames(2)
	query := &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100}
	a, err := querySamples(primary, query, testConfig(), time.Now())
	require.NoError(t, err)
	b, err := querySamples(shadow, query, testConfig(), time.Now())
	require.NoError(t, err)
	d := differenceDiagnostic(firstSampleDifference(a, b), primary, shadow)
	require.Empty(t, d.Error)
	require.Equal(t, "1", d.Primary.Histogram.Count.Value)
	require.Equal(t, "2", d.Shadow.Histogram.Count.Value)
	require.Contains(t, d.HistogramFields, "Count")
	require.Contains(t, d.HistogramFields, "Sum")
}

func TestDiagnosticBoundsAndSpecialFloats(t *testing.T) {
	h := &histogram.FloatHistogram{PositiveSpans: make([]histogram.Span, 100), PositiveBuckets: make([]float64, 100)}
	d := histogramDiagnostic(h, histogram.UnknownCounterReset)
	require.Len(t, d.PositiveBuckets, diagnosticBuckets)
	require.Len(t, d.PositiveSpans, diagnosticBuckets)
	require.True(t, d.Truncated)
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Copysign(0, -1)} {
		_, err := json.Marshal(floatDiagnostic(v))
		require.NoError(t, err)
	}
	budget := 256
	fields := appendDiagnosticJSON(nil, "query", map[string]string{"matchers": strings.Repeat("\"", 10000)}, &budget)
	encoded := fields[1].(string)
	require.LessOrEqual(t, len(encoded), 256)
	require.GreaterOrEqual(t, budget, 0)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(encoded), &decoded))
	require.Equal(t, true, decoded["truncated"])
	require.Equal(t, []any{"query_truncated", true}, fields[2:])
}

func TestHistogramBucketDifferenceBeyondLoggedPrefix(t *testing.T) {
	frames := func(changed bool) []frame {
		buckets := make([]float64, 20)
		for i := range buckets {
			buckets[i] = 1
		}
		h := &histogram.FloatHistogram{CounterResetHint: histogram.GaugeType, Schema: 0, Count: 20, Sum: 100, ZeroThreshold: 0.001, PositiveSpans: []histogram.Span{{Length: 20}}, PositiveBuckets: buckets}
		if changed {
			h.PositiveBuckets[10] = 2
			h.Count = 21
			h.Sum = 101
		}
		c := chunkenc.NewFloatHistogramChunk()
		a, err := c.Appender()
		require.NoError(t, err)
		next, _, _, err := a.AppendFloatHistogram(nil, 0, 100, h, false)
		require.NoError(t, err)
		require.Nil(t, next)
		return streamFrames(t, client.Chunk{Encoding: 6, Data: c.Bytes(), StartTimestampMs: 100, EndTimestampMs: 100})
	}
	primary, shadow := frames(false), frames(true)
	q := &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100}
	a, err := querySamples(primary, q, testConfig(), time.Now())
	require.NoError(t, err)
	b, err := querySamples(shadow, q, testConfig(), time.Now())
	require.NoError(t, err)
	d := differenceDiagnostic(firstSampleDifference(a, b), primary, shadow)
	require.Empty(t, d.Error)
	require.True(t, d.Primary.Histogram.Truncated)
	bucket := d.HistogramBucketDifference.(map[string]any)
	require.Equal(t, "PositiveBuckets", bucket["field"])
	require.Equal(t, 10, bucket["index"])
	require.Equal(t, "1", bucket["primary"].(*diagnosticFloat).Value)
	require.Equal(t, "2", bucket["shadow"].(*diagnosticFloat).Value)
}
