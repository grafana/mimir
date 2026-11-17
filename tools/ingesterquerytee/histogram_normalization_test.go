// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestHistogramNormalizationPreservesIterator(t *testing.T) {
	for _, floating := range []bool{false, true} {
		name := "integer"
		if floating {
			name = "float"
		}
		t.Run(name, func(t *testing.T) {
			makeChunk := func(timestamps ...int64) client.Chunk {
				var c chunkenc.Chunk = chunkenc.NewHistogramChunk()
				encoding := int32(5)
				if floating {
					c = chunkenc.NewFloatHistogramChunk()
					encoding = 6
				}
				a, err := c.Appender()
				require.NoError(t, err)
				for _, ts := range timestamps {
					h := &histogram.Histogram{Schema: 2, Count: 10, Sum: 10, ZeroThreshold: 0.001,
						PositiveSpans: []histogram.Span{{Offset: 1, Length: 3}}, PositiveBuckets: []int64{5, -5, 5}}
					var next chunkenc.Chunk
					if floating {
						next, _, a, err = a.AppendFloatHistogram(nil, 0, ts, h.ToFloat(nil), false)
					} else {
						next, _, a, err = a.AppendHistogram(nil, 0, ts, h, false)
					}
					require.NoError(t, err)
					require.Nil(t, next)
				}
				return client.Chunk{Encoding: encoding, Data: c.Bytes(), StartTimestampMs: timestamps[0], EndTimestampMs: timestamps[len(timestamps)-1]}
			}
			primary := streamFrames(t, makeChunk(100), makeChunk(200), makeChunk(300))
			shadow := streamFrames(t, makeChunk(100, 200, 300))
			q := &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 300}
			require.NoError(t, compareResponses("QueryStream", wire(t, q), primary, shadow, testConfig(), time.Now()))
			require.NoError(t, compareResponses("QueryStream", wire(t, q), shadow, primary, testConfig(), time.Now()))
		})
	}
}
