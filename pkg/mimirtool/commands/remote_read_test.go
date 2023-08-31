// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
)

func TestTimeSeriesIterator(t *testing.T) {

	for _, tc := range []struct {
		name               string
		timeSeries         []*prompb.TimeSeries
		expectedLabels     []string
		expectedTimestamps []int64
		expectedValues     []float64
	}{
		{
			name: "empty time series",
		},
		{
			name: "simple",
			timeSeries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "up",
						},
					},
					Samples: []prompb.Sample{
						{
							Value:     1.23,
							Timestamp: 1000,
						},
						{
							Value:     1.24,
							Timestamp: 2000,
						},
					},
				},
			},
			expectedLabels: []string{
				`{__name__="up"}`,
				`{__name__="up"}`,
			},
			expectedTimestamps: []int64{
				1000,
				2000,
			},
			expectedValues: []float64{
				1.23,
				1.24,
			},
		},
		{
			name: "edge-cases",
			timeSeries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{},
					Samples: []prompb.Sample{
						{
							Value:     1.23,
							Timestamp: 1000,
						},
						{
							Value:     1.24,
							Timestamp: 2000,
						},
					},
				},
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "upper",
						},
					},
					Samples: []prompb.Sample{
						{
							Value:     2.34,
							Timestamp: 1050,
						},
					},
				},
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "uppest",
						},
					},
					Samples: []prompb.Sample{},
				},
			},
			expectedLabels: []string{
				`{}`,
				`{}`,
				`{__name__="upper"}`,
			},
			expectedTimestamps: []int64{
				1000,
				2000,
				1050,
			},
			expectedValues: []float64{
				1.23,
				1.24,
				2.34,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			iter := newTimeSeriesIterator(tc.timeSeries)

			var (
				labels     []string
				timestamps []int64
				values     []float64
			)

			for {
				err := iter.Next()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					assert.NoError(t, err, "unexpected error")
					break
				}

				labels = append(labels, iter.Labels().String())
				ts, v := iter.Sample()
				timestamps = append(timestamps, ts)
				values = append(values, v)
			}

			assert.Equal(t, tc.expectedLabels, labels)
			assert.Equal(t, tc.expectedTimestamps, timestamps)
			assert.Equal(t, tc.expectedValues, values)
		})
	}

}

// TestEarlyCommit writes samples of many series that don't fit into the same
// append commit. It makes sure that batching the samples into many commits
// doesn't cause the appends to advance the head block too far and make future
// appends invalid.
func TestEarlyCommit(t *testing.T) {
	maxSamplesPerBlock := 1000
	series := 100
	samples := 140

	start := int64(time.Date(2023, 8, 30, 11, 42, 17, 0, time.UTC).UnixNano())
	inc := int64(time.Minute / time.Millisecond)
	end := start + (inc * int64(samples))
	ts := make([]*prompb.TimeSeries, series)
	for i := 0; i < series; i++ {
		s := &prompb.TimeSeries{
			Labels: []prompb.Label{
				{
					Name:  "__name__",
					Value: fmt.Sprintf("metric_%d", i),
				},
			},
			Samples: make([]prompb.Sample, samples),
		}
		for j := 0; j < samples; j++ {
			s.Samples[j] = prompb.Sample{
				Value:     float64(j),
				Timestamp: start + (inc * int64(j)),
			}
		}
		ts[i] = s
	}
	iterator := func() backfill.Iterator {
		return newTimeSeriesIterator(ts)
	}
	err := backfill.CreateBlocks(iterator, start, end, maxSamplesPerBlock, t.TempDir(), true, io.Discard)
	assert.NoError(t, err)
}
