package commands

import (
	"io"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
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
				if err == io.EOF {
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
