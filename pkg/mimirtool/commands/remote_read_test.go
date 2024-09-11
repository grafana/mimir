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
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
)

type mockSeriesSet struct {
	idx    int
	series []storage.Series

	warnings annotations.Annotations
	err      error
}

func NewMockSeriesSet(series ...storage.Series) storage.SeriesSet {
	return &mockSeriesSet{
		idx:    -1,
		series: series,
	}
}

func (m *mockSeriesSet) Next() bool {
	if m.err != nil {
		return false
	}
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockSeriesSet) At() storage.Series { return m.series[m.idx] }

func (m *mockSeriesSet) Err() error { return m.err }

func (m *mockSeriesSet) Warnings() annotations.Annotations { return m.warnings }

func TestTimeSeriesIterator(t *testing.T) {

	for _, tc := range []struct {
		name               string
		seriesSet          storage.SeriesSet
		expectedLabels     []string
		expectedTimestamps []int64
		expectedValues     []float64
	}{
		{
			name:      "empty time series",
			seriesSet: storage.EmptySeriesSet(),
		},
		{
			name:      "simple",
			seriesSet: NewMockSeriesSet(storage.MockSeries([]int64{1000, 2000}, []float64{1.23, 1.24}, []string{"__name__", "up"})),
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
			seriesSet: NewMockSeriesSet(
				storage.MockSeries([]int64{1000, 2000}, []float64{1.23, 1.24}, []string{}),
				storage.MockSeries([]int64{1050}, []float64{2.34}, []string{"__name__", "upper"}),
				storage.MockSeries([]int64{}, []float64{}, []string{"__name__", "uppest"}),
			),
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

			iter := newTimeSeriesIterator(tc.seriesSet)

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
				ts, v, _, _ := iter.Sample()
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
	seriesCount := 100
	samplesCount := 140

	start := int64(time.Date(2023, 8, 30, 11, 42, 17, 0, time.UTC).UnixNano())
	inc := int64(time.Minute / time.Millisecond)
	end := start + (inc * int64(samplesCount))

	samples := make([]float64, samplesCount)
	for i := 0; i < samplesCount; i++ {
		samples[i] = float64(i)
	}
	timestamps := make([]int64, samplesCount)
	for i := 0; i < samplesCount; i++ {
		timestamps[i] = start + (inc * int64(i))
	}

	series := make([]storage.Series, seriesCount)
	for i := 0; i < seriesCount; i++ {
		series[i] = storage.MockSeries(timestamps, samples, []string{"__name__", fmt.Sprintf("metric_%d", i)})
	}

	iterator := func() backfill.Iterator {
		return newTimeSeriesIterator(NewMockSeriesSet(series...))
	}
	err := backfill.CreateBlocks(iterator, start, end, maxSamplesPerBlock, t.TempDir(), true, io.Discard)
	assert.NoError(t, err)
}
