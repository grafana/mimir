// SPDX-License-Identifier: AGPL-3.0-only

//go:build !nopools

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimeseriesFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := TimeseriesFromPool()
		second := TimeseriesFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		ts := TimeseriesFromPool()
		ts.Labels = []LabelAdapter{{Name: "foo", Value: "bar"}}
		ts.Samples = []Sample{{Value: 1, TimestampMs: 2}}
		ReuseTimeseries(ts)

		reused := TimeseriesFromPool()
		assert.Len(t, reused.Labels, 0)
		assert.Len(t, reused.Samples, 0)
	})

	// Test that TimeseriesFromPool panics when it receives a dirty object from the pool.
	dirtyPoolTests := []struct {
		name    string
		dirtyTS *TimeSeries
	}{
		{"labels", &TimeSeries{Labels: []LabelAdapter{{Name: "foo", Value: "bar"}}}},
		{"samples", &TimeSeries{Samples: []Sample{{Value: 1, TimestampMs: 2}}}},
		{"histograms", &TimeSeries{Histograms: []Histogram{{Sum: 1.0}}}},
		{"exemplars", &TimeSeries{Exemplars: []Exemplar{{Value: 1, TimestampMs: 2}}}},
		{"CreatedTimestamp", &TimeSeries{CreatedTimestamp: 1234567890}},
		{"SkipUnmarshalingExemplars", &TimeSeries{SkipUnmarshalingExemplars: true}},
	}
	for _, tc := range dirtyPoolTests {
		t.Run("panics if pool returns dirty TimeSeries with "+tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				// Drain the pool to ensure isolation. sync.Pool.Get() never returns nil
				// (it calls New if empty), so we just drain a fixed count.
				for range 1000 {
					timeSeriesPool.Get()
				}
			})
			// Flood the pool with dirty objects because sync.Pool doesn't guarantee returning
			// the same object that was just put in.
			for range 100 {
				timeSeriesPool.Put(tc.dirtyTS)
			}

			assert.Panics(t, func() {
				TimeseriesFromPool()
			})
		})
	}
}
