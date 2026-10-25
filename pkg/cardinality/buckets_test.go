// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuckets(t *testing.T) {
	for name, tc := range map[string]struct {
		minT, maxT, step, window int64
		want                     int
		err                      string
	}{
		"window 0 is the step":            {minT: 0, maxT: 100, step: 10, want: 10},
		"window equal to the step":        {minT: 0, maxT: 100, step: 10, window: 10, want: 10},
		"window wider than the step":      {minT: 0, maxT: 100, step: 10, window: 30, want: 8},
		"one bucket as wide as the range": {minT: 0, maxT: 100, step: 10, window: 100, want: 1},
		"window shorter than the step":    {minT: 0, maxT: 100, step: 10, window: 5, err: "shorter than step"},
		"window longer than the range":    {minT: 0, maxT: 100, step: 10, window: 110, err: "longer than"},
		"step doesn't divide":             {minT: 0, maxT: 100, step: 10, window: 25, err: "doesn't divide"},
		"negative window":                 {minT: 0, maxT: 100, step: 10, window: -1, err: "negative"},
		"no step":                         {minT: 0, maxT: 100, err: "step must be positive"},
		"too many buckets":                {minT: 0, maxT: 2 * MaxCounterBuckets, step: 1, window: 2, err: "more than"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Buckets(tc.minT, tc.maxT, tc.step, tc.window)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestBucketSpan checks BucketSpan against the definition: bucket i covers
// [minT+i*step, minT+i*step+window), and an inclusive range overlaps it when
// it starts before the bucket ends and ends at or after the bucket starts.
func TestBucketSpan(t *testing.T) {
	const minT, maxT = 1000, 1120
	for _, step := range []int64{1, 7, 10, 40} {
		for _, window := range []int64{0, step, 2 * step, 3*step + step, maxT - minT} {
			if window != 0 && (window > maxT-minT || (maxT-minT-window)%step != 0) {
				continue
			}
			n, err := Buckets(minT, maxT, step, window)
			if err != nil {
				continue
			}
			w := window
			if w == 0 {
				w = step
			}
			for from := int64(minT - 50); from < maxT+50; from += 3 {
				for _, length := range []int64{0, 1, 5, 40, 200} {
					through := from + length
					first, last := BucketSpan(minT, step, window, n, from, through)
					for i := int64(0); i < int64(n); i++ {
						start := minT + i*step
						overlaps := through >= start && from < start+w
						assert.Equal(t, overlaps, i >= first && i <= last, "step %d, window %d, [%d, %d], bucket %d", step, window, from, through, i)
					}
				}
			}
		}
	}
}

func TestSeriesCounter_Window(t *testing.T) {
	a := labels.FromStrings("__name__", "a")
	b := labels.FromStrings("__name__", "b")

	// A bucket every 10 ms, each 30 ms wide, over [0, 100): buckets end at 30, 40, ..., 100.
	c, err := NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 100, Step: 10, Window: 30})
	require.NoError(t, err)
	assert.True(t, c.Add(a, []ChunkRange{{MinTime: 25, MaxTime: 25}}))
	assert.True(t, c.Add(b, []ChunkRange{{MinTime: 0, MaxTime: 5}, {MinTime: 95, MaxTime: 99}}))
	groups, err := c.Groups()
	require.NoError(t, err)
	require.Len(t, groups, 2)
	// A sample at 25 is in the 30 ms before the ends at 30, 40 and 50.
	assert.Equal(t, []int64{1, 1, 1, 0, 0, 0, 0, 0}, groups[0].Counts)
	assert.Equal(t, []int64{1, 0, 0, 0, 0, 0, 0, 1}, groups[1].Counts, "a series counts once per bucket")

	_, err = NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 100, Window: 30})
	require.ErrorContains(t, err, "a window needs a step")
	_, err = NewSeriesCounter(CounterOptions{MinT: 0, MaxT: 100, Step: 10, Window: 5})
	require.ErrorContains(t, err, "shorter than step")
}
