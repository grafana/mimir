// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"math/rand"
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

// bruteForceBucketCounts counts, for each bucket, the series with a chunk
// that overlaps it, straight from the definition.
func bruteForceBucketCounts(minT, step, window int64, buckets int, series [][]ChunkRange) []int64 {
	if window == 0 {
		window = step
	}
	counts := make([]int64, buckets)
	for _, chunks := range series {
		for i := range buckets {
			start := minT + int64(i)*step
			for _, r := range chunks {
				if r.MaxTime >= start && r.MinTime < start+window {
					counts[i]++
					break
				}
			}
		}
	}
	return counts
}

// randomChunkSeries returns n series of a few chunks each, with times around
// [minT, maxT) so some fall outside the window.
func randomChunkSeries(rnd *rand.Rand, n int, minT, maxT int64) [][]ChunkRange {
	span := maxT - minT
	series := make([][]ChunkRange, n)
	for i := range series {
		for range 1 + rnd.Intn(3) {
			from := minT - span/10 + rnd.Int63n(span+span/5)
			series[i] = append(series[i], ChunkRange{MinTime: from, MaxTime: from + rnd.Int63n(span/4)})
		}
	}
	return series
}

// With many buckets, the counter agrees with the definition of a bucket
// count for every bucket.
func TestSeriesCounter_ManyBucketsAgainstBruteForce(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	const minT, step = 1000, 10
	for _, window := range []int64{0, 3 * step} {
		buckets := MaxCounterBuckets
		w := max(window, step)
		maxT := minT + w + int64(buckets-1)*step
		c, err := NewSeriesCounter(CounterOptions{MinT: minT, MaxT: maxT, Step: step, Window: window})
		require.NoError(t, err)
		require.Equal(t, buckets, c.buckets)

		series := randomChunkSeries(rnd, 200, minT, maxT)
		lset := labels.FromStrings("__name__", "a")
		for _, chunks := range series {
			c.Add(lset, chunks)
		}
		groups, err := c.Groups()
		require.NoError(t, err)
		require.Len(t, groups, 1, "window %d", window)
		assert.Equal(t, bruteForceBucketCounts(minT, step, window, buckets, series), groups[0].Counts, "window %d", window)
	}
}

func TestBucketMarks(t *testing.T) {
	m := NewBucketMarks(5)
	counts := make([]int64, 5)
	m.Mark(1, 3)
	m.Mark(2, 4)
	m.Mark(4, 1)
	m.Flush(counts)
	assert.Equal(t, []int64{0, 1, 1, 1, 1}, counts, "once per bucket, and an empty span marks none")
	m.Mark(0, 0)
	m.Flush(counts)
	assert.Equal(t, []int64{1, 1, 1, 1, 1}, counts, "a flush clears the marks")
}

// One chunk per series over the most buckets: the per-series work is the
// buckets the chunk touches, not all of them.
func BenchmarkSeriesCounter_AddManyBuckets(b *testing.B) {
	const minT, step = 0, 1
	c, err := NewSeriesCounter(CounterOptions{MinT: minT, MaxT: minT + MaxCounterBuckets*step, Step: step})
	require.NoError(b, err)
	lset := labels.FromStrings("__name__", "a")
	chunks := []ChunkRange{{MinTime: 700, MaxTime: 702}}
	b.ReportAllocs()
	for b.Loop() {
		c.Add(lset, chunks)
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
