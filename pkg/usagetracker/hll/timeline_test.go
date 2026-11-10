// SPDX-License-Identifier: AGPL-3.0-only

package hll

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimeline(t *testing.T) {
	const p = 8
	const minutes = 20
	const start = int64(29_000_000) // Some unix minute in 2025.

	// sketchOf returns a sketch with the values [from, to).
	sketchOf := func(from, to int) Sketch {
		s := NewSketch(p)
		for v := from; v < to; v++ {
			s.Insert(Hash(uint64(v), 0))
		}
		return s
	}
	estimate := func(tl *Timeline, now int64) float64 {
		return tl.Cleanup(now, NewSketch(p))
	}
	minutesWithSketches := func(tl *Timeline, now int64) (got []int64) {
		tl.Sketches(now, func(minute int64, _ Sketch) { got = append(got, minute) })
		return got
	}

	t.Run("values are counted once over the window", func(t *testing.T) {
		tl := NewTimeline(minutes)
		for m := start; m < start+minutes; m++ {
			// The same 100 values every minute, and both merges of a minute go into the same sketch.
			tl.Merge(m, m, sketchOf(0, 50))
			tl.Merge(m, m, sketchOf(50, 100))
		}
		require.Equal(t, sketchOf(0, 100).Estimate(), estimate(tl, start+minutes-1))
	})

	t.Run("minutes out of the window are forgotten", func(t *testing.T) {
		tl := NewTimeline(minutes)
		tl.Merge(start, start, sketchOf(0, 100))
		tl.Merge(start+1, start+1, sketchOf(100, 200))

		require.Equal(t, sketchOf(0, 200).Estimate(), estimate(tl, start+minutes-1))
		// At start+minutes, the first minute is out of the window.
		require.Equal(t, sketchOf(100, 200).Estimate(), estimate(tl, start+minutes))
		// And Cleanup dropped its sketch.
		require.Equal(t, []int64{start + 1}, minutesWithSketches(tl, start+minutes))
		require.Zero(t, estimate(tl, start+minutes+1))
	})

	t.Run("a new minute replaces the minute of its slot that is out of the window", func(t *testing.T) {
		tl := NewTimeline(minutes)
		tl.Merge(start, start, sketchOf(0, 100))
		tl.Merge(start+minutes, start+minutes, sketchOf(100, 110))
		require.Equal(t, sketchOf(100, 110).Estimate(), estimate(tl, start+minutes))
	})

	t.Run("minutes out of the window are not merged", func(t *testing.T) {
		tl := NewTimeline(minutes)
		now := start + 100
		tl.Merge(now, now, sketchOf(0, 100))
		tl.Merge(now-minutes, now, sketchOf(100, 200)) // Too old.
		tl.Merge(now+1, now, sketchOf(200, 300))       // From the future: it would replace a minute in the window.
		tl.Merge(now+minutes, now, sketchOf(300, 400)) // From the future, same slot as now.
		require.Equal(t, []int64{now}, minutesWithSketches(tl, now))
		require.Equal(t, sketchOf(0, 100).Estimate(), estimate(tl, now))
	})

	t.Run("an older minute in the window doesn't replace a newer one", func(t *testing.T) {
		tl := NewTimeline(minutes)
		now := start + 100
		tl.Merge(now, now, sketchOf(0, 100))
		// The clock went back a whole window: now-minutes is in the window again, and its slot holds now.
		tl.Merge(now-minutes, now-1, sketchOf(100, 200))
		require.Equal(t, sketchOf(0, 100).Estimate(), estimate(tl, now))
	})

	t.Run("merge keeps a copy of the sketch", func(t *testing.T) {
		tl := NewTimeline(minutes)
		s := sketchOf(0, 100)
		tl.Merge(start, start, s)
		clear(s)
		require.Equal(t, sketchOf(0, 100).Estimate(), estimate(tl, start))
	})
}
