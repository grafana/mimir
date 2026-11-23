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
			s.insert(hash(uint64(v), 0), p)
		}
		return s
	}
	estimate := func(tl *Timeline, now int64) float64 {
		return tl.Estimate(now, NewSketch(p))
	}
	minutesWithSketches := func(tl *Timeline, now int64) (got []int64) {
		for minute := range tl.Sketches(now) {
			got = append(got, minute)
		}
		return got
	}

	t.Run("an estimate with an empty scratch doesn't hang", func(t *testing.T) {
		require.Zero(t, NewTimeline(minutes).Estimate(start, nil))
	})

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
		require.Zero(t, estimate(tl, start+minutes+1))

		// Cleanup drops the sketches.
		tl.Merge(start+minutes+1, start+minutes+1, sketchOf(0, 100))
		tl.Cleanup(start + 2*minutes + 1)
		tl.mtx.Lock()
		defer tl.mtx.Unlock()
		for _, slot := range tl.slots {
			require.Nil(t, slot.sketch)
		}
	})

	t.Run("merges between estimates are counted, including the late ones", func(t *testing.T) {
		tl := NewTimeline(minutes)
		tl.Merge(start, start, sketchOf(0, 100))
		tl.Merge(start+1, start+1, sketchOf(100, 200))
		require.Equal(t, sketchOf(0, 200).Estimate(), estimate(tl, start+1))

		// The current minute.
		tl.Merge(start+1, start+1, sketchOf(200, 300))
		require.Equal(t, sketchOf(0, 300).Estimate(), estimate(tl, start+1))

		// A minute before now: a call that started in the previous minute, or a snapshot.
		tl.Merge(start, start+1, sketchOf(300, 400))
		require.Equal(t, sketchOf(0, 400).Estimate(), estimate(tl, start+1))

		// The clock is ahead of the estimates: the minute after now is not counted until now moves.
		tl.Merge(start+2, start+2, sketchOf(400, 500))
		require.Equal(t, sketchOf(0, 400).Estimate(), estimate(tl, start+1))
		require.Equal(t, sketchOf(0, 500).Estimate(), estimate(tl, start+2))

		// When the window moves, the minutes out of the window are not counted.
		expected := sketchOf(100, 300)
		expected.Merge(sketchOf(400, 500))
		require.Equal(t, expected.Estimate(), estimate(tl, start+minutes))
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

	t.Run("a cleanup for an earlier minute keeps the next minute", func(t *testing.T) {
		tl := NewTimeline(minutes)
		now := start + 100
		// A write crosses the minute boundary after cleanup reads the clock.
		tl.Merge(now+1, now+1, sketchOf(0, 100))
		// A sketch farther ahead models a backward clock jump.
		tl.Merge(now+2, now+2, sketchOf(100, 200))
		tl.Cleanup(now)
		require.Equal(t, []int64{now + 1}, minutesWithSketches(tl, now+2))
	})

	t.Run("a cleanup for an earlier minute keeps the next minute of a one-minute window", func(t *testing.T) {
		// An idle timeout of a minute or less has one slot, so the next minute takes the slot of now.
		tl := NewTimeline(1)
		now := start + 100
		tl.Merge(now+1, now+1, sketchOf(0, 100))
		tl.Cleanup(now)
		require.Equal(t, []int64{now + 1}, minutesWithSketches(tl, now+1))
	})

	t.Run("a timeline is empty when no sketch is in the window or after it", func(t *testing.T) {
		tl := NewTimeline(minutes)
		tl.Merge(start, start, sketchOf(0, 100))
		// A cleanup that read the clock before the minute of the sketch must not delete its tenant.
		require.False(t, tl.Empty(start-1))
		require.False(t, tl.Empty(start))
		// No cleanup dropped the sketch of start yet, but it's out of this window.
		require.True(t, tl.Empty(start+minutes))
	})

	t.Run("merge keeps a copy of the sketch", func(t *testing.T) {
		tl := NewTimeline(minutes)
		s := sketchOf(0, 100)
		tl.Merge(start, start, s)
		clear(s)
		require.Equal(t, sketchOf(0, 100).Estimate(), estimate(tl, start))
	})
}
