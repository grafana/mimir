// SPDX-License-Identifier: AGPL-3.0-only

package hll

import (
	"iter"
	"slices"
	"sync"
)

// Timeline holds one sketch per unix minute for the last few minutes, so that it can estimate the distinct values
// of a sliding window and forget the old ones. The sketches are in a ring indexed by unix minute modulo the number
// of minutes, and each one remembers its minute, so a sketch of a minute that is out of the window is never used.
// It is safe for concurrent use.
type Timeline struct {
	mtx sync.Mutex

	// +checklocks:mtx
	slots []timelineSlot
}

type timelineSlot struct {
	// minute is the unix minute of the values in sketch. It's only meaningful if sketch is not nil.
	minute int64
	sketch Sketch
}

// NewTimeline returns an empty timeline with a window of the given number of minutes.
func NewTimeline(minutes int) *Timeline {
	return &Timeline{slots: make([]timelineSlot, minutes)}
}

// Merge merges s into the sketch of minute, which is a unix minute, in the window that ends at now.
// The timeline keeps a copy of s, and s must have the same precision as the sketches merged before.
//
// Merge does nothing if minute is not in the window. That includes a minute after now: its slot holds a minute
// that is still in the window, and replacing it would lose values.
func (t *Timeline) Merge(minute, now int64, s Sketch) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	if !t.inWindow(minute, now) {
		return
	}
	slot := &t.slots[minute%int64(len(t.slots))]
	switch {
	case slot.sketch == nil:
		slot.minute, slot.sketch = minute, slices.Clone(s)
	case slot.minute < minute:
		// The slot holds a minute before the window, which is never used again: reuse its sketch.
		slot.minute = minute
		copy(slot.sketch, s)
	case slot.minute == minute:
		slot.sketch.Merge(s)
	default:
		// The slot holds a minute after the window of minute, so the clock went back. Drop s.
	}
}

// Cleanup drops sketches older than the window ending at now or later than now+1.
func (t *Timeline) Cleanup(now int64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for i, slot := range t.slots {
		// Keep the next minute if a merge crossed the minute boundary after the caller read now.
		// Dropping later minutes frees slots for new writes after the clock goes back.
		if slot.sketch != nil && slot.minute != now+1 && !t.inWindow(slot.minute, now) {
			t.slots[i] = timelineSlot{}
		}
	}
}

// Estimate returns the estimated number of distinct values merged into the minutes of the window ending at now.
// It merges the sketches of the window into scratch with the timeline locked, and estimates after the unlock.
// scratch is overwritten, and it must have the same precision as the merged sketches.
func (t *Timeline) Estimate(now int64, scratch Sketch) float64 {
	clear(scratch)
	for _, s := range t.Sketches(now) {
		scratch.Merge(s)
	}
	return scratch.Estimate()
}

// Sketches iterates over the minutes in the window ending at now that have a sketch, and their sketches.
// The timeline is locked during the loop, and the loop must not keep a sketch after its iteration.
func (t *Timeline) Sketches(now int64) iter.Seq2[int64, Sketch] {
	return func(yield func(int64, Sketch) bool) {
		t.mtx.Lock()
		defer t.mtx.Unlock()
		for _, slot := range t.slots {
			if slot.sketch != nil && t.inWindow(slot.minute, now) && !yield(slot.minute, slot.sketch) {
				return
			}
		}
	}
}

// Empty returns whether no sketch is in the window ending at now or after it.
func (t *Timeline) Empty(now int64) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for _, slot := range t.slots {
		if slot.sketch != nil && slot.minute > now-int64(len(t.slots)) {
			return false
		}
	}
	return true
}

// +checklocks:t.mtx
func (t *Timeline) inWindow(minute, now int64) bool {
	return minute <= now && minute > now-int64(len(t.slots))
}
