// SPDX-License-Identifier: AGPL-3.0-only

package hll

import (
	"slices"
	"sync"
)

// Timeline holds one sketch per unix minute for the last few minutes, so that it can estimate the distinct values
// of a sliding window and forget the old ones. The sketches are in a ring indexed by unix minute modulo the number
// of minutes, and each one remembers its minute, so a sketch of a minute that is out of the window is never used.
// It is safe for concurrent use.
type Timeline struct {
	mtx   sync.Mutex
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
	if !t.inWindow(minute, now) {
		return
	}

	t.mtx.Lock()
	defer t.mtx.Unlock()
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
	}
	// Otherwise, the slot holds a minute after the window of minute, so the clock went back. Drop s.
}

// Cleanup drops the sketches of the minutes that are not in the window ending at now, and returns the estimated
// number of distinct values merged into the minutes of the window.
// scratch is overwritten, and it must have the same precision as the merged sketches.
func (t *Timeline) Cleanup(now int64, scratch Sketch) float64 {
	clear(scratch)
	t.mtx.Lock()
	for i := range t.slots {
		slot := &t.slots[i]
		if slot.sketch == nil {
			continue
		}
		if !t.inWindow(slot.minute, now) {
			*slot = timelineSlot{}
			continue
		}
		scratch.Merge(slot.sketch)
	}
	t.mtx.Unlock()
	return scratch.Estimate()
}

// Sketches calls f with each sketch in the window ending at now, and its minute.
// The timeline is locked while f runs, and f must not keep s after it returns.
func (t *Timeline) Sketches(now int64, f func(minute int64, s Sketch)) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for _, slot := range t.slots {
		if slot.sketch != nil && t.inWindow(slot.minute, now) {
			f(slot.minute, slot.sketch)
		}
	}
}

func (t *Timeline) inWindow(minute, now int64) bool {
	return minute <= now && minute > now-int64(len(t.slots))
}
