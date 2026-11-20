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

	// past caches the union of the minutes of the window ending at pastNow, without pastNow itself, so that
	// an estimate at pastNow only merges past with the sketch of pastNow. updatePast rebuilds it from the slots
	// when now changes, and Merge keeps it up to date in between. It's nil until a minute before pastNow has
	// a sketch, and a nil past is not a cache: updatePast always rebuilds it.
	past    Sketch
	pastNow int64
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
	default:
		// The slot holds a minute after the window of minute, so the clock went back. Drop s.
		return
	}
	if t.past != nil && minute < t.pastNow && minute > t.pastNow-int64(len(t.slots)) {
		t.past.Merge(s)
	}
}

// Cleanup drops the sketches of the minutes before the window ending at now, and of the minutes a whole window or
// more after now. It also merges the minutes before now, so that the estimates at now don't have to.
func (t *Timeline) Cleanup(now int64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.updatePast(now)
}

// Estimate returns the estimated number of distinct values merged into the minutes of the window ending at now.
// The first call for a minute merges the minutes of the window, like Cleanup, and the next ones only merge two
// sketches. The estimate is computed after the timeline is unlocked.
// scratch is overwritten, and it must have the same precision as the merged sketches.
func (t *Timeline) Estimate(now int64, scratch Sketch) float64 {
	t.mtx.Lock()
	t.updatePast(now)
	if t.past != nil {
		copy(scratch, t.past)
	} else {
		clear(scratch)
	}
	if slot := t.slots[now%int64(len(t.slots))]; slot.sketch != nil && slot.minute == now {
		scratch.Merge(slot.sketch)
	}
	t.mtx.Unlock()
	return scratch.Estimate()
}

// updatePast drops the sketches that Cleanup drops, and sets past to the union of the minutes before now,
// unless it's already set for now. t.mtx must be held.
func (t *Timeline) updatePast(now int64) {
	if t.past != nil && t.pastNow == now {
		return
	}
	clear(t.past)
	t.pastNow = now
	for i := range t.slots {
		slot := &t.slots[i]
		switch {
		case slot.sketch == nil || slot.minute == now:
		case slot.minute > now && slot.minute < now+int64(len(t.slots)):
			// A caller whose clock is ahead merged it, like a call just after the minute changed while a scrape or
			// a cleanup that read the clock before runs. Keep it for when now gets there.
			// The case below drops the minutes further ahead, which only a clock that went back a whole window
			// leaves: they would take the slot of the current minute until the clock gets there.
		case !t.inWindow(slot.minute, now):
			*slot = timelineSlot{}
		default:
			if t.past == nil {
				t.past = make(Sketch, len(slot.sketch))
			}
			t.past.Merge(slot.sketch)
		}
	}
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

// Empty returns whether no sketch is in the window ending at now.
func (t *Timeline) Empty(now int64) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for _, slot := range t.slots {
		if slot.sketch != nil && t.inWindow(slot.minute, now) {
			return false
		}
	}
	return true
}

func (t *Timeline) inWindow(minute, now int64) bool {
	return minute <= now && minute > now-int64(len(t.slots))
}
