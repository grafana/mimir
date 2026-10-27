// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
)

// Buckets returns how many buckets a per-bucket count over [minT, maxT) has
// when a bucket is window wide and one starts every step: bucket i covers
// [minT+i*step, minT+i*step+window), and the last one ends at maxT. A
// window of 0 is the step, which cuts [minT, maxT) into adjacent buckets.
// A window wider than the step makes the buckets overlap, so each bucket's
// count is the series with a sample in the window before its end, however
// wide the step is.
func Buckets(minT, maxT, step, window int64) (int, error) {
	if step <= 0 {
		return 0, errors.New("step must be positive")
	}
	if window < 0 {
		return 0, errors.New("window can't be negative")
	}
	if window == 0 {
		window = step
	}
	if window < step {
		return 0, fmt.Errorf("window %d is shorter than step %d, which would leave time between buckets uncounted", window, step)
	}
	if window > maxT-minT {
		return 0, fmt.Errorf("window %d is longer than [%d, %d)", window, minT, maxT)
	}
	if (maxT-minT-window)%step != 0 {
		return 0, fmt.Errorf("step %d doesn't divide [%d, %d) less one window of %d", step, minT, maxT, window)
	}
	n := (maxT-minT-window)/step + 1
	if n > MaxCounterBuckets {
		return 0, fmt.Errorf("the window has %d buckets of the step, more than %d", n, MaxCounterBuckets)
	}
	return int(n), nil
}

// BucketSpan returns the first and last of the buckets Buckets describes
// that [from, through], both ends included, overlaps. first > last when it
// overlaps none.
func BucketSpan(minT, step, window int64, buckets int, from, through int64) (first, last int64) {
	if window == 0 {
		window = step
	}
	// Bucket i overlaps when through >= minT+i*step and from < minT+i*step+window.
	first = max(0, floorDiv(from-minT-window, step)+1)
	last = min(int64(buckets-1), floorDiv(through-minT, step))
	return first, last
}

// BucketMarks counts one series into the buckets its chunks touch, once per
// bucket however many chunks touch it, in time proportional to the buckets
// touched rather than to all of them. Mark the span of each chunk, then
// Flush into the group's counts.
type BucketMarks struct {
	seen    []bool
	touched []int64
}

// NewBucketMarks returns marks for the given number of buckets.
func NewBucketMarks(buckets int) *BucketMarks {
	return &BucketMarks{seen: make([]bool, buckets)}
}

// Mark marks buckets first through last, both included, as touched. It
// marks none when first > last.
func (m *BucketMarks) Mark(first, last int64) {
	for i := first; i <= last; i++ {
		if !m.seen[i] {
			m.seen[i] = true
			m.touched = append(m.touched, i)
		}
	}
}

// Flush adds one to counts for every bucket marked since the last Flush and
// clears the marks.
func (m *BucketMarks) Flush(counts []int64) {
	for _, i := range m.touched {
		counts[i]++
		m.seen[i] = false
	}
	m.touched = m.touched[:0]
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
