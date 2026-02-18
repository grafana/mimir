package util

import (
	"math"
	"time"
)

// RollingSum maintains a sum over a rolling window.
//
// This type is not concurrency safe.
type RollingSum struct {
	// For variation and covariance
	samples []float64
	size    int
	index   int

	// Rolling sum fields
	sumY       float64 // Y values are the samples
	sumSquares float64
}

func NewRollingSum(capacity uint) RollingSum {
	return RollingSum{samples: make([]float64, capacity)}
}

// Add adds the value to the window if it's non-zero, updates the sums, and returns the old value along with whether the
// window is full.
func (r *RollingSum) Add(value float64) (oldValue float64, full bool) {
	if value != 0 {
		if r.size == len(r.samples) {
			full = true

			// Remove oldest value
			oldValue = r.samples[r.index]
			r.sumY -= oldValue
			r.sumSquares -= oldValue * oldValue
		} else {
			r.size++
		}

		// Add new value
		r.samples[r.index] = value

		// Update rolling computations
		r.sumY += value
		r.sumSquares += value * value

		// Move index forward
		r.index = (r.index + 1) % len(r.samples)
	}

	return oldValue, full
}

// CalculateCV calculates the coefficient of variation (relative variance), mean, and variance for the sum. Returns NaN
// values if there are < 2 samples, the variance is < 0, or the mean is 0.
func (r *RollingSum) CalculateCV() (cv, mean, variance float64) {
	if r.size < 2 {
		return math.NaN(), math.NaN(), math.NaN()
	}

	mean = r.sumY / float64(r.size)
	variance = (r.sumSquares / float64(r.size)) - (mean * mean)
	if variance < 0 || mean == 0 {
		return math.NaN(), math.NaN(), math.NaN()
	}

	cv = math.Sqrt(variance) / mean
	return cv, mean, variance
}

// Reset resets the sum to its initial state.
func (r *RollingSum) Reset() {
	for i := range r.samples {
		r.samples[i] = 0
	}
	r.size = 0
	r.index = 0
	r.sumY = 0
	r.sumSquares = 0
}

// CorrelationWindow maintains the correlation between two rolling windows.
//
// This type is not concurrency safe.
type CorrelationWindow struct {
	warmupSamples uint8

	// Mutable state
	xSamples  RollingSum
	ySamples  RollingSum
	corrSumXY float64
}

func NewCorrelationWindow(capacity uint, warmupSamples uint8) CorrelationWindow {
	return CorrelationWindow{
		warmupSamples: warmupSamples,
		xSamples:      NewRollingSum(capacity),
		ySamples:      NewRollingSum(capacity),
	}
}

// Add adds the values to the window and returns the current correlation coefficient.
// Returns a value between 0 and 1 when a correlation between increasing x and y values is present.
// Returns a value between -1 and 0 when a correlation between increasing x and decreasing y values is present.
// Returns 0 values if < warmup or low CV (< .01)
func (w *CorrelationWindow) Add(x, y float64) (correlation, cvX, cvY float64) {
	if math.IsInf(x, 0) || math.IsInf(y, 0) {
		return 0, 0, 0
	}

	oldX, full := w.xSamples.Add(x)
	oldY, _ := w.ySamples.Add(y)
	cvX, meanX, varX := w.xSamples.CalculateCV()
	cvY, meanY, varY := w.ySamples.CalculateCV()

	if full {
		// Remove old value
		w.corrSumXY -= oldX * oldY
	}

	// Add new value
	w.corrSumXY += x * y

	if math.IsNaN(cvX) || math.IsNaN(cvY) {
		return 0, 0, 0
	}

	// Ignore warmup
	if w.xSamples.size < int(w.warmupSamples) {
		return 0, 0, 0
	}

	// Ignore measurements that vary by less than 1%
	minCV := 0.01
	if cvX < minCV || cvY < minCV {
		return 0, cvX, cvY
	}

	covariance := (w.corrSumXY / float64(w.xSamples.size)) - (meanX * meanY)
	correlation = covariance / (math.Sqrt(varX) * math.Sqrt(varY))

	return correlation, cvX, cvY
}

// Reset resets the window to its initial state.
func (w *CorrelationWindow) Reset() {
	w.xSamples.Reset()
	w.ySamples.Reset()
	w.corrSumXY = 0
}

// MaxWindow maintains the maximum value over a sliding time window using a monotonic deque.
// This provides O(1) amortized insertion and O(1) max retrieval.
//
// This type is not concurrency safe.
type MaxWindow struct {
	window time.Duration
	deque  []maxWindowEntry
}

type maxWindowEntry struct {
	value     int
	timestamp time.Time
}

func NewMaxWindow(window time.Duration) MaxWindow {
	return MaxWindow{window: window}
}

// Configured returns true if the MaxWindow has a non-zero window duration.
func (w *MaxWindow) Configured() bool {
	return w.window > 0
}

// Add adds a value to the window and returns the current maximum.
func (w *MaxWindow) Add(value int, now time.Time) int {
	// Remove expired entries from front
	cutoff := now.Add(-w.window)
	start := 0
	for start < len(w.deque) && !w.deque[start].timestamp.After(cutoff) {
		start++
	}
	w.deque = w.deque[start:]

	// Remove entries from back that are <= the new value
	for len(w.deque) > 0 && w.deque[len(w.deque)-1].value <= value {
		w.deque = w.deque[:len(w.deque)-1]
	}

	// Add new entry
	w.deque = append(w.deque, maxWindowEntry{value: value, timestamp: now})
	return w.Value()
}

// Value gets the current value of the moving average.
func (w *MaxWindow) Value() int {
	if !w.Configured() {
		return 0
	}
	return w.deque[0].value
}

// Reset resets the window to its initial state.
func (w *MaxWindow) Reset() {
	if w.Configured() {
		w.deque = w.deque[:0]
	}
}

// BucketedWindow is a time based bucketed sliding window.
// T is the bucket type.
//
// This type is not concurrency safe.
type BucketedWindow[T any] struct {
	Clock
	BucketCount int64
	BucketNanos int64

	// Use function references instead of having T constrained by an interface.
	// This allows users to provide T as a value type rather than pointer, which saves on allocations.
	AddFn    func(summary *T, bucket *T)
	RemoveFn func(summary *T, bucket *T)
	ResetFn  func(bucket *T)

	// Mutable state
	Buckets  []T
	Summary  T
	HeadTime int64
}

// ExpireBuckets resets any old buckets and returns the current bucket, sliding the window as needed.
func (w *BucketedWindow[T]) ExpireBuckets() *T {
	newHead := w.Clock.Now().UnixNano() / w.BucketNanos

	if newHead > w.HeadTime {
		bucketsToMove := min(w.BucketCount, newHead-w.HeadTime)
		for i := int64(0); i < bucketsToMove; i++ {
			bucket := &w.Buckets[(w.HeadTime+i+1)%w.BucketCount]
			w.RemoveFn(&w.Summary, bucket)
			w.ResetFn(bucket)
		}
		w.HeadTime = newHead
	}

	return &w.Buckets[w.HeadTime%w.BucketCount]
}

func (w *BucketedWindow[T]) Reset() {
	for i := range w.Buckets {
		w.ResetFn(&w.Buckets[i])
	}
	w.ResetFn(&w.Summary)
	w.HeadTime = 0
}
