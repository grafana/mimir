package measurements

import (
	"math"
	"sync"
)

// SimpleMovingVariance implements a simple moving variance calculation based on the simple moving average.
type SimpleMovingVariance struct {
	average  *SimpleExponentialMovingAverage
	variance *SimpleExponentialMovingAverage

	stdev      float64 // square root of the estimated variance
	normalized float64 // (input - mean) / stdev

	mu sync.RWMutex
}

// NewSimpleMovingVariance will create a new exponential moving variance approximation based on the SimpleMovingAverage
func NewSimpleMovingVariance(
	alphaAverage float64,
	alphaVariance float64,
) (*SimpleMovingVariance, error) {
	movingAverage, err := NewSimpleExponentialMovingAverage(alphaAverage)
	if err != nil {
		return nil, err
	}
	variance, err := NewSimpleExponentialMovingAverage(alphaVariance)
	if err != nil {
		return nil, err
	}
	return &SimpleMovingVariance{
		average:  movingAverage,
		variance: variance,
	}, nil
}

// Add a single sample and update the internal state.
// returns true if the internal state was updated, also return the current value.
func (m *SimpleMovingVariance) Add(value float64) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	if m.average.seenSamples > 0 {
		m.variance.Add(math.Pow(value-m.average.Get(), 2))
	}
	m.average.Add(value)

	mean := m.average.Get()
	variance := m.variance.Get()
	stdev := math.Sqrt(variance)
	normalized := m.normalized
	if stdev != 0 {
		// edge case
		normalized = (value - mean) / stdev
	}

	if stdev != m.stdev || normalized != m.normalized {
		changed = true
	}
	m.stdev = stdev
	m.normalized = normalized
	return stdev, changed
}

// Get the current value.
func (m *SimpleMovingVariance) Get() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.variance.Get()
}

// Reset the internal state as if no samples were ever added.
func (m *SimpleMovingVariance) Reset() {
	m.mu.Lock()
	m.average.Reset()
	m.variance.Reset()
	m.stdev = 0
	m.normalized = 0
	m.mu.Unlock()
}

// Update will update the value given an operation function
func (m *SimpleMovingVariance) Update(operation func(value float64) float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stdev = operation(m.variance.Get())
}
