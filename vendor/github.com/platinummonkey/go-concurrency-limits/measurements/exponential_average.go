package measurements

import (
	"fmt"
	"sync"
)

// ExponentialAverageMeasurement is an exponential average measurement implementation.
type ExponentialAverageMeasurement struct {
	value        float64
	sum          float64
	window       int
	warmupWindow int
	count        int

	mu sync.RWMutex
}

// NewExponentialAverageMeasurement will create a new ExponentialAverageMeasurement
func NewExponentialAverageMeasurement(
	window int,
	warmupWindow int,
) *ExponentialAverageMeasurement {
	return &ExponentialAverageMeasurement{
		window:       window,
		warmupWindow: warmupWindow,
	}
}

// Add a single sample and update the internal state.
func (m *ExponentialAverageMeasurement) Add(value float64) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count < m.warmupWindow {
		m.count++
		m.sum += value
		m.value = m.sum / float64(m.count)
	} else {
		f := factor(m.window)
		m.value = m.value*(1-f) + value*f
	}
	return m.value, true
}

// Get the current value.
func (m *ExponentialAverageMeasurement) Get() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}

// Reset the internal state as if no samples were ever added.
func (m *ExponentialAverageMeasurement) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = 0
	m.count = 0
	m.sum = 0
}

// Update will update the value given an operation function
func (m *ExponentialAverageMeasurement) Update(operation func(value float64) float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = operation(m.value)
}

func factor(n int) float64 {
	return 2.0 / float64(n+1)
}

func (m *ExponentialAverageMeasurement) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf(
		"ExponentialAverageMeasurement{value=%0.5f, count=%d, window=%d, warmupWindow=%d}",
		m.value, m.count, m.window, m.warmupWindow,
	)
}
