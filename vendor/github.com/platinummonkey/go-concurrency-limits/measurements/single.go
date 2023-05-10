package measurements

import (
	"fmt"
	"sync"
)

// SingleMeasurement only keeps the latest value used.
type SingleMeasurement struct {
	value float64
	mu    sync.RWMutex
}

// Add a single sample and update the internal state.
func (m *SingleMeasurement) Add(value float64) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = value
	return m.value, true
}

// Get the current value.
func (m *SingleMeasurement) Get() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}

// Reset the internal state as if no samples were ever added.
func (m *SingleMeasurement) Reset() {
	m.mu.Lock()
	m.value = 0
	m.mu.Unlock()
}

// Update will update the value given an operation function
func (m *SingleMeasurement) Update(operation func(value float64) float64) {
	m.mu.Lock()
	m.value = operation(m.value)
	m.mu.Unlock()
}

func (m *SingleMeasurement) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf("SingleMeasurement{value=%0.5f}", m.value)
}
