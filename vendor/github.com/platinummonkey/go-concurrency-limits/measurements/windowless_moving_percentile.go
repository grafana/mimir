package measurements

import (
	"fmt"
	"sync"
)

// WindowlessMovingPercentile implements a moving percentile.
// This implementation uses a windowless calculation that while not strictly always accurate,
// provides a very close estimation in O(1) time and space.
// Much credit goes to Martin Jambon here: https://mjambon.com/2016-07-23-moving-percentile/
// a copy can be found in github.com/platinummonkey/go-concurrency-limits/docs/assets/moving_percentile_reference.pdf
// and this is a port of the OCaml implementation provided in that reference.
type WindowlessMovingPercentile struct {
	p            float64
	deltaInitial float64

	value      float64
	delta      float64
	deltaState *SimpleMovingVariance

	seenCount int

	mu sync.RWMutex
}

// NewWindowlessMovingPercentile creates a new Windowless Moving Percentile
// p - percentile requested, accepts (0,1)
// deltaInitial - the initial delta value, here 0 is acceptable if you expect it to be rather stable at start, otherwise
//                choose a larger value. This would be estimated: `delta := stdev * r` where `r` is a user chosen
//                constant. Good values are generally from 0.001 to 0.01
// movingAvgAlphaAvg - this is the alpha value for the simple moving average. A good start is 0.05. Accepts [0,1]
// movingVarianceAlphaAvg - this is the alpha value for the simple moving variance. A good start is 0.05. Accepts [0,1]
func NewWindowlessMovingPercentile(
	p float64, // percentile requested
	deltaInitial float64,
	movingAvgAlphaAvg float64,
	movingVarianceAlphaVar float64,
) (*WindowlessMovingPercentile, error) {
	if p <= 0 || p >= 1 {
		return nil, fmt.Errorf("p must be between (0,1)")
	}
	variance, err := NewSimpleMovingVariance(movingAvgAlphaAvg, movingVarianceAlphaVar)
	if err != nil {
		return nil, err
	}

	return &WindowlessMovingPercentile{
		p:            p,
		deltaInitial: deltaInitial,
		delta:        deltaInitial,
		deltaState:   variance,
	}, nil
}

// Add a single sample and update the internal state.
// returns true if the internal state was updated, also return the current value.
func (m *WindowlessMovingPercentile) Add(value float64) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.add(value)
}

func (m *WindowlessMovingPercentile) add(value float64) (float64, bool) {
	changed := false
	if m.seenCount < 2 {
		// we only need 2 samples to continue
		m.seenCount++
	}
	originalDelta := m.delta
	stdev, _ := m.deltaState.Add(value)
	if m.seenCount >= 2 {
		m.delta = m.deltaInitial * stdev
		if m.delta != originalDelta {
			changed = true
		}
	}
	newValue := float64(m.value)
	if m.seenCount == 1 {
		newValue = value
		changed = true
	} else if value < m.value {
		newValue = m.value - m.delta/m.p
	} else if value > m.value {
		newValue = m.value + m.delta/(1-m.p)
	}
	// else the same
	if newValue != m.value {
		changed = true
	}
	m.value = newValue
	return m.value, changed
}

// Get the current value.
func (m *WindowlessMovingPercentile) Get() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}

// Reset the internal state as if no samples were ever added.
func (m *WindowlessMovingPercentile) Reset() {
	m.mu.Lock()
	m.value = 0
	m.seenCount = 0
	m.mu.Unlock()
}

// Update will update the value given an operation function
func (m *WindowlessMovingPercentile) Update(operation func(value float64) float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newValue, _ := m.add(m.value)
	m.value = operation(newValue)
}
