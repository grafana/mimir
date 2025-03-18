// SPDX-License-Identifier: AGPL-3.0-only

package math

// Ewma provides an exponentially weighted moving average for some windowed values, without strict time-based
// adjustments.
//
// This type is not concurrency safe.
type Ewma interface {
	// Add adds a value to the series and updates the moving average.
	Add(float64) float64

	// Value gets the current value of the moving average.
	Value() float64

	// Set sets a value for the moving average.
	Set(float64)
}

// NewEwma creates a new exponentially weighted moving average for the windowSize and warmupSamples.
// windowSize controls how many samples are effectively stored in the Ewma before they decay out.
// warmupSamples controls how many samples must be recorded before decay begins.
func NewEwma(windowSize uint, warmupSamples uint8) Ewma {
	return &ewma{
		warmupSamples:   warmupSamples,
		smoothingFactor: 2 / (float64(windowSize) + 1),
	}
}

type ewma struct {
	warmupSamples   uint8
	count           uint8
	smoothingFactor float64
	value           float64
	sum             float64
}

// Add decays the Ewma value via (oldValue * (1 - smoothingFactor)) + (newValue * smoothingFactor)
func (e *ewma) Add(newValue float64) float64 {
	switch {
	case e.count < e.warmupSamples:
		e.count++
		e.sum += newValue
		e.value = e.sum / float64(e.count)
	default:
		e.value = Smooth(e.value, newValue, e.smoothingFactor)
	}
	return e.value
}

func (e *ewma) Value() float64 {
	return e.value
}

func (e *ewma) Set(value float64) {
	e.value = value
	// Skip any incomplete warmup
	if e.count < e.warmupSamples {
		e.count = e.warmupSamples
	}
}

// Smooth returns a value that is decreased by some portion of the oldValue, and increased by some portion of the
// newValue, based on the factor.
func Smooth(oldValue, newValue, factor float64) float64 {
	return oldValue*(1-factor) + newValue*factor
}
