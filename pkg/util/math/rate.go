// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/math/rate.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package math

import (
	"sync"
	"time"

	"go.uber.org/atomic"
)

// EwmaRate tracks an exponentially weighted moving average of a per-second rate.
type EwmaRate struct {
	newEvents atomic.Int64

	alpha    float64
	interval time.Duration

	mutex         sync.RWMutex
	lastRate      float64
	init          bool
	count         uint8   // count is the number of warmup ticks observed so far.
	warmupSamples uint8   // warmupSamples is the number of ticks to accumulate before the rate is seeded; zero disables warmup.
	warmupSum     float64 // warmupSum is the running sum of instant rates observed during warmup.
}

// NewEWMARate returns an EwmaRate with no warmup; a rate is reported from the first tick.
func NewEWMARate(alpha float64, interval time.Duration) *EwmaRate {
	return &EwmaRate{
		alpha:         alpha,
		interval:      interval,
		warmupSamples: 0,
	}
}

// NewEWMARateWithWarmup returns an EwmaRate that reports a rate of 0 until warmupSamples ticks have been observed.
// Once warmup completes, the rate is seeded from the arithmetic mean of the warmup samples, after which normal EWMA blending begins.
// A warmupSamples value of 0 behaves identically to NewEWMARate with no warmup phase.
func NewEWMARateWithWarmup(alpha float64, interval time.Duration, warmupSamples uint8) *EwmaRate {
	return &EwmaRate{
		alpha:         alpha,
		interval:      interval,
		warmupSamples: warmupSamples,
	}
}

// Rate returns the per-second rate.
// It returns 0 until the warmup phase is complete.
func (r *EwmaRate) Rate() float64 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.count < r.warmupSamples {
		return 0
	}

	return r.lastRate
}

// Tick assumes to be called every r.interval.
func (r *EwmaRate) Tick() {
	newEvents := r.newEvents.Swap(0)
	instantRate := float64(newEvents) / r.interval.Seconds()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.count < r.warmupSamples {
		r.warmupSum += instantRate
		r.count++
		if r.count == r.warmupSamples {
			r.lastRate = r.warmupSum / float64(r.count) // Seed from the arithmetic mean of the warmup samples so no single reading dominates the initial rate.
			r.init = true
		}
		return
	}

	if r.init {
		r.lastRate += r.alpha * (instantRate - r.lastRate)
	} else {
		r.init = true
		r.lastRate = instantRate
	}
}

// Inc counts one event.
func (r *EwmaRate) Inc() {
	r.newEvents.Inc()
}

func (r *EwmaRate) Add(delta int64) {
	r.newEvents.Add(delta)
}
