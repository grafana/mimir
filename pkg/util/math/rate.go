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
	count         uint8
	warmupSamples uint8
	warmupSum     float64
}

func NewEWMARate(alpha float64, interval time.Duration) *EwmaRate {
	return &EwmaRate{
		alpha:         alpha,
		interval:      interval,
		warmupSamples: 0,
	}
}

func NewEWMARateWithWarmup(alpha float64, interval time.Duration, warmupSamples uint8) *EwmaRate {
	return &EwmaRate{
		alpha:         alpha,
		interval:      interval,
		warmupSamples: warmupSamples,
	}
}

// Rate returns the per-second rate.
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
			r.lastRate = r.warmupSum / float64(r.count)
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
