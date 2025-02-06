// SPDX-License-Identifier: AGPL-3.0-only

package adaptivelimiter

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/influxdata/tdigest"
	"go.uber.org/atomic"
)

// Prioritizer computes a rejection rate and priority threshold for one or more priority limiters, which can be used to
// determine whether to accept or reject an execution.
type Prioritizer interface {
	// RejectionRate returns the current rate, from 0 to 1, at which the limiter will reject requests, based on recent
	// execution times.
	RejectionRate() float64

	// RejectionThreshold is the priority threshold below which requests will be rejected, based on their priority, from 0 to 499.
	RejectionThreshold() int

	// Calibrate calibrates the RejectionRate based on recent execution times from registered limiters.
	Calibrate()

	register(limiter *priorityLimiter)
	recordPriority(priority int)
}

type RejectionPrioritizerConfig struct {
	CalibrationInterval time.Duration `yaml:"calibration_interval" category:"experimental"`
}

func (cfg *RejectionPrioritizerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.CalibrationInterval, prefix+"calibration-interval", time.Second, "The interval at which the rejection threshold is calibrated")
}

func NewPrioritizer(logger log.Logger) Prioritizer {
	return &prioritizer{
		logger: logger,
		digest: tdigest.NewWithCompression(100),
	}
}

type prioritizer struct {
	logger log.Logger

	// Mutable state
	priorityThreshold atomic.Int32
	mu                sync.Mutex
	limiters          []*priorityLimiter // Guarded by mu
	digest            *tdigest.TDigest   // Guarded by mu
	rejectionRate     float64            // Guarded by mu
}

func (r *prioritizer) register(limiter *priorityLimiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limiters = append(r.limiters, limiter)
}

func (r *prioritizer) RejectionRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rejectionRate
}

func (r *prioritizer) RejectionThreshold() int {
	return int(r.priorityThreshold.Load())
}

func (r *prioritizer) Calibrate() {
	r.mu.Lock()

	// Compute queue stats across all registered limiters
	var totalIn, totalOut, totalLimit, totalQueued, totalFreeInflight, totalRejectionThresh, totalMaxQueue int
	for _, limiter := range r.limiters {
		in, out, limit, inflight, queued, rejectionThresh, maxQueue := limiter.getAndResetStats()
		totalIn += in
		totalOut += out
		totalFreeInflight += limit - inflight
		totalLimit += limit
		totalQueued += queued
		totalRejectionThresh += rejectionThresh
		totalMaxQueue += maxQueue
	}

	// Update rejection rate and priority threshold
	newRate := computeRejectionRate(totalQueued, totalRejectionThresh, totalMaxQueue)
	r.rejectionRate = newRate
	var newThresh int32
	if newRate > 0 {
		newThresh = int32(r.digest.Quantile(newRate))
	}
	r.mu.Unlock()
	r.priorityThreshold.Swap(newThresh)

	level.Debug(r.logger).Log("msg", "prioritizer calibration",
		"newRate", fmt.Sprintf("%.2f", newRate),
		"newThresh", newThresh,
		"in", totalIn,
		"out", totalOut,
		"limit", totalLimit,
		"blocked", totalQueued)
}

func computeRejectionRate(queueSize, rejectionThreshold, maxQueueSize int) float64 {
	if queueSize <= rejectionThreshold {
		return 0
	}
	if queueSize >= maxQueueSize {
		return 1
	}
	return float64(queueSize-rejectionThreshold) / float64(maxQueueSize-rejectionThreshold)
}

func (r *prioritizer) recordPriority(priority int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.digest.Add(float64(priority), 1.0)
}
