// SPDX-License-Identifier: AGPL-3.0-only

package protection

import (
	"context"
	"log"
	"runtime/metrics"
	"sync/atomic"
	"syscall"
	"time"

	ingestermetrics "github.com/grafana/mimir/cmd/go-kafka-ingester/internal/metrics"
)

// Reason is why reads are limited, empty when they aren't. Read on every request, so it's a
// lock-free pointer.
type Reason struct {
	value atomic.Pointer[string]
}

func (r *Reason) Load() string {
	if value := r.value.Load(); value != nil {
		return *value
	}
	return ""
}

func (r *Reason) store(value string) {
	r.value.Store(&value)
}

// UtilizationScanner reads process CPU time in seconds and memory in bytes.
type UtilizationScanner interface {
	Scan() (cpuSeconds float64, memoryBytes uint64, ok bool)
}

// ProcessScanner reads this process's CPU time and heap object bytes, which the Go ingester's
// limiter compares with its memory limit.
type ProcessScanner struct {
	samples []metrics.Sample
}

func NewProcessScanner() *ProcessScanner {
	return &ProcessScanner{samples: []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}}
}

func (s *ProcessScanner) Scan() (float64, uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, false
	}
	cpu := time.Duration(usage.Utime.Nano() + usage.Stime.Nano()).Seconds()
	metrics.Read(s.samples)
	if s.samples[0].Value.Kind() != metrics.KindUint64 {
		return 0, 0, false
	}
	return cpu, s.samples[0].Value.Uint64(), true
}

const (
	updateInterval = time.Second
	slidingWindow  = time.Minute
)

// UtilizationLimiter is Go's `UtilizationBasedLimiter`: a one-minute moving average of CPU
// cores, sampled every second and reported after a one-minute warmup, and the current memory use.
type UtilizationLimiter struct {
	cpuLimit    float64
	memoryLimit uint64
	alpha       float64

	rate     float64
	hasRate  bool
	last     time.Time
	lastCPU  float64
	hasLast  bool
	first    time.Time
	hasFirst bool
	reason   *Reason
}

func NewUtilizationLimiter(cpuLimit float64, memoryLimit uint64) *UtilizationLimiter {
	return &UtilizationLimiter{
		cpuLimit:    cpuLimit,
		memoryLimit: memoryLimit,
		alpha:       2 / (slidingWindow.Seconds()/updateInterval.Seconds() + 1),
		reason:      &Reason{},
	}
}

// Reason is the limiter's current limiting reason, for the read protection.
func (l *UtilizationLimiter) Reason() *Reason { return l.reason }

// Compute is one update tick; it returns the CPU utilization and memory it computed.
func (l *UtilizationLimiter) Compute(now time.Time, cpuTime float64, memory uint64, ok bool) (float64, uint64) {
	if !ok {
		l.reason.store("")
		return 0, 0
	}
	if l.hasLast {
		elapsed := now.Sub(l.last)
		if elapsed > updateInterval/2 {
			instant := (cpuTime - l.lastCPU) / elapsed.Seconds()
			// Go's EWMA counts whole hundredths of a core.
			instant = float64(int64(instant * 100))
			if l.hasRate {
				l.rate += l.alpha * (instant - l.rate)
			} else {
				l.rate, l.hasRate = instant, true
			}
			l.last, l.lastCPU = now, cpuTime
		}
	} else {
		l.last, l.lastCPU, l.hasLast = now, cpuTime, true
	}
	cpu := 0.0
	if !l.hasFirst {
		l.first, l.hasFirst = now, true
	} else if now.Sub(l.first) >= slidingWindow {
		cpu = l.rate / 100
	}
	reason := ""
	switch {
	case l.memoryLimit > 0 && memory >= l.memoryLimit:
		reason = "memory"
	case l.cpuLimit > 0 && cpu >= l.cpuLimit:
		reason = "cpu"
	}
	if (reason == "") != (l.reason.Load() == "") {
		log.Printf("phase=utilization_limiting enabled=%t reason=%s cpu=%.2f memory_bytes=%d", reason != "", reason, cpu, memory)
	}
	l.reason.store(reason)
	return cpu, memory
}

// Run ticks every second until ctx ends.
func (l *UtilizationLimiter) Run(ctx context.Context, scanner UtilizationScanner) {
	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()
	for {
		cpuTime, memory, ok := scanner.Scan()
		cpu, memory := l.Compute(time.Now(), cpuTime, memory, ok)
		ingestermetrics.UtilizationCPU.Set(cpu)
		ingestermetrics.UtilizationMemory.Set(float64(memory))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
