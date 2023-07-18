// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/procfs"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/math"
)

const (
	// Interval for updating resource (CPU/memory) utilization.
	resourceUtilizationUpdateInterval = time.Second

	// How long is the sliding window used to compute the moving average.
	resourceUtilizationSlidingWindow = 60 * time.Second
)

type utilizationScanner interface {
	// Scan returns CPU time in seconds and memory utilization in bytes, or an error.
	Scan() (float64, uint64, error)
}

type procfsScanner struct {
	proc procfs.Proc
}

func (s procfsScanner) Scan() (float64, uint64, error) {
	ps, err := s.proc.Stat()
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get process stats")
	}

	return ps.CPUTime(), uint64(ps.ResidentMemory()), nil
}

// UtilizationBasedLimiter is a Service offering limiting based on CPU and memory utilization.
//
// The respective CPU and memory utilization limits are configurable.
type UtilizationBasedLimiter struct {
	services.Service

	logger             log.Logger
	utilizationScanner utilizationScanner

	// Memory limit in bytes. The limit is enabled if the value is > 0.
	memoryLimit uint64
	// CPU limit in cores. The limit is enabled if the value is > 0.
	cpuLimit float64
	// Last CPU time counter
	lastCPUTime float64
	// The time of the first update
	firstUpdate time.Time
	// The time of the last update
	lastUpdate     time.Time
	cpuMovingAvg   *math.EwmaRate
	limitingReason atomic.String
	currCPUUtil    atomic.Float64
	currMemoryUtil atomic.Uint64
}

// NewUtilizationBasedLimiter returns a UtilizationBasedLimiter configured with cpuLimit and memoryLimit.
func NewUtilizationBasedLimiter(cpuLimit float64, memoryLimit uint64, logger log.Logger, reg prometheus.Registerer) *UtilizationBasedLimiter {
	// Calculate alpha for a minute long window
	// https://github.com/VividCortex/ewma#choosing-alpha
	alpha := 2 / (resourceUtilizationSlidingWindow.Seconds()/resourceUtilizationUpdateInterval.Seconds() + 1)
	l := &UtilizationBasedLimiter{
		logger:      logger,
		cpuLimit:    cpuLimit,
		memoryLimit: memoryLimit,
		// Use a minute long window, each sample being a second apart
		cpuMovingAvg: math.NewEWMARate(alpha, resourceUtilizationUpdateInterval),
	}
	l.Service = services.NewTimerService(resourceUtilizationUpdateInterval, l.starting, l.update, nil)

	if reg != nil {
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "utilization_limiter_current_cpu_load",
			Help: "Current average CPU load calculated by utilization based limiter.",
		}, func() float64 {
			return l.currCPUUtil.Load()
		})
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "utilization_limiter_current_memory_usage_bytes",
			Help: "Current memory usage calculated by utilization based limiter.",
		}, func() float64 {
			return float64(l.currMemoryUtil.Load())
		})
	}

	return l
}

// LimitingReason returns the current reason for limiting, if any.
// If an empty string is returned, limiting is disabled.
func (l *UtilizationBasedLimiter) LimitingReason() string {
	return l.limitingReason.Load()
}

func (l *UtilizationBasedLimiter) starting(_ context.Context) error {
	p, err := procfs.Self()
	if err != nil {
		return errors.Wrap(err, "unable to detect CPU/memory utilization, unsupported platform. Please disable utilization based limiting")
	}

	l.utilizationScanner = procfsScanner{
		proc: p,
	}
	return nil
}

func (l *UtilizationBasedLimiter) update(_ context.Context) error {
	l.compute(time.Now)
	return nil
}

// compute and return the current CPU and memory utilization.
// This function must be called at a regular interval (resourceUtilizationUpdateInterval) to get a predictable behaviour.
func (l *UtilizationBasedLimiter) compute(nowFn func() time.Time) (currCPUUtil float64, currMemoryUtil uint64) {
	cpuTime, currMemoryUtil, err := l.utilizationScanner.Scan()
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats", "err", err.Error())
		// Disable any limiting, since we can't tell resource utilization
		l.limitingReason.Store("")
		return
	}

	// Get wall time after CPU time, in case there's a delay before CPU time is returned,
	// which would cause us to compute too high of a CPU load
	now := nowFn()
	l.currMemoryUtil.Store(currMemoryUtil)

	// Add the instant CPU utilization to the moving average. The instant CPU
	// utilization can only be computed starting from the 2nd tick.
	if prevUpdate, prevCPUTime := l.lastUpdate, l.lastCPUTime; !prevUpdate.IsZero() {
		cpuUtil := (cpuTime - prevCPUTime) / now.Sub(prevUpdate).Seconds()
		l.cpuMovingAvg.Add(int64(cpuUtil * 100))
		l.cpuMovingAvg.Tick()
	}

	l.lastUpdate = now
	l.lastCPUTime = cpuTime

	// The CPU utilization moving average requires a warmup period before getting
	// stable results. In this implementation we use a warmup period equal to the
	// sliding window. During the warmup, the reported CPU utilization will be 0.
	if l.firstUpdate.IsZero() {
		l.firstUpdate = now
	} else if now.Sub(l.firstUpdate) >= resourceUtilizationSlidingWindow {
		currCPUUtil = l.cpuMovingAvg.Rate() / 100
		l.currCPUUtil.Store(currCPUUtil)
	}

	var reason string
	if l.memoryLimit > 0 && currMemoryUtil >= l.memoryLimit {
		reason = "memory"
	} else if l.cpuLimit > 0 && currCPUUtil >= l.cpuLimit {
		reason = "cpu"
	}

	enable := reason != ""
	prevEnable := l.limitingReason.Load() != ""
	if enable == prevEnable {
		// No change
		return
	}

	if enable {
		level.Info(l.logger).Log("msg", "enabling resource utilization based limiting",
			"reason", reason, "memory_limit", formatMemoryLimit(l.memoryLimit), "memory_utilization", formatMemory(currMemoryUtil),
			"cpu_limit", formatCPULimit(l.cpuLimit), "cpu_utilization", formatCPU(currCPUUtil))
	} else {
		level.Info(l.logger).Log("msg", "disabling resource utilization based limiting",
			"memory_limit", formatMemoryLimit(l.memoryLimit), "memory_utilization", formatMemory(currMemoryUtil),
			"cpu_limit", formatCPULimit(l.cpuLimit), "cpu_utilization", formatCPU(currCPUUtil))
	}

	l.limitingReason.Store(reason)
	return
}

func formatCPU(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func formatCPULimit(limit float64) string {
	if limit == 0 {
		return "disabled"
	}
	return formatCPU(limit)
}

func formatMemory(value uint64) string {
	// We're not using a nicer format like units.Base2Bytes() because the actual value
	// is harder to read and compare when not a multiple of a unit (e.g. not a multiple of GB).
	return fmt.Sprintf("%d", value)
}

func formatMemoryLimit(limit uint64) string {
	if limit == 0 {
		return "disabled"
	}
	return formatMemory(limit)
}
