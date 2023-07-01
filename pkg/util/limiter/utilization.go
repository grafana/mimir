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
	"github.com/prometheus/procfs"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/math"
)

const (
	// Interval for updating resource (CPU/memory) utilization
	resourceUtilizationUpdateInterval = time.Second
)

type utilizationScanner interface {
	fmt.Stringer

	// Scan returns CPU time in seconds and memory utilization in bytes, or an error.
	Scan() (float64, uint64, error)
}

type procfsScanner struct {
	proc procfs.Proc
}

func (s procfsScanner) String() string {
	return "/proc"
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
	// The time of the last update
	lastUpdate     time.Time
	cpuMovingAvg   *math.EwmaRate
	limitingReason atomic.String
}

// NewUtilizationBasedLimiter returns a UtilizationBasedLimiter configured with cpuLimit and memoryLimit.
func NewUtilizationBasedLimiter(cpuLimit float64, memoryLimit uint64, logger log.Logger) *UtilizationBasedLimiter {
	// Calculate alpha for a minute long window
	// https://github.com/VividCortex/ewma#choosing-alpha
	alpha := 2 / (60/resourceUtilizationUpdateInterval.Seconds() + 1)
	l := &UtilizationBasedLimiter{
		logger:      logger,
		cpuLimit:    cpuLimit,
		memoryLimit: memoryLimit,
		// Use a minute long window, each sample being a second apart
		cpuMovingAvg: math.NewEWMARate(alpha, resourceUtilizationUpdateInterval),
	}
	l.Service = services.NewTimerService(resourceUtilizationUpdateInterval, l.starting, l.update, nil)
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
	l.compute(time.Now())
	return nil
}

func (l *UtilizationBasedLimiter) compute(now time.Time) {
	cpuTime, memUtil, err := l.utilizationScanner.Scan()
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats", "method",
			l.utilizationScanner.String(), "err", err.Error())
		// Disable any limiting, since we can't tell resource utilization
		l.limitingReason.Store("")
		return
	}

	lastUpdate := l.lastUpdate
	l.lastUpdate = now

	lastCPUTime := l.lastCPUTime
	l.lastCPUTime = cpuTime

	if lastUpdate.IsZero() {
		return
	}

	cpuUtil := (cpuTime - lastCPUTime) / now.Sub(lastUpdate).Seconds()
	l.cpuMovingAvg.Add(int64(cpuUtil * 100))
	l.cpuMovingAvg.Tick()
	cpuA := float64(l.cpuMovingAvg.Rate()) / 100

	var reason string
	if l.memoryLimit > 0 && memUtil >= l.memoryLimit {
		reason = "memory"
	} else if l.cpuLimit > 0 && cpuA >= l.cpuLimit {
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
			"reason", reason, "memory_limit", formatMemoryLimit(l.memoryLimit), "memory_utilization", formatMemory(memUtil),
			"cpu_limit", formatCPULimit(l.cpuLimit), "cpu_utilization", formatCPU(cpuA))
	} else {
		level.Info(l.logger).Log("msg", "disabling resource utilization based limiting",
			"memory_limit", formatMemoryLimit(l.memoryLimit), "memory_utilization", formatMemory(memUtil),
			"cpu_limit", formatCPULimit(l.cpuLimit), "cpu_utilization", formatCPU(cpuA))
	}
	l.limitingReason.Store(reason)
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
