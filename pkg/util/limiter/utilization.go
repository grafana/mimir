// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"io"
	"os"
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

	// Memory limit in bytes
	memoryLimit uint64
	// CPU limit in cores
	cpuLimit float64
	// Last CPU time counter
	lastCPUTime float64
	// The time of the last update
	lastUpdate     time.Time
	movingAvg      *math.EwmaRate
	LimitingReason atomic.String
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
		movingAvg: math.NewEWMARate(alpha, resourceUtilizationUpdateInterval),
	}
	l.Service = services.NewTimerService(resourceUtilizationUpdateInterval, l.init, l.update, nil)
	return l
}

func (l *UtilizationBasedLimiter) init(_ context.Context) error {
	p, err := procfs.Self()
	if err != nil {
		return errors.Wrap(err, "unable to detect CPU/memory utilization, unsupported platform")
	}

	l.utilizationScanner = procfsScanner{
		proc: p,
	}
	return nil
}

func (l *UtilizationBasedLimiter) update(ctx context.Context) error {
	cpuTime, memUtil, err := l.utilizationScanner.Scan()
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats", "method",
			l.utilizationScanner.String(), "err", err.Error())
		// Disable any limiting, since we can't tell resource utilization
		l.LimitingReason.Store("")
		return ctx.Err()
	}

	now := time.Now().UTC()
	lastUpdate := l.lastUpdate
	l.lastUpdate = now

	lastCPUTime := l.lastCPUTime
	l.lastCPUTime = cpuTime

	if lastUpdate.IsZero() {
		return ctx.Err()
	}

	cpuUtil := (cpuTime - lastCPUTime) / now.Sub(lastUpdate).Seconds()
	l.movingAvg.Add(int64(cpuUtil * 100))
	l.movingAvg.Tick()
	cpuA := float64(l.movingAvg.Rate()) / 100

	level.Debug(l.logger).Log("msg", "process resource utilization", "method", l.utilizationScanner.String(),
		"memory_utilization", memUtil, "smoothed_cpu_utilization", cpuA, "raw_cpu_utilization", cpuUtil)

	memPercent := 100 * (float64(memUtil) / float64(l.memoryLimit))
	cpuPercent := 100 * (cpuA / l.cpuLimit)

	var reason string
	if memPercent >= 100 {
		reason = "memory"
	} else if cpuPercent >= 100 {
		reason = "cpu"
	}

	enable := reason != ""
	prevEnable := l.LimitingReason.Load() != ""
	if enable == prevEnable {
		// No change
		return ctx.Err()
	}

	if enable {
		level.Info(l.logger).Log("msg", "enabling resource utilization based limiting",
			"reason", reason, "memory_limit", l.memoryLimit,
			"memory_percentage_of_limit", memPercent, "cpu_limit", l.cpuLimit,
			"cpu_percentage_of_limit", cpuPercent)
	} else {
		level.Info(l.logger).Log("msg", "disabling resource utilization based limiting",
			"memory_limit", l.memoryLimit, "memory_percentage_of_limit", memPercent,
			"cpu_limit", l.cpuLimit, "cpu_percentage_of_limit", cpuPercent)
	}
	l.LimitingReason.Store(reason)

	return ctx.Err()
}

// readFileNoStat returns an io.ReadCloser for fpath.
//
// We make sure to avoid calling os.Stat, since many files in /proc and /sys report incorrect file sizes (either 0 or 4096).
// The reader is limited at 1024 kB.
func readFileNoStat(fpath string) (io.ReadCloser, error) {
	const maxBufferSize = 1024 * 1024

	f, err := os.Open(fpath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, errors.Wrapf(err, "failed to open %q", fpath)
	}

	return readCloser{Reader: io.LimitReader(f, maxBufferSize), Closer: f}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}
