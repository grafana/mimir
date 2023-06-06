// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
	"go.uber.org/atomic"
)

const (
	// Interval for updating resource (CPU/memory) utilization
	resourceUtilizationUpdateInterval = time.Second

	memPathV1 = "/sys/fs/cgroup/memory/memory.stat"
	memPathV2 = "/sys/fs/cgroup/memory.current"
	cpuPathV1 = "/sys/fs/cgroup/cpu/cpuacct.usage"
	cpuPathV2 = "/sys/fs/cgroup/cpu.stat"
)

type utilizationScanner interface {
	// Method returns the method for getting resource utilization.
	Method() string

	// Scan returns CPU time and memory utilization, or an error.
	Scan() (float64, uint64, error)
}

type cgroupV2Scanner struct {
}

func (s cgroupV2Scanner) Method() string {
	return "cgroup v2"
}

func (s cgroupV2Scanner) Scan() (float64, uint64, error) {
	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	memR, err := readFileNoStat(memPathV2)
	if err != nil {
		return 0, 0, err
	}
	defer memR.Close()
	var memUtil uint64
	if _, err := fmt.Fscanf(memR, "%d", &memUtil); err != nil {
		return 0, 0, errors.Wrapf(err, "failed scanning %s", memPathV2)
	}

	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	cpuR, err := readFileNoStat(cpuPathV2)
	if err != nil {
		return 0, 0, err
	}
	defer cpuR.Close()
	var name string
	var cpuTime uint64
	for {
		if _, err := fmt.Fscanf(cpuR, "%s %d\n", &name, &cpuTime); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, errors.Wrapf(err, "failed scanning %s", cpuPathV2)
		}

		if name == "usage_usec" {
			// This is CPU time in microseconds
			break
		}
	}
	if name != "usage_usec" {
		return 0, 0, fmt.Errorf("failed scanning %s", cpuPathV2)
	}

	return float64(cpuTime) / 1e06, memUtil, nil
}

func newCgroupV2Scanner() (utilizationScanner, bool) {
	if _, err := os.Stat(memPathV2); err != nil {
		return cgroupV2Scanner{}, false
	}
	if _, err := os.Stat(cpuPathV2); err != nil {
		return cgroupV2Scanner{}, false
	}
	return cgroupV2Scanner{}, true
}

type cgroupV1Scanner struct {
}

func (s cgroupV1Scanner) Method() string {
	return "cgroup v1"
}

func (s cgroupV1Scanner) Scan() (float64, uint64, error) {
	memR, err := readFileNoStat(memPathV1)
	if err != nil {
		return 0, 0, err
	}
	defer memR.Close()
	// Summing RSS and cache for memory usage
	// For reference, see section 5.5 in https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
	var memUtil uint64
	var numMemStats int
	for {
		var name string
		var val uint64
		if _, err := fmt.Fscanf(memR, "%s %d\n", &name, &val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, errors.Wrapf(err, "failed scanning %s", memPathV1)
		}

		if name == "rss" || name == "cache" {
			memUtil += val
			numMemStats++
			if numMemStats == 2 {
				break
			}
		}
	}
	if numMemStats != 2 {
		return 0, 0, fmt.Errorf("failed scanning %s", memPathV1)
	}

	// For reference, see https://www.kernel.org/doc/Documentation/cgroup-v1/cpuacct.txt
	cpuR, err := readFileNoStat(cpuPathV1)
	if err != nil {
		return 0, 0, err
	}
	defer cpuR.Close()
	// CPU time in nanoseconds
	var cpuTime uint64
	if _, err := fmt.Fscanf(cpuR, "%d", &cpuTime); err != nil {
		return 0, 0, errors.Wrapf(err, "failed scanning %s", cpuPathV1)
	}

	return float64(cpuTime) / 1e09, memUtil, nil
}

func newCgroupV1Scanner() (utilizationScanner, bool) {
	if _, err := os.Stat(memPathV1); err != nil {
		return cgroupV1Scanner{}, false
	}
	if _, err := os.Stat(cpuPathV1); err != nil {
		return cgroupV1Scanner{}, false
	}
	return cgroupV1Scanner{}, true
}

type procfsScanner struct {
	proc procfs.Proc
}

func (s procfsScanner) Method() string {
	return "/proc"
}

func (s procfsScanner) Scan() (float64, uint64, error) {
	ps, err := s.proc.Stat()
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get process stats")
	}

	return ps.CPUTime(), uint64(ps.ResidentMemory()), nil
}

func newProcfsScanner() (utilizationScanner, bool) {
	p, err := procfs.Self()
	if err != nil {
		return procfsScanner{}, false
	}

	return procfsScanner{
		proc: p,
	}, true
}

// UtilizationBasedLimiter is a Service offering limiting based on CPU and memory utilization.
//
// The respective CPU and memory utilization thresholds are configurable.
type UtilizationBasedLimiter struct {
	services.Service

	logger             log.Logger
	utilizationScanner utilizationScanner

	// Memory threshold in bytes
	memoryThreshold uint64
	// CPU threshold in cores
	cpuThreshold float64
	// Last CPU time counter
	lastCPUTime float64
	// The time of the last update
	lastUpdate     time.Time
	movingAvg      ewma.MovingAverage
	LimitingReason atomic.String
}

// NewUtilizationBasedLimiter returns a UtilizationBasedLimiter configured with cpuThreshold and memoryThreshold.
func NewUtilizationBasedLimiter(cpuThreshold float64, memoryThreshold uint64, logger log.Logger) *UtilizationBasedLimiter {
	l := &UtilizationBasedLimiter{
		logger:          logger,
		cpuThreshold:    cpuThreshold,
		memoryThreshold: memoryThreshold,
		// Use a minute long window, each sample being a second apart
		movingAvg: ewma.NewMovingAverage(60),
	}
	l.Service = services.NewTimerService(resourceUtilizationUpdateInterval, l.init, l.update, nil)
	return l
}

func (l *UtilizationBasedLimiter) init(_ context.Context) error {
	s, ok := newCgroupV2Scanner()
	if ok {
		l.utilizationScanner = s
		return nil
	}
	s, ok = newCgroupV1Scanner()
	if ok {
		l.utilizationScanner = s
		return nil
	}
	s, ok = newProcfsScanner()
	if ok {
		l.utilizationScanner = s
		return nil
	}

	return fmt.Errorf("unable to detect CPU/memory utilization, unsupported platform")
}

func (l *UtilizationBasedLimiter) update(ctx context.Context) error {
	cpuTime, memUtil, err := l.utilizationScanner.Scan()
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats", "method",
			l.utilizationScanner.Method(), "err", err.Error())
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
	l.movingAvg.Add(cpuUtil)
	cpuA := l.movingAvg.Value()

	level.Debug(l.logger).Log("msg", "process resource utilization", "method", l.utilizationScanner.Method(),
		"memory_utilization", memUtil, "smoothed_cpu_utilization", cpuA, "raw_cpu_utilization", cpuUtil)

	memPercent := 100 * (float64(memUtil) / float64(l.memoryThreshold))
	cpuPercent := 100 * (cpuA / l.cpuThreshold)

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
			"reason", reason, "memory_threshold", l.memoryThreshold,
			"memory_percentage_of_threshold", memPercent, "cpu_threshold", l.cpuThreshold,
			"cpu_percentage_of_threshold", cpuPercent)
	} else {
		level.Info(l.logger).Log("msg", "disabling resource utilization based limiting",
			"memory_threshold", l.memoryThreshold, "memory_percentage_of_threshold", memPercent,
			"cpu_threshold", l.cpuThreshold, "cpu_percentage_of_threshold", cpuPercent)
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
