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

	memPathV2 = "/sys/fs/cgroup/memory.stat"
	cpuPathV2 = "/sys/fs/cgroup/cpu.stat"
	memPathV1 = "/sys/fs/cgroup/memory/memory.stat"
	cpuPathV1 = "/sys/fs/cgroup/cpu/cpuacct.usage"
)

type utilizationScanner interface {
	fmt.Stringer

	// Scan returns CPU time in seconds and memory utilization in bytes, or an error.
	Scan() (float64, uint64, error)
}

type cgroupV2Scanner struct {
}

func (s cgroupV2Scanner) String() string {
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
	found := false
	for {
		var name string
		var val uint64
		if _, err := fmt.Fscanf(memR, "%s %d\n", &name, &val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, errors.Wrapf(err, "failed scanning %s", memPathV2)
		}

		// TODO: Find out if there are more memory types that should be included
		// The "rss" field for cgroup v1 equates anonymous and swap cache memory ("includes transparent hugepages")
		if name == "anon" {
			memUtil += val
			found = true
			break
		}
	}
	if !found {
		return 0, 0, fmt.Errorf("failed scanning %s", memPathV2)
	}

	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	cpuR, err := readFileNoStat(cpuPathV2)
	if err != nil {
		return 0, 0, err
	}
	defer cpuR.Close()
	var name string
	var cpuTime uint64
	const usageUsec = "usage_usec"
	for {
		if _, err := fmt.Fscanf(cpuR, "%s %d\n", &name, &cpuTime); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, errors.Wrapf(err, "failed scanning %s", cpuPathV2)
		}

		if name == usageUsec {
			// This is CPU time in microseconds
			break
		}
	}
	if name != usageUsec {
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

func (s cgroupV1Scanner) String() string {
	return "cgroup v1"
}

func (s cgroupV1Scanner) Scan() (float64, uint64, error) {
	memR, err := readFileNoStat(memPathV1)
	if err != nil {
		return 0, 0, err
	}
	defer memR.Close()
	// Using RSS for memory usage
	// For reference, see section 5.5 in https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
	var memUtil uint64
	found := false
	for {
		var name string
		var val uint64
		if _, err := fmt.Fscanf(memR, "%s %d\n", &name, &val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, errors.Wrapf(err, "failed scanning %s", memPathV1)
		}

		if name == "rss" {
			memUtil += val
			found = true
			break
		}
	}
	if !found {
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
