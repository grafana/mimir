// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
)

const (
	// This is the closest fitting Prometheus API error code for requests rejected due to limiting.
	queryLimitingCode = http.StatusServiceUnavailable

	// Interval for updating resource (CPU/memory) utilization
	resourceUtilizationUpdateInterval = time.Second
)

func scanCgroupV2() (float64, uint64, error) {
	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	const memPath = "/sys/fs/cgroup/memory.current"
	memR, err := readFileNoStat(memPath)
	if err != nil {
		return 0, 0, err
	}
	defer memR.Close()
	var memUtil uint64
	if _, err := fmt.Fscanf(memR, "%d", &memUtil); err != nil {
		return 0, 0, errors.Wrapf(err, "failed scanning %s", memPath)
	}

	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	const cpuPath = "/sys/fs/cgroup/cpu.stat"
	cpuR, err := readFileNoStat(cpuPath)
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

			return 0, 0, errors.Wrapf(err, "failed scanning %s", cpuPath)
		}

		if name == "usage_usec" {
			// This is CPU time in microseconds
			break
		}
	}
	if name != "usage_usec" {
		return 0, 0, fmt.Errorf("failed scanning %s", cpuPath)
	}

	return float64(cpuTime) / 1e06, memUtil, nil
}

func scanCgroupV1() (float64, uint64, error) {
	const memPath = "/sys/fs/cgroup/memory/memory.stat"
	memR, err := readFileNoStat(memPath)
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

			return 0, 0, errors.Wrapf(err, "failed scanning %s", memPath)
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
		return 0, 0, fmt.Errorf("failed scanning %s", memPath)
	}

	// For reference, see https://www.kernel.org/doc/Documentation/cgroup-v1/cpuacct.txt
	const cpuPath = "/sys/fs/cgroup/cpu/cpuacct.usage"
	cpuR, err := readFileNoStat(cpuPath)
	if err != nil {
		return 0, 0, err
	}
	defer cpuR.Close()
	// CPU time in nanoseconds
	var cpuTime uint64
	if _, err := fmt.Fscanf(cpuR, "%d", &cpuTime); err != nil {
		return 0, 0, errors.Wrapf(err, "failed scanning %s", cpuPath)
	}

	return float64(cpuTime) / 1e09, memUtil, nil
}

func scanProcFS() (float64, uint64, error) {
	p, err := procfs.Self()
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get process info")
	}
	ps, err := p.Stat()
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get process stats")
	}

	return ps.CPUTime(), uint64(ps.ResidentMemory()), nil
}

type utilizationBasedLimiter struct {
	services.Service

	logger log.Logger

	// Read path memory threshold in bytes
	readPathMemoryThreshold uint64
	// Read path CPU threshold as a fraction of 1
	readPathCPUThreshold float64
	// Memory utilization in bytes
	memoryUtilization atomic.Uint64
	// CPU utilization as fraction
	cpuUtilization atomic.Float64
	// Last CPU time counter
	lastCPUTime atomic.Float64
	// Last time resource utilization was computed
	lastResourceUtilizationUpdate atomic.Time
	movingAvg                     ewma.MovingAverage
}

func newUtilizationBasedLimiter(cfg Config, logger log.Logger) *utilizationBasedLimiter {
	l := &utilizationBasedLimiter{
		logger:                  logger,
		readPathCPUThreshold:    cfg.ReadPathUtilizationRatio * cfg.CPUUtilizationTarget,
		readPathMemoryThreshold: uint64(cfg.ReadPathUtilizationRatio * float64(cfg.MemoryUtilizationTarget)),
		// Use a minute long window, each sample being a second apart
		movingAvg: ewma.NewMovingAverage(60),
	}
	l.Service = services.NewTimerService(resourceUtilizationUpdateInterval, nil, l.updateResourceUtilization, nil)
	return l
}

func (l *utilizationBasedLimiter) updateResourceUtilization(ctx context.Context) error {
	lastUpdate := l.lastResourceUtilizationUpdate.Load()

	var method string
	cpuTime, memUtil, err := scanCgroupV2()
	if err != nil && !os.IsNotExist(err) {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats from cgroup v2", "err", err.Error())
		return ctx.Err()
	}
	if err == nil {
		method = "cgroup v2"
	} else {
		// cgroup v2 not detected, try v1
		cpuTime, memUtil, err = scanCgroupV1()
		if err != nil && !os.IsNotExist(err) {
			level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats from cgroup v1", "err", err.Error())
			return ctx.Err()
		}
		if err == nil {
			method = "cgroup v1"
		} else {
			// cgroup not detected, fall back to /proc
			cpuTime, memUtil, err = scanProcFS()
			if err != nil {
				level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats from /proc", "err", err.Error())
				return ctx.Err()
			}
			method = "/proc"
		}
	}

	l.memoryUtilization.Store(memUtil)

	now := time.Now().UTC()

	lastCPUTime := l.lastCPUTime.Load()
	l.lastCPUTime.Store(cpuTime)

	l.lastResourceUtilizationUpdate.Store(now)

	if lastUpdate.IsZero() {
		return ctx.Err()
	}

	cpuUtil := (cpuTime - lastCPUTime) / now.Sub(lastUpdate).Seconds()
	l.movingAvg.Add(cpuUtil)
	cpuA := l.movingAvg.Value()
	l.cpuUtilization.Store(cpuA)

	level.Debug(l.logger).Log("msg", "process resource utilization", "method", method, "memory_utilization", memUtil,
		"smoothed_cpu_utilization", cpuA, "raw_cpu_utilization", cpuUtil)
	return ctx.Err()
}

// checkReadOverloaded checks whether the ingester read path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkReadOverloaded() error {
	if !i.cfg.UtilizationBasedLimitingEnabled {
		return nil
	}

	return i.utilizationBasedLimiter.checkReadOverloaded()
}

func (l *utilizationBasedLimiter) checkReadOverloaded() error {
	memUtil := l.memoryUtilization.Load()
	cpuUtil := l.cpuUtilization.Load()
	lastUpdate := l.lastResourceUtilizationUpdate.Load()
	if lastUpdate.IsZero() {
		return nil
	}

	memPercent := 100 * (float64(memUtil) / float64(l.readPathMemoryThreshold))
	cpuPercent := 100 * (cpuUtil / l.readPathCPUThreshold)

	var reason string
	if memPercent >= 100 {
		reason = "memory"
	} else if cpuPercent >= 100 {
		reason = "cpu"
	}
	if reason != "" {
		level.Debug(l.logger).Log("msg", "read path resource utilization based limiting", "reason", reason,
			"memory_threshold", l.readPathMemoryThreshold, "memory_percentage_of_threshold", memPercent,
			"cpu_threshold", l.readPathCPUThreshold, "cpu_percentage_of_threshold", cpuPercent,
			"memory_utilization", memUtil, "cpu_utilization", cpuUtil)

		return httpgrpc.Errorf(queryLimitingCode, "the ingester is currently too busy to process queries, try again later")
	}

	return nil
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
