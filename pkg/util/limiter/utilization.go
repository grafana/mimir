// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
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
	// Scan returns CPU time in seconds, memory RSS in bytes, and memory working set in bytes, or an error.
	Scan() (float64, uint64, uint64, error)
}

type procfsScanner struct {
	proc procfs.Proc
}

func (s procfsScanner) Scan() (float64, uint64, uint64, error) {
	ps, err := s.proc.Stat()
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "failed to get process stats")
	}

	// We lack a definition of working set via procfs
	return ps.CPUTime(), uint64(ps.ResidentMemory()), 0, nil
}

func newProcfsScanner() (procfsScanner, error) {
	p, err := procfs.Self()
	return procfsScanner{
		proc: p,
	}, err
}

const cgroupV1MemStatPath = "/sys/fs/cgroup/memory/memory.stat"

type cgroupV1Scanner struct {
	procfsScanner
}

func (s cgroupV1Scanner) Scan() (float64, uint64, uint64, error) {
	const memUsagePath = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	const memHierarchyPath = "/sys/fs/cgroup/memory/memory.use_hierarchy"

	// For the time being, get CPU utilization from procfs since it's well tested
	cpuUtil, _, _, err := s.procfsScanner.Scan()
	if err != nil {
		return 0, 0, 0, err
	}

	useHierarchy, err := readUint64FromFile(memHierarchyPath)
	if err != nil {
		return 0, 0, 0, err
	}
	workingSet, err := readUint64FromFile(memUsagePath)
	if err != nil {
		return 0, 0, 0, err
	}

	memR, err := readFileNoStat(cgroupV1MemStatPath)
	if err != nil {
		return 0, 0, 0, errors.Wrapf(err, "failed opening %s", cgroupV1MemStatPath)
	}
	defer memR.Close()

	var rss uint64
	foundRSS := false
	rssField := "rss"
	if useHierarchy == 1 {
		// Hierarchy is enabled for the cgroup
		rssField = "total_rss"
	}
	for {
		var name string
		var val uint64
		if _, err := fmt.Fscanf(memR, "%s %d\n", &name, &val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, 0, errors.Wrapf(err, "failed scanning %s", cgroupV1MemStatPath)
		}

		switch name {
		case "total_inactive_file:":
			if workingSet < val {
				workingSet = 0
			} else {
				workingSet -= val
			}
		case rssField:
			rss = val
			foundRSS = true
		}
	}
	if !foundRSS {
		return 0, 0, 0, fmt.Errorf("failed scanning %s", cgroupV1MemStatPath)
	}

	return cpuUtil, rss, workingSet, nil
}

func newCgroupV1Scanner() (cgroupV1Scanner, error) {
	// Verify that cgroup v1 is available
	r, err := readFileNoStat(cgroupV1MemStatPath)
	if err != nil {
		return cgroupV1Scanner{}, errors.Wrapf(err, "failed opening %s", cgroupV1MemStatPath)
	}
	r.Close()

	procfsScanner, err := newProcfsScanner()
	return cgroupV1Scanner{
		procfsScanner: procfsScanner,
	}, err
}

type cgroupV2Scanner struct {
	procfsScanner
}

// This file is v2 only
const cgroupV2MemUtilPath = "/sys/fs/cgroup/memory.current"

func (s cgroupV2Scanner) Scan() (float64, uint64, uint64, error) {
	// For the time being, get CPU utilization from procfs since it's well tested
	cpuUtil, _, _, err := s.procfsScanner.Scan()
	if err != nil {
		return 0, 0, 0, err
	}

	// For reference, see https://git.kernel.org/pub/scm/linux/kernel/git/tj/cgroup.git/tree/Documentation/admin-guide/cgroup-v2.rst
	const memStatPath = "/sys/fs/cgroup/memory.stat"

	workingSet, err := readUint64FromFile(cgroupV2MemUtilPath)
	if err != nil {
		return 0, 0, 0, err
	}

	memR, err := readFileNoStat(memStatPath)
	if err != nil {
		return 0, 0, 0, errors.Wrapf(err, "failed opening %s", memStatPath)
	}
	defer memR.Close()

	var rss uint64
	foundRSS := false
	for {
		var name string
		var val uint64
		if _, err := fmt.Fscanf(memR, "%s %d\n", &name, &val); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, 0, 0, errors.Wrapf(err, "failed scanning %s", memStatPath)
		}

		switch name {
		case "anon:":
			rss = val
			foundRSS = true
		case "inactive_file":
			if val > workingSet {
				workingSet = 0
			} else {
				workingSet -= val
			}
		}
	}
	if !foundRSS {
		return 0, 0, 0, fmt.Errorf("failed scanning %s", memStatPath)
	}

	return cpuUtil, rss, workingSet, nil
}

func newCgroupV2Scanner() (cgroupV2Scanner, error) {
	r, err := readFileNoStat(cgroupV2MemUtilPath)
	if err != nil {
		return cgroupV2Scanner{}, errors.Wrapf(err, "failed opening %s", cgroupV2MemUtilPath)
	}
	r.Close()

	procfsScanner, err := newProcfsScanner()
	return cgroupV2Scanner{
		procfsScanner: procfsScanner,
	}, err
}

func readUint64FromFile(path string) (uint64, error) {
	r, err := readFileNoStat(path)
	if err != nil {
		return 0, errors.Wrapf(err, "failed opening %s", path)
	}
	defer r.Close()

	var val uint64
	if _, err := fmt.Fscanf(r, "%d\n", &val); err != nil {
		return 0, errors.Wrapf(err, "failed scanning %s", path)
	}

	return val, nil
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
	// Last CPU utilization time counter.
	lastCPUTime float64
	// The time of the first CPU update.
	firstCPUUpdate time.Time
	// The time of the last CPU update.
	lastCPUUpdate        time.Time
	cpuMovingAvg         *math.EwmaRate
	limitingReason       atomic.String
	currCPUUtil          atomic.Float64
	currMemoryRSS        atomic.Uint64
	currMemoryWorkingSet atomic.Uint64
	// For logging of input to CPU load EWMA calculation, keep window of source samples
	cpuSamples *cpuSampleBuffer
}

// NewUtilizationBasedLimiter returns a UtilizationBasedLimiter configured with cpuLimit and memoryLimit.
func NewUtilizationBasedLimiter(cpuLimit float64, memoryLimit uint64, logCPUSamples bool, logger log.Logger,
	reg prometheus.Registerer) *UtilizationBasedLimiter {
	// Calculate alpha for a minute long window
	// https://github.com/VividCortex/ewma#choosing-alpha
	alpha := 2 / (resourceUtilizationSlidingWindow.Seconds()/resourceUtilizationUpdateInterval.Seconds() + 1)
	var cpuSamples *cpuSampleBuffer
	if logCPUSamples {
		cpuSamples = newCPUSampleBuffer(int(resourceUtilizationSlidingWindow.Seconds()))
	}
	l := &UtilizationBasedLimiter{
		logger:      logger,
		cpuLimit:    cpuLimit,
		memoryLimit: memoryLimit,
		// Use a minute long window, each sample being a second apart
		cpuMovingAvg: math.NewEWMARate(alpha, resourceUtilizationUpdateInterval),
		cpuSamples:   cpuSamples,
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
			return float64(max(l.currMemoryRSS.Load(), l.currMemoryWorkingSet.Load()))
		})
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "utilization_limiter_current_memory_rss_bytes",
			Help: "Current memory RSS calculated by utilization based limiter.",
		}, func() float64 {
			return float64(l.currMemoryRSS.Load())
		})
		promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "utilization_limiter_current_memory_working_set_bytes",
			Help: "Current memory working set calculated by utilization based limiter.",
		}, func() float64 {
			return float64(l.currMemoryWorkingSet.Load())
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
	var err error
	l.utilizationScanner, err = newCgroupV2Scanner()
	if err == nil {
		return nil
	}
	l.utilizationScanner, err = newCgroupV1Scanner()
	if err == nil {
		return nil
	}
	l.utilizationScanner, err = newProcfsScanner()
	return errors.Wrap(err, "unable to detect CPU/memory utilization, unsupported platform. Please disable utilization based limiting")
}

func (l *UtilizationBasedLimiter) update(_ context.Context) error {
	l.compute(time.Now)
	return nil
}

// compute and return the current CPU and memory utilization.
// This function must be called at a regular interval (resourceUtilizationUpdateInterval) to get a predictable behaviour.
func (l *UtilizationBasedLimiter) compute(nowFn func() time.Time) (currCPUUtil float64, currMemoryUtil uint64) {
	cpuTime, currRSS, currWorkingSet, err := l.utilizationScanner.Scan()
	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to get CPU and memory stats", "err", err.Error())
		// Disable any limiting, since we can't tell resource utilization
		l.limitingReason.Store("")
		return
	}

	// Get wall time after CPU time, in case there's a delay before CPU time is returned,
	// which would cause us to compute too high of a CPU load
	now := nowFn()
	l.currMemoryRSS.Store(currRSS)
	l.currMemoryWorkingSet.Store(currWorkingSet)

	// Add the instant CPU utilization to the moving average. The instant CPU
	// utilization can only be computed starting from the 2nd tick.
	if prevUpdate, prevCPUTime := l.lastCPUUpdate, l.lastCPUTime; !prevUpdate.IsZero() {
		// Provided that nowFn returns a Time with monotonic clock reading (like time.Now()), time.Sub is robust
		// against wall clock changes, since it's based on the monotonic clock:
		// https://pkg.go.dev/time#hdr-Monotonic_Clocks
		timeSincePrevUpdate := now.Sub(prevUpdate)

		// We expect the CPU utilization to be updated at a regular interval (resourceUtilizationUpdateInterval).
		// Under some edge conditions (e.g. overloaded process / node), the time.Ticker used to periodically call
		// the UtilizationBasedLimiter.compute() function, may call compute() two times consecutively (or with a very
		// short delay). We detect these cases and skip the update (it will be updated during the next regular tick).
		if timeSincePrevUpdate > resourceUtilizationUpdateInterval/2 {
			cpuUtil := (cpuTime - prevCPUTime) / timeSincePrevUpdate.Seconds()
			l.cpuMovingAvg.Add(int64(cpuUtil * 100))
			l.cpuMovingAvg.Tick()
			if l.cpuSamples != nil {
				l.cpuSamples.Add(cpuUtil)
			}

			l.lastCPUUpdate = now
			l.lastCPUTime = cpuTime
		}
	} else {
		// First time we read the CPU utilization.
		l.lastCPUUpdate = now
		l.lastCPUTime = cpuTime
	}

	// The CPU utilization moving average requires a warmup period before getting
	// stable results. In this implementation we use a warmup period equal to the
	// sliding window. During the warmup, the reported CPU utilization will be 0.
	if l.firstCPUUpdate.IsZero() {
		l.firstCPUUpdate = now
	} else if now.Sub(l.firstCPUUpdate) >= resourceUtilizationSlidingWindow {
		currCPUUtil = l.cpuMovingAvg.Rate() / 100
		l.currCPUUtil.Store(currCPUUtil)
	}

	// If running in a container, we should be able to measure both memory RSS and working set (otherwise just RSS).
	// Under Kubernetes, the OOM killer should kill a container if either RSS or working set reaches the memory limit,
	// so take the max.
	currMemoryUtil = max(currRSS, currWorkingSet)

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
		logger := l.logger
		if l.cpuSamples != nil {
			// Log also the CPU samples the CPU load EWMA is based on
			logger = log.WithSuffix(logger, "source_samples", l.cpuSamples.String())
		}
		level.Info(logger).Log("msg", "enabling resource utilization based limiting",
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

// cpuSampleBuffer is a circular buffer of CPU samples.
type cpuSampleBuffer struct {
	samples []float64
	head    int
}

func newCPUSampleBuffer(size int) *cpuSampleBuffer {
	return &cpuSampleBuffer{
		samples: make([]float64, size),
	}
}

// Add adds a sample to the buffer.
func (b *cpuSampleBuffer) Add(sample float64) {
	b.samples[b.head] = sample
	b.head = (b.head + 1) % len(b.samples)
}

// String returns a comma-separated string representation of the buffer.
func (b *cpuSampleBuffer) String() string {
	var sb strings.Builder
	for i := range b.samples {
		s := b.samples[(b.head+i)%len(b.samples)]
		sb.WriteString(fmt.Sprintf("%.2f", s))
		if i < len(b.samples)-1 {
			sb.WriteByte(',')
		}
	}

	return sb.String()
}
