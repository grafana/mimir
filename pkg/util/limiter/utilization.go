// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
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
	// Last CPU utilization time counter.
	lastCPUTime float64
	// The time of the first CPU update.
	firstCPUUpdate time.Time
	// The time of the last CPU update.
	lastCPUUpdate  time.Time
	cpuMovingAvg   *math.EwmaRate
	limitingReason atomic.String
	currCPUUtil    atomic.Float64
	currMemoryUtil atomic.Uint64
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
