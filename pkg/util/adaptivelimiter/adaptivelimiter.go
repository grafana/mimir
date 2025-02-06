// SPDX-License-Identifier: AGPL-3.0-only

package adaptivelimiter

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/influxdata/tdigest"

	math2 "github.com/grafana/mimir/pkg/util/math"
	sync2 "github.com/grafana/mimir/pkg/util/sync"
)

// ErrExceeded is returned when an execution exceeds the current limit.
var ErrExceeded = errors.New("limit exceeded")

const (
	warmupSamples   = 10
	smoothedSamples = 5
)

// Metrics provides info about the adaptive limiter.
//
// This type is concurrency safe.
type Metrics interface {
	// Limit returns the concurrent execution limit, as calculated by the adaptive limiter.
	Limit() int

	// Inflight returns the current number of inflight executions.
	Inflight() int

	// Blocked returns the current number of blocked executions.
	Blocked() int

	// RejectionRate for blocking limiters returns the current rate, from 0 to 1, at which the limiter will reject requests.
	// Returns 0 for limiters that are not blocking.
	RejectionRate() float64
}

// Permit is a permit to perform an execution that must be completed by calling Record or Drop.
type Permit interface {
	// Record records an execution completion and releases a permit back to the limiter. The execution duration will be used
	// to influence the limiter.
	Record()

	// Drop releases an execution permit back to the limiter without recording a completion. This should be used when an
	// execution completes prematurely, such as via a timeout, and we don't want the execution duration to influence the
	// limiter.
	Drop()
}

type Config struct {
	Enabled                bool          `yaml:"enabled" category:"experimental"`
	ShortWindowMinDuration time.Duration `yaml:"short_window_min_duration" category:"experimental"`
	ShortWindowMaxDuration time.Duration `yaml:"short_window_max_duration" category:"experimental"`
	ShortWindowMinSamples  uint          `yaml:"short_window_min_samples" category:"experimental"`
	LongWindow             uint          `yaml:"long_window" category:"experimental"`
	SampleQuantile         float64       `yaml:"sample_quantile" category:"experimental"`
	MinInflightLimit       uint          `yaml:"min_inflight_limit" category:"experimental"`
	MaxInflightLimit       uint          `yaml:"max_inflight_limit" category:"experimental"`
	InitialInflightLimit   uint          `yaml:"initial_inflight_limit" category:"experimental"`
	MaxLimitFactor         float64       `yaml:"max_limit_factor" category:"experimental"`
	CorrelationWindow      uint          `yaml:"correlation_window" category:"experimental"`
	InitialRejectionFactor float64       `yaml:"initial_rejection_factor" category:"experimental"`
	MaxRejectionFactor     float64       `yaml:"max_rejection_factor" category:"experimental"`

	alphaFunc, betaFunc        func(int) int
	increaseFunc, decreaseFunc func(int) int
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable adaptive limiting when making requests to ingesters")
	f.DurationVar(&cfg.ShortWindowMinDuration, prefix+"short-window-min-duration", time.Second, "Min duration of the window that is used to determine recent, short-term load on the system")
	f.DurationVar(&cfg.ShortWindowMaxDuration, prefix+"short-window-max-duration", 30*time.Second, "Max duration of the window that is used to determine recent, short-term load on the system")
	f.UintVar(&cfg.ShortWindowMinSamples, prefix+"short-window-min-samples", 50, "Min number of samples that must be recorded in the window")
	f.UintVar(&cfg.LongWindow, prefix+"long-window", 60, "Short-term window measurements that will be stored in an exponentially weighted moving average window, representing the long-term baseline inflight time")
	f.Float64Var(&cfg.SampleQuantile, prefix+"sample-quantile", .9, "The quantile of recorded response times to consider when adjusting the concurrency limit")
	f.UintVar(&cfg.MinInflightLimit, prefix+"min-inflight-limit", 2, "Min inflight requests limit")
	f.UintVar(&cfg.MaxInflightLimit, prefix+"max-inflight-limit", 200, "Max inflight requests limit")
	f.UintVar(&cfg.InitialInflightLimit, prefix+"initial-inflight-limit", 20, "Initial inflight requests limit")
	f.Float64Var(&cfg.MaxLimitFactor, prefix+"max-limit-factor", 5, "The max limit as a multiple of current inflight requests")
	f.UintVar(&cfg.CorrelationWindow, prefix+"correlation-window", 50, "How many recent limit and inflight time measurements are stored to detect whether increases in limits correlate with increases in inflight times")
	f.Float64Var(&cfg.InitialRejectionFactor, prefix+"initial-rejection-factor", 2, "The number of allowed queued requests, as a multiple of current inflight requests, after which rejections will start")
	f.Float64Var(&cfg.MaxRejectionFactor, prefix+"max-rejection-factor", 3, "The number of allowed queued requests, as a multiple of current inflight requests, after which all requests will be rejected")
}

func newLimiter(config *Config, logger log.Logger) *adaptiveLimiter {
	config.alphaFunc = math2.Log10Func(3)
	config.betaFunc = math2.Log10Func(6)
	config.increaseFunc = math2.Log10Func(1)
	config.decreaseFunc = math2.Log10Func(1)
	return &adaptiveLimiter{
		logger:                logger,
		config:                config,
		minLimit:              float64(config.MinInflightLimit),
		maxLimit:              float64(config.MaxInflightLimit),
		semaphore:             sync2.NewDynamicSemaphore(int64(config.InitialInflightLimit)),
		limit:                 float64(config.InitialInflightLimit),
		shortRTT:              &tDigestSample{TDigest: tdigest.NewWithCompression(100)},
		longRTT:               math2.NewEwma(config.LongWindow, warmupSamples),
		nextUpdateTime:        time.Now(),
		rttCorrelation:        math2.NewCorrelationWindow(config.CorrelationWindow, warmupSamples),
		throughputCorrelation: math2.NewCorrelationWindow(config.CorrelationWindow, warmupSamples),
		medianFilter:          math2.NewMedianFilter(smoothedSamples),
		smoothedShortRTT:      math2.NewEwma(smoothedSamples, warmupSamples),
	}
}

type limitChange int

const (
	increase limitChange = iota
	decrease
	hold
)

type tDigestSample struct {
	MinRTT      time.Duration
	MaxInflight int
	Size        uint
	*tdigest.TDigest
}

func (td *tDigestSample) Add(rtt time.Duration, inflight int) {
	if td.Size == 0 {
		td.MinRTT = rtt
		td.MaxInflight = inflight
	} else {
		td.MinRTT = min(td.MinRTT, rtt)
		td.MaxInflight = max(td.MaxInflight, inflight)
	}
	td.Size++
	td.TDigest.Add(float64(rtt), 1)
}

func (td *tDigestSample) Reset() {
	td.TDigest.Reset()
	td.MinRTT = 0
	td.MaxInflight = 0
	td.Size = 0
}

type adaptiveLimiter struct {
	logger             log.Logger
	config             *Config
	minLimit, maxLimit float64

	// Mutable state
	semaphore *sync2.DynamicSemaphore
	mu        sync.Mutex

	// Guarded by mu
	limit            float64        // The current concurrency limit
	shortRTT         *tDigestSample // Short term execution times
	medianFilter     *math2.MedianFilter
	smoothedShortRTT math2.Ewma
	longRTT          math2.Ewma // Tracks long term average execution time
	nextUpdateTime   time.Time  // Tracks when the limit can next be updated

	throughputCorrelation *math2.CorrelationWindow // Tracks the correlation between concurrency and throughput
	rttCorrelation        *math2.CorrelationWindow // Tracks the correlation between concurrency and round trip times (RTT)
}

func (l *adaptiveLimiter) AcquirePermit(ctx context.Context) (Permit, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	err := l.semaphore.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &recordingPermit{
		limiter:         l,
		startTime:       time.Now(),
		currentInflight: l.semaphore.Used(),
	}, nil
}

func (l *adaptiveLimiter) TryAcquirePermit() (Permit, bool) {
	if !l.semaphore.TryAcquire() {
		return nil, false
	}
	return &recordingPermit{
		limiter:         l,
		startTime:       time.Now(),
		currentInflight: l.semaphore.Used(),
	}, true
}

func (l *adaptiveLimiter) CanAcquirePermit() bool {
	return !l.semaphore.IsFull()
}

func (l *adaptiveLimiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.limit)
}

func (l *adaptiveLimiter) Inflight() int {
	return l.semaphore.Used()
}

func (l *adaptiveLimiter) Blocked() int {
	return l.semaphore.Waiters()
}

func (l *adaptiveLimiter) RejectionRate() float64 {
	return 0
}

// Records the duration of a completed execution, updating the concurrency limit if the short shortRTT window is full.
func (l *adaptiveLimiter) record(now time.Time, rtt time.Duration, inflight int, dropped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !dropped {
		l.shortRTT.Add(rtt, inflight)
	}

	if now.After(l.nextUpdateTime) && l.shortRTT.Size >= l.config.ShortWindowMinSamples {
		quantile := l.shortRTT.Quantile(l.config.SampleQuantile)
		filteredRTT := l.medianFilter.Add(quantile)
		smoothedRTT := l.smoothedShortRTT.Add(filteredRTT)
		l.updateLimit(smoothedRTT, l.shortRTT.MaxInflight)
		minRTT := l.shortRTT.MinRTT
		l.shortRTT.Reset()
		minWindowTime := max(minRTT*2, l.config.ShortWindowMinDuration)
		l.nextUpdateTime = now.Add(min(minWindowTime, l.config.ShortWindowMaxDuration))
	}

	l.semaphore.Release()
}

// updateLimit updates the concurrency limit based on the gradient between the shortRTT and historical longRTT.
// A stability check prevents unnecessary decreases during steady state.
// A correlation adjustment prevents upward drift during overload.
func (l *adaptiveLimiter) updateLimit(shortRTT float64, inflight int) {
	// Update long term RTT and calculate the queue size
	// This is the primary signal that we threshold off of to detect overload
	longRTT := l.longRTT.Add(shortRTT)
	gradient := longRTT / shortRTT
	queueSize := int(math.Ceil(l.limit * (1 - gradient)))

	// Calculate throughput correlation, throughput CV, and RTT correlation
	// These are the secondary signals that we threshold off of to detect overload
	throughput := float64(inflight) / (shortRTT / 1e9) // Convert to RPS
	throughputCorr, _, throughputCV := l.throughputCorrelation.Add(float64(inflight), throughput)
	rttCorr, _, _ := l.rttCorrelation.Add(float64(inflight), shortRTT)

	// Additional values for thresholding the limit
	overloaded := l.semaphore.IsFull()
	alpha := l.config.alphaFunc(int(l.limit)) // alpha is the queueSize threshold below which we increase
	beta := l.config.betaFunc(int(l.limit))   // beta is the queueSize threshold above which we decrease

	change, reason := computeChange(queueSize, alpha, beta, overloaded, throughputCorr, throughputCV, rttCorr)

	newLimit := l.limit
	var direction string
	switch change {
	case decrease:
		direction = "decrease"
		newLimit = l.limit - float64(l.config.decreaseFunc(int(l.limit)))
	case increase:
		direction = "increase"
		newLimit = l.limit + float64(l.config.increaseFunc(int(l.limit)))
	default:
		direction = "hold"
	}

	// Decrease the limit if needed, based on the max limit factor
	if newLimit > float64(inflight)*l.config.MaxLimitFactor {
		direction = "decrease"
		reason = "max"
		newLimit = l.limit - float64(l.config.decreaseFunc(int(l.limit)))
	}

	// Clamp the limit
	if newLimit > l.maxLimit {
		if l.limit == l.maxLimit {
			direction = "hold"
			reason = "max"
		}
		newLimit = l.maxLimit
	} else if newLimit < l.minLimit {
		if l.limit == l.minLimit {
			direction = "hold"
			reason = "min"
		}
		newLimit = l.minLimit
	}

	l.logLimit(direction, reason, newLimit, gradient, queueSize, inflight, shortRTT, longRTT, rttCorr, throughput, throughputCorr, throughputCV)

	l.semaphore.SetSize(int64(newLimit))
	l.limit = newLimit
}

func computeChange(queueSize, alpha, beta int, overloaded bool, throughputCorr, throughputCV, rttCorr float64) (change limitChange, reason string) {
	if queueSize > beta {
		// This condition handles severe overload where recent RTT significantly exceeds the baseline
		return decrease, "queue"
	} else if overloaded && throughputCorr < 0 {
		// This condition prevents runaway limit increases during moderate overload where inflight is increasing but throughput is decreasing
		return decrease, "thrptCorr"
	} else if overloaded && throughputCorr < .3 && rttCorr > .5 {
		// This condition prevents runaway limit increases during moderate overload where throughputCorr is weak and rttCorr is high
		// This indicates overload since latency is increasing with inflight, but throughput is not
		return decrease, "thrptCorrRtt"
	} else if overloaded && throughputCV < .2 && rttCorr > .5 {
		// This condition prevents runaway limit increases during moderate overload where throughputCV low and rttCorr is high
		// This indicates overload since latency is increasing with inflight, but throughput is not
		return decrease, "thrptCV"
	} else if queueSize < alpha {
		// If our queue size is sufficiently small, increase until we detect overload
		return increase, "queue"
	}
	// If queueSize is between alpha and beta, leave the limit unchanged
	return hold, "queue"
}

func (l *adaptiveLimiter) logLimit(direction, reason string, limit float64, gradient float64, queueSize, inflight int, shortRTT, longRTT, rttCorr, throughput, throughputCorr, throughputCV float64) {
	level.Debug(l.logger).Log("msg", "limit update",
		"direction", direction,
		"reason", reason,
		"limit", fmt.Sprintf("%.2f", limit),
		"gradient", fmt.Sprintf("%.2f", gradient),
		"queueSize", fmt.Sprintf("%d", queueSize),
		"inflight", inflight,
		"shortRTT", time.Duration(shortRTT).Round(time.Microsecond),
		"longRTT", time.Duration(longRTT).Round(time.Microsecond),
		"thrpt", fmt.Sprintf("%.2f", throughput),
		"thrptCorr", fmt.Sprintf("%.2f", throughputCorr),
		"thrptCV", fmt.Sprintf("%.2f", throughputCV),
		"rttCorr", fmt.Sprintf("%.2f", rttCorr))
}

type recordingPermit struct {
	limiter         *adaptiveLimiter
	startTime       time.Time
	currentInflight int
}

func (p *recordingPermit) Record() {
	now := time.Now()
	p.limiter.record(now, now.Sub(p.startTime), p.currentInflight, false)
}

func (p *recordingPermit) Drop() {
	now := time.Now()
	p.limiter.record(now, now.Sub(p.startTime), p.currentInflight, true)
}
