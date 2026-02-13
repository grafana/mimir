package adaptivelimiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/influxdata/tdigest"

	"github.com/failsafe-go/failsafe-go/internal"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal/util"
	"github.com/failsafe-go/failsafe-go/policy"
	"github.com/failsafe-go/failsafe-go/priority"
)

var (
	// ErrExceeded is returned when an execution exceeds the current limit.
	ErrExceeded = errors.New("limit exceeded")

	// Limit thresholding and adjustment functions
	alphaFunc    = util.Log10Func(3)
	betaFunc     = util.Log10Func(6)
	increaseFunc = util.Log10Func(1)
	decreaseFunc = util.Log10Func(1)
)

const (
	warmupSamples   = 10
	smoothedSamples = 5
)

// AdaptiveLimiter is an adaptive concurrency limiter that adjusts its limit up or down based on execution time trends:
//  - When recent execution times are trending up relative to baseline execution times, the concurrency limit is decreased.
//  - When recent execution times are trending down relative to baseline execution times, the concurrency limit is increased.
//
// To accomplish this, recent execution times are tracked and regularly compared to a weighted moving average of
// baseline execution times. Limit increases are additionally controlled to ensure they don't increase execution times.
// Any executions in excess of the limit will be rejected with ErrExceeded.
//
// By default, during overload, an AdaptiveLimiter will converge on a concurrency limit that represents the capacity of
// the machine it's running on, and avoids having executions queue. Since enforcing a limit without allowing for
// queueing is too strict in some cases and may cause unexpected rejections, optional queueing of executions when the
// limiter is full can be enabled via WithQueueing.
//
// Prioritized rejections can be enabled via BuildPrioritized, which accepts a Prioritizer that regularly determines a
// rejection threshold based on recent queueing across limiters.
//
// R is the execution result type. This type is concurrency safe.
type AdaptiveLimiter[R any] interface {
	failsafe.ResultAgnosticPolicy[R]
	Metrics

	// AcquirePermit attempts to acquire a permit to perform an execution via the limiter, waiting until one is available or
	// the execution is canceled. Returns [context.Canceled] if the ctx is canceled. Callers must call Record or Drop to
	// release a successfully acquired permit back to the limiter. ctx may be nil.
	AcquirePermit(context.Context) (Permit, error)

	// AcquirePermitWithMaxWait attempts to acquire a permit to perform an execution via the limiter, waiting until one is
	// available, the execution is canceled, or the maxWaitTime is exceeded.
	AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error)

	// TryAcquirePermit attempts to acquire a permit to perform an execution via the limiter, returning whether the Permit
	// was acquired or not. This method will never block, and only considers whether the limiter is full - it does not allow
	// queueing. Callers must call Record or Drop to release a successfully acquired permit back to the limiter.
	TryAcquirePermit() (Permit, bool)

	// CanAcquirePermit returns whether it's currently possible to acquire a permit.
	CanAcquirePermit() bool

	// Reset resets the limiter to its initial limit.
	Reset()
}

// Metrics provides info about the adaptive limiter.
//
// R is the execution result type. This type is concurrency safe.
type Metrics interface {
	// Limit returns the concurrent execution limit, as calculated by the adaptive limiter.
	Limit() int

	// Inflight returns the current number of inflight executions. The limit is adjusted using the max inflight requests for
	// a sampling period, which may be higher than the amount at a given point in time.
	Inflight() int

	// MaxInflight returns the max number of inflight executions during the most recent sampling period. This is used to
	// adjust the limit at the end of the sampling period, and should reflect how the current limit was set.
	MaxInflight() int

	// Queued returns the current number of queued executions when the limiter is full.
	Queued() int
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

/*
Builder defines base behavior for building AdaptiveLimiter instances.

This type is not concurrency safe.
*/
type Builder[R any] interface {
	// WithLimits configures min, max, and initial concurrency limits.
	//
	// The default values are 1, 200, and 20.
	// Panics if minLimit is not <= maxLimit or initialLimit is not between minLimit and maxLimit.
	WithLimits(minLimit uint, maxLimit uint, initialLimit uint) Builder[R]

	// WithMaxLimitFactor configures a maxLimitFactor which caps the limit as some multiple of the current inflight
	// executions. When the limiter is healthy, the limit will increase up to this multiple of the current inflight
	// executions. This is intended to provide headroom for bursts of executions that may happen during normal operation,
	// avoiding unnecessary rejections before the limiter has time to adjust. For example, with a maxLimitFactor of 5.0:
	//  - 1 inflight causes a max limit of 5
	//  - 10 inflight causes a max limit of 50
	//  - 100 inflight causes a max limit of 500
	//
	// The default value is 5, which means the limit will only rise to 5 times the inflight executions.
	// Panics if maxLimitFactor < 1.
	WithMaxLimitFactor(maxLimitFactor float64) Builder[R]

	// WithMaxLimitFactorDecay allows different effective maxLimitFactor values based on the current inflight requests. By
	// default, decay is disabled, and the max limit factor scales linearly. With a decay, the maxLimitFactor is reduced by
	// the decay for each 10x increase in inflight executions. For example, with maxLimitFactor=5 and decay=1.0:
	//  - 1 inflight causes a 5x factor (max limit of 5)
	//  - 10 inflight causes a 4x factor (max limit of 40)
	//  - 100 inflight causes a 3x factor (max limit of 300)
	//  - 1000 inflight causes a 2x factor (max limit of 2000)
	//
	// Higher maxLimitFactorDecay values make the limit scale more conservatively at higher loads.
	// When a decay is configured, the effective max limit factor will never drop below the minLimitFactor.
	// Panics if maxLimitFactorDecay < 0 or minLimitFactor < 1.
	WithMaxLimitFactorDecay(maxLimitFactorDecay, minLimitFactor float64) Builder[R]

	// WithMaxLimitFunc allows a max limit to be specified given the current inflight executions. When configured, a
	// maxLimitFunc replaces any configured max limit factor and decay.
	WithMaxLimitFunc(maxLimitFunc func(inflight int) float64) Builder[R]

	// WithMaxLimitStabilizationWindow configures a stabilization window that remembers the peak inflight executions
	// over the given duration. This prevents temporary dips in inflight from pulling down the max limit calculation.
	//
	// Without a stabilization window, oscillating inflight patterns can cause the limit to get stuck near the peak
	// inflight value, unable to grow to the configured maxLimitFactor headroom above it.
	//
	// Disabled by default.
	// Panics if window is negative.
	WithMaxLimitStabilizationWindow(window time.Duration) Builder[R]

	// WithRecentWindow configures how recent execution times are collected and summarized. These help the limiter determine
	// when execution times are trending up or down, relative to the baseline, which helps detect overload. The minDuration
	// and maxDuration define the time bounds for sample collection, while minSamples ensures enough data is collected
	// before adjusting the limit.
	//
	// The default values are 1s, 30s, and 50.
	// Panics if minDuration is not <= maxDuration.
	WithRecentWindow(minDuration time.Duration, maxDuration time.Duration, minSamples uint) Builder[R]

	// WithRecentQuantile configures the recentQuantile of recent execution times to consider when adjusting the concurrency limit.
	//
	// Defaults to 0.9 which uses p90 samples.
	// Panics if recentQuantile is negative.
	WithRecentQuantile(quantile float64) Builder[R]

	// WithBaselineWindow configures how the baseline execution times are maintained and updated. The baseline represents
	// the long-term average execution times that recent execution times are compared against to detect overload. When the
	// recent window is filled an aggregated recent sample is added to the baseline window. The baselineAge controls how
	// many samples the baseline window remembers - smaller values make the baseline adapt faster to recent changes, while
	// larger values keep it more stable by retaining influence from older measurements. The default value is 10.
	WithBaselineWindow(baselineAge uint) Builder[R]

	// WithCorrelationWindow configures how many recent limit and execution time measurements are stored to detect whether
	// increases in limits correlate with increases in execution times, which causes the limit to be adjusted down.
	//
	// The default value is 50.
	WithCorrelationWindow(size uint) Builder[R]

	// WithQueueing enables additional queueing of executions when the limiter is full. Queueing allows short execution
	// spikes to be absorbed without strictly rejecting executions.
	//
	// When queueing is enabled and the limiter is full, executions are queued up to the current limit times the
	// initialRejectionFactor, after which they will gradually start to be rejected up to the limit times the
	// maxRejectionFactor. This allows rejection to gradually adjust based on how many executions are queued, relative to the
	// limit. Queueing allows short execution spikes to be absorbed without strictly rejecting executions.
	//
	// WithQueueing is disabled by default, which means no executions will queue when the limiter is full.
	// Panics if initialRejectionFactor or maxRejectionFactor are < 1 or if initialRejectionFactor is not <= maxRejectionFactor.
	WithQueueing(initialRejectionFactor, maxRejectionFactor float64) Builder[R]

	// WithMaxWaitTime configures the maxWaitTime to wait for a permit to be available, when the limiter is full.
	WithMaxWaitTime(maxWaitTime time.Duration) Builder[R]

	// OnLimitExceeded registers the listener to be called when the limit is exceeded.
	OnLimitExceeded(listener func(event failsafe.ExecutionEvent[R])) Builder[R]

	// OnLimitChanged configures a listener to be called with the limit changes.
	OnLimitChanged(listener func(event LimitChangedEvent)) Builder[R]

	// WithLogger configures a logger which provides debug logging of limit adjustments.
	WithLogger(logger *slog.Logger) Builder[R]

	// Build returns a new AdaptiveLimiter using the builder's configuration.
	Build() AdaptiveLimiter[R]

	// BuildPrioritized returns a new PrioritizedLimiter using the builder's configuration. This enables queueing and
	// prioritized rejections of executions when the limiter is full, where executions block while waiting for a permit.
	// Enabling this allows short execution spikes to be absorbed without strictly rejecting executions when the limiter is
	// full. Rejections are performed using the Prioritizer, which sets a rejection threshold based on the limits and queues
	// of all the limiters being used by the Prioritizer. The amount of queueing can be configured via WithQueueing, and
	// defaults to an initialRejectionFactor of 2 times the current limit and a maxRejectionFactor of 3 times the current
	// limit.
	//
	// The Prioritizer can and should be shared across all limiter instances that need to coordinate prioritization.
	// Prioritized rejection is disabled by default, which means no executions will block when the limiter is full.
	BuildPrioritized(prioritizer priority.Prioritizer) PriorityLimiter[R]
}

// LimitChangedEvent indicates an AdaptiveLimiter's limit has changed.
type LimitChangedEvent struct {
	OldLimit uint
	NewLimit uint
}

type config[R any] struct {
	maxWaitTime time.Duration
	logger      *slog.Logger

	// Limit config
	minLimit, maxLimit          float64
	initialLimit                uint
	maxLimitFunc                func(inflight int) float64
	maxLimitFactor              float64
	maxLimitFactorDecay         float64
	minLimitFactor              float64
	maxLimitStabilizationWindow time.Duration

	// Windowing config
	recentWindowMinDuration time.Duration
	recentWindowMaxDuration time.Duration
	recentWindowMinSamples  uint
	recentQuantile          float64
	baselineWindowAge       uint
	correlationWindowSize   uint

	// Rejection config
	initialRejectionFactor float64
	maxRejectionFactor     float64

	// Listeners
	onLimitExceeded func(failsafe.ExecutionEvent[R])
	onLimitChanged  func(LimitChangedEvent)
}

var _ Builder[any] = &config[any]{}

// NewWithDefaults creates an AdaptiveLimiter with min, max, and initial limits of 1, 200, and 20 respectively, and a maxLimitFactor of 5.
// The recent window's min and max durations are 1 and 30 seconds respectively, and the min samples is 50.
// The baseline window age is 10 and the correlation window size is 50.
// To configure additional options on an AdaptiveLimiter, use NewBuilder() instead.
func NewWithDefaults[R any]() AdaptiveLimiter[R] {
	return NewBuilder[R]().Build()
}

// NewBuilder creates a Builder for execution result type R.
// The min, max, and initial limits default to 1, 200, and 20 respectively, and the maxLimitFactor to 5.
// The recent window's min and max durations default to 1 and 30 seconds respectively, and the min samples to 50.
// The baseline window age defaults to 10 and the correlation window size to 50.
func NewBuilder[R any]() Builder[R] {
	return &config[R]{
		minLimit:            1,
		maxLimit:            200,
		initialLimit:        20,
		maxLimitFactor:      5.0,
		maxLimitFactorDecay: 0.0,

		recentWindowMinDuration: time.Second,
		recentWindowMaxDuration: 30 * time.Second,
		recentWindowMinSamples:  50,
		recentQuantile:          0.9,
		baselineWindowAge:       10,
		correlationWindowSize:   50,
	}
}

func (c *config[R]) WithLimits(minLimit uint, maxLimit uint, initialLimit uint) Builder[R] {
	util.Assert(minLimit <= maxLimit, "minLimit must be <= maxLimit")
	util.Assert(minLimit <= initialLimit && initialLimit <= maxLimit, "initialLimit must be between minLimit and maxLimit")
	c.minLimit = float64(max(1, minLimit))
	c.maxLimit = float64(maxLimit)
	c.initialLimit = initialLimit
	return c
}

func (c *config[R]) WithMaxLimitFactor(maxLimitFactor float64) Builder[R] {
	util.Assert(maxLimitFactor >= 1, "maxLimitFactor must be >= 1")
	c.maxLimitFactor = maxLimitFactor
	return c
}

func (c *config[R]) WithMaxLimitFactorDecay(maxLimitFactorDecay, minLimitFactor float64) Builder[R] {
	util.Assert(maxLimitFactorDecay >= 0, "maxLimitFactorDecay must be >= 0")
	util.Assert(minLimitFactor >= 1, "minLimitFactor must be >= 1")
	c.maxLimitFactorDecay = maxLimitFactorDecay
	c.minLimitFactor = minLimitFactor
	return c
}

func (c *config[R]) WithMaxLimitFunc(maxLimitFunc func(inflight int) float64) Builder[R] {
	c.maxLimitFunc = maxLimitFunc
	return c
}

func (c *config[R]) WithMaxLimitStabilizationWindow(window time.Duration) Builder[R] {
	util.Assert(window >= 0, "maxLimitStabilizationWindow must be >= 0")
	c.maxLimitStabilizationWindow = window
	return c
}

func (c *config[R]) WithRecentWindow(minDuration time.Duration, maxDuration time.Duration, minSamples uint) Builder[R] {
	util.Assert(minDuration <= maxDuration, "minDuration must be <= maxDuration")
	c.recentWindowMinDuration = minDuration
	c.recentWindowMaxDuration = maxDuration
	c.recentWindowMinSamples = minSamples
	return c
}

func (c *config[R]) WithRecentQuantile(quantile float64) Builder[R] {
	util.Assert(quantile >= 0, "recentQuantile must be >= 0")
	c.recentQuantile = quantile
	return c
}

func (c *config[R]) WithBaselineWindow(baselineAge uint) Builder[R] {
	c.baselineWindowAge = baselineAge
	return c
}

func (c *config[R]) WithCorrelationWindow(size uint) Builder[R] {
	c.correlationWindowSize = size
	return c
}

func (c *config[R]) WithQueueing(initialRejectionFactor, maxRejectionFactor float64) Builder[R] {
	util.Assert(initialRejectionFactor >= 1, "initialRejectionFactor must be >= 1")
	util.Assert(maxRejectionFactor >= 1, "maxRejectionFactor must be >= 1")
	util.Assert(initialRejectionFactor <= maxRejectionFactor, "initialRejectionFactor must be <= maxRejectionFactor")
	c.initialRejectionFactor = initialRejectionFactor
	c.maxRejectionFactor = maxRejectionFactor
	return c
}

func (c *config[R]) WithMaxWaitTime(maxWaitTime time.Duration) Builder[R] {
	c.maxWaitTime = maxWaitTime
	return c
}

func (c *config[R]) WithLogger(logger *slog.Logger) Builder[R] {
	c.logger = logger
	return c
}

func (c *config[R]) OnLimitExceeded(listener func(event failsafe.ExecutionEvent[R])) Builder[R] {
	c.onLimitExceeded = listener
	return c
}

func (c *config[R]) OnLimitChanged(listener func(event LimitChangedEvent)) Builder[R] {
	c.onLimitChanged = listener
	return c
}

func (c *config[R]) Build() AdaptiveLimiter[R] {
	limiter := &adaptiveLimiter[R]{
		config:                *c,
		semaphore:             util.NewDynamicSemaphore(int(c.initialLimit)),
		limit:                 float64(c.initialLimit),
		recentRTT:             tdigestSample{TDigest: tdigest.NewWithCompression(100)},
		medianFilter:          util.NewMedianFilter(smoothedSamples),
		smoothedRecentRTT:     util.NewEwma(smoothedSamples, warmupSamples),
		baselineRTT:           util.NewEwma(c.baselineWindowAge, warmupSamples),
		nextUpdateTime:        time.Now(),
		rttCorrelation:        util.NewCorrelationWindow(c.correlationWindowSize, warmupSamples),
		throughputCorrelation: util.NewCorrelationWindow(c.correlationWindowSize, warmupSamples),
	}
	if c.maxLimitStabilizationWindow != 0 {
		limiter.maxInflightWindow = util.NewMaxWindow(c.maxLimitStabilizationWindow)
	}
	if c.initialRejectionFactor != 0 && c.maxRejectionFactor != 0 {
		if c.maxWaitTime == 0 {
			limiter.config.maxWaitTime = -1 // Wait indefinitely for queued executions
		}
		return &queueingLimiter[R]{adaptiveLimiter: limiter}
	}
	return limiter
}

func (c *config[R]) BuildPrioritized(p priority.Prioritizer) PriorityLimiter[R] {
	if c.initialRejectionFactor == 0 && c.maxRejectionFactor == 0 {
		c.initialRejectionFactor = 2
		c.maxRejectionFactor = 3
	}
	limiter := &priorityLimiter[R]{
		queueingLimiter: c.Build().(*queueingLimiter[R]),
		prioritizer:     p.(*internal.BasePrioritizer[*queueStats]),
	}
	limiter.prioritizer.Register(limiter.getQueueStats)
	return limiter
}

type limitChange int

const (
	increase limitChange = iota
	decrease
	hold
)

type tdigestSample struct {
	MinRTT      time.Duration
	MaxInflight int
	Size        uint
	*tdigest.TDigest
}

func (td *tdigestSample) Add(rtt time.Duration, inflight int) {
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

func (td *tdigestSample) Reset() {
	td.TDigest.Reset()
	td.MinRTT = 0
	td.MaxInflight = 0
	td.Size = 0
}

type adaptiveLimiter[R any] struct {
	config[R]

	// Mutable state
	semaphore *util.DynamicSemaphore
	mu        sync.RWMutex

	// Guarded by mu
	limit                 float64        // The current concurrency limit
	maxInflightWindow     util.MaxWindow // Tracks the max inflight over a stabilization window
	recentRTT             tdigestSample  // Recent execution times
	lastMaxInflight       int            // The max inflight requests for the last sampling period
	medianFilter          util.MedianFilter
	smoothedRecentRTT     util.Ewma
	baselineRTT           util.Ewma              // Tracks baseline execution time
	nextUpdateTime        time.Time              // Tracks when the limit can next be updated
	throughputCorrelation util.CorrelationWindow // Tracks the correlation between concurrency and throughput
	rttCorrelation        util.CorrelationWindow // Tracks the correlation between concurrency and round trip times (RTT)
}

func (*adaptiveLimiter[R]) ResultAgnostic() {}

func (l *adaptiveLimiter[R]) AcquirePermit(ctx context.Context) (Permit, error) {
	if err := l.semaphore.Acquire(ctx); err != nil {
		return nil, err
	}
	return l.newPermit(), nil
}

func (l *adaptiveLimiter[R]) AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error) {
	if err := l.semaphore.AcquireWithMaxWait(ctx, maxWaitTime); err != nil {
		if errors.Is(err, util.ErrWaitExceeded) {
			err = ErrExceeded
		}
		return nil, err
	}
	return l.newPermit(), nil
}

func (l *adaptiveLimiter[R]) TryAcquirePermit() (Permit, bool) {
	if !l.semaphore.TryAcquire() {
		return nil, false
	}
	return l.newPermit(), true
}

func (l *adaptiveLimiter[R]) newPermit() Permit {
	return &recordingPermit[R]{
		clock:           util.WallClock,
		limiter:         l,
		startTime:       time.Now(),
		currentInflight: l.semaphore.Used(),
	}
}

func (l *adaptiveLimiter[R]) CanAcquirePermit() bool {
	return !l.semaphore.IsFull()
}

func (l *adaptiveLimiter[R]) Limit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int(l.limit)
}

func (l *adaptiveLimiter[R]) Inflight() int {
	return l.semaphore.Used()
}

func (l *adaptiveLimiter[R]) MaxInflight() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastMaxInflight
}

func (l *adaptiveLimiter[R]) Queued() int {
	return l.semaphore.Waiters()
}

func (l *adaptiveLimiter[R]) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.semaphore.SetSize(int(l.config.initialLimit))
	l.limit = float64(l.config.initialLimit)
	l.maxInflightWindow.Reset()
	l.recentRTT.Reset()
	l.medianFilter.Reset()
	l.smoothedRecentRTT.Reset()
	l.baselineRTT.Reset()
	l.nextUpdateTime = time.Now()
	l.rttCorrelation.Reset()
	l.throughputCorrelation.Reset()
}

// Records the duration of a completed execution, updating the concurrency limit if the recentRTT window is full.
func (l *adaptiveLimiter[R]) record(now time.Time, rtt time.Duration, inflight int, dropped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !dropped {
		l.recentRTT.Add(rtt, inflight)
	}

	if now.After(l.nextUpdateTime) && l.recentRTT.Size >= l.recentWindowMinSamples {
		quantile := l.recentRTT.Quantile(l.recentQuantile)
		filteredRTT := l.medianFilter.Add(quantile)
		smoothedRTT := l.smoothedRecentRTT.Add(filteredRTT)
		l.lastMaxInflight = l.recentRTT.MaxInflight
		l.updateLimit(smoothedRTT, l.recentRTT.MaxInflight, now)
		minRTT := l.recentRTT.MinRTT
		l.recentRTT.Reset()
		minWindowTime := max(minRTT*2, l.recentWindowMinDuration)
		l.nextUpdateTime = now.Add(min(minWindowTime, l.recentWindowMaxDuration))
	}

	l.semaphore.Release()
}

// updateLimit updates the concurrency limit based on the gradient between the recentRTT and historical baselineRTT.
// A stability check prevents unnecessary decreases during steady state.
// A correlation adjustment prevents upward drift during overload.
func (l *adaptiveLimiter[R]) updateLimit(recentRTT float64, inflight int, now time.Time) {
	// Update baseline RTT and calculate the queue size
	// This is the primary signal that we threshold off of to detect overload
	baselineRTT := l.baselineRTT.Add(recentRTT)
	gradient := baselineRTT / recentRTT
	queueSize := int(math.Ceil(l.limit * (1 - gradient)))

	// Calculate throughput correlation, throughput CV, and RTT correlation
	// These are the secondary signals that we threshold off of to detect overload
	throughput := float64(inflight) / (recentRTT / 1e9) // Convert to RPS
	throughputCorr, _, throughputCV := l.throughputCorrelation.Add(float64(inflight), throughput)
	rttCorr, _, _ := l.rttCorrelation.Add(float64(inflight), recentRTT)

	// Additional values for thresholding the limit
	overloaded := l.semaphore.IsFull()
	alpha := alphaFunc(int(l.limit)) // alpha is the queueSize threshold below which we increase
	beta := betaFunc(int(l.limit))   // beta is the queueSize threshold above which we decrease

	change, reason := computeChange(queueSize, alpha, beta, overloaded, throughputCorr, throughputCV, rttCorr)

	oldLimit := l.limit
	newLimit := oldLimit
	var direction string
	switch change {
	case decrease:
		direction = "decrease"
		newLimit = oldLimit - float64(decreaseFunc(int(oldLimit)))
	case increase:
		direction = "increase"
		newLimit = oldLimit + float64(increaseFunc(int(oldLimit)))
	default:
		direction = "hold"
	}

	// Get the max inflight over the stabilization window
	maxInflight := inflight
	if l.maxInflightWindow.Configured() {
		maxInflight = l.maxInflightWindow.Add(inflight, now)
	}

	maxLimit := l.computeMaxLimit(maxInflight)
	if newLimit > maxLimit {
		if oldLimit > maxLimit {
			direction = "decrease"
			newLimit = oldLimit - float64(decreaseFunc(int(oldLimit))) // Decrease gradually to avoid noise if inflights fluctuate
		} else if oldLimit < maxLimit {
			direction = "increase"
			newLimit = maxLimit
		} else {
			direction = "hold"
			newLimit = maxLimit
		}
		reason = "max"
	}

	// Clamp the limit based on absolute min and max
	if newLimit > l.maxLimit {
		if oldLimit == l.maxLimit {
			direction = "hold"
			reason = "max"
		}
		newLimit = l.maxLimit
	} else if newLimit < l.minLimit {
		if oldLimit == l.minLimit {
			direction = "hold"
			reason = "min"
		}
		newLimit = l.minLimit
	}

	l.logLimit(direction, reason, newLimit, gradient, queueSize, inflight, recentRTT, baselineRTT, rttCorr, throughput, throughputCorr, throughputCV)

	if uint(oldLimit) != uint(newLimit) && l.onLimitChanged != nil {
		l.mu.Unlock()
		l.onLimitChanged(LimitChangedEvent{
			OldLimit: uint(oldLimit),
			NewLimit: uint(newLimit),
		})
		l.mu.Lock()
	}

	l.semaphore.SetSize(int(newLimit))
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
	} else {
		// If queueSize is between alpha and beta, leave the limit unchanged
		return hold, "queue"
	}
}

func (l *adaptiveLimiter[R]) logLimit(direction, reason string, limit float64, gradient float64, queueSize, inflight int, recentRTT, baselineRTT, rttCorr, throughput, throughputCorr, throughputCV float64) {
	if l.logger != nil && l.logger.Enabled(nil, slog.LevelDebug) {
		l.logger.Debug("limit update",
			"direction", direction,
			"reason", reason,
			"inflight", inflight,
			"limit", fmt.Sprintf("%.2f", limit),
			"gradient", fmt.Sprintf("%.2f", gradient),
			"queueSize", fmt.Sprintf("%d", queueSize),
			"recentRTT", time.Duration(recentRTT).Round(time.Microsecond),
			"baselineRTT", time.Duration(baselineRTT).Round(time.Microsecond),
			"thrpt", fmt.Sprintf("%.2f", throughput),
			"thrptCorr", fmt.Sprintf("%.2f", throughputCorr),
			"thrptCV", fmt.Sprintf("%.2f", throughputCV),
			"rttCorr", fmt.Sprintf("%.2f", rttCorr))
	}
}

// computeMaxLimit computes the max limit using a provided function, else based on max limit factor, with optional
// logarithmic decay.
func (l *adaptiveLimiter[R]) computeMaxLimit(inflight int) float64 {
	if l.maxLimitFunc != nil {
		return l.maxLimitFunc(inflight)
	}

	effectiveFactor := l.maxLimitFactor
	if l.maxLimitFactorDecay > 0 && inflight > 0 {
		// Apply logarithmic decay, where the factor decreases by the decay amount for each order of magnitude increase in inflights
		effectiveFactor = l.maxLimitFactor - (l.maxLimitFactorDecay * math.Log10(float64(inflight)))
		effectiveFactor = max(effectiveFactor, l.minLimitFactor)
	}
	return float64(inflight) * effectiveFactor
}

func (l *adaptiveLimiter[R]) ToExecutor(_ R) any {
	e := &executor[R]{
		BaseExecutor:    policy.BaseExecutor[R]{},
		blockingLimiter: l,
	}
	e.Executor = e
	return e
}

func (l *adaptiveLimiter[R]) canAcquirePermit(_ context.Context) bool {
	return l.CanAcquirePermit()
}

func (l *adaptiveLimiter[R]) configRef() *config[R] {
	return &l.config
}

type recordingPermit[R any] struct {
	clock           util.Clock
	limiter         *adaptiveLimiter[R]
	startTime       time.Time
	currentInflight int
	userID          string
	usageTracker    priority.UsageTracker
}

func (p *recordingPermit[R]) Record() {
	if p.userID != "" && p.usageTracker != nil {
		p.RecordUsage(p.userID, -1)
		return
	}

	now := p.clock.Now()
	p.limiter.record(now, now.Sub(p.startTime), p.currentInflight, false)
}

func (p *recordingPermit[R]) RecordUsage(userID string, usage int64) {
	now := p.clock.Now()
	duration := now.Sub(p.startTime)

	if userID != "" && p.usageTracker != nil {
		if usage == -1 {
			usage = duration.Nanoseconds()
		}
		p.usageTracker.RecordUsage(userID, usage)
	}

	p.limiter.record(now, duration, p.currentInflight, false)
}

func (p *recordingPermit[R]) Drop() {
	now := p.clock.Now()
	p.limiter.record(now, now.Sub(p.startTime), p.currentInflight, true)
}
