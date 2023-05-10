package limiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/limit"
	"github.com/platinummonkey/go-concurrency-limits/measurements"
)

const (
	defaultMinWindowTime   = int64(1e9) // (1 s) nanoseconds
	defaultMaxWindowTime   = int64(1e9) // (1 s) nanoseconds
	defaultMinRTTThreshold = int64(1e5) // (100 µs) nanoseconds
	defaultWindowSize      = int(100)   // Minimum observed samples to filter out sample windows with not enough significant samples
)

// DefaultListener for
type DefaultListener struct {
	currentMaxInFlight int64
	inFlight           *int64
	token              core.StrategyToken
	startTime          int64
	minRTTThreshold    int64
	limiter            *DefaultLimiter
	nextUpdateTime     int64
}

// OnSuccess is called as a notification that the operation succeeded and internally measured latency should be
// used as an RTT sample.
func (l *DefaultListener) OnSuccess() {
	atomic.AddInt64(l.inFlight, -1)
	l.token.Release()
	endTime := time.Now().UnixNano()
	rtt := endTime - l.startTime

	if rtt < l.minRTTThreshold {
		return
	}
	_, current := l.limiter.updateAndGetSample(
		func(window measurements.ImmutableSampleWindow) measurements.ImmutableSampleWindow {
			return *(window.AddSample(-1, rtt, int(l.currentMaxInFlight)))
		},
	)

	if endTime > l.nextUpdateTime {
		// double check just to be sure
		l.limiter.mu.Lock()
		defer l.limiter.mu.Unlock()
		if endTime > l.limiter.nextUpdateTime {
			if l.limiter.isWindowReady(current) {
				l.limiter.sample = measurements.NewImmutableSampleWindow(
					-1,
					0,
					0,
					0,
					0,
					false,
				)
				minWindowTime := current.CandidateRTTNanoseconds() * 2
				if l.limiter.minWindowTime > minWindowTime {
					minWindowTime = l.limiter.minWindowTime
				}
				minVal := l.limiter.maxWindowTime
				if minWindowTime < minVal {
					minVal = minWindowTime
				}
				l.limiter.nextUpdateTime = endTime + minVal
				l.limiter.limit.OnSample(
					0,
					current.CandidateRTTNanoseconds(),
					current.MaxInFlight(),
					current.DidDrop(),
				)
				l.limiter.strategy.SetLimit(l.limiter.limit.EstimatedLimit())
			}
		}
	}

}

// OnIgnore is called to indicate the operation failed before any meaningful RTT measurement could be made and
// should be ignored to not introduce an artificially low RTT.
func (l *DefaultListener) OnIgnore() {
	atomic.AddInt64(l.inFlight, -1)
	l.token.Release()
}

// OnDropped is called to indicate the request failed and was dropped due to being rejected by an external limit or
// hitting a timeout.  Loss based Limit implementations will likely do an aggressive reducing in limit when this
// happens.
func (l *DefaultListener) OnDropped() {
	atomic.AddInt64(l.inFlight, -1)
	l.token.Release()
	l.limiter.updateAndGetSample(func(window measurements.ImmutableSampleWindow) measurements.ImmutableSampleWindow {
		return *(window.AddDroppedSample(-1, int(l.currentMaxInFlight)))
	})
}

// DefaultLimiter is a Limiter that combines a plugable limit algorithm and enforcement strategy to enforce concurrency
// limits to a fixed resource.
type DefaultLimiter struct {
	limit           core.Limit
	strategy        core.Strategy
	minWindowTime   int64
	maxWindowTime   int64
	windowSize      int
	minRTTThreshold int64
	logger          limit.Logger
	registry        core.MetricRegistry

	sample         *measurements.ImmutableSampleWindow
	inFlight       *int64
	nextUpdateTime int64
	mu             sync.RWMutex
}

// NewDefaultLimiterWithDefaults will create a DefaultLimit Limiter with the provided minimum config.
func NewDefaultLimiterWithDefaults(
	name string,
	strategy core.Strategy,
	logger limit.Logger,
	registry core.MetricRegistry,
	tags ...string,
) (*DefaultLimiter, error) {
	return NewDefaultLimiter(
		limit.NewDefaultVegasLimit(name, logger, registry, tags...),
		defaultMinWindowTime,
		defaultMaxWindowTime,
		defaultMinRTTThreshold,
		defaultWindowSize,
		strategy,
		logger,
		registry,
	)
}

// NewDefaultLimiter creates a new DefaultLimiter.
func NewDefaultLimiter(
	limit core.Limit,
	minWindowTime int64,
	maxWindowTime int64,
	minRTTThreshold int64,
	windowSize int,
	strategy core.Strategy,
	logger limit.Logger,
	registry core.MetricRegistry,
) (*DefaultLimiter, error) {
	if limit == nil {
		return nil, fmt.Errorf("limit must be provided")
	}
	if strategy == nil {
		return nil, fmt.Errorf("stratewy must be provided")
	}
	if minWindowTime <= 0 {
		return nil, fmt.Errorf("minWindowTime must be > 0 ns")
	}
	if maxWindowTime <= 0 {
		return nil, fmt.Errorf("maxWindowTime must be > 0 ns")
	}
	if maxWindowTime < minWindowTime {
		return nil, fmt.Errorf("minWindowTime must be <= maxWindowTime")
	}
	if windowSize < 10 {
		return nil, fmt.Errorf("windowSize must be >= 10")
	}

	inFlight := int64(0)

	strategy.SetLimit(limit.EstimatedLimit())
	return &DefaultLimiter{
		limit:           limit,
		strategy:        strategy,
		minWindowTime:   minWindowTime,
		maxWindowTime:   maxWindowTime,
		minRTTThreshold: minRTTThreshold,
		windowSize:      windowSize,
		inFlight:        &inFlight,
		sample:          measurements.NewDefaultImmutableSampleWindow(),
		logger:          logger,
		registry:        registry,
	}, nil
}

// Acquire a token from the limiter.  Returns an Optional.empty() if the limit has been exceeded.
// If acquired the caller must call one of the Listener methods when the operation has been completed to release
// the count.
//
// context Context for the request. The context is used by advanced strategies such as LookupPartitionStrategy.
func (l *DefaultLimiter) Acquire(ctx context.Context) (core.Listener, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Did we exceed the limit?
	token, ok := l.strategy.TryAcquire(ctx)
	if !ok || token == nil {
		return nil, false
	}

	startTime := time.Now().UnixNano()
	currentMaxInFlight := atomic.AddInt64(l.inFlight, 1)
	return &DefaultListener{
		currentMaxInFlight: currentMaxInFlight,
		inFlight:           l.inFlight,
		token:              token,
		startTime:          startTime,
		minRTTThreshold:    l.minRTTThreshold,
		limiter:            l,
		nextUpdateTime:     l.nextUpdateTime,
	}, true

}

func (l *DefaultLimiter) updateAndGetSample(
	f func(sample measurements.ImmutableSampleWindow) measurements.ImmutableSampleWindow,
) (measurements.ImmutableSampleWindow, measurements.ImmutableSampleWindow) {
	l.mu.Lock()
	defer l.mu.Unlock()
	before := *l.sample
	after := f(before)
	l.sample = &after
	return before, after
}

func (l *DefaultLimiter) isWindowReady(sample measurements.ImmutableSampleWindow) bool {
	return sample.CandidateRTTNanoseconds() < math.MaxInt64 && sample.SampleCount() > l.windowSize
}

// EstimatedLimit will return the current estimated limit.
func (l *DefaultLimiter) EstimatedLimit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.limit.EstimatedLimit()
}

func (l *DefaultLimiter) String() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	rttCandidate := int64(-1)
	if l.sample != nil {
		rttCandidate = l.sample.CandidateRTTNanoseconds() / 1000
	}
	return fmt.Sprintf(
		"DefaultLimiter{RTTCandidate=%d ms, maxInFlight=%d, limit=%v, strategy=%v}",
		rttCandidate, l.inFlight, l.limit, l.strategy)
}
