package limit

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/measurements"
)

// WindowedLimit implements a windowed limit
type WindowedLimit struct {
	minWindowTime   int64 // Minimum window duration for sampling a new minRtt
	maxWindowTime   int64 // Maximum window duration for sampling a new minRtt
	nextUpdateTime  int64 // End time for the sampling window at which point the limit should be updated
	windowSize      int32 // Minimum sampling window size for finding a new minimum rtt
	minRTTThreshold int64

	delegate      core.Limit
	sample        *measurements.ImmutableSampleWindow
	listeners     []core.LimitChangeListener
	registry      core.MetricRegistry
	commonSampler *core.CommonMetricSampler

	mu sync.RWMutex
}

const (
	defaultWindowedMinWindowTime   = 1e9 // 1 second
	defaultWindowedMaxWindowTime   = 1e9 // 1 second
	defaultWindowedMinRTTThreshold = 1e8 // 100 microseconds
	defaultWindowedWindowSize      = 10
)

// NewDefaultWindowedLimit will create a new default WindowedLimit
func NewDefaultWindowedLimit(
	name string,
	delegate core.Limit,
	registry core.MetricRegistry,
	tags ...string,
) *WindowedLimit {
	l, _ := NewWindowedLimit(
		name,
		defaultWindowedMinWindowTime,
		defaultWindowedMaxWindowTime,
		defaultWindowedWindowSize,
		defaultWindowedMinRTTThreshold,
		delegate,
		registry,
		tags...,
	)
	return l
}

// NewWindowedLimit will create a new WindowedLimit
func NewWindowedLimit(
	name string,
	minWindowTime int64,
	maxWindowTime int64,
	windowSize int32,
	minRTTThreshold int64,
	delegate core.Limit,
	registry core.MetricRegistry,
	tags ...string,
) (*WindowedLimit, error) {
	if minWindowTime < (time.Duration(100) * time.Millisecond).Nanoseconds() {
		return nil, fmt.Errorf("minWindowTime must be >= 100 ms")
	}

	if maxWindowTime < (time.Duration(100) * time.Millisecond).Nanoseconds() {
		return nil, fmt.Errorf("maxWindowTime must be >= 100 ms")
	}

	if windowSize < 10 {
		return nil, fmt.Errorf("windowSize must be >= 10 ms")
	}

	if delegate == nil {
		return nil, fmt.Errorf("delegate must be specified")
	}

	if registry == nil {
		registry = core.EmptyMetricRegistryInstance
	}

	l := &WindowedLimit{
		minWindowTime:   minWindowTime,
		maxWindowTime:   maxWindowTime,
		nextUpdateTime:  0,
		windowSize:      windowSize,
		minRTTThreshold: minRTTThreshold,
		delegate:        delegate,
		sample:          measurements.NewDefaultImmutableSampleWindow(),
		listeners:       make([]core.LimitChangeListener, 0),
		registry:        registry,
	}
	l.commonSampler = core.NewCommonMetricSampler(registry, l, name, tags...)
	return l, nil

}

// EstimatedLimit returns the current estimated limit.
func (l *WindowedLimit) EstimatedLimit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.delegate.EstimatedLimit()
}

// NotifyOnChange will register a callback to receive notification whenever the limit is updated to a new value.
func (l *WindowedLimit) NotifyOnChange(consumer core.LimitChangeListener) {
	l.mu.Lock()
	l.listeners = append(l.listeners, consumer)
	l.delegate.NotifyOnChange(consumer)
	l.mu.Unlock()
}

// notifyListeners will call the callbacks on limit changes
func (l *WindowedLimit) notifyListeners(newLimit int) {
	for _, listener := range l.listeners {
		listener(newLimit)
	}
}

// OnSample the concurrency limit using a new rtt sample.
func (l *WindowedLimit) OnSample(startTime int64, rtt int64, inFlight int, didDrop bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commonSampler.Sample(rtt, inFlight, didDrop)

	endTime := startTime + rtt
	if rtt < l.minRTTThreshold {
		return
	}

	if didDrop {
		l.sample = l.sample.AddDroppedSample(-1, inFlight)
	} else {
		l.sample = l.sample.AddSample(-1, rtt, inFlight)
	}

	if endTime > l.nextUpdateTime && l.isWindowReady(rtt, inFlight) {
		current := l.sample
		l.sample = measurements.NewDefaultImmutableSampleWindow()
		l.nextUpdateTime = endTime + minInt64(maxInt64(current.CandidateRTTNanoseconds()*2, l.minWindowTime), l.maxWindowTime)
		l.delegate.OnSample(startTime, current.AverageRTTNanoseconds(), current.MaxInFlight(), didDrop)
	}
}

func (l *WindowedLimit) String() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return fmt.Sprintf("WindowedLimit{minWindowTime=%d, maxWindowTime=%d, minRTTThreshold=%d, windowSize=%d,"+
		" delegate=%v", l.minWindowTime, l.maxWindowTime, l.minRTTThreshold, l.windowSize, l.delegate)
}

func (l *WindowedLimit) isWindowReady(rtt int64, inFlight int) bool {
	return rtt < int64(math.MaxInt64) && int32(inFlight) > l.windowSize
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
