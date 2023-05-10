package measurements

import (
	"fmt"
	"math"
	"time"
)

// ImmutableSampleWindow is used to track immutable samples atomically.
type ImmutableSampleWindow struct {
	startTime   int64
	minRTT      int64
	maxInFlight int
	sampleCount int
	sum         int64
	didDrop     bool
}

// NewDefaultImmutableSampleWindow will create a new ImmutableSampleWindow with defaults
func NewDefaultImmutableSampleWindow() *ImmutableSampleWindow {
	return NewImmutableSampleWindow(
		time.Now().UnixNano(),
		math.MaxInt64,
		0,
		0,
		0,
		false,
	)
}

// NewImmutableSampleWindow will create a new ImmutableSampleWindow with defaults
func NewImmutableSampleWindow(
	startTime int64,
	minRTT int64,
	sum int64,
	maxInFlight int,
	sampleCount int,
	didDrop bool,
) *ImmutableSampleWindow {
	if minRTT == 0 {
		minRTT = math.MaxInt64
	}
	return &ImmutableSampleWindow{
		startTime:   startTime,
		minRTT:      minRTT,
		sum:         sum,
		maxInFlight: maxInFlight,
		sampleCount: sampleCount,
		didDrop:     didDrop,
	}
}

// AddSample will create a new immutable sample for which to use.
func (s *ImmutableSampleWindow) AddSample(startTime int64, rtt int64, maxInFlight int) *ImmutableSampleWindow {
	minRTT := s.minRTT
	if rtt < s.minRTT {
		minRTT = rtt
	}
	if maxInFlight < s.maxInFlight {
		maxInFlight = s.maxInFlight
	}
	if startTime < 0 {
		startTime = time.Now().UnixNano()
	}
	return NewImmutableSampleWindow(startTime, minRTT, s.sum+rtt, maxInFlight, s.sampleCount+1, s.didDrop)
}

// AddDroppedSample will create a new immutable sample that was dropped.
func (s *ImmutableSampleWindow) AddDroppedSample(startTime int64, maxInFlight int) *ImmutableSampleWindow {
	if maxInFlight < s.maxInFlight {
		maxInFlight = s.maxInFlight
	}
	if startTime < 0 {
		startTime = time.Now().UnixNano()
	}
	return NewImmutableSampleWindow(startTime, s.minRTT, s.sum, maxInFlight, s.sampleCount, true)
}

// StartTimeNanoseconds returns the epoch start time in nanoseconds.
func (s *ImmutableSampleWindow) StartTimeNanoseconds() int64 {
	return s.startTime
}

// CandidateRTTNanoseconds returns the candidate RTT in the sample window. This is traditionally the minimum rtt.
func (s *ImmutableSampleWindow) CandidateRTTNanoseconds() int64 {
	return s.minRTT
}

// AverageRTTNanoseconds returns the average RTT in the sample window.  Excludes timeouts and dropped rtt.
func (s *ImmutableSampleWindow) AverageRTTNanoseconds() int64 {
	if s.sampleCount == 0 {
		return 0
	}
	return s.sum / int64(s.sampleCount)
}

// MaxInFlight returns the maximum number of in-flight observed during the sample window.
func (s *ImmutableSampleWindow) MaxInFlight() int {
	return s.maxInFlight
}

// SampleCount is the number of observed RTTs in the sample window.
func (s *ImmutableSampleWindow) SampleCount() int {
	return s.sampleCount
}

// DidDrop returns True if there was a timeout.
func (s *ImmutableSampleWindow) DidDrop() bool {
	return s.didDrop
}

func (s *ImmutableSampleWindow) String() string {
	return fmt.Sprintf(
		"ImmutableSampleWindow{minRTT=%d, averageRTT=%d, maxInFlight=%d, sampleCount=%d, didDrop=%t}",
		s.minRTT, s.AverageRTTNanoseconds(), s.maxInFlight, s.sampleCount, s.didDrop)
}
