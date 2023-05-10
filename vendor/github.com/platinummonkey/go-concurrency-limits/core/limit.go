package core

import (
	"context"
)

// MeasurementInterface defines the contract for tracking a measurement such as a minimum or average of a sample set.
type MeasurementInterface interface {
	// Add a single sample and update the internal state.
	// returns true if the internal state was updated, also return the current value.
	Add(value float64) (float64, bool)

	// Get the current value.
	Get() float64

	// Reset the internal state as if no samples were ever added.
	Reset()

	// Update will update the value given an operation function
	Update(operation func(value float64) float64)
}

// SampleWindow represents the details of the current sample window
type SampleWindow interface {
	// StartTimeNanoseconds returns the epoch start time in nanoseconds.
	StartTimeNanoseconds() int64
	// CandidateRTTNanoseconds returns the candidate RTT in the sample window. This is traditionally the minimum rtt.
	CandidateRTTNanoseconds() int64
	// AverageRTTNanoseconds returns the average RTT in the sample window.  Excludes timeouts and dropped rtt.
	AverageRTTNanoseconds() int64
	// MaxInFlight returns the maximum number of in-flight observed during the sample window.
	MaxInFlight() int
	// SampleCount is the number of observed RTTs in the sample window.
	SampleCount() int
	// DidDrop returns True if there was a timeout.
	DidDrop() bool
}

// LimitChangeListener is a callback method to receive a notification whenever the limit is updated to a new value.
type LimitChangeListener func(limit int)

// Limit is a Contract for an algorithm that calculates a concurrency limit based on rtt measurements.
type Limit interface {
	// EstimatedLimit returns the current estimated limit.
	EstimatedLimit() int

	// NotifyOnChange will register a callback to receive notification whenever the limit is updated to a new value.
	//
	// consumer - the callback
	NotifyOnChange(consumer LimitChangeListener)

	// OnSample the concurrency limit using a new rtt sample.
	//
	// startTime - in epoch nanoseconds
	// rtt - round trip time of sample
	// inFlight - in flight observed count during the sample
	// didDrop - true if there was a timeout
	OnSample(startTime int64, rtt int64, inFlight int, didDrop bool)
}

// Listener implements token listener for callback to the limiter when and how it should be released.
type Listener interface {
	// OnSuccess is called as a notification that the operation succeeded and internally measured latency should be
	// used as an RTT sample.
	OnSuccess()
	// OnIgnore is called to indicate the operation failed before any meaningful RTT measurement could be made and
	// should be ignored to not introduce an artificially low RTT.
	OnIgnore()
	// OnDropped is called to indicate the request failed and was dropped due to being rejected by an external limit or
	// hitting a timeout.  Loss based Limit implementations will likely do an aggressive reducing in limit when this
	// happens.
	OnDropped()
}

// Limiter defines the contract for a concurrency limiter.  The caller is expected to call acquire() for each request
// and must also release the returned listener when the operation completes.  Releasing the Listener
// may trigger an update to the concurrency limit based on error rate or latency measurement.
type Limiter interface {
	// Acquire a token from the limiter.  Returns a nil listener if the limit has been exceeded.
	// If acquired the caller must call one of the Listener methods when the operation has been completed to release
	// the count.
	//
	// context - Context for the request. The context is used by advanced strategies such as LookupPartitionStrategy.
	Acquire(ctx context.Context) (listener Listener, ok bool)
}
