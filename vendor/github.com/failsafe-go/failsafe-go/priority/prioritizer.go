package priority

import (
	"context"
	"log/slog"
	"time"
)

// Prioritizer computes rejection rates and thresholds for policies bsaed on system overload. When policies become full,
// the Prioritizer determines which priority levels should be rejected to shed load. Individual executions can be
// assigned different priority levels, with higher priorities more likely to be accepted during overload. A Prioritizer
// can coordinate across multiple policies to make rejection decisions based on overall system queueing.
//
// In order to operate correctly, a Prioritizer needs to be regularly calibrated, either by calling Calibrate at regular
// intervals, or by using ScheduleCalibrations.
//
// This type is concurrency safe.
type Prioritizer interface {
	// RejectionRate returns the current rate, from 0 to 1, at which executions will be rejected, based on recent
	// queueing levels across registered limiters.
	RejectionRate() float64

	// RejectionThreshold returns the threshold below which executions will be rejected, based on their priority level
	// (0-499). Higher priority executions are more likely to be accepted when the system is overloaded.
	RejectionThreshold() int

	// RegisteredPolicies returns the number of policies that have been registered to the Prioritizer.
	RegisteredPolicies() int

	// Calibrate recalculates the RejectionRate and RejectionThreshold based on current queueing levels from registered limiters.
	Calibrate()

	// ScheduleCalibrations runs Calibrate on the interval until the ctx is done or the returned CancelFunc is called.
	ScheduleCalibrations(ctx context.Context, interval time.Duration) context.CancelFunc
}

// PrioritizerBuilder builds Prioritizer instances.
//
// This type is not concurrency safe.
type PrioritizerBuilder interface {
	// WithLevelTracker configures a level tracker to use with the prioritizer. The level tracker can be shared across
	// different policy instances and types.
	WithLevelTracker(levelTracker LevelTracker) PrioritizerBuilder

	// WithLogger configures a logger which provides debug logging of calibrations.
	WithLogger(logger *slog.Logger) PrioritizerBuilder

	// OnThresholdChanged configures a listener to be called when the rejection threshold changes.
	OnThresholdChanged(listener func(event ThresholdChangedEvent)) PrioritizerBuilder

	// Build returns a new Prioritizer using the builder's configuration.
	Build() Prioritizer
}

// ThresholdChangedEvent indicates a Prioritizer's rejection threshold has changed.
type ThresholdChangedEvent struct {
	OldThreshold uint
	NewThreshold uint
}
