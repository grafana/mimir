package priority

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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

// BasePrioritizerConfig provides a base for implementing a PrioritizerBuilder.
type BasePrioritizerConfig[S Stats] struct {
	logger             *slog.Logger
	LevelTracker       LevelTracker
	UsageTracker       UsageTracker
	Strategy           RejectionStrategy[S]
	onThresholdChanged func(event ThresholdChangedEvent)
}

var _ PrioritizerBuilder = &BasePrioritizerConfig[Stats]{}

func (c *BasePrioritizerConfig[S]) WithLevelTracker(levelTracker LevelTracker) PrioritizerBuilder {
	c.LevelTracker = levelTracker
	return c
}

func (c *BasePrioritizerConfig[S]) WithLogger(logger *slog.Logger) PrioritizerBuilder {
	c.logger = logger
	return c
}

func (c *BasePrioritizerConfig[S]) OnThresholdChanged(listener func(event ThresholdChangedEvent)) PrioritizerBuilder {
	c.onThresholdChanged = listener
	return c
}

func (c *BasePrioritizerConfig[S]) Build() Prioritizer {
	pCopy := *c
	if pCopy.LevelTracker == nil {
		pCopy.LevelTracker = NewLevelTracker()
	}
	return &BasePrioritizer[S]{
		BasePrioritizerConfig: &pCopy, // TODO copy base fields
	}
}

type Stats interface {
	// ComputeRejectionRate returns the rate at which future executions should be rejected, given the window.
	ComputeRejectionRate() float64

	// DebugLogArgs returns any args you'd like to include in the prioritizer's debug logs.
	DebugLogArgs() []any
}

type RejectionStrategy[S any] interface {
	CombineStats(statsFuncs []func() S) (totalStats S)
}

// BasePrioritizer provides a base implementation of a Prioritizer.
type BasePrioritizer[S Stats] struct {
	*BasePrioritizerConfig[S]

	// Mutable state
	mu              sync.Mutex
	statsFuncs      []func() S // Guarded by mu
	numStats        atomic.Int32
	rejectionRate   float64 // Guarded by mu
	RejectionThresh atomic.Int32
}

func (p *BasePrioritizer[S]) Register(statsFunc func() S) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statsFuncs = append(p.statsFuncs, statsFunc)
	p.numStats.Add(1)
}

func (p *BasePrioritizer[S]) RejectionRate() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rejectionRate
}

func (p *BasePrioritizer[S]) RejectionThreshold() int {
	return int(p.RejectionThresh.Load())
}

func (p *BasePrioritizer[S]) RegisteredPolicies() int {
	return int(p.numStats.Load())
}

// Calibrate computes a combined rejection rate and threshold based on all registered statsFuncs.
func (p *BasePrioritizer[S]) Calibrate() {
	p.mu.Lock()

	combinedStats := p.Strategy.CombineStats(p.statsFuncs)
	p.rejectionRate = combinedStats.ComputeRejectionRate()

	var newThresh int32
	if p.rejectionRate > 0 {
		newThresh = int32(p.LevelTracker.GetLevel(p.rejectionRate))
	}
	p.mu.Unlock()
	oldThresh := p.RejectionThresh.Swap(newThresh)

	if p.logger != nil && p.logger.Enabled(nil, slog.LevelDebug) {
		extraArgs := combinedStats.DebugLogArgs()
		p.logger.Debug("prioritizer calibration",
			append([]any{
				"newRate", fmt.Sprintf("%.2f", p.rejectionRate),
				"newThresh", newThresh,
			}, extraArgs...)...,
		)
	}

	if oldThresh != newThresh && p.onThresholdChanged != nil {
		p.onThresholdChanged(ThresholdChangedEvent{
			OldThreshold: uint(oldThresh),
			NewThreshold: uint(newThresh),
		})
	}

	if p.UsageTracker != nil {
		p.UsageTracker.Calibrate()
	}
}

// LevelFromContext gets a level based on usage from the user in the context, else returns LevelFromContext, else 0.
func (p *BasePrioritizer[S]) LevelFromContext(ctx context.Context) int {
	if ctx != nil {
		if p.UsageTracker != nil {
			if priority := FromContext(ctx); priority != -1 {
				return p.LevelFromContextWithPriority(ctx, priority)
			}
		}
		if level := LevelFromContext(ctx); level != -1 {
			return level
		}
	}
	return 0
}

// LevelFromContextWithPriority gets a level based on usage from the user in the context and the priority, else returns
// LevelFromContext, else a random level from the priority.
func (p *BasePrioritizer[S]) LevelFromContextWithPriority(ctx context.Context, priority Priority) int {
	if ctx != nil {
		if p.UsageTracker != nil {
			if userID := UserFromContext(ctx); userID != "" {
				return p.UsageTracker.GetLevel(userID, priority)
			}
		}
		if level := LevelFromContext(ctx); level != -1 {
			return level
		}
	}
	return priority.RandomLevel()
}

func (p *BasePrioritizer[S]) ScheduleCalibrations(ctx context.Context, interval time.Duration) context.CancelFunc {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				p.Calibrate()
			}
		}
	}()

	return func() {
		close(done)
	}
}
