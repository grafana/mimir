package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go/priority"
)

// BasePrioritizerConfig provides a base for implementing a PrioritizerBuilder.
type BasePrioritizerConfig[S Stats] struct {
	logger             *slog.Logger
	LevelTracker       priority.LevelTracker
	UsageTracker       priority.UsageTracker
	Strategy           RejectionStrategy[S]
	onThresholdChanged func(event priority.ThresholdChangedEvent)
}

var _ priority.PrioritizerBuilder = &BasePrioritizerConfig[Stats]{}

func (c *BasePrioritizerConfig[S]) WithLevelTracker(levelTracker priority.LevelTracker) priority.PrioritizerBuilder {
	c.LevelTracker = levelTracker
	return c
}

func (c *BasePrioritizerConfig[S]) WithLogger(logger *slog.Logger) priority.PrioritizerBuilder {
	c.logger = logger
	return c
}

func (c *BasePrioritizerConfig[S]) OnThresholdChanged(listener func(event priority.ThresholdChangedEvent)) priority.PrioritizerBuilder {
	c.onThresholdChanged = listener
	return c
}

func (c *BasePrioritizerConfig[S]) Build() priority.Prioritizer {
	pCopy := *c
	if pCopy.LevelTracker == nil {
		pCopy.LevelTracker = priority.NewLevelTracker()
	}
	return &BasePrioritizer[S]{
		BasePrioritizerConfig: pCopy, // TODO copy base fields
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
	BasePrioritizerConfig[S]

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
		p.onThresholdChanged(priority.ThresholdChangedEvent{
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
			if priority := priority.FromContext(ctx); priority != -1 {
				return p.LevelFromContextWithPriority(ctx, priority)
			}
		}
		if level := priority.LevelFromContext(ctx); level != -1 {
			return level
		}
	}
	return 0
}

// LevelFromContextWithPriority gets a level based on usage from the user in the context and the priority, else returns
// LevelFromContext, else a random level from the priority.
func (p *BasePrioritizer[S]) LevelFromContextWithPriority(ctx context.Context, prt priority.Priority) int {
	if ctx != nil {
		if p.UsageTracker != nil {
			if userID := priority.UserFromContext(ctx); userID != "" {
				return p.UsageTracker.GetLevel(userID, prt)
			}
		}
		if level := priority.LevelFromContext(ctx); level != -1 {
			return level
		}
	}
	return prt.RandomLevel()
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
