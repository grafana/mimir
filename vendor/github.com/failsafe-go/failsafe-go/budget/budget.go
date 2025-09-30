package budget

import (
	"errors"
	"sync/atomic"

	"github.com/failsafe-go/failsafe-go"
)

// ErrExceeded is returned when an execution attempt exceeds the budget.
var ErrExceeded = errors.New("budget exceeded")

// Budget restricts concurrent executions as a way of preventing system overload.
//
// R is the execution result type. This type is concurrency safe.
type Budget interface {
	// RetryRate returns the current rate of retries relative to total executions, from 0 to 1.
	RetryRate() float64

	// HedgeRate returns the current rate of hedges relative to total executions, from 0 to 1.
	HedgeRate() float64
}

// Builder builds Budget instances.
//
// R is the execution result type. This type is not concurrency safe.
type Builder interface {
	// WithMaxRate configures the max rate of inflight executions that can be retries and/or hedges.
	WithMaxRate(maxRate float64) Builder

	// WithMinConcurrency configures the min number of budgeted retries and/or hedges that can be executed, regardless of
	// the total number of inflight executions.
	WithMinConcurrency(minConcurrency uint) Builder

	// OnBudgetExceeded registers the listener to be called when the budget is exceeded.
	OnBudgetExceeded(listener func(ExceededEvent)) Builder

	// Build returns a new Budget using the builder's configuration.
	Build() Budget
}

// ExecutionType indicates the type of execution used by the budget.
type ExecutionType string

const (
	// RetryExecution indicates a retry execution was used with the budget.
	RetryExecution ExecutionType = "retry"
	// HedgeExecution indicates a hedge execution was used with the budget.
	HedgeExecution ExecutionType = "hedge"
)

// ExceededEvent indicates a budget limit has been exceeded.
type ExceededEvent struct {
	// ExecutionType indicates the type of execution that exceeded the budget.
	ExecutionType ExecutionType

	// ExecutionInfo provides information about the execution that caused the budget to be exceeded.
	failsafe.ExecutionInfo

	// Budget provides access to the current budget state.
	Budget
}

type config struct {
	maxRate          float64
	minConcurrency   uint
	onBudgetExceeded func(ExceededEvent)
}

var _ Builder = &config{}

// New returns a new budget with a default maxRate of .2 and minConcurrency of 3.
func New() Budget {
	return NewBuilder().Build()
}

// NewBuilder returns a TypeBuilder for execution result type R which builds Budgets with a default maxRate of .2 and
// minConcurrency of 3.
func NewBuilder() Builder {
	return &config{
		maxRate:        .2,
		minConcurrency: 3,
	}
}

func (c *config) WithMaxRate(maxRate float64) Builder {
	c.maxRate = maxRate
	return c
}

func (c *config) WithMinConcurrency(minConcurrency uint) Builder {
	c.minConcurrency = minConcurrency
	return c
}

func (c *config) OnBudgetExceeded(listener func(ExceededEvent)) Builder {
	c.onBudgetExceeded = listener
	return c
}

func (c *config) Build() Budget {
	return &budget{
		config: *c, // TODO copy base fields
	}
}

type budget struct {
	config

	executions atomic.Int32
	retries    atomic.Int32
	hedges     atomic.Int32
}

func (b *budget) TryAcquireRetryPermit() bool {
	if b.RetryRate() > b.maxRate {
		return false
	}

	b.retries.Add(1)
	b.executions.Add(1)
	return true
}

func (b *budget) TryAcquireHedgePermit() bool {
	if b.HedgeRate() > b.maxRate {
		return false
	}

	b.hedges.Add(1)
	b.executions.Add(1)
	return true
}

func (b *budget) ReleaseRetryPermit() {
	b.retries.Add(-1)
	b.executions.Add(-1)
}

func (b *budget) ReleaseHedgePermit() {
	b.hedges.Add(-1)
	b.executions.Add(-1)
}

func (b *budget) RetryRate() float64 {
	return float64(b.retries.Load()) / float64(b.executions.Load())
}

func (b *budget) HedgeRate() float64 {
	return float64(b.hedges.Load()) / float64(b.executions.Load())
}

func (b *budget) OnBudgetExceeded(executionType ExecutionType, info failsafe.ExecutionInfo) {
	if b.onBudgetExceeded != nil {
		b.onBudgetExceeded(ExceededEvent{
			ExecutionType: executionType,
			ExecutionInfo: info,
			Budget:        b,
		})
	}
}
