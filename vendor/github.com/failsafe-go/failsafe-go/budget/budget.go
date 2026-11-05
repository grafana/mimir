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
// This type is concurrency safe.
type Budget interface {
	// Rate returns the current combined rate of retries and hedges relative to total inflight executions, from 0 to 1.
	Rate() float64
}

// Builder builds Budget instances.
//
// This type is not concurrency safe.
type Builder interface {
	// WithMaxRate configures the max rate of inflight executions that can be retries and/or hedges.
	WithMaxRate(maxRate float64) Builder

	// WithMinConcurrency configures a minimum execution budget. At least minConcurrency retries and/or hedges may execute
	// concurrently, even when the budget computed from WithMaxRate and the current inflight execution count falls below
	// minConcurrency.
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
	inflight   atomic.Int32 // current count of inflight retries and hedges
}

func (b *budget) RecordExecution() {
	b.executions.Add(1)
}

func (b *budget) ReleaseExecution() {
	b.executions.Add(-1)
}

func (b *budget) TryAcquirePermit() bool {
	budgeted := b.inflight.Load()
	if budgeted >= int32(b.minConcurrency) && b.Rate() > b.maxRate {
		return false
	}

	b.inflight.Add(1)
	b.RecordExecution()
	return true
}

func (b *budget) ReleasePermit() {
	b.inflight.Add(-1)
	b.ReleaseExecution()
}

func (b *budget) Rate() float64 {
	executions := b.executions.Load()
	if executions == 0 {
		return 0
	}
	return float64(b.inflight.Load()) / float64(executions)
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
