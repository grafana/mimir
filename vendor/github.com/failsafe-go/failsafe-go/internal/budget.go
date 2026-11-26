package internal

import (
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/budget"
)

type Budget interface {
	budget.Budget

	// RecordExecution records that a primary execution has started.
	RecordExecution()

	// ReleaseExecution records that a primary execution has ended.
	ReleaseExecution()

	// TryAcquirePermit acquires a permit for a retry or hedge execution, returning false if the budget is exceeded.
	TryAcquirePermit() bool

	// ReleasePermit releases a previously acquired retry or hedge permit.
	ReleasePermit()

	// OnBudgetExceeded calls the OnBudgetExceeded event listener, if one is configured.
	OnBudgetExceeded(executionType budget.ExecutionType, info failsafe.ExecutionInfo)
}
