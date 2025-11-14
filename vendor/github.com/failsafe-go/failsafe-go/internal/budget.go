package internal

import (
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/budget"
)

type Budget interface {
	budget.Budget

	// TryAcquireRetryPermit acquires a permit to retry an execution, else returns false if the budget is exceeded.
	TryAcquireRetryPermit() bool

	// TryAcquireHedgePermit acquires a permit to perform a hedged execution, else returns false if the budget is exceeded.
	TryAcquireHedgePermit() bool

	// ReleaseRetryPermit releases a previously acquired retry permit back to the budget.
	ReleaseRetryPermit()

	// ReleaseHedgePermit releases a previously acquired hedge permit back to the budget.
	ReleaseHedgePermit()

	// OnBudgetExceeded calls the OnBudgetExceeded event listener, if one is configured.
	OnBudgetExceeded(executionType budget.ExecutionType, info failsafe.ExecutionInfo)
}
