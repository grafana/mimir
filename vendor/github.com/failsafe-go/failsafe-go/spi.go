package failsafe

import (
	"time"
)

// ExecutionResult represents the internal result of an execution attempt for zero or more policies, before or after the policy has handled
// the result. If a policy is done handling a result or is no longer able to handle a result, such as when retries are exceeded, the
// ExecutionResult should be marked as complete.
//
// Part of the Failsafe-go SPI.
type ExecutionResult[R any] struct {
	Result     R
	Err        error
	Complete   bool
	Success    bool
	SuccessAll bool
}

// WithComplete returns a new ExecutionResult for the complete and success values.
func (er *ExecutionResult[R]) WithComplete(complete bool, success bool) *ExecutionResult[R] {
	c := *er
	c.Complete = complete
	c.Success = success
	c.SuccessAll = success && c.SuccessAll
	return &c
}

// WithFailure returns a new ExecutionResult that is marked as not successful.
func (er *ExecutionResult[R]) WithFailure() *ExecutionResult[R] {
	c := *er
	c.Success = false
	c.SuccessAll = false
	return &c
}

// ExecutionInternal contains internal execution APIs.
//
// Part of the Failsafe-go SPI.
type ExecutionInternal[R any] struct {
	Execution[R]
	// Guarded by mtx
	attemptRecorded bool
}

// InitializeAttempt prepares a new execution attempt, returning false if the attempt cannot be initialized since it was canceled.
func (e *ExecutionInternal[R]) InitializeAttempt(policyExecutor PolicyExecutor[R]) bool {
	// Lock to guard against a race with a Timeout canceling the execution
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if policyExecutor != nil && e.isCancelled(policyExecutor) {
		return false
	}
	e.attemptRecorded = false
	e.Attempts++
	e.AttemptStartTime = time.Now()
	e.canceledIndex = -1
	return true
}

// Record records the result of an execution attempt.
func (e *ExecutionInternal[R]) Record(result *ExecutionResult[R]) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.record(result)
}

// Cancel marks the execution as having been cancelled by the policyExecutor, which will also cancel pending executions of any inner
// policies of the policyExecutor, and also records the result. Outer policies of the policyExecutor will be unaffected.
func (e *ExecutionInternal[R]) Cancel(policyExecutor PolicyExecutor[R], result *ExecutionResult[R]) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.canceledIndex = policyExecutor.GetPolicyIndex()
	e.record(result)
}

// IsCanceled returns whether the execution is considered canceled for the policyExecutor.
func (e *ExecutionInternal[R]) IsCanceled(policyExecutor PolicyExecutor[R]) bool {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.isCancelled(policyExecutor)
}

// Requires locking externally.
func (e *ExecutionInternal[R]) isCancelled(policyExecutor PolicyExecutor[R]) bool {
	return e.canceledIndex > policyExecutor.GetPolicyIndex() || (e.Context != nil && e.Context.Err() != nil)
}

// Requires locking externally.
func (e *ExecutionInternal[R]) record(result *ExecutionResult[R]) {
	if !e.attemptRecorded {
		e.attemptRecorded = true
		e.LastResult = result.Result
		e.LastErr = result.Err
	}
}

// ExecutionHandler returns an ExecutionResult for an ExecutionInternal.
//
// Part of the Failsafe-go SPI.
type ExecutionHandler[R any] func(*ExecutionInternal[R]) *ExecutionResult[R]

// PolicyExecutor handles execution and execution results according to a policy. May contain pre-execution and post-execution behaviors.
// Each PolicyExecutor makes its own determination about whether an execution result is a success or failure.
//
// Part of the Failsafe-go SPI.
type PolicyExecutor[R any] interface {
	// PreExecute is called before execution to return an alternative result or error, such as if execution is not allowed or needed.
	PreExecute(exec *ExecutionInternal[R]) *ExecutionResult[R]

	// Apply performs an execution by calling PreExecute and returning any result, else calling the innerFn PostExecute.
	Apply(innerFn ExecutionHandler[R]) ExecutionHandler[R]

	// PostExecute performs synchronous post-execution handling for an execution result.
	PostExecute(exec *ExecutionInternal[R], result *ExecutionResult[R]) *ExecutionResult[R]

	// IsFailure returns whether the result is a failure according to the corresponding policy.
	IsFailure(result *ExecutionResult[R]) bool

	// OnSuccess performs post-execution handling for a result that is considered a success according to IsFailure.
	OnSuccess(result *ExecutionResult[R])

	// OnFailure performs post-execution handling for a result that is considered a failure according to IsFailure, possibly creating a new
	// result, else returning the original result.
	OnFailure(exec *Execution[R], result *ExecutionResult[R]) *ExecutionResult[R]

	// GetPolicyIndex returns the index of the policy relative to other policies in a composition, where the innermost policy in a
	// composition has an index of 0.
	GetPolicyIndex() int
}
