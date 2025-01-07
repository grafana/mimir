package failsafe

import (
	"errors"
	"sync/atomic"

	"github.com/failsafe-go/failsafe-go/common"
)

// ErrExecutionCanceled indicates that an execution was canceled by ExecutionResult.Cancel.
var ErrExecutionCanceled = errors.New("execution canceled")

// ExecutionResult provides the result of an asynchronous execution.
type ExecutionResult[R any] interface {
	// Done is a channel that is closed when the execution is done and the result can be retrieved via Get, Result, or Error.
	Done() <-chan any

	// IsDone returns whether the execution is done and the result can be retrieved via Get.
	IsDone() bool

	// Get returns the execution result and error, else the default values, blocking until the execution is done.
	Get() (R, error)

	// Result returns the execution result else its default value, blocking until the execution is done.
	Result() R

	// Error returns the execution error else nil, blocking until the execution is done.
	Error() error

	// Cancel cancels the execution if it is not already done, with ErrExecutionCanceled as the error. If a Context was
	// configured with the execution, a child context will be created for the execution and canceled as well.
	Cancel()
}

type executionResult[R any] struct {
	*execution[R]
	cancelFunc func()
	doneChan   chan any
	done       atomic.Bool
	result     atomic.Pointer[*common.PolicyResult[R]]
}

func (e *executionResult[R]) record(result *common.PolicyResult[R]) {
	e.result.Store(&result)
	e.done.Store(true)
	close(e.doneChan)
}

func (e *executionResult[R]) Done() <-chan any {
	return e.doneChan
}

func (e *executionResult[R]) IsDone() bool {
	return e.done.Load()
}

func (e *executionResult[R]) Get() (R, error) {
	<-e.doneChan
	result := e.result.Load()
	if result != nil {
		return (*result).Result, (*result).Error
	}
	return *new(R), nil
}

func (e *executionResult[R]) Result() R {
	result, _ := e.Get()
	return result
}

func (e *executionResult[R]) Error() error {
	_, err := e.Get()
	return err
}

func (e *executionResult[R]) Cancel() {
	// Propagate cancelation to contexts
	e.execution.Cancel(&common.PolicyResult[R]{
		Error: ErrExecutionCanceled,
		Done:  true,
	})
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}
