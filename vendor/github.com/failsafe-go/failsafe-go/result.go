package failsafe

import (
	"sync/atomic"

	"github.com/failsafe-go/failsafe-go/common"
)

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
}

type executionResult[R any] struct {
	doneChan chan any
	done     atomic.Bool
	result   atomic.Pointer[*common.PolicyResult[R]]
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
	select {
	case <-e.doneChan:
	}
	result := e.result.Load()
	if result != nil {
		return (*result).Result, (*result).Error
	}
	return *(new(R)), nil
}

func (e *executionResult[R]) Result() R {
	result, _ := e.Get()
	return result
}

func (e *executionResult[R]) Error() error {
	_, err := e.Get()
	return err
}
