package failsafe

import (
	"context"

	"github.com/failsafe-go/failsafe-go/common"
)

// Run executes the fn, with failures being handled by the policies, until successful or until the policies are exceeded.
func Run(fn func() error, policies ...Policy[any]) error {
	return NewExecutor[any](policies...).Run(fn)
}

// RunWithExecution executes the fn, with failures being handled by the policies, until successful or until the policies
// are exceeded.
func RunWithExecution(fn func(exec Execution[any]) error, policies ...Policy[any]) error {
	return NewExecutor[any](policies...).RunWithExecution(fn)
}

// Get executes the fn, with failures being handled by the policies, until a successful result is returned or the
// policies are exceeded.
func Get[R any](fn func() (R, error), policies ...Policy[R]) (R, error) {
	return NewExecutor[R](policies...).Get(fn)
}

// GetWithExecution executes the fn, with failures being handled by the policies, until a successful result is returned
// or the policies are exceeded.
func GetWithExecution[R any](fn func(exec Execution[R]) (R, error), policies ...Policy[R]) (R, error) {
	return NewExecutor[R](policies...).GetWithExecution(fn)
}

// RunAsync executes the fn in a goroutine, with failures being handled by the policies, until successful or until the
// policies are exceeded.
func RunAsync(fn func() error, policies ...Policy[any]) ExecutionResult[any] {
	return NewExecutor[any](policies...).RunAsync(fn)
}

// RunWithExecutionAsync executes the fn in a goroutine, with failures being handled by the policies, until successful or
// until the policies are exceeded.
func RunWithExecutionAsync(fn func(exec Execution[any]) error, policies ...Policy[any]) ExecutionResult[any] {
	return NewExecutor[any](policies...).RunWithExecutionAsync(fn)
}

// GetAsync executes the fn in a goroutine, with failures being handled by the policies, until a successful result is returned
// or the policies are exceeded.
func GetAsync[R any](fn func() (R, error), policies ...Policy[R]) ExecutionResult[R] {
	return NewExecutor[R](policies...).GetAsync(fn)
}

// GetWithExecutionAsync executes the fn in a goroutine, with failures being handled by the policies, until a successful
// result is returned or the policies are exceeded.
func GetWithExecutionAsync[R any](fn func(exec Execution[R]) (R, error), policies ...Policy[R]) ExecutionResult[R] {
	return NewExecutor[R](policies...).GetWithExecutionAsync(fn)
}

// Executor handles failures according to configured policies. See [NewExecutor] for details.
//
// This type is concurrency safe.
type Executor[R any] interface {
	// WithContext returns a new copy of the Executor with the ctx configured. Any executions created with the resulting
	// Executor will be canceled when the ctx is done. Executions can cooperate with cancellation by checking
	// Execution.Canceled or Execution.IsCanceled.
	WithContext(ctx context.Context) Executor[R]

	// OnDone registers the listener to be called when an execution is done.
	OnDone(listener func(ExecutionDoneEvent[R])) Executor[R]

	// OnSuccess registers the listener to be called when an execution is successful. If multiple policies, are configured,
	// this handler is called when execution is done and all policies succeed. If all policies do not succeed, then the
	// OnFailure registered listener is called instead.
	OnSuccess(listener func(ExecutionDoneEvent[R])) Executor[R]

	// OnFailure registers the listener to be called when an execution fails. This occurs when the execution fails according
	// to some policy, and all policies have been exceeded.
	OnFailure(listener func(ExecutionDoneEvent[R])) Executor[R]

	// Run executes the fn until successful or until the configured policies are exceeded.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	Run(fn func() error) error

	// RunWithExecution executes the fn until successful or until the configured policies are exceeded, while providing an
	// Execution to the fn.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	RunWithExecution(fn func(exec Execution[R]) error) error

	// Get executes the fn until a successful result is returned or the configured policies are exceeded.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	Get(fn func() (R, error)) (R, error)

	// GetWithExecution executes the fn until a successful result is returned or the configured policies are exceeded, while
	// providing an Execution to the fn.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	GetWithExecution(fn func(exec Execution[R]) (R, error)) (R, error)

	// RunAsync executes the fn in a goroutine until successful or until the configured policies are exceeded.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	RunAsync(fn func() error) ExecutionResult[R]

	// RunWithExecutionAsync executes the fn in a goroutine until successful or until the configured policies are exceeded,
	// while providing an Execution to the fn.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	RunWithExecutionAsync(fn func(exec Execution[R]) error) ExecutionResult[R]

	// GetAsync executes the fn in a goroutine until a successful result is returned or the configured policies are exceeded.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	GetAsync(fn func() (R, error)) ExecutionResult[R]

	// GetWithExecutionAsync executes the fn in a goroutine until a successful result is returned or the configured policies
	// are exceeded, while providing an Execution to the fn.
	//
	// Any panic causes the execution to stop immediately without calling any event listeners.
	GetWithExecutionAsync(fn func(exec Execution[R]) (R, error)) ExecutionResult[R]
}

type executor[R any] struct {
	policies  []Policy[R]
	ctx       context.Context
	onDone    func(ExecutionDoneEvent[R])
	onSuccess func(ExecutionDoneEvent[R])
	onFailure func(ExecutionDoneEvent[R])
}

// NewExecutor creates and returns a new Executor for result type R that will handle failures according to the given
// policies. The policies are composed around a func and will handle its results in reverse order. For example, consider:
//
//	failsafe.NewExecutor(fallback, retryPolicy, circuitBreaker).Get(fn)
//
// This creates the following composition when executing a func and handling its result:
//
//	Fallback(RetryPolicy(CircuitBreaker(func)))
func NewExecutor[R any](policies ...Policy[R]) Executor[R] {
	return &executor[R]{
		policies: policies,
		ctx:      context.Background(),
	}
}

func (e *executor[R]) WithContext(ctx context.Context) Executor[R] {
	c := *e
	if ctx != nil {
		c.ctx = ctx
	}
	return &c
}

func (e *executor[R]) OnDone(listener func(ExecutionDoneEvent[R])) Executor[R] {
	e.onDone = listener
	return e
}

func (e *executor[R]) OnSuccess(listener func(ExecutionDoneEvent[R])) Executor[R] {
	e.onSuccess = listener
	return e
}

func (e *executor[R]) OnFailure(listener func(ExecutionDoneEvent[R])) Executor[R] {
	e.onFailure = listener
	return e
}

func (e *executor[R]) Run(fn func() error) error {
	_, err := e.executeSync(func(_ Execution[R]) (R, error) {
		return *new(R), fn()
	}, false)
	return err
}

func (e *executor[R]) RunWithExecution(fn func(exec Execution[R]) error) error {
	_, err := e.executeSync(func(exec Execution[R]) (R, error) {
		return *new(R), fn(exec)
	}, true)
	return err
}

func (e *executor[R]) Get(fn func() (R, error)) (R, error) {
	return e.executeSync(func(_ Execution[R]) (R, error) {
		return fn()
	}, false)
}

func (e *executor[R]) GetWithExecution(fn func(exec Execution[R]) (R, error)) (R, error) {
	return e.executeSync(func(exec Execution[R]) (R, error) {
		return fn(exec)
	}, true)
}

func (e *executor[R]) RunAsync(fn func() error) ExecutionResult[R] {
	return e.executeAsync(func(_ Execution[R]) (R, error) {
		return *new(R), fn()
	}, false)
}

func (e *executor[R]) RunWithExecutionAsync(fn func(exec Execution[R]) error) ExecutionResult[R] {
	return e.executeAsync(func(exec Execution[R]) (R, error) {
		return *new(R), fn(exec)
	}, true)
}

func (e *executor[R]) GetAsync(fn func() (R, error)) ExecutionResult[R] {
	return e.executeAsync(func(e Execution[R]) (R, error) {
		return fn()
	}, false)
}

func (e *executor[R]) GetWithExecutionAsync(fn func(exec Execution[R]) (R, error)) ExecutionResult[R] {
	return e.executeAsync(func(exec Execution[R]) (R, error) {
		return fn(exec)
	}, true)
}

// This type mirrors part of policy.Executor, which we don't import here to avoid a cycle.
type policyExecutor[R any] interface {
	Apply(innerFn func(Execution[R]) *common.PolicyResult[R]) func(Execution[R]) *common.PolicyResult[R]
}

func (e *executor[R]) executeSync(fn func(exec Execution[R]) (R, error), withExec bool) (R, error) {
	er := e.execute(fn, newExecution[R](e.ctx), withExec)
	return er.Result, er.Error
}

func (e *executor[R]) executeAsync(fn func(exec Execution[R]) (R, error), withExec bool) ExecutionResult[R] {
	var cancelFunc func()
	ctx := e.ctx
	if ctx != nil {
		ctx, cancelFunc = context.WithCancel(ctx)
	}
	exec := newExecution[R](ctx)
	result := &executionResult[R]{
		execution:  exec,
		cancelFunc: cancelFunc,
		doneChan:   make(chan any, 1),
	}
	go func() {
		result.record(e.execute(fn, exec, withExec))
	}()
	return result
}

func (e *executor[R]) execute(fn func(exec Execution[R]) (R, error), outerExec *execution[R], withExec bool) *common.PolicyResult[R] {
	outerFn := func(exec Execution[R]) *common.PolicyResult[R] {
		execInternal := exec.(*execution[R])
		var execForUser Execution[R]
		if withExec {
			// Only copy and provide an execution to the user fn if needed
			execForUser = execInternal.copy()
		}
		result, err := fn(execForUser)
		execInternal.record()
		return &common.PolicyResult[R]{
			Result:     result,
			Error:      err,
			Done:       true,
			Success:    true,
			SuccessAll: true,
		}
	}

	// Compose policy executors from the innermost policy to the outermost
	for i := len(e.policies) - 1; i >= 0; i-- {
		pe := e.policies[i].ToExecutor(*new(R)).(policyExecutor[R])
		outerFn = pe.Apply(outerFn)
	}

	// Execute
	er := outerFn(outerExec)

	if e.onSuccess != nil && er.SuccessAll {
		e.onSuccess(newExecutionDoneEvent(outerExec, er))
	} else if e.onFailure != nil && !er.SuccessAll {
		e.onFailure(newExecutionDoneEvent(outerExec, er))
	}
	if e.onDone != nil {
		e.onDone(newExecutionDoneEvent(outerExec, er))
	}
	return er
}
