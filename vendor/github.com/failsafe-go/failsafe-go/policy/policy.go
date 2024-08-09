package policy

import (
	"errors"
	"reflect"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal/util"
)

// BaseFailurePolicy provides a base for implementing FailurePolicyBuilder.
type BaseFailurePolicy[R any] struct {
	// Indicates whether errors are checked by a configured failure condition
	errorsChecked bool
	// Conditions that determine whether an execution is a failure
	failureConditions []func(result R, err error) bool
	onSuccess         func(failsafe.ExecutionEvent[R])
	onFailure         func(failsafe.ExecutionEvent[R])
}

func (p *BaseFailurePolicy[R]) HandleErrors(errs ...error) {
	for _, target := range errs {
		t := target
		p.failureConditions = append(p.failureConditions, func(r R, actualErr error) bool {
			return errors.Is(actualErr, t)
		})
	}
	p.errorsChecked = true
}

func (p *BaseFailurePolicy[R]) HandleErrorTypes(errs ...any) {
	for _, target := range errs {
		t := target
		p.failureConditions = append(p.failureConditions, func(r R, actualErr error) bool {
			return util.ErrorTypesMatch(actualErr, t)
		})
	}
	p.errorsChecked = true
}

func (p *BaseFailurePolicy[R]) HandleResult(result R) {
	p.failureConditions = append(p.failureConditions, func(r R, err error) bool {
		return reflect.DeepEqual(r, result)
	})
}

func (p *BaseFailurePolicy[R]) HandleIf(predicate func(R, error) bool) {
	p.failureConditions = append(p.failureConditions, predicate)
	p.errorsChecked = true
}

func (p *BaseFailurePolicy[R]) OnSuccess(listener func(event failsafe.ExecutionEvent[R])) {
	p.onSuccess = listener
}

func (p *BaseFailurePolicy[R]) OnFailure(listener func(event failsafe.ExecutionEvent[R])) {
	p.onFailure = listener
}

func (p *BaseFailurePolicy[R]) IsFailure(result R, err error) bool {
	if len(p.failureConditions) == 0 {
		return err != nil
	}
	if util.AppliesToAny(p.failureConditions, result, err) {
		return true
	}

	// Fail by default if an error exists and was not checked by a condition
	return err != nil && !p.errorsChecked
}

// BaseDelayablePolicy provides a base for implementing DelayablePolicyBuilder.
type BaseDelayablePolicy[R any] struct {
	Delay     time.Duration
	DelayFunc failsafe.DelayFunc[R]
}

func (d *BaseDelayablePolicy[R]) WithDelay(delay time.Duration) {
	d.Delay = delay
}

func (d *BaseDelayablePolicy[R]) WithDelayFunc(delayFunc failsafe.DelayFunc[R]) {
	d.DelayFunc = delayFunc
}

// ComputeDelay returns a computed delay else -1 if no delay could be computed.
func (d *BaseDelayablePolicy[R]) ComputeDelay(exec failsafe.ExecutionAttempt[R]) time.Duration {
	if exec != nil && d.DelayFunc != nil {
		return d.DelayFunc(exec)
	}
	return -1
}

// BaseAbortablePolicy provides a base for implementing policies that can be aborted or canceled.
type BaseAbortablePolicy[R any] struct {
	// Conditions that determine whether the policy should be aborted
	abortConditions []func(result R, err error) bool
}

func (c *BaseAbortablePolicy[R]) AbortOnResult(result R) {
	c.abortConditions = append(c.abortConditions, func(r R, err error) bool {
		return reflect.DeepEqual(r, result)
	})
}

func (c *BaseAbortablePolicy[R]) AbortOnErrors(errs ...error) {
	for _, target := range errs {
		t := target
		c.abortConditions = append(c.abortConditions, func(result R, actualErr error) bool {
			return errors.Is(actualErr, t)
		})
	}
}

func (c *BaseAbortablePolicy[R]) AbortOnErrorTypes(errs ...any) {
	for _, target := range errs {
		t := target
		c.abortConditions = append(c.abortConditions, func(result R, actualErr error) bool {
			return util.ErrorTypesMatch(actualErr, t)
		})
	}
}

func (c *BaseAbortablePolicy[R]) AbortIf(predicate func(R, error) bool) {
	c.abortConditions = append(c.abortConditions, func(result R, err error) bool {
		return predicate(result, err)
	})
}

func (c *BaseAbortablePolicy[R]) IsConfigured() bool {
	return len(c.abortConditions) > 0
}

func (c *BaseAbortablePolicy[R]) IsAbortable(result R, err error) bool {
	return util.AppliesToAny(c.abortConditions, result, err)
}
