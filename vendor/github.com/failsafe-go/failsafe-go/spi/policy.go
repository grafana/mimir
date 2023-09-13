package spi

import (
	"errors"
	"reflect"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal/util"
)

// BaseListenablePolicy provides a base for implementing ListenablePolicyBuilder.
type BaseListenablePolicy[R any] struct {
	successListener func(failsafe.ExecutionCompletedEvent[R])
	failureListener func(failsafe.ExecutionCompletedEvent[R])
}

func (bp *BaseListenablePolicy[R]) OnSuccess(listener func(event failsafe.ExecutionCompletedEvent[R])) {
	bp.successListener = listener
}

func (bp *BaseListenablePolicy[R]) OnFailure(listener func(event failsafe.ExecutionCompletedEvent[R])) {
	bp.failureListener = listener
}

// BaseFailurePolicy provides a base for implementing FailurePolicyBuilder.
type BaseFailurePolicy[R any] struct {
	// Indicates whether errors are checked by a configured failure condition
	errorsChecked bool
	// Conditions that determine whether an execution is a failure
	failureConditions []func(result R, err error) bool
}

func (p *BaseFailurePolicy[R]) HandleErrors(errs ...error) {
	for _, target := range errs {
		p.failureConditions = append(p.failureConditions, func(r R, actualErr error) bool {
			return errors.Is(actualErr, target)
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
	Delay   time.Duration
	DelayFn failsafe.DelayFunction[R]
}

func (d *BaseDelayablePolicy[R]) WithDelay(delay time.Duration) {
	d.Delay = delay
}

func (d *BaseDelayablePolicy[R]) WithDelayFn(delayFn failsafe.DelayFunction[R]) {
	d.DelayFn = delayFn
}

// ComputeDelay returns a computed delay else -1 if no delay could be computed.
func (d *BaseDelayablePolicy[R]) ComputeDelay(exec *failsafe.Execution[R]) time.Duration {
	if exec != nil && d.DelayFn != nil {
		return d.DelayFn(exec)
	}
	return -1
}
