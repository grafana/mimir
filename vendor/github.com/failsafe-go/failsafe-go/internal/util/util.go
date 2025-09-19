package util

import (
	"context"
	"math"
	"reflect"
	"time"
)

type number interface {
	~int | ~int64 | ~uint | ~uint64
}

func noop(_ error) {}

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// ErrorTypesMatch indicates whether the err or any unwrapped causes of the err are assignable to the target type. This is
// similar to the test that errors.As performs, but does not actually assign a value and allows a non-pointer target.
// This method also allows a non-pointer target for an error that's implemented with pointer receivers.
// Panics if target is nil or not an error.
func ErrorTypesMatch(err error, target any) bool {
	if err == nil {
		return false
	}
	if target == nil {
		panic("target cannot be nil")
	}
	targetType := reflect.TypeOf(target)
	if targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}
	if targetType.Kind() != reflect.Interface && !targetType.Implements(errorType) {
		// If targetType is not an error, convert it to a pointer and check again
		targetType = reflect.PointerTo(targetType)
		if !targetType.Implements(errorType) {
			panic("target must be interface or implement error")
		}
	}
	return errorAs(err, targetType)
}

func errorAs(err error, targetType reflect.Type) bool {
	for {
		if reflect.TypeOf(err).AssignableTo(targetType) {
			return true
		}
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			err = x.Unwrap()
			if err == nil {
				return false
			}
		case interface{ Unwrap() []error }:
			for _, err := range x.Unwrap() {
				if err == nil {
					continue
				}
				if errorAs(err, targetType) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
}

// MergeContexts returns a context that is canceled when either ctx1 or ctx2 are Done.
func MergeContexts(ctx1, ctx2 context.Context) (context.Context, context.CancelCauseFunc) {
	if ctx1 == ctx2 {
		return ctx1, noop
	}
	bgContext := context.Background()
	if ctx1 == bgContext {
		return ctx2, noop
	}
	if ctx2 == bgContext {
		return ctx1, noop
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		select {
		case <-ctx1.Done():
			cancel(ctx1.Err())
		case <-ctx2.Done():
			cancel(ctx2.Err())
		}
	}()
	return ctx, cancel
}

// AppliesToAny returns true if any of the biPredicates evaluate to true for the values.
func AppliesToAny[A any, B any](biPredicates []func(A, B) bool, value1 A, value2 B) bool {
	for _, p := range biPredicates {
		if p(value1, value2) {
			return true
		}
	}
	return false
}

// RoundDown returns the input rounded down to the nearest interval.
func RoundDown[T number](input T, interval T) T {
	return input - input%interval
}

func RandomDelayInRange[T number](delayMin T, delayMax T, random float64) T {
	min64 := float64(delayMin)
	max64 := float64(delayMax)
	return T(random*(max64-min64) + min64)
}

func RandomDelay[T number](delay T, jitter T, random float64) T {
	randomAddend := (1 - random*2) * float64(jitter)
	return delay + T(randomAddend)
}

func RandomDelayFactor[T number](delay T, jitterFactor float64, random float64) T {
	randomFactor := 1 + (1-random*2)*jitterFactor
	return T(float64(delay) * randomFactor)
}

// Smooth returns a value that is decreased by some portion of the oldValue, and increased by some portion of the
// newValue, based on the factor.
func Smooth(oldValue, newValue, factor float64) float64 {
	return oldValue*(1-factor) + newValue*factor
}

var log10Values []int

func init() {
	for i := 0; i < 100; i++ {
		log10Values = append(log10Values, 1)
	}
	for i := 100; i < 1000; i++ {
		log10Values = append(log10Values, 2)
	}
}

func Log10Func(factor int) func(limit int) int {
	return func(limit int) int {
		if limit < len(log10Values) {
			return factor * log10Values[limit]
		}
		return factor * int(math.Log10(float64(limit)))
	}
}

type Clock interface {
	Now() time.Time
}

var WallClock = &wallClock{}

type wallClock struct {
}

func (wc *wallClock) Now() time.Time {
	return time.Now()
}

type Stopwatch interface {
	ElapsedTime() time.Duration

	Reset()
}

type wallClockStopwatch struct {
	startTime time.Time
}

func NewStopwatch() Stopwatch {
	return &wallClockStopwatch{
		startTime: time.Now(),
	}
}

func (s *wallClockStopwatch) ElapsedTime() time.Duration {
	return time.Since(s.startTime)
}

func (s *wallClockStopwatch) Reset() {
	s.startTime = time.Now()
}

func Round(v float64) float64 {
	return math.Round(v*100) / 100
}
