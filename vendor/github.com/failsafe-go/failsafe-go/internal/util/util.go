package util

import (
	"time"
)

type number interface {
	~int | ~int64 | ~uint | ~uint64
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
func RoundDown(input time.Duration, interval time.Duration) time.Duration {
	return (input / interval) * interval
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

func RandomDelayFactor[T number](delay T, jitterFactor float32, random float32) T {
	randomFactor := 1 + (1-random*2)*jitterFactor
	return T(float32(delay) * randomFactor)
}

type Clock interface {
	CurrentUnixNano() int64
}

type wallClock struct {
}

func (wc *wallClock) CurrentUnixNano() int64 {
	return time.Now().UnixNano()
}

func NewClock() Clock {
	return &wallClock{}
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
