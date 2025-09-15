package util

import (
	"time"

	"github.com/bits-and-blooms/bitset"
)

// ExecutionStats for tracking execution results.
// Implementations are not concurrency safe and must be guarded externally.
type ExecutionStats interface {
	ExecutionCount() uint
	FailureCount() uint
	FailureRate() float64
	SuccessCount() uint
	SuccessRate() float64
	RecordFailure()
	RecordSuccess()
	Reset()
}

// countingStats is a ExecutionStats implementation that counts execution results using a BitSet.
type countingStats struct {
	bitSet       *bitset.BitSet
	head         uint
	occupiedBits uint
	successes    uint
	failures     uint
}

func NewCountingStats(size uint) ExecutionStats {
	return &countingStats{
		bitSet: bitset.New(size),
	}
}

/*
Sets the value of the next bit in the bitset, returning the previous value, else -1 if no previous value was set for the bit.

value is true if positive/success, false if negative/failure
*/
func (c *countingStats) setNext(value bool) int {
	previousValue := -1
	if c.occupiedBits < c.bitSet.Len() {
		c.occupiedBits++
	} else {
		if c.bitSet.Test(c.head) {
			previousValue = 1
			c.successes--
		} else {
			previousValue = 0
			c.failures--
		}
	}

	if value {
		c.successes++
	} else {
		c.failures++
	}

	c.bitSet.SetTo(c.head, value)
	c.head = (c.head + 1) % c.bitSet.Len()

	return previousValue
}

func (c *countingStats) ExecutionCount() uint {
	return c.occupiedBits
}

func (c *countingStats) FailureCount() uint {
	return c.failures
}

func (c *countingStats) FailureRate() float64 {
	if c.occupiedBits == 0 {
		return 0
	}
	return Round(float64(c.failures) / float64(c.occupiedBits))
}

func (c *countingStats) SuccessCount() uint {
	return c.successes
}

func (c *countingStats) SuccessRate() float64 {
	if c.occupiedBits == 0 {
		return 0
	}
	return Round(float64(c.successes) / float64(c.occupiedBits))
}

func (c *countingStats) RecordFailure() {
	c.setNext(false)
}

func (c *countingStats) RecordSuccess() {
	c.setNext(true)
}

func (c *countingStats) Reset() {
	c.bitSet.ClearAll()
	c.head = 0
	c.occupiedBits = 0
	c.successes = 0
	c.failures = 0
}

// timedStats is a ExecutionStats implementation that counts execution results within a time period, and buckets results to minimize overhead.
type timedStats struct {
	clock       Clock
	bucketCount int64
	bucketNanos int64

	// Mutable state
	buckets  []stat
	summary  stat
	headTime int64
}

type stat struct {
	successes uint
	failures  uint
}

func (s *stat) reset() {
	s.successes = 0
	s.failures = 0
}

func (s *stat) remove(bucket *stat) {
	s.successes -= bucket.successes
	s.failures -= bucket.failures
}

func NewTimedStats(bucketCount int, thresholdingPeriod time.Duration, clock Clock) ExecutionStats {
	buckets := make([]stat, bucketCount)
	return &timedStats{
		clock:       clock,
		bucketCount: int64(bucketCount),
		bucketNanos: (thresholdingPeriod / time.Duration(bucketCount)).Nanoseconds(),
		buckets:     buckets,
		summary:     stat{},
	}
}

func (s *timedStats) currentBucket() *stat {
	newHead := s.clock.Now().UnixNano() / s.bucketNanos

	if newHead > s.headTime {
		bucketsToMove := min(s.bucketCount, newHead-s.headTime)
		for i := int64(0); i < bucketsToMove; i++ {
			currentBucket := &s.buckets[(s.headTime+i+1)%s.bucketCount]
			s.summary.remove(currentBucket)
			currentBucket.reset()
		}
		s.headTime = newHead
	}

	return &s.buckets[s.headTime%s.bucketCount]
}

func (s *timedStats) ExecutionCount() uint {
	return s.summary.successes + s.summary.failures
}

func (s *timedStats) FailureCount() uint {
	return s.summary.failures
}

func (s *timedStats) FailureRate() float64 {
	executions := s.ExecutionCount()
	if executions == 0 {
		return 0
	}
	return Round(float64(s.summary.failures) / float64(executions))
}

func (s *timedStats) SuccessCount() uint {
	return s.summary.successes
}

func (s *timedStats) SuccessRate() float64 {
	executions := s.ExecutionCount()
	if executions == 0 {
		return 0
	}
	return Round(float64(s.summary.successes) / float64(executions))
}

func (s *timedStats) RecordFailure() {
	s.currentBucket().failures++
	s.summary.failures++
}

func (s *timedStats) RecordSuccess() {
	s.currentBucket().successes++
	s.summary.successes++
}

func (s *timedStats) Reset() {
	for i := range s.buckets {
		(&s.buckets[i]).reset()
	}
	s.summary.reset()
	s.headTime = 0
}
