package circuitbreaker

import (
	"math"
	"time"

	"github.com/bits-and-blooms/bitset"

	"github.com/failsafe-go/failsafe-go/internal/util"
)

// Stats for a CircuitBreaker.
// Implementations are not concurrency safe and must be guarded externally.
type stats interface {
	executionCount() uint
	failureCount() uint
	failureRate() uint
	successCount() uint
	successRate() uint
	recordFailure()
	recordSuccess()
	reset()
}

// The default number of buckets to aggregate time-based stats into.
const defaultBucketCount = 10

// countingStats is a stats implementation that counts execution results using a BitSet.
type countingStats struct {
	bitSet *bitset.BitSet
	size   uint

	// Index to write next entry to
	head         uint
	occupiedBits uint
	successes    uint
	failures     uint
}

func newStats[R any](config *config[R], supportsTimeBased bool, capacity uint) stats {
	if supportsTimeBased && config.failureThresholdingPeriod != 0 {
		return newTimedStats(defaultBucketCount, config.failureThresholdingPeriod, config.clock)
	}
	return newCountingStats(capacity)
}

func newCountingStats(size uint) *countingStats {
	return &countingStats{
		bitSet: bitset.New(size),
		size:   size,
	}
}

/*
Sets the value of the next bit in the bitset, returning the previous value, else -1 if no previous value was set for the bit.

value is true if positive/success, false if negative/failure
*/
func (c *countingStats) setNext(value bool) int {
	previousValue := -1
	if c.occupiedBits < c.size {
		c.occupiedBits++
	} else {
		if c.bitSet.Test(c.head) {
			previousValue = 1
		} else {
			previousValue = 0
		}
	}

	c.bitSet.SetTo(c.head, value)
	c.head = (c.head + 1) % c.size

	if value {
		if previousValue != 1 {
			c.successes++
		}
		if previousValue == 0 {
			c.failures--
		}
	} else {
		if previousValue != 0 {
			c.failures++
		}
		if previousValue == 1 {
			c.successes--
		}
	}

	return previousValue
}

func (c *countingStats) executionCount() uint {
	return c.occupiedBits
}

func (c *countingStats) failureCount() uint {
	return c.failures
}

func (c *countingStats) failureRate() uint {
	if c.occupiedBits == 0 {
		return 0
	}
	return uint(math.Round(float64(c.failures) / float64(c.occupiedBits) * 100.0))
}

func (c *countingStats) successCount() uint {
	return c.successes
}

func (c *countingStats) successRate() uint {
	if c.occupiedBits == 0 {
		return 0
	}
	return uint(math.Round(float64(c.successes) / float64(c.occupiedBits) * 100.0))
}

func (c *countingStats) recordFailure() {
	c.setNext(false)
}

func (c *countingStats) recordSuccess() {
	c.setNext(true)
}

func (c *countingStats) reset() {
	c.bitSet.ClearAll()
	c.head = 0
	c.occupiedBits = 0
	c.successes = 0
	c.failures = 0
}

// timedStats is a stats implementation that counts execution results within a time period, and buckets results to minimize overhead.
type timedStats struct {
	clock       util.Clock
	bucketCount int64
	bucketNanos int64

	// Mutable state
	buckets []stat
	summary stat
	head    int64
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

func newTimedStats(bucketCount int, thresholdingPeriod time.Duration, clock util.Clock) *timedStats {
	buckets := make([]stat, bucketCount)
	for i := 0; i < bucketCount; i++ {
		buckets[i] = stat{}
	}
	return &timedStats{
		clock:       clock,
		bucketCount: int64(bucketCount),
		bucketNanos: (thresholdingPeriod / time.Duration(bucketCount)).Nanoseconds(),
		buckets:     buckets,
		summary:     stat{},
	}
}

func (s *timedStats) currentBucket() *stat {
	newHead := s.clock.CurrentUnixNano() / s.bucketNanos

	if newHead > s.head {
		bucketsToMove := min(s.bucketCount, newHead-s.head)
		for i := int64(0); i < bucketsToMove; i++ {
			currentBucket := &s.buckets[(s.head+i+1)%s.bucketCount]
			s.summary.remove(currentBucket)
			currentBucket.reset()
		}
		s.head = newHead
	}

	return &s.buckets[s.head%s.bucketCount]
}

func (s *timedStats) executionCount() uint {
	return s.summary.successes + s.summary.failures
}

func (s *timedStats) failureCount() uint {
	return s.summary.failures
}

func (s *timedStats) failureRate() uint {
	executions := s.executionCount()
	if executions == 0 {
		return 0
	}
	return uint(math.Round(float64(s.summary.failures) / float64(executions) * 100.0))
}

func (s *timedStats) successCount() uint {
	return s.summary.successes
}

func (s *timedStats) successRate() uint {
	executions := s.executionCount()
	if executions == 0 {
		return 0
	}
	return uint(math.Round(float64(s.summary.successes) / float64(executions) * 100.0))
}

func (s *timedStats) recordFailure() {
	s.currentBucket().failures++
	s.summary.failures++
}

func (s *timedStats) recordSuccess() {
	s.currentBucket().successes++
	s.summary.successes++
}

func (s *timedStats) reset() {
	for i := range s.buckets {
		(&s.buckets[i]).reset()
	}
	s.summary.reset()
	s.head = 0
}
