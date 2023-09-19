package circuitbreaker

import (
	"math"
	"time"

	"github.com/bits-and-blooms/bitset"

	"github.com/failsafe-go/failsafe-go/internal/util"
)

// stats for a CircuitBreaker.
type circuitStats interface {
	getExecutionCount() uint
	getFailureCount() uint
	getFailureRate() uint
	getSuccessCount() uint
	getSuccessRate() uint
	recordFailure()
	recordSuccess()
	reset()
}

// The default number of buckets to aggregate time-based stats into.
const defaultBucketCount = 10

// A circuitStats implementation that counts execution results using a BitSet.
type countingCircuitStats struct {
	bitSet *bitset.BitSet
	size   uint

	// Index to write next entry to
	currentIndex uint
	occupiedBits uint
	successes    uint
	failures     uint
}

func newStats[R any](config *circuitBreakerConfig[R], supportsTimeBased bool, capacity uint) circuitStats {
	if supportsTimeBased && config.failureThresholdingPeriod != 0 {
		return newTimedCircuitStats(defaultBucketCount, config.failureThresholdingPeriod, config.clock)
	}
	return newCountingCircuitStats(capacity)
}

func newCountingCircuitStats(size uint) *countingCircuitStats {
	return &countingCircuitStats{
		bitSet: bitset.New(size),
		size:   size,
	}
}

/*
Sets the value of the next bit in the bitset, returning the previous value, else -1 if no previous value was set for the bit.

value is true if positive/success, false if negative/failure
*/
func (c *countingCircuitStats) setNext(value bool) int {
	previousValue := -1
	if c.occupiedBits < c.size {
		c.occupiedBits++
	} else {
		if c.bitSet.Test(c.currentIndex) {
			previousValue = 1
		} else {
			previousValue = 0
		}
	}

	c.bitSet.SetTo(c.currentIndex, value)
	c.currentIndex = c.indexAfter(c.currentIndex)

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

func (c *countingCircuitStats) indexAfter(index uint) uint {
	if index == c.size-1 {
		return 0
	}
	return index + 1
}

func (c *countingCircuitStats) getExecutionCount() uint {
	return c.occupiedBits
}

func (c *countingCircuitStats) getFailureCount() uint {
	return c.failures
}

func (c *countingCircuitStats) getFailureRate() uint {
	if c.occupiedBits == 0 {
		return 0
	}
	return uint(math.Round(float64(c.failures) / float64(c.occupiedBits) * 100.0))
}

func (c *countingCircuitStats) getSuccessCount() uint {
	return c.successes
}

func (c *countingCircuitStats) getSuccessRate() uint {
	if c.occupiedBits == 0 {
		return 0
	}
	return uint(math.Round(float64(c.successes) / float64(c.occupiedBits) * 100.0))
}

func (c *countingCircuitStats) recordFailure() {
	c.setNext(false)
}

func (c *countingCircuitStats) recordSuccess() {
	c.setNext(true)
}

func (c *countingCircuitStats) reset() {
	c.bitSet.ClearAll()
	c.currentIndex = 0
	c.occupiedBits = 0
	c.successes = 0
	c.failures = 0
}

// A circuitStats implementation that counts execution results within a time period, and buckets results to minimize overhead.
type timedCircuitStats struct {
	clock      util.Clock
	bucketSize time.Duration
	windowSize time.Duration

	// Mutable state
	buckets      []*bucket
	summary      stat
	currentIndex int
}

type bucket struct {
	*stat
	startTime int64
}

type stat struct {
	successes uint
	failures  uint
}

func (s *stat) reset() {
	s.successes = 0
	s.failures = 0
}

func (s *stat) add(bucket *bucket) {
	s.successes += bucket.successes
	s.failures += bucket.failures
}

func (s *stat) remove(bucket *bucket) {
	s.successes -= bucket.successes
	s.failures -= bucket.failures
}

func newTimedCircuitStats(bucketCount int, thresholdingPeriod time.Duration, clock util.Clock) *timedCircuitStats {
	buckets := make([]*bucket, bucketCount)
	for i := 0; i < bucketCount; i++ {
		buckets[i] = &bucket{
			stat:      &stat{},
			startTime: -1,
		}
	}
	buckets[0].startTime = clock.CurrentUnixNano()
	result := &timedCircuitStats{
		buckets:    buckets,
		windowSize: thresholdingPeriod,
		bucketSize: thresholdingPeriod / time.Duration(bucketCount),
		clock:      clock,
		summary:    stat{},
	}
	return result
}

func (s *timedCircuitStats) getCurrentBucket() *bucket {
	previousBucket := s.buckets[s.currentIndex]
	currentBucket := previousBucket
	timeDiff := s.clock.CurrentUnixNano() - currentBucket.startTime
	if timeDiff >= s.bucketSize.Nanoseconds() {
		bucketsToMove := int(timeDiff / s.bucketSize.Nanoseconds())
		if bucketsToMove <= len(s.buckets) {
			// Reset some buckets
			for ; bucketsToMove > 0; bucketsToMove-- {
				s.currentIndex = s.nextIndex()
				previousBucket = currentBucket
				currentBucket = s.buckets[s.currentIndex]
				var bucketStartTime int64
				if currentBucket.startTime == -1 {
					bucketStartTime = previousBucket.startTime + s.bucketSize.Nanoseconds()
				} else {
					bucketStartTime = currentBucket.startTime + s.windowSize.Nanoseconds()
				}
				s.summary.remove(currentBucket)
				currentBucket.reset()
				currentBucket.startTime = bucketStartTime
			}
		} else {
			// Reset all buckets
			s.reset()
		}
	}

	return currentBucket
}

func (s *timedCircuitStats) nextIndex() int {
	return (s.currentIndex + 1) % len(s.buckets)
}

func (s *timedCircuitStats) getExecutionCount() uint {
	return s.summary.successes + s.summary.failures
}

func (s *timedCircuitStats) getFailureCount() uint {
	return s.summary.failures
}

func (s *timedCircuitStats) getFailureRate() uint {
	executions := s.getExecutionCount()
	if executions == 0 {
		return 0
	}
	return uint(math.Round(float64(s.summary.failures) / float64(executions) * 100.0))
}

func (s *timedCircuitStats) getSuccessCount() uint {
	return s.summary.successes
}

func (s *timedCircuitStats) getSuccessRate() uint {
	executions := s.getExecutionCount()
	if executions == 0 {
		return 0
	}
	return uint(math.Round(float64(s.summary.successes) / float64(executions) * 100.0))
}

func (s *timedCircuitStats) recordFailure() {
	s.getCurrentBucket().failures++
	s.summary.failures++
}

func (s *timedCircuitStats) recordSuccess() {
	s.getCurrentBucket().successes++
	s.summary.successes++
}

func (s *timedCircuitStats) reset() {
	startTime := s.clock.CurrentUnixNano()
	for _, bucket := range s.buckets {
		bucket.reset()
		bucket.startTime = startTime
		startTime += s.bucketSize.Nanoseconds()
	}
	s.summary.reset()
	s.currentIndex = 0
}
