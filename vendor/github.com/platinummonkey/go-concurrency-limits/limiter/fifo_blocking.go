package limiter

import (
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// FifoBlockingLimiter implements a Limiter that blocks the caller when the limit has been reached.  This strategy
// ensures the resource is properly protected but favors availability over latency by not fast failing requests when
// the limit has been reached.  To help keep success latencies low and minimize timeouts any blocked requests are
// processed in last in/first out order.
//
// Use this limiter only when the concurrency model allows the limiter to be blocked.
// Deprecated in favor of QueueBlockingLimiter
type FifoBlockingLimiter struct {
	*QueueBlockingLimiter
}

// NewFifoBlockingLimiter will create a new FifoBlockingLimiter
// Deprecated, use NewQueueBlockingLimiterFromConfig instead
func NewFifoBlockingLimiter(
	delegate core.Limiter,
	maxBacklogSize int,
	maxBacklogTimeout time.Duration,
) *FifoBlockingLimiter {

	return &FifoBlockingLimiter{
		NewQueueBlockingLimiterFromConfig(delegate, QueueLimiterConfig{
			Ordering:          OrderingFIFO,
			MaxBacklogSize:    maxBacklogSize,
			MaxBacklogTimeout: maxBacklogTimeout,
		}),
	}
}

// NewFifoBlockingLimiterWithDefaults will create a new FifoBlockingLimiter with default values.
// Deprecated, use NewQueueBlockingLimiterWithDefaults instead
func NewFifoBlockingLimiterWithDefaults(
	delegate core.Limiter,
) *FifoBlockingLimiter {
	return NewFifoBlockingLimiter(delegate, 100, time.Millisecond*1000)
}
