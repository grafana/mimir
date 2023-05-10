package limiter

import (
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// LifoBlockingLimiter implements a Limiter that blocks the caller when the limit has been reached.  This strategy
// ensures the resource is properly protected but favors availability over latency by not fast failing requests when
// the limit has been reached.  To help keep success latencies low and minimize timeouts any blocked requests are
// processed in last in/first out order.
//
// Use this limiter only when the concurrency model allows the limiter to be blocked.
// Deprecated in favor of QueueBlockingLimiter
type LifoBlockingLimiter struct {
	*QueueBlockingLimiter
}

// NewLifoBlockingLimiter will create a new LifoBlockingLimiter
// Deprecated, use NewQueueBlockingLimiterFromConfig instead
func NewLifoBlockingLimiter(
	delegate core.Limiter,
	maxBacklogSize int,
	maxBacklogTimeout time.Duration,
	registry core.MetricRegistry,
	tags ...string,
) *LifoBlockingLimiter {

	return &LifoBlockingLimiter{
		NewQueueBlockingLimiterFromConfig(
			delegate,
			QueueLimiterConfig{
				MaxBacklogSize:    maxBacklogSize,
				MaxBacklogTimeout: maxBacklogTimeout,
				MetricRegistry:    registry,
				Tags:              tags,
			},
		),
	}
}

// NewLifoBlockingLimiterWithDefaults will create a new LifoBlockingLimiter with default values.
// Deprecated, use NewQueueBlockingLimiterFromConfig
func NewLifoBlockingLimiterWithDefaults(
	delegate core.Limiter,
) *LifoBlockingLimiter {
	return NewLifoBlockingLimiter(delegate, 0, 0, nil)
}
