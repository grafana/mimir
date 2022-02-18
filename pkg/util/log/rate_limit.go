// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"time"

	"github.com/go-kit/log"
	"go.uber.org/atomic"
)

// RateLimitedLogger implements log.Logger and permits only a single log invocation every interval.
type RateLimitedLogger struct {
	now         func() time.Time
	interval    time.Duration
	nextLogTime *atomic.Value
	delegate    log.Logger
}

func NewRateLimitedLogger(interval time.Duration, delegate log.Logger, now func() time.Time) RateLimitedLogger {
	nextLogTime := &atomic.Value{}
	nextLogTime.Store(now())

	return RateLimitedLogger{
		now:         now,
		nextLogTime: nextLogTime,
		interval:    interval,
		delegate:    delegate,
	}
}

func (r RateLimitedLogger) Log(keyvals ...interface{}) error {
	now := r.now()
	nextLogTime := r.nextLogTime.Load().(time.Time)

	if !now.After(nextLogTime) {
		return nil
	}
	if r.nextLogTime.CompareAndSwap(nextLogTime, now.Add(r.interval)) {
		return r.delegate.Log(keyvals...)
	}
	return nil
}
