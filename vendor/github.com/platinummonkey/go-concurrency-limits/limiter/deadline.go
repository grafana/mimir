package limiter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/limit"
)

// DeadlineLimiter that blocks the caller when the limit has been reached.  The caller is
// blocked until the limiter has been released, or a deadline has been passed.
type DeadlineLimiter struct {
	logger   limit.Logger
	delegate core.Limiter
	deadline time.Time
	c        *sync.Cond
}

// NewDeadlineLimiter will create a new DeadlineLimiter that will wrap a limiter such that acquire will block until a
// provided deadline if the limit was reached instead of returning an empty listener immediately.
func NewDeadlineLimiter(
	delegate core.Limiter,
	deadline time.Time,
	logger limit.Logger,
) *DeadlineLimiter {
	mu := sync.Mutex{}
	if logger == nil {
		logger = limit.NoopLimitLogger{}
	}
	return &DeadlineLimiter{
		logger:   logger,
		delegate: delegate,
		c:        sync.NewCond(&mu),
		deadline: deadline,
	}
}

// tryAcquire will block when attempting to acquire a token
func (l *DeadlineLimiter) tryAcquire(ctx context.Context) (listener core.Listener, ok bool) {
	l.c.L.Lock()
	defer l.c.L.Unlock()
	for {
		// if the deadline has passed, fail quickly
		if time.Now().UTC().After(l.deadline) {
			return nil, false
		}

		// try to acquire a new token and return immediately if successful
		listener, ok := l.delegate.Acquire(ctx)
		if ok && listener != nil {
			l.logger.Debugf("delegate returned a listener ctx=%v", ctx)
			return listener, true
		}

		// We have reached the limit so block until a token is released
		timeout := l.deadline.Sub(time.Now().UTC())

		// block until we timeout
		timeoutWaiter := newTimeoutWaiter(l.c, timeout)
		timeoutWaiter.start()
		l.logger.Debugf("Blocking waiting for release or timeout ctx=%v", ctx)
		<-timeoutWaiter.wait()
		l.logger.Debugf("blocking released, trying again to acquire ctx=%v", ctx)
	}
}

// Acquire a token from the limiter.  Returns `nil, false` if the limit has been exceeded.
// If acquired the caller must call one of the Listener methods when the operation has been completed to release
// the count.
//
// context Context for the request. The context is used by advanced strategies such as LookupPartitionStrategy.
func (l *DeadlineLimiter) Acquire(ctx context.Context) (listener core.Listener, ok bool) {
	delegateListener, ok := l.tryAcquire(ctx)
	if !ok && delegateListener == nil {
		l.logger.Debugf("did not acquire ctx=%v", ctx)
		return nil, false
	}
	l.logger.Debugf("acquired, returning listener ctx=%v", ctx)
	return &DelegateListener{
		delegateListener: delegateListener,
		c:                l.c,
	}, true
}

// String implements Stringer for easy debugging.
func (l DeadlineLimiter) String() string {
	return fmt.Sprintf("DeadlineLimiter{delegate=%v}", l.delegate)
}
