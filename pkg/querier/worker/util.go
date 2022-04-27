// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"
)

// executionContext wraps the context.Context used to run the querier's worker loop and execute
// queries. The purpose of the execution context is to gracefully shutdown queriers, waiting
// until inflight queries are terminated before the querier process exits.
//
// How it's used:
//
// - The querier worker's loop run in a dedicated context, called the "execution context".
//
// - The execution context is canceled when the worker context gets cancelled (ie. querier is shutting down)
// and there's no inflight query execution. In case there's an inflight query, the execution context is canceled
// once the inflight query terminates and the response has been sent.
type executionContext struct {
	execCtx       context.Context
	execCancel    context.CancelFunc
	inflightQuery *atomic.Bool
}

// newExecutionContext returns a new executionContext. The caller must call cancel() on it once done.
func newExecutionContext(workerCtx context.Context, logger log.Logger) *executionContext {
	execCtx, execCancel := context.WithCancel(context.Background())

	c := &executionContext{
		execCtx:       execCtx,
		execCancel:    execCancel,
		inflightQuery: atomic.NewBool(false),
	}

	go func() {
		// Wait until it's safe to cancel the execution context, which is when one of the following conditions happen:
		// - The worker context has been canceled and there's no inflight query
		// - The execution context itself has been explicitly canceled
		select {
		case <-workerCtx.Done():
			level.Debug(logger).Log("msg", "querier worker context has been canceled, waiting until there's no inflight query")

			for c.inflightQuery.Load() {
				select {
				case <-execCtx.Done():
					// In the meanwhile, the execution context has been explicitly canceled, so we should just terminate.
					return
				case <-time.After(100 * time.Millisecond):
					// Going to check it again.
				}
			}

			level.Debug(logger).Log("msg", "querier worker context has been canceled and there's no inflight query, canceling the execution context too")
			execCancel()
		case <-execCtx.Done():
			// Nothing to do. The execution context has been explicitly canceled.
		}
	}()

	return c
}

func (c *executionContext) queryStarted() {
	c.inflightQuery.Store(true)
}

func (c *executionContext) queryEnded() {
	c.inflightQuery.Store(false)
}

func (c *executionContext) context() context.Context {
	return c.execCtx
}

func (c *executionContext) cancel() {
	c.execCancel()
}
