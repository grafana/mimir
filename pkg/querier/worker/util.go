// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcutil"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
)

// newExecutionContext returns a new execution context (execCtx) that wraps the input workerCtx and
// it used to run the querier's worker loop and execute queries.
// The purpose of the execution context is to gracefully shutdown queriers, waiting
// until inflight queries are terminated before the querier process exits.
//
// The caller must call execCancel() once done.
//
// How it's used:
//
// - The querier worker's loop run in a dedicated context, called the "execution context".
//
// - The execution context is canceled when the worker context gets cancelled (ie. querier is shutting down)
// and there's no inflight query execution. In case there's an inflight query, the execution context is canceled
// once the inflight query terminates and the response has been sent.
func newExecutionContext(workerCtx context.Context, logger log.Logger) (execCtx context.Context, execCancel context.CancelCauseFunc, inflightQuery *atomic.Bool) {
	execCtx, execCancel = context.WithCancelCause(context.Background())
	inflightQuery = atomic.NewBool(false)

	go func() {
		// Wait until it's safe to cancel the execution context, which is when one of the following conditions happen:
		// - The worker context has been canceled and there's no inflight query
		// - The execution context itself has been explicitly canceled
		select {
		case <-workerCtx.Done():
			level.Debug(logger).Log("msg", "querier worker context has been canceled, waiting until there's no inflight query")

			for inflightQuery.Load() {
				select {
				case <-execCtx.Done():
					// In the meanwhile, the execution context has been explicitly canceled, so we should just terminate.
					return
				case <-time.After(100 * time.Millisecond):
					// Going to check it again.
				}
			}

			level.Debug(logger).Log("msg", "querier worker context has been canceled and there's no inflight query, canceling the execution context too")
			execCancel(cancellation.NewErrorf("querier worker context cancelled: %w", context.Cause(workerCtx)))
		case <-execCtx.Done():
			// Nothing to do. The execution context has been explicitly canceled.
		}
	}()

	return
}

func isErrCancel(err error, logger log.Logger) bool {
	if grpcutil.IsCanceled(err) {
		return true
	}
	if s, ok := grpcutil.ErrorToStatus(err); ok && (s.Code() == codes.Unavailable && strings.Contains(s.Message(), `"max_age"`)) {
		// On "grpc_server_max_connection_age" the server resets the stream's connection.
		// Safe to ignore (ref grafana/mimir#7023).
		level.Debug(logger).Log("msg", "server reset stream due to max age", "err", err)
		return true
	}
	return false
}
