// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go/ext"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func newFrontendRunningMiddleware(readinessAwaiter ReadinessAwaiter, timeout time.Duration) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return frontendRunningMiddleware{
			readinessAwaiter: readinessAwaiter,
			timeout:          timeout,

			next: next,
		}
	})
}

type frontendRunningMiddleware struct {
	readinessAwaiter ReadinessAwaiter
	timeout          time.Duration

	next Handler
}

func (f frontendRunningMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	if err := f.waitForRunning(ctx); err != nil {
		return nil, err
	}

	return f.next.Do(ctx, r)
}

func (f frontendRunningMiddleware) waitForRunning(ctx context.Context) (err error) {
	if err := f.readinessAwaiter.Await(ctx, f.timeout); err != nil {
		return apierror.New(apierror.TypeUnavailable, err.Error())
	}

	return nil
}

type ReadinessAwaiter interface {
	Await(ctx context.Context, timeout time.Duration) error
}

type serviceReadinessAwaiter struct {
	log             log.Logger
	serviceProvider func() services.Service
}

func NewServiceReadinessAwaiter(log log.Logger, serviceProvider func() services.Service) ReadinessAwaiter {
	return &serviceReadinessAwaiter{log, serviceProvider}
}

func (s *serviceReadinessAwaiter) Await(ctx context.Context, timeout time.Duration) error {
	service := s.serviceProvider()

	if state := service.State(); state == services.Running {
		// Fast path: frontend is already running, nothing more to do.
		return nil
	} else if timeout == 0 {
		// If waiting for the frontend to be ready is disabled by config, and it's not ready, abort now.
		return fmt.Errorf("frontend not running: %v", state)
	}

	spanLog, ctx := spanlogger.NewWithLogger(ctx, s.log, "awaitQueryFrontendReady")
	defer spanLog.Finish()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := service.AwaitRunning(ctx); err != nil {
		ext.Error.Set(spanLog.Span, true)

		if ctx.Err() != nil {
			level.Warn(spanLog).Log("msg", "frontend not running, timed out waiting for it to be running", "state", service.State(), "timeout", timeout)
			return fmt.Errorf("frontend not running (is %v), timed out waiting for it to be running after %v", service.State(), timeout)
		}

		level.Warn(spanLog).Log("msg", "failed waiting for frontend to be running", "err", err)
		return fmt.Errorf("frontend not running: %w", err)
	}

	return nil
}
