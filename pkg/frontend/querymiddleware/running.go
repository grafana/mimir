// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func newFrontendRunningMiddleware(frontendState func() services.State, notRunningBackoffDuration time.Duration, maxRetries int, log log.Logger) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return frontendRunningMiddleware{
			frontendState:             frontendState,
			notRunningBackoffDuration: notRunningBackoffDuration,
			maxRetries:                maxRetries,
			log:                       log,

			next: next,
		}
	})
}

type frontendRunningMiddleware struct {
	frontendState             func() services.State
	notRunningBackoffDuration time.Duration
	maxRetries                int
	log                       log.Logger

	next Handler
}

func (f frontendRunningMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	if err := f.waitForRunning(ctx); err != nil {
		return nil, err
	}

	return f.next.Do(ctx, r)
}

func (f frontendRunningMiddleware) waitForRunning(ctx context.Context) (err error) {
	spanLog, _ := spanlogger.NewWithLogger(ctx, f.log, "waitForRunning")
	defer spanLog.Finish()

	attempt := 1

	for {
		state := f.frontendState()

		switch state {
		case services.Running:
			return nil
		case services.Stopping, services.Terminated, services.Failed:
			level.Error(spanLog).Log("msg", "returning error: frontend is shutting down", "state", state)
			return apierror.New(apierror.TypeUnavailable, fmt.Sprintf("frontend shutting down: %v", state))
		}

		if f.notRunningBackoffDuration == 0 {
			// Retries disabled, stop now.
			level.Error(spanLog).Log("msg", "returning error: frontend is not running and backoff / retry is disabled", "state", state)
			return apierror.New(apierror.TypeUnavailable, fmt.Sprintf("frontend not running: %v", state))
		} else if attempt >= f.maxRetries {
			level.Error(spanLog).Log("msg", "returning error: frontend is still not running after backoff / retry", "state", state)
			return apierror.New(apierror.TypeUnavailable, fmt.Sprintf("frontend not running after %v: %v", time.Duration(f.maxRetries-1)*f.notRunningBackoffDuration, state))
		}

		level.Warn(spanLog).Log("msg", "frontend is not running, will back off and check again", "state", state, "attempt", attempt)
		attempt++
		time.Sleep(f.notRunningBackoffDuration)
	}
}
