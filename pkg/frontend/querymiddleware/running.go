// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go/ext"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func NewFrontendRunningRoundTripper(next http.RoundTripper, service services.Service, timeout time.Duration, log log.Logger) http.RoundTripper {
	return &frontendRunningRoundTripper{
		next:    next,
		service: service,
		timeout: timeout,
		log:     log,
	}
}

type frontendRunningRoundTripper struct {
	next    http.RoundTripper
	service services.Service
	timeout time.Duration
	log     log.Logger
}

func (f *frontendRunningRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := awaitQueryFrontendServiceRunning(request.Context(), f.service, f.timeout, f.log); err != nil {
		return nil, apierror.New(apierror.TypeUnavailable, err.Error())
	}

	return f.next.RoundTrip(request)
}

// This method is not on frontendRunningRoundTripper to make it easier to test this logic.
func awaitQueryFrontendServiceRunning(ctx context.Context, service services.Service, timeout time.Duration, log log.Logger) error {
	if state := service.State(); state == services.Running {
		// Fast path: frontend is already running, nothing more to do.
		return nil
	} else if timeout == 0 {
		// If waiting for the frontend to be ready is disabled by config, and it's not ready, abort now.
		return fmt.Errorf("frontend not running: %v", state)
	}

	spanLog, ctx := spanlogger.NewWithLogger(ctx, log, "awaitQueryFrontendServiceRunning")
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
