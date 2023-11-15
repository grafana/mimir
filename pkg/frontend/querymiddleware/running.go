// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	apierror "github.com/grafana/mimir/pkg/api/error"
	"time"

	"github.com/go-kit/log"
)

func newFrontendRunningMiddleware(awaitFrontendReady func(ctx context.Context, timeout time.Duration) error, timeout time.Duration, log log.Logger) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return frontendRunningMiddleware{
			awaitFrontendReady: awaitFrontendReady,
			timeout:            timeout,
			log:                log,

			next: next,
		}
	})
}

type frontendRunningMiddleware struct {
	awaitFrontendReady func(ctx context.Context, timeout time.Duration) error
	timeout            time.Duration
	log                log.Logger

	next Handler
}

func (f frontendRunningMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	if err := f.waitForRunning(ctx); err != nil {
		return nil, err
	}

	return f.next.Do(ctx, r)
}

func (f frontendRunningMiddleware) waitForRunning(ctx context.Context) (err error) {
	if err := f.awaitFrontendReady(ctx, f.timeout); err != nil {
		return apierror.New(apierror.TypeUnavailable, err.Error())
	}

	return nil
}
