// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Readcache implements the same `ingesterReceiver` shape (defined in
// pkg/mimir) the gRPC inflight-method limiter expects, so that
// requests addressed to `/cortex.Ingester/*` on a readcache pod can
// proceed through the limiter even when the ingester module isn't
// registered.
//
// All push-side hooks reject because readcache is read-only; the
// underlying Push RPC stub already returns codes.Unimplemented but
// the limiter runs first and would block the call earlier without
// these stubs. The read-side hooks are no-ops: readcache has no
// reactiveLimiter, no per-request lifecycle tracking, and no
// per-request finish hook today.

// StartPushRequest rejects: readcache does not accept writes.
func (r *Readcache) StartPushRequest(ctx context.Context, _ int64) (context.Context, error) {
	return ctx, status.Error(codes.Unimplemented, "readcache is read-only; pushes are served by the ingester")
}

// PreparePushRequest rejects: readcache does not accept writes.
func (r *Readcache) PreparePushRequest(_ context.Context) (func(error), error) {
	return nil, status.Error(codes.Unimplemented, "readcache is read-only; pushes are served by the ingester")
}

// FinishPushRequest is a no-op: readcache never enters the push state.
func (r *Readcache) FinishPushRequest(_ context.Context) {}

// StartReadRequest is a no-op: readcache has no reactive limiter.
func (r *Readcache) StartReadRequest(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// PrepareReadRequest is a no-op: readcache has no per-request
// lifecycle hooks today.
func (r *Readcache) PrepareReadRequest(_ context.Context) (func(error), error) {
	return func(error) {}, nil
}
