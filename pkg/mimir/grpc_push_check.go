// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type pushReceiver interface {
	StartPushRequest() error
	FinishPushRequest()
}

// getPushReceiver function must be constant -- return same value on each call.
func newGrpcInflightMethodLimiter(getIngester func() pushReceiver) *grpcInflightMethodLimiter {
	return &grpcInflightMethodLimiter{getIngester: getIngester}
}

// grpcInflightMethodLimiter implements gRPC TapHandle and gRPC stats.Handler.
type grpcInflightMethodLimiter struct {
	getIngester func() pushReceiver
}

type ctxKey int

const (
	ingesterPushStarted ctxKey = 1

	ingesterPushMethod string = "/cortex.Ingester/Push"
)

var errNoIngester = status.Error(codes.Unavailable, "no ingester")

func (g *grpcInflightMethodLimiter) RPCCallStarting(ctx context.Context, methodName string, _ metadata.MD) (context.Context, error) {
	if methodName == ingesterPushMethod {
		ing := g.getIngester()
		if ing == nil {
			// We return error here, to make sure that RPCCallFinished doesn't get called for this RPC call.
			return ctx, errNoIngester
		}

		err := ing.StartPushRequest()
		if err != nil {
			return ctx, status.Error(codes.Unavailable, err.Error())
		}

		return context.WithValue(ctx, ingesterPushStarted, true), nil
	}
	return ctx, nil
}

func (g *grpcInflightMethodLimiter) RPCCallFinished(ctx context.Context) {
	if v, ok := ctx.Value(ingesterPushStarted).(bool); ok && v {
		g.getIngester().FinishPushRequest()
	}
}
