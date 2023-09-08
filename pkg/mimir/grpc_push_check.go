// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"

	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"
)

type pushReceiver interface {
	StartPushRequest() error
	FinishPushRequest()
}

func newGrpcPushCheck(getIngester func() pushReceiver) *grpcPushCheck {
	return &grpcPushCheck{getIngester: getIngester}
}

// grpcPushCheck implements gRPC TapHandle and gRPC stats.Handler.
// grpcPushCheck can track push inflight requests, and reject requests before even reading them into memory.
type grpcPushCheck struct {
	getIngester func() pushReceiver
}

// Custom type to hide it from other packages.
type grpcPushCheckContextKey int

const (
	pushRequestStartedKey grpcPushCheckContextKey = 1
)

// TapHandle is called after receiving grpc request and headers, but before reading any request data yet.
// If we reject request here, it won't be counted towards any metrics (eg. dskit middleware).
// If we accept request (not return error), eventually HandleRPC with stats.End notification will be called.
func (g *grpcPushCheck) TapHandle(ctx context.Context, info *tap.Info) (context.Context, error) {
	pushRequestStarted := false

	var err error
	if info.FullMethodName == "/cortex.Ingester/Push" {
		ing := g.getIngester()
		if ing != nil {
			// All errors returned by StartPushRequest can be converted to gRPC status.Status.
			err = ing.StartPushRequest()
			if err == nil {
				pushRequestStarted = true
			}
		}
	}

	ctx = context.WithValue(ctx, pushRequestStartedKey, pushRequestStarted)
	return ctx, err
}

func (g *grpcPushCheck) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (g *grpcPushCheck) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	// when request ends, and we started push request tracking for this request, finish it.
	if _, ok := rpcStats.(*stats.End); ok {
		if b, ok := ctx.Value(pushRequestStartedKey).(bool); ok && b {
			g.getIngester().FinishPushRequest()
		}
	}
}

func (g *grpcPushCheck) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (g *grpcPushCheck) HandleConn(_ context.Context, _ stats.ConnStats) {
	// Not interested.
}
