// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pushReceiver interface {
	StartPushRequest() error
	FinishPushRequest()
}

func newGrpcInflightMethodLimiter(getIngester func() pushReceiver) *grpcInflightMethodLimiter {
	once := sync.OnceValue(getIngester)

	return &grpcInflightMethodLimiter{getIngester: once}
}

// grpcInflightMethodLimiter implements gRPC TapHandle and gRPC stats.Handler.
type grpcInflightMethodLimiter struct {
	getIngester func() pushReceiver
}

const ingesterPushMethod = "/cortex.Ingester/Push"

func (g *grpcInflightMethodLimiter) RPCCallStarting(methodName string) error {
	if methodName == ingesterPushMethod {
		ing := g.getIngester()
		if ing == nil {
			// We return error here, to make sure that RPCCallFinished doesn't get called for this RPC call.
			return status.Error(codes.Unavailable, "no ingester")
		}

		return ing.StartPushRequest()
	}
	return nil
}

func (g *grpcInflightMethodLimiter) RPCCallFinished(methodName string) {
	if methodName == ingesterPushMethod {
		g.getIngester().FinishPushRequest()
	}
}
