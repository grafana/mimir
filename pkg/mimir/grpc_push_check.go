// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"google.golang.org/grpc/codes"
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

const ingesterPushMethod = "/cortex.Ingester/Push"

var errNoIngester = status.Error(codes.Unavailable, "no ingester")

func (g *grpcInflightMethodLimiter) RPCCallStarting(methodName string) error {
	if methodName == ingesterPushMethod {
		ing := g.getIngester()
		if ing == nil {
			// We return error here, to make sure that RPCCallFinished doesn't get called for this RPC call.
			return errNoIngester
		}

		if err := ing.StartPushRequest(); err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
	}
	return nil
}

func (g *grpcInflightMethodLimiter) RPCCallFinished(methodName string) {
	if methodName == ingesterPushMethod {
		g.getIngester().FinishPushRequest()
	}
}
