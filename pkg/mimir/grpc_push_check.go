// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/api"
)

type ingesterPushReceiver interface {
	StartPushRequest(requestSize int64) error
	FinishPushRequest(requestSize int64)
}

// Interface exposed by Distributor.
type distributorPushReceiver interface {
	StartPushRequest(ctx context.Context, httpgrpcRequestSize int64) (context.Context, error)
	FinishPushRequest(ctx context.Context)
}

// getPushReceiver function must be constant -- return same value on each call.
// if getIngester or getDistributor functions are nil, those specific checks are not used.
func newGrpcInflightMethodLimiter(getIngester func() ingesterPushReceiver, getDistributor func() distributorPushReceiver) *grpcInflightMethodLimiter {
	return &grpcInflightMethodLimiter{getIngester: getIngester, getDistributor: getDistributor}
}

// grpcInflightMethodLimiter implements gRPC TapHandle and gRPC stats.Handler.
type grpcInflightMethodLimiter struct {
	getIngester    func() ingesterPushReceiver
	getDistributor func() distributorPushReceiver
}

type ctxKey int

const (
	pushTypeCtxKey ctxKey = 1

	ingesterPush        = 1
	distributorPush     = 2
	ingesterRequestSize = 3

	ingesterPushMethod   string = "/cortex.Ingester/Push"
	httpgrpcHandleMethod string = "/httpgrpc.HTTP/Handle"
)

var errNoIngester = status.Error(codes.Unavailable, "no ingester")
var errNoDistributor = status.Error(codes.Unavailable, "no distributor")

func (g *grpcInflightMethodLimiter) RPCCallStarting(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
	if g.getIngester != nil && methodName == ingesterPushMethod {
		ing := g.getIngester()
		if ing == nil {
			// We return error here, to make sure that RPCCallFinished doesn't get called for this RPC call.
			return ctx, errNoIngester
		}

		reqSize := getMessageSize(md, grpcutil.MetadataMessageSize)

		err := ing.StartPushRequest(reqSize)
		if err != nil {
			return ctx, status.Error(codes.Unavailable, err.Error())
		}

		ctx = context.WithValue(ctx, ingesterRequestSize, reqSize)
		return context.WithValue(ctx, pushTypeCtxKey, ingesterPush), nil
	}

	if g.getDistributor != nil && methodName == httpgrpcHandleMethod {
		// Let's check httpgrpc metadata, if present.
		httpMethod := getSingleMetadata(md, httpgrpc.MetadataMethod)
		httpURL := getSingleMetadata(md, httpgrpc.MetadataURL)

		if httpMethod == http.MethodPost && (strings.HasSuffix(httpURL, api.PrometheusPushEndpoint) || strings.HasSuffix(httpURL, api.OTLPPushEndpoint)) {
			dist := g.getDistributor()
			if dist == nil {
				return ctx, errNoDistributor
			}

			ctx, err := dist.StartPushRequest(ctx, getMessageSize(md, grpcutil.MetadataMessageSize))
			if err != nil {
				return ctx, status.Error(codes.Unavailable, err.Error())
			}

			return context.WithValue(ctx, pushTypeCtxKey, distributorPush), nil
		}
	}

	return ctx, nil
}

func (g *grpcInflightMethodLimiter) RPCCallFinished(ctx context.Context) {
	if pt, ok := ctx.Value(pushTypeCtxKey).(int); ok {
		switch pt {
		case ingesterPush:
			// Using two-outputs here to avoid panics, if value is not of int64 type. reqSize will be 0 in that case, which is fine.
			reqSize, _ := ctx.Value(ingesterRequestSize).(int64)
			g.getIngester().FinishPushRequest(reqSize)

		case distributorPush:
			g.getDistributor().FinishPushRequest(ctx)
		}
	}
}

func getMessageSize(md metadata.MD, key string) int64 {
	s := getSingleMetadata(md, key)
	if s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

func getSingleMetadata(md metadata.MD, key string) string {
	val := md[key]
	l := len(val)
	if l == 0 || l > 1 {
		return ""
	}
	return val[0]
}
