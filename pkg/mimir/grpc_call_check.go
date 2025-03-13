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

type pushReceiver interface {
	StartPushRequest(ctx context.Context, requestSize int64) (context.Context, error)
	PreparePushRequest(ctx context.Context) (func(error), error)
	FinishPushRequest(ctx context.Context)
}

type ingesterReceiver interface {
	pushReceiver
	// StartReadRequest is called before a request has been allocated resources, and hasn't yet been scheduled for processing.
	StartReadRequest(ctx context.Context) (context.Context, error)
	// PrepareReadRequest is called when a request is being processed, and may return a finish func.
	PrepareReadRequest(ctx context.Context) (func(error), error)
}

// pushReceiver function must be pure, returning the same value on each call.
// if getIngester or getDistributor functions are nil, those specific checks are not used.
func newGrpcInflightMethodLimiter(getIngester func() ingesterReceiver, getDistributor func() pushReceiver) *grpcInflightMethodLimiter {
	return &grpcInflightMethodLimiter{getIngester: getIngester, getDistributor: getDistributor}
}

// grpcInflightMethodLimiter implements gRPC TapHandle and gRPC stats.Handler.
type grpcInflightMethodLimiter struct {
	getIngester    func() ingesterReceiver
	getDistributor func() pushReceiver
}

type ctxKey int

const (
	rpcCallCtxKey ctxKey = 1 // ingester or distributor call

	rpcCallIngesterPush    = 1
	rpcCallIngesterRead    = 2
	rpcCallDistributorPush = 3

	ingesterMethod       string = "/cortex.Ingester"
	ingesterPushMethod   string = "/cortex.Ingester/Push"
	httpgrpcHandleMethod string = "/httpgrpc.HTTP/Handle"
)

var errNoIngester = status.Error(codes.Unavailable, "no ingester")
var errNoDistributor = status.Error(codes.Unavailable, "no distributor")

func (g *grpcInflightMethodLimiter) RPCCallStarting(ctx context.Context, methodName string, md metadata.MD) (context.Context, error) {
	if g.getIngester != nil && strings.HasPrefix(methodName, ingesterMethod) {
		ing := g.getIngester()
		if ing == nil {
			// We return error here, to make sure that RPCCallFinished doesn't get called for this RPC call.
			return ctx, errNoIngester
		}

		var err error
		var rpcCallCtxValue int
		if methodName == ingesterPushMethod {
			ctx, err = ing.StartPushRequest(ctx, getMessageSize(md, grpcutil.MetadataMessageSize))
			rpcCallCtxValue = rpcCallIngesterPush
		} else {
			ctx, err = ing.StartReadRequest(ctx)
			rpcCallCtxValue = rpcCallIngesterRead
		}

		if err != nil {
			return ctx, status.Error(codes.Unavailable, err.Error())
		}
		return context.WithValue(ctx, rpcCallCtxKey, rpcCallCtxValue), nil
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

			return context.WithValue(ctx, rpcCallCtxKey, rpcCallDistributorPush), nil
		}
	}

	return ctx, nil
}

func (g *grpcInflightMethodLimiter) RPCCallProcessing(ctx context.Context, _ string) (func(error), error) {
	if rpcCall, ok := ctx.Value(rpcCallCtxKey).(int); ok {
		switch rpcCall {
		case rpcCallIngesterPush:
			if ing := g.getIngester(); ing != nil {
				fn, err := ing.PreparePushRequest(ctx)
				if err != nil {
					return nil, status.Error(codes.Unavailable, err.Error())
				}
				return fn, nil
			}
		case rpcCallIngesterRead:
			if ing := g.getIngester(); ing != nil {
				fn, err := ing.PrepareReadRequest(ctx)
				if err != nil {
					return nil, status.Error(codes.Unavailable, err.Error())
				}
				return fn, nil
			}
		}
	}
	return nil, nil
}

func (g *grpcInflightMethodLimiter) RPCCallFinished(ctx context.Context) {
	if rpcCall, ok := ctx.Value(rpcCallCtxKey).(int); ok {
		switch rpcCall {
		case rpcCallIngesterPush:
			g.getIngester().FinishPushRequest(ctx)
		case rpcCallDistributorPush:
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
