// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"

	"github.com/grafana/dskit/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// customStoreGatewayClient is a custom client which wraps the real gRPC client and re-maps well known
// gRPC errors into standard golang errors.
type customStoreGatewayClient struct {
	wrapped StoreGatewayClient
}

func NewCustomStoreGatewayClient(cc *grpc.ClientConn) StoreGatewayClient {
	return &customStoreGatewayClient{
		wrapped: NewStoreGatewayClient(cc),
	}
}

// Series implements StoreGatewayClient.
func (c *customStoreGatewayClient) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (StoreGateway_SeriesClient, error) {
	client, err := c.wrapped.Series(ctx, in, opts...)
	if err != nil {
		return client, mapError(err)
	}

	return &customSeriesClient{client}, nil
}

// LabelNames implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	res, err := c.wrapped.LabelNames(ctx, in, opts...)
	return res, mapError(err)
}

// LabelValues implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	res, err := c.wrapped.LabelValues(ctx, in, opts...)
	return res, mapError(err)
}

type customSeriesClient struct {
	StoreGateway_SeriesClient
}

func (c *customSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	res, err := c.StoreGateway_SeriesClient.Recv()
	return res, mapError(err)
}

func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case grpcutil.IsCanceled(err):
		return context.Canceled
	case grpcutil.ErrorToStatusCode(err) == codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		return err
	}
}
