// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"
	"errors"

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
		return client, wrapContextError(err)
	}

	return &customSeriesClient{client}, nil
}

// LabelNames implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	res, err := c.wrapped.LabelNames(ctx, in, opts...)
	return res, wrapContextError(err)
}

// LabelValues implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	res, err := c.wrapped.LabelValues(ctx, in, opts...)
	return res, wrapContextError(err)
}

type customSeriesClient struct {
	StoreGateway_SeriesClient
}

func (c *customSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	res, err := c.StoreGateway_SeriesClient.Recv()
	return res, wrapContextError(err)
}

func wrapContextError(err error) error {
	switch {
	case err == nil:
		return nil
	case err != context.Canceled && grpcutil.IsCanceled(err):
		return &grpcContextError{grpcErr: err, stdErr: context.Canceled}
	case grpcutil.ErrorToStatusCode(err) == codes.DeadlineExceeded:
		return &grpcContextError{grpcErr: err, stdErr: context.DeadlineExceeded}
	default:
		return err
	}
}

// grpcContextError is a custom error used to wrap gRPC errors which maps to golang standard context errors.
type grpcContextError struct {
	// grpcErr is the gRPC error wrapped by grpcContextError.
	grpcErr error

	// stdErr is the equivalent golang standard context error.
	stdErr error
}

func (e *grpcContextError) Error() string {
	return e.grpcErr.Error()
}

func (e *grpcContextError) Unwrap() error {
	return e.grpcErr
}

func (e *grpcContextError) Is(err error) bool {
	return errors.Is(e.stdErr, err) || errors.Is(e.grpcErr, err)
}

func (e *grpcContextError) As(target any) bool {
	if errors.As(e.stdErr, target) {
		return true
	}

	return errors.As(e.grpcErr, target)
}
