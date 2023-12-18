// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// customStoreGatewayClient is a custom StoreGatewayClient which wraps well known gRPC errors into standard golang errors.
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

	return newCustomSeriesClient(client), nil
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

// customStoreGatewayClient is a custom StoreGateway_SeriesClient which wraps well known gRPC errors into standard golang errors.
type customSeriesClient struct {
	*customClientStream

	wrapped StoreGateway_SeriesClient
}

func newCustomSeriesClient(client StoreGateway_SeriesClient) *customSeriesClient {
	return &customSeriesClient{
		customClientStream: &customClientStream{client},
		wrapped:            client,
	}
}

func (c *customSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	res, err := c.wrapped.Recv()
	return res, wrapContextError(err)
}

// customClientStream is a custom grpc.ClientStream which wraps well known gRPC errors into standard golang errors.
type customClientStream struct {
	wrapped grpc.ClientStream
}

// Header implements grpc.ClientStream.
func (c *customClientStream) Header() (metadata.MD, error) {
	md, err := c.wrapped.Header()
	return md, wrapContextError(err)
}

// Trailer implements grpc.ClientStream.
func (c *customClientStream) Trailer() metadata.MD {
	return c.wrapped.Trailer()
}

// CloseSend implements grpc.ClientStream.
func (c *customClientStream) CloseSend() error {
	//nolint:forbidigo // Here we're just wrapping CloseSend() so it's OK to call it.
	return wrapContextError(c.wrapped.CloseSend())
}

// Context implements grpc.ClientStream.
func (c *customClientStream) Context() context.Context {
	return c.wrapped.Context()
}

// SendMsg implements grpc.ClientStream.
func (c *customClientStream) SendMsg(m any) error {
	return wrapContextError(c.wrapped.SendMsg(m))
}

// RecvMsg implements grpc.ClientStream.
func (c *customClientStream) RecvMsg(m any) error {
	return wrapContextError(c.wrapped.RecvMsg(m))
}

func wrapContextError(err error) error {
	if err == nil {
		return nil
	}

	// Get the gRPC status from the error.
	type grpcErrorStatus interface{ GRPCStatus() *grpcstatus.Status }
	var grpcError grpcErrorStatus
	if !errors.As(err, &grpcError) {
		return err
	}

	switch status := grpcError.GRPCStatus(); {
	case status.Code() == codes.Canceled:
		return &grpcContextError{grpcErr: err, grpcStatus: status, stdErr: context.Canceled}
	case status.Code() == codes.DeadlineExceeded:
		return &grpcContextError{grpcErr: err, grpcStatus: status, stdErr: context.DeadlineExceeded}
	default:
		return err
	}
}

// grpcContextError is a custom error used to wrap gRPC errors which maps to golang standard context errors.
type grpcContextError struct {
	// grpcErr is the gRPC error wrapped by grpcContextError.
	grpcErr error

	// grpcStatus is the gRPC status associated with the gRPC error. It's guaranteed to be non-nil.
	grpcStatus *grpcstatus.Status

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

func (e *grpcContextError) GRPCStatus() *grpcstatus.Status {
	return e.grpcStatus
}
