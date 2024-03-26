// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// wrappedIngesterClient is a custom IngesterClient which wraps well known gRPC errors into standard golang errors.
type wrappedIngesterClient struct {
	wrapped IngesterClient
}

func NewWrappedIngesterClient(cc *grpc.ClientConn) IngesterClient {
	return &wrappedIngesterClient{
		wrapped: NewIngesterClient(cc),
	}
}

func (c *wrappedIngesterClient) Push(ctx context.Context, in *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	res, err := c.wrapped.Push(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) QueryStream(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (Ingester_QueryStreamClient, error) {
	res, err := c.wrapped.QueryStream(ctx, in, opts...)
	return wrapIngesterQueryStreamClient(res), wrapContextError(err)
}

func (c *wrappedIngesterClient) QueryExemplars(ctx context.Context, in *ExemplarQueryRequest, opts ...grpc.CallOption) (*ExemplarQueryResponse, error) {
	res, err := c.wrapped.QueryExemplars(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) LabelValues(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error) {
	res, err := c.wrapped.LabelValues(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) LabelNames(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error) {
	res, err := c.wrapped.LabelNames(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) UserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UserStatsResponse, error) {
	res, err := c.wrapped.UserStats(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) AllUserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UsersStatsResponse, error) {
	res, err := c.wrapped.AllUserStats(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) MetricsForLabelMatchers(ctx context.Context, in *MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*MetricsForLabelMatchersResponse, error) {
	res, err := c.wrapped.MetricsForLabelMatchers(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) MetricsMetadata(ctx context.Context, in *MetricsMetadataRequest, opts ...grpc.CallOption) (*MetricsMetadataResponse, error) {
	res, err := c.wrapped.MetricsMetadata(ctx, in, opts...)
	return res, wrapContextError(err)
}

func (c *wrappedIngesterClient) LabelNamesAndValues(ctx context.Context, in *LabelNamesAndValuesRequest, opts ...grpc.CallOption) (Ingester_LabelNamesAndValuesClient, error) {
	res, err := c.wrapped.LabelNamesAndValues(ctx, in, opts...)
	return wrapIngesterLabelNamesAndValuesClient(res), wrapContextError(err)
}

func (c *wrappedIngesterClient) LabelValuesCardinality(ctx context.Context, in *LabelValuesCardinalityRequest, opts ...grpc.CallOption) (Ingester_LabelValuesCardinalityClient, error) {
	res, err := c.wrapped.LabelValuesCardinality(ctx, in, opts...)
	return wrapIngesterLabelValuesCardinalityClient(res), wrapContextError(err)
}

func (c *wrappedIngesterClient) ActiveSeries(ctx context.Context, in *ActiveSeriesRequest, opts ...grpc.CallOption) (Ingester_ActiveSeriesClient, error) {
	res, err := c.wrapped.ActiveSeries(ctx, in, opts...)
	return wrapIngesterActiveSeriesClient(res), wrapContextError(err)
}

type wrappedIngesterQueryStreamClient struct {
	*wrappedClientStream

	wrapped Ingester_QueryStreamClient
}

func wrapIngesterQueryStreamClient(client Ingester_QueryStreamClient) *wrappedIngesterQueryStreamClient {
	if client == nil {
		return nil
	}

	return &wrappedIngesterQueryStreamClient{
		wrappedClientStream: &wrappedClientStream{client},
		wrapped:             client,
	}
}

func (c *wrappedIngesterQueryStreamClient) Recv() (*QueryStreamResponse, error) {
	res, err := c.wrapped.Recv()
	return res, wrapContextError(err)
}

type wrappedIngesterLabelNamesAndValuesClient struct {
	*wrappedClientStream

	wrapped Ingester_LabelNamesAndValuesClient
}

func wrapIngesterLabelNamesAndValuesClient(client Ingester_LabelNamesAndValuesClient) *wrappedIngesterLabelNamesAndValuesClient {
	if client == nil {
		return nil
	}

	return &wrappedIngesterLabelNamesAndValuesClient{
		wrappedClientStream: &wrappedClientStream{client},
		wrapped:             client,
	}
}

func (c *wrappedIngesterLabelNamesAndValuesClient) Recv() (*LabelNamesAndValuesResponse, error) {
	res, err := c.wrapped.Recv()
	return res, wrapContextError(err)
}

type wrappedIngesterLabelValuesCardinalityClient struct {
	*wrappedClientStream

	wrapped Ingester_LabelValuesCardinalityClient
}

func wrapIngesterLabelValuesCardinalityClient(client Ingester_LabelValuesCardinalityClient) *wrappedIngesterLabelValuesCardinalityClient {
	if client == nil {
		return nil
	}

	return &wrappedIngesterLabelValuesCardinalityClient{
		wrappedClientStream: &wrappedClientStream{client},
		wrapped:             client,
	}
}

func (c *wrappedIngesterLabelValuesCardinalityClient) Recv() (*LabelValuesCardinalityResponse, error) {
	res, err := c.wrapped.Recv()
	return res, wrapContextError(err)
}

type wrappedIngesterActiveSeriesClient struct {
	*wrappedClientStream

	wrapped Ingester_ActiveSeriesClient
}

func wrapIngesterActiveSeriesClient(client Ingester_ActiveSeriesClient) *wrappedIngesterActiveSeriesClient {
	if client == nil {
		return nil
	}

	return &wrappedIngesterActiveSeriesClient{
		wrappedClientStream: &wrappedClientStream{client},
		wrapped:             client,
	}
}

func (c *wrappedIngesterActiveSeriesClient) Recv() (*ActiveSeriesResponse, error) {
	res, err := c.wrapped.Recv()
	return res, wrapContextError(err)
}

// TODO move everything below this to dskit

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

// wrappedClientStream is a custom grpc.ClientStream which wraps well known gRPC errors into standard golang errors.
type wrappedClientStream struct {
	wrapped grpc.ClientStream
}

// Header implements grpc.ClientStream.
func (c *wrappedClientStream) Header() (metadata.MD, error) {
	md, err := c.wrapped.Header()
	return md, wrapContextError(err)
}

// Trailer implements grpc.ClientStream.
func (c *wrappedClientStream) Trailer() metadata.MD {
	return c.wrapped.Trailer()
}

// CloseSend implements grpc.ClientStream.
func (c *wrappedClientStream) CloseSend() error {
	//nolint:forbidigo // Here we're just wrapping CloseSend() so it's OK to call it.
	return wrapContextError(c.wrapped.CloseSend())
}

// Context implements grpc.ClientStream.
func (c *wrappedClientStream) Context() context.Context {
	return c.wrapped.Context()
}

// SendMsg implements grpc.ClientStream.
func (c *wrappedClientStream) SendMsg(m any) error {
	return wrapContextError(c.wrapped.SendMsg(m))
}

// RecvMsg implements grpc.ClientStream.
func (c *wrappedClientStream) RecvMsg(m any) error {
	return wrapContextError(c.wrapped.RecvMsg(m))
}
