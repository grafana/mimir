// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/globalerror"
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
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}

	return newCustomSeriesClient(client), nil
}

// LabelNames implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	res, err := c.wrapped.LabelNames(ctx, in, opts...)
	return res, globalerror.WrapGRPCErrorWithContextError(ctx, err)
}

// LabelValues implements StoreGatewayClient.
func (c *customStoreGatewayClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	res, err := c.wrapped.LabelValues(ctx, in, opts...)
	return res, globalerror.WrapGRPCErrorWithContextError(ctx, err)
}

// SearchLabelNames implements StoreGatewayClient.
func (c *customStoreGatewayClient) SearchLabelNames(ctx context.Context, in *storepb.SearchLabelNamesRequest, opts ...grpc.CallOption) (StoreGateway_SearchLabelNamesClient, error) {
	client, err := c.wrapped.SearchLabelNames(ctx, in, opts...)
	if err != nil {
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}
	return newCustomSearchClient(client), nil
}

// SearchLabelValues implements StoreGatewayClient.
func (c *customStoreGatewayClient) SearchLabelValues(ctx context.Context, in *storepb.SearchLabelValuesRequest, opts ...grpc.CallOption) (StoreGateway_SearchLabelValuesClient, error) {
	client, err := c.wrapped.SearchLabelValues(ctx, in, opts...)
	if err != nil {
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}
	return newCustomSearchClient(client), nil
}

// customSearchClient wraps a StoreGateway search streaming client to apply error wrapping.
// It is used for both SearchLabelNames and SearchLabelValues since both stream StoreSearchResponse.
type customSearchClient struct {
	*customClientStream
	wrapped interface {
		Recv() (*storepb.StoreSearchResponse, error)
		grpc.ClientStream
	}
}

func newCustomSearchClient[T interface {
	Recv() (*storepb.StoreSearchResponse, error)
	grpc.ClientStream
}](client T) *customSearchClient {
	return &customSearchClient{
		customClientStream: &customClientStream{client},
		wrapped:            client,
	}
}

func (c *customSearchClient) Recv() (*storepb.StoreSearchResponse, error) {
	res, err := c.wrapped.Recv()
	return res, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
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
	return res, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
}

// customClientStream is a custom grpc.ClientStream which wraps well known gRPC errors into standard golang errors.
type customClientStream struct {
	wrapped grpc.ClientStream
}

// Header implements grpc.ClientStream.
func (c *customClientStream) Header() (metadata.MD, error) {
	md, err := c.wrapped.Header()
	return md, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
}

// Trailer implements grpc.ClientStream.
func (c *customClientStream) Trailer() metadata.MD {
	return c.wrapped.Trailer()
}

// CloseSend implements grpc.ClientStream.
func (c *customClientStream) CloseSend() error {
	//nolint:forbidigo // Here we're just wrapping CloseSend() so it's OK to call it.
	return globalerror.WrapGRPCErrorWithContextError(c.Context(), c.wrapped.CloseSend())
}

// Context implements grpc.ClientStream.
func (c *customClientStream) Context() context.Context {
	return c.wrapped.Context()
}

// SendMsg implements grpc.ClientStream.
func (c *customClientStream) SendMsg(m any) error {
	return globalerror.WrapGRPCErrorWithContextError(c.Context(), c.wrapped.SendMsg(m))
}

// RecvMsg implements grpc.ClientStream.
func (c *customClientStream) RecvMsg(m any) error {
	return globalerror.WrapGRPCErrorWithContextError(c.Context(), c.wrapped.RecvMsg(m))
}
