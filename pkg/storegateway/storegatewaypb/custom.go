// SPDX-License-Identifier: AGPL-3.0-only

package storegatewaypb

import (
	"context"
	"errors"
	"io"

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
	return newCustomSearchLabelNamesClient(client), nil
}

// SearchLabelValues implements StoreGatewayClient.
func (c *customStoreGatewayClient) SearchLabelValues(ctx context.Context, in *storepb.SearchLabelValuesRequest, opts ...grpc.CallOption) (StoreGateway_SearchLabelValuesClient, error) {
	client, err := c.wrapped.SearchLabelValues(ctx, in, opts...)
	if err != nil {
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}
	return newCustomSearchLabelValuesClient(client), nil
}

// MetricNameCounts implements StoreGatewayClient.
func (c *customStoreGatewayClient) MetricNameCounts(ctx context.Context, in *MetricNameCountsRequest, opts ...grpc.CallOption) (StoreGateway_MetricNameCountsClient, error) {
	client, err := c.wrapped.MetricNameCounts(ctx, in, opts...)
	if err != nil {
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}
	return &customMetricNameCountsClient{customClientStream: &customClientStream{client}, wrapped: client}, nil
}

// customMetricNameCountsClient wraps StoreGateway_MetricNameCountsClient so
// Recv translates raw gRPC status errors into context errors when the
// caller's context is gone, like customSeriesClient.
type customMetricNameCountsClient struct {
	*customClientStream

	wrapped StoreGateway_MetricNameCountsClient
}

func (c *customMetricNameCountsClient) Recv() (*MetricNameCountsResponse, error) {
	res, err := c.wrapped.Recv()
	return res, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
}

// SeriesCounts implements StoreGatewayClient.
func (c *customStoreGatewayClient) SeriesCounts(ctx context.Context, in *SeriesCountsRequest, opts ...grpc.CallOption) (StoreGateway_SeriesCountsClient, error) {
	client, err := c.wrapped.SeriesCounts(ctx, in, opts...)
	if err != nil {
		return client, globalerror.WrapGRPCErrorWithContextError(ctx, err)
	}
	return &customSeriesCountsClient{customClientStream: &customClientStream{client}, wrapped: client}, nil
}

// customSeriesCountsClient wraps StoreGateway_SeriesCountsClient so Recv
// translates raw gRPC status errors into context errors when the caller's
// context is gone, like customSeriesClient.
type customSeriesCountsClient struct {
	*customClientStream

	wrapped StoreGateway_SeriesCountsClient
}

func (c *customSeriesCountsClient) Recv() (*SeriesCountsResponse, error) {
	res, err := c.wrapped.Recv()
	return res, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
}

// A SeriesCounts answer is streamed in messages of at most
// SeriesCountsMessageGroups groups and about SeriesCountsMessageBytes, well
// under the gRPC message size limits; a group of hashes can be large.
const (
	SeriesCountsMessageGroups = 10_000
	SeriesCountsMessageBytes  = 1 << 20
)

// SendSeriesCountsResponse streams response: its groups, in the order they
// are in, in batches, and every other field on the first message, which is
// sent even with no groups.
func SendSeriesCountsResponse(s StoreGateway_SeriesCountsServer, response *SeriesCountsResponse) error {
	groups := response.Groups
	first := *response
	first.Groups = nil
	msg := &first
	size := msg.Size()
	for _, g := range groups {
		// A group costs its own size, its field tag and its length.
		gs := g.Size()
		gs += 1 + sovGateway(uint64(gs))
		if len(msg.Groups) > 0 && (size+gs > SeriesCountsMessageBytes || len(msg.Groups) >= SeriesCountsMessageGroups) {
			if err := s.Send(msg); err != nil {
				return err
			}
			msg = &SeriesCountsResponse{}
			size = 0
		}
		msg.Groups = append(msg.Groups, g)
		size += gs
	}
	return s.Send(msg)
}

// ReceiveSeriesCountsResponse reads every message of a SeriesCounts stream
// and returns them as one response.
func ReceiveSeriesCountsResponse(s StoreGateway_SeriesCountsClient) (*SeriesCountsResponse, error) {
	var out *SeriesCountsResponse
	for {
		msg, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = msg
			continue
		}
		out.Groups = append(out.Groups, msg.Groups...)
	}
	if out == nil {
		return nil, errors.New("SeriesCounts stream ended without a message")
	}
	return out, nil
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

// customSearchLabelNamesClient wraps StoreGateway_SearchLabelNamesClient so
// Recv translates raw gRPC status errors into context.Canceled / DeadlineExceeded
// when the caller's context is gone. Without this, shouldRetry misclassifies
// canceled searches as retriable.
type customSearchLabelNamesClient struct {
	*customClientStream

	wrapped StoreGateway_SearchLabelNamesClient
}

func newCustomSearchLabelNamesClient(client StoreGateway_SearchLabelNamesClient) *customSearchLabelNamesClient {
	return &customSearchLabelNamesClient{
		customClientStream: &customClientStream{client},
		wrapped:            client,
	}
}

func (c *customSearchLabelNamesClient) Recv() (*storepb.SearchResultBatch, error) {
	res, err := c.wrapped.Recv()
	return res, globalerror.WrapGRPCErrorWithContextError(c.Context(), err)
}

// customSearchLabelValuesClient mirrors customSearchLabelNamesClient for
// SearchLabelValues.
type customSearchLabelValuesClient struct {
	*customClientStream

	wrapped StoreGateway_SearchLabelValuesClient
}

func newCustomSearchLabelValuesClient(client StoreGateway_SearchLabelValuesClient) *customSearchLabelValuesClient {
	return &customSearchLabelValuesClient{
		customClientStream: &customClientStream{client},
		wrapped:            client,
	}
}

func (c *customSearchLabelValuesClient) Recv() (*storepb.SearchResultBatch, error) {
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
