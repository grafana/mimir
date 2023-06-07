package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// This is a copy of (*ingesterClient).Push method, but accepting any message type.
func pushRaw(ctx context.Context, conn *grpc.ClientConn, msg interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	out := new(mimirpb.WriteResponse)
	err := conn.Invoke(ctx, "/cortex.Ingester/Push", msg, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// bufferPoolingIngesterClient implements IngesterClient, but overrides Push method to add pooling of buffers used to marshal write requests.
type bufferPoolingIngesterClient struct {
	client IngesterClient

	conn *grpc.ClientConn

	// This refers to pushRaw function, but is overriden in the benchmark to avoid doing actual grpc calls.
	pushRawFn func(ctx context.Context, conn *grpc.ClientConn, msg interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error)
}

func newWriteRequestBufferingClient(client IngesterClient, conn *grpc.ClientConn) *bufferPoolingIngesterClient {
	c := &bufferPoolingIngesterClient{
		client:    client,
		conn:      conn,
		pushRawFn: pushRaw,
	}
	return c
}

// Push wraps WriteRequest to implement buffer pooling.
func (c *bufferPoolingIngesterClient) Push(ctx context.Context, in *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	p := GetPool(ctx)
	if p == nil {
		return c.client.Push(ctx, in, opts...)
	}

	wr := &wrappedRequest{
		WriteRequest: in,
		slabPool:     p,
	}
	// We can return all buffers back to slabPool when this method finishes.
	defer wr.ReturnBuffersToPool()

	return c.pushRawFn(ctx, c.conn, wr, opts...)
}

func (c *bufferPoolingIngesterClient) QueryStream(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (Ingester_QueryStreamClient, error) {
	return c.client.QueryStream(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) QueryExemplars(ctx context.Context, in *ExemplarQueryRequest, opts ...grpc.CallOption) (*ExemplarQueryResponse, error) {
	return c.client.QueryExemplars(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) LabelValues(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error) {
	return c.client.LabelValues(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) LabelNames(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error) {
	return c.client.LabelNames(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) UserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UserStatsResponse, error) {
	return c.client.UserStats(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) AllUserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UsersStatsResponse, error) {
	return c.client.AllUserStats(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) MetricsForLabelMatchers(ctx context.Context, in *MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*MetricsForLabelMatchersResponse, error) {
	return c.client.MetricsForLabelMatchers(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) MetricsMetadata(ctx context.Context, in *MetricsMetadataRequest, opts ...grpc.CallOption) (*MetricsMetadataResponse, error) {
	return c.client.MetricsMetadata(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) LabelNamesAndValues(ctx context.Context, in *LabelNamesAndValuesRequest, opts ...grpc.CallOption) (Ingester_LabelNamesAndValuesClient, error) {
	return c.client.LabelNamesAndValues(ctx, in, opts...)
}

func (c *bufferPoolingIngesterClient) LabelValuesCardinality(ctx context.Context, in *LabelValuesCardinalityRequest, opts ...grpc.CallOption) (Ingester_LabelValuesCardinalityClient, error) {
	return c.client.LabelValuesCardinality(ctx, in, opts...)
}

type poolKey int

var poolKeyValue poolKey = 1

func WithPool(ctx context.Context, pool *pool.FastReleasingSlabPool[byte]) context.Context {
	if pool != nil {
		return context.WithValue(ctx, poolKeyValue, pool)
	}
	return ctx
}

func GetPool(ctx context.Context) *pool.FastReleasingSlabPool[byte] {
	v := ctx.Value(poolKeyValue)
	if p, ok := v.(*pool.FastReleasingSlabPool[byte]); ok {
		return p
	}
	return nil
}

type wrappedRequest struct {
	*mimirpb.WriteRequest

	slabPool    *pool.FastReleasingSlabPool[byte]
	slabId      int
	moreSlabIds []int // Used in case when Marshal gets called multiple times.
}

func (w *wrappedRequest) Marshal() ([]byte, error) {
	size := w.WriteRequest.Size()
	buf, slabId := w.slabPool.Get(size)

	if w.slabId == 0 {
		w.slabId = slabId
	} else {
		w.moreSlabIds = append(w.moreSlabIds, slabId)
	}

	n, err := w.WriteRequest.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (w *wrappedRequest) ReturnBuffersToPool() {
	if w.slabId != 0 {
		w.slabPool.Release(w.slabId)
		w.slabId = 0
	}
	for _, s := range w.moreSlabIds {
		w.slabPool.Release(s)
	}
	w.moreSlabIds = nil
}
