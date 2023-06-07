package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// bufferPoolingClient implements IngesterClient, but overrides Push method to add pooling of buffers used to marshal write requests.
type bufferPoolingClient struct {
	upstream HealthAndIngesterClient

	conn *grpc.ClientConn
}

func NewWriteRequestBufferingClient(upstream HealthAndIngesterClient, conn *grpc.ClientConn) HealthAndIngesterClient {
	return &bufferPoolingClient{
		upstream: upstream,
		conn:     conn,
	}
}

// Push request is overridden from IngesterClient interface.
func (c *bufferPoolingClient) Push(ctx context.Context, in *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	p := GetPool(ctx)
	if p == nil {
		return c.upstream.Push(ctx, in, opts...)
	}

	wr := &wrappedRequest{
		WriteRequest: in,
		slabPool:     p,
	}
	// We can return all buffers back to slabPool after this method is finished.
	defer wr.ReturnBuffersToPool()

	// Use underlying grpc client to invoke method with our wrapped request.
	// This is a copy of (*ingesterClient).Push method, but passing the wrapped request.
	// When this wrapped request is marshaled, it will use buffer from our pool.
	out := new(mimirpb.WriteResponse)
	err := c.conn.Invoke(ctx, "/cortex.Ingester/Push", wr, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil

}

func (c *bufferPoolingClient) QueryStream(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (Ingester_QueryStreamClient, error) {
	return c.upstream.QueryStream(ctx, in, opts...)
}

func (c *bufferPoolingClient) QueryExemplars(ctx context.Context, in *ExemplarQueryRequest, opts ...grpc.CallOption) (*ExemplarQueryResponse, error) {
	return c.upstream.QueryExemplars(ctx, in, opts...)
}

func (c *bufferPoolingClient) LabelValues(ctx context.Context, in *LabelValuesRequest, opts ...grpc.CallOption) (*LabelValuesResponse, error) {
	return c.upstream.LabelValues(ctx, in, opts...)
}

func (c *bufferPoolingClient) LabelNames(ctx context.Context, in *LabelNamesRequest, opts ...grpc.CallOption) (*LabelNamesResponse, error) {
	return c.upstream.LabelNames(ctx, in, opts...)
}

func (c *bufferPoolingClient) UserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UserStatsResponse, error) {
	return c.upstream.UserStats(ctx, in, opts...)
}

func (c *bufferPoolingClient) AllUserStats(ctx context.Context, in *UserStatsRequest, opts ...grpc.CallOption) (*UsersStatsResponse, error) {
	return c.upstream.AllUserStats(ctx, in, opts...)
}

func (c *bufferPoolingClient) MetricsForLabelMatchers(ctx context.Context, in *MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*MetricsForLabelMatchersResponse, error) {
	return c.upstream.MetricsForLabelMatchers(ctx, in, opts...)
}

func (c *bufferPoolingClient) MetricsMetadata(ctx context.Context, in *MetricsMetadataRequest, opts ...grpc.CallOption) (*MetricsMetadataResponse, error) {
	return c.upstream.MetricsMetadata(ctx, in, opts...)
}

func (c *bufferPoolingClient) LabelNamesAndValues(ctx context.Context, in *LabelNamesAndValuesRequest, opts ...grpc.CallOption) (Ingester_LabelNamesAndValuesClient, error) {
	return c.upstream.LabelNamesAndValues(ctx, in, opts...)
}

func (c *bufferPoolingClient) LabelValuesCardinality(ctx context.Context, in *LabelValuesCardinalityRequest, opts ...grpc.CallOption) (Ingester_LabelValuesCardinalityClient, error) {
	return c.upstream.LabelValuesCardinality(ctx, in, opts...)
}

func (c *bufferPoolingClient) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return c.upstream.Check(ctx, in, opts...)
}

func (c *bufferPoolingClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	return c.upstream.Watch(ctx, in, opts...)
}

func (c *bufferPoolingClient) Close() error {
	return c.upstream.Close()
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
	moreSlabIds []int // In case Marshal gets called multiple times.
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
