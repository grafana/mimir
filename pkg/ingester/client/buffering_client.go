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
	IngesterClient

	conn *grpc.ClientConn

	// This refers to pushRaw function, but is overridden in the benchmark to avoid doing actual grpc calls.
	pushRawFn func(ctx context.Context, conn *grpc.ClientConn, msg interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error)
}

func newBufferPoolingIngesterClient(client IngesterClient, conn *grpc.ClientConn) *bufferPoolingIngesterClient {
	c := &bufferPoolingIngesterClient{
		IngesterClient: client,
		conn:           conn,
		pushRawFn:      pushRaw,
	}
	return c
}

// Push wraps WriteRequest to implement buffer pooling.
func (c *bufferPoolingIngesterClient) Push(ctx context.Context, in *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	p := getPool(ctx)
	if p == nil {
		return c.IngesterClient.Push(ctx, in, opts...)
	}

	wr := &wrappedRequest{
		WriteRequest: in,
		slabPool:     p,
	}
	// We can return all buffers back to slabPool when this method finishes.
	defer wr.ReturnBuffersToPool()

	return c.pushRawFn(ctx, c.conn, wr, opts...)
}

type poolKey int

var poolKeyValue poolKey = 1

func WithSlabPool(ctx context.Context, pool *pool.FastReleasingSlabPool[byte]) context.Context {
	if pool != nil {
		return context.WithValue(ctx, poolKeyValue, pool)
	}
	return ctx
}

func getPool(ctx context.Context) *pool.FastReleasingSlabPool[byte] {
	v := ctx.Value(poolKeyValue)
	if p, ok := v.(*pool.FastReleasingSlabPool[byte]); ok {
		return p
	}
	return nil
}

type wrappedRequest struct {
	*mimirpb.WriteRequest

	slabPool    *pool.FastReleasingSlabPool[byte]
	slabID      int
	moreSlabIDs []int // Used in case when Marshal gets called multiple times.
}

func (w *wrappedRequest) Marshal() ([]byte, error) {
	size := w.WriteRequest.Size()
	buf, slabID := w.slabPool.Get(size)

	if w.slabID == 0 {
		w.slabID = slabID
	} else {
		w.moreSlabIDs = append(w.moreSlabIDs, slabID)
	}

	n, err := w.WriteRequest.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (w *wrappedRequest) ReturnBuffersToPool() {
	if w.slabID != 0 {
		w.slabPool.Release(w.slabID)
		w.slabID = 0
	}
	for _, s := range w.moreSlabIDs {
		w.slabPool.Release(s)
	}
	w.moreSlabIDs = nil
}
