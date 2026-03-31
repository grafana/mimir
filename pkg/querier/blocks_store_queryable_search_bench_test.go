// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// generateBenchNames returns n unique label-name strings of the form "label_XXXXXXXX".
func generateBenchNames(n int) []string {
	vals := make([]string, n)
	for i := range n {
		vals[i] = fmt.Sprintf("label_%08d", i)
	}
	return vals
}

const benchBatchSize = 1024

// batchedSearchStreamClient delivers results in batches of benchBatchSize, simulating the real
// store-gateway streaming behaviour. The final batch carries ResponseHints with the queried block ID.
type batchedSearchStreamClient struct {
	grpc.ClientStream // embedded interface; only Recv is called
	values            []string
	blockID           ulid.ULID
	pos               int
}

func (c *batchedSearchStreamClient) Recv() (*storepb.StoreSearchResponse, error) {
	if c.pos >= len(c.values) {
		return nil, io.EOF
	}
	end := c.pos + benchBatchSize
	if end > len(c.values) {
		end = len(c.values)
	}
	results := make([]*storepb.StoreSearchResult, end-c.pos)
	for i, v := range c.values[c.pos:end] {
		results[i] = &storepb.StoreSearchResult{Value: v}
	}
	c.pos = end
	resp := &storepb.StoreSearchResponse{Results: results}
	// Attach hints only on the final batch so the consistency check can verify queried blocks.
	if c.pos >= len(c.values) {
		resp.ResponseHints = &storepb.StoreSearchResponseHints{
			QueriedBlocks: []storepb.Block{{Id: c.blockID.String()}},
		}
	}
	return resp, nil
}

// batchedSearchClientMock is a BlocksStoreClient that returns names/values via the batched stream,
// and also satisfies the old LabelNames/LabelValues API for the comparison benchmark.
// blockID is included in all response hints so the blocks consistency check passes.
type batchedSearchClientMock struct {
	remoteAddr string
	blockID    ulid.ULID
	names      []string
	values     []string
}

func (m *batchedSearchClientMock) RemoteAddress() string { return m.remoteAddr }
func (m *batchedSearchClientMock) RemoteZone() string    { return "" }
func (m *batchedSearchClientMock) Close() error          { return nil }

func (m *batchedSearchClientMock) Series(_ context.Context, _ *storepb.SeriesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	return nil, nil
}

func (m *batchedSearchClientMock) LabelNames(_ context.Context, _ *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	// Clone names to simulate gRPC deserialization creating fresh string data, as in production.
	names := make([]string, len(m.names))
	copy(names, m.names)
	return &storepb.LabelNamesResponse{
		Names: names,
		ResponseHints: &storepb.LabelNamesResponseHints{
			QueriedBlocks: []storepb.Block{{Id: m.blockID.String()}},
		},
	}, nil
}

func (m *batchedSearchClientMock) LabelValues(_ context.Context, _ *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	// Clone values to simulate gRPC deserialization creating fresh string data, as in production.
	values := make([]string, len(m.values))
	copy(values, m.values)
	return &storepb.LabelValuesResponse{
		Values: values,
		ResponseHints: &storepb.LabelValuesResponseHints{
			QueriedBlocks: []storepb.Block{{Id: m.blockID.String()}},
		},
	}, nil
}

func (m *batchedSearchClientMock) SearchLabelNames(_ context.Context, _ *storepb.SearchLabelNamesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelNamesClient, error) {
	return &batchedSearchStreamClient{values: m.names, blockID: m.blockID}, nil
}

func (m *batchedSearchClientMock) SearchLabelValues(_ context.Context, _ *storepb.SearchLabelValuesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelValuesClient, error) {
	return &batchedSearchStreamClient{values: m.values, blockID: m.blockID}, nil
}

// unlimitedBlocksStoreSetMock always returns the same single client, allowing any number of calls.
type unlimitedBlocksStoreSetMock struct {
	services.Service // embedded nil; never called in benchmarks
	client           BlocksStoreClient
	block            ulid.ULID
}

func (m *unlimitedBlocksStoreSetMock) GetClientsFor(_ string, _ bucketindex.Blocks, _ map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error) {
	return map[BlocksStoreClient][]ulid.ULID{m.client: {m.block}}, nil
}

// newBenchQuerier constructs a blocksStoreQuerier backed by a single client returning the given label names.
func newBenchQuerier(b *testing.B, names []string) (*blocksStoreQuerier, context.Context) {
	b.Helper()

	block1 := ulid.MustNew(1, nil)
	client := &batchedSearchClientMock{
		remoteAddr: "bench",
		blockID:    block1,
		names:      names,
		values:     names,
	}

	stores := &unlimitedBlocksStoreSetMock{client: client, block: block1}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "bench-user", mock.Anything, mock.Anything).
		Return(bucketindex.Blocks{{ID: block1}}, &bucketindex.Metadata{}, nil)

	reg := prometheus.NewRegistry()
	q := &blocksStoreQuerier{
		minT:               0,
		maxT:               1000,
		finder:             finder,
		stores:             stores,
		dynamicReplication: newDynamicReplication(),
		consistency:        NewBlocksConsistency(0, reg),
		logger:             log.NewNopLogger(),
		metrics:            newBlocksStoreQueryableMetrics(reg),
		limits:             &blocksStoreLimitsMock{},
	}

	ctx := user.InjectOrgID(context.Background(), "bench-user")
	return q, ctx
}

// BenchmarkBlocksStoreQuerier_LabelNames_Old benchmarks the existing (buffered) label names path.
// All names from all gateways are collected into a slice before dedup and return.
func BenchmarkBlocksStoreQuerier_LabelNames_Old(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		names := generateBenchNames(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			q, ctx := newBenchQuerier(b, names)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, _, err := q.LabelNames(ctx, &storage.LabelHints{})
				require.NoError(b, err)
				_ = result
			}
		})
	}
}

// BenchmarkBlocksStoreQuerier_SearchLabelNames_New benchmarks the new streaming label names path.
// Results are forwarded per batch (Fix #1), deduplicated eagerly, and returned as a stream.
func BenchmarkBlocksStoreQuerier_SearchLabelNames_New(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		names := generateBenchNames(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			q, ctx := newBenchQuerier(b, names)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				vset, _ := q.SearchLabelNames(ctx, &mimirstorage.MimirSearchHints{})
				for vset.Next() {
				}
				require.NoError(b, vset.Err())
				vset.Close()
			}
		})
	}
}
