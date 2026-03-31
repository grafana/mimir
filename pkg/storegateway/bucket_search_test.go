// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// testSearchServer collects StoreSearchResponse batches sent by SearchLabelNames/SearchLabelValues.
// It implements both StoreGateway_SearchLabelNamesServer and StoreGateway_SearchLabelValuesServer.
type testSearchServer struct {
	grpc.ServerStream
	ctx     context.Context
	results []string
}

func newTestSearchServer(ctx context.Context) *testSearchServer {
	return &testSearchServer{ctx: ctx}
}

func (s *testSearchServer) Send(r *storepb.StoreSearchResponse) error {
	for _, res := range r.Results {
		s.results = append(s.results, res.Value)
	}
	return nil
}

func (s *testSearchServer) Context() context.Context { return s.ctx }

// Satisfy grpc.ServerStream methods that may be called by the gRPC framework.
func (s *testSearchServer) SetHeader(metadata.MD) error  { return nil }
func (s *testSearchServer) SendHeader(metadata.MD) error { return nil }
func (s *testSearchServer) SetTrailer(metadata.MD)       {}
func (s *testSearchServer) SendMsg(m any) error          { return nil }
func (s *testSearchServer) RecvMsg(m any) error          { return nil }

// Ensure testSearchServer implements both streaming server interfaces.
var _ storegatewaypb.StoreGateway_SearchLabelNamesServer = (*testSearchServer)(nil)
var _ storegatewaypb.StoreGateway_SearchLabelValuesServer = (*testSearchServer)(nil)

func TestBucketStore_SearchLabelNames(t *testing.T) {
	// Default test data (from defaultPrepareStoreConfig) has:
	//   labels a, b, c with values 1, 2
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		for name, tc := range map[string]struct {
			req      *storepb.SearchLabelNamesRequest
			expected []string
			ordered  bool // whether to assert exact order
		}{
			"no filter returns all names": {
				req: &storepb.SearchLabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"a", "b", "c"},
				ordered:  false, // concurrent blocks, arrival order non-deterministic
			},
			"filter matching subset": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"a"}},
				},
				expected: []string{"a"},
				ordered:  true,
			},
			"filter matching multiple names": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"b", "c"}},
				},
				expected: []string{"b", "c"},
				ordered:  false,
			},
			"filter matching nothing": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"zzz"}},
				},
				expected: nil,
				ordered:  true,
			},
			"sort alpha asc then limit": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SortBy: 1, SortOrder: 0},
					Limit:        1,
				},
				expected: []string{"a"},
				ordered:  true,
			},
			"filter then sort alpha asc then limit": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"b", "c"}, SortBy: 1, SortOrder: 0},
					Limit:        1,
				},
				expected: []string{"b"},
				ordered:  true,
			},
			"outside time range": {
				req: &storepb.SearchLabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
				ordered:  true,
			},
			"case insensitive filter": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"A"}, CaseInsensitive: true},
				},
				expected: []string{"a"},
				ordered:  true,
			},
			"case sensitive filter does not match lowercase": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"A"}, CaseInsensitive: false},
				},
				expected: nil,
				ordered:  true,
			},
			"sort alpha desc": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SortBy: 1, SortOrder: 1},
				},
				expected: []string{"c", "b", "a"},
				ordered:  true,
			},
		} {
			t.Run(name, func(t *testing.T) {
				srv := newTestSearchServer(ctx)
				err := s.store.SearchLabelNames(tc.req, srv)
				require.NoError(t, err)
				if tc.ordered {
					assert.Equal(t, tc.expected, srv.results)
				} else {
					assert.ElementsMatch(t, tc.expected, srv.results)
				}
			})
		}
	})
}

func TestBucketStore_SearchLabelValues(t *testing.T) {
	// Default test data (from defaultPrepareStoreConfig) has:
	//   a={1,2}, b={1,2}, c={1,2}
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		for name, tc := range map[string]struct {
			req      *storepb.SearchLabelValuesRequest
			expected []string
		}{
			"no filter returns all values": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"1", "2"},
			},
			"filter matching one value": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"1"}},
				},
				expected: []string{"1"},
			},
			"filter matching nothing": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"zzz"}},
				},
				expected: nil,
			},
			"limit applied after filter": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Limit: 1,
				},
				expected: []string{"1"},
			},
			"filter then limit": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "b",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"1", "2"}},
					Limit:        1,
				},
				expected: []string{"1"},
			},
			"unknown label returns empty": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "unknown",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: nil,
			},
			"outside time range": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"sort alpha desc": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SortBy: 1, SortOrder: 1},
				},
				expected: []string{"2", "1"},
			},
		} {
			t.Run(name, func(t *testing.T) {
				srv := newTestSearchServer(ctx)
				err := s.store.SearchLabelValues(tc.req, srv)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, srv.results)
			})
		}
	})
}

func TestBucketStore_SearchLabelNames_MultipleBlocks(t *testing.T) {
	// Use in-memory bucket so we can control block layout.
	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, defaultPrepareStoreConfig(t))

	ctx := context.Background()

	// All blocks are queried and results are merged/deduplicated.
	// Results arrive in goroutine completion order, so use ElementsMatch.
	srv := newTestSearchServer(ctx)
	err := s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	}, srv)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, srv.results)

	// Filter applied across all blocks.
	srv = newTestSearchServer(ctx)
	err = s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start:        timestamp.FromTime(minTime),
		End:          timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"a"}},
	}, srv)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, srv.results)
}
