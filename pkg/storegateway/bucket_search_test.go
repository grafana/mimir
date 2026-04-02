// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
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
					SearchFilter: &storepb.SearchFilter{SortBy: storepb.SORT_BY_ALPHA, SortOrder: storepb.SORT_ORDER_ASC},
					Limit:        1,
				},
				expected: []string{"a"},
				ordered:  true,
			},
			"filter then sort alpha asc then limit": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"b", "c"}, SortBy: storepb.SORT_BY_ALPHA, SortOrder: storepb.SORT_ORDER_ASC},
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
					SearchFilter: &storepb.SearchFilter{SortBy: storepb.SORT_BY_ALPHA, SortOrder: storepb.SORT_ORDER_DESC},
				},
				expected: []string{"c", "b", "a"},
				ordered:  true,
			},
			// Series with b=1 are {a,b} only; no series with b=1 also has label c.
			"matchers restrict label names": {
				req: &storepb.SearchLabelNamesRequest{
					Start:    timestamp.FromTime(minTime),
					End:      timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"}},
				},
				expected: []string{"a", "b"},
				ordered:  false,
			},
		} {
			t.Run(name, func(t *testing.T) {
				srv := newTestSearchServer(ctx)
				require.NoError(t, s.store.SearchLabelNames(tc.req, srv))
				if tc.ordered {
					require.Equal(t, tc.expected, srv.results)
				} else {
					require.ElementsMatch(t, tc.expected, srv.results)
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
			req     *storepb.SearchLabelValuesRequest
			expected []string
			ordered  bool // whether to assert exact order
			wantLen  int  // if >0, only assert result length (use when result is non-deterministic)
		}{
			"no filter returns all values": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"1", "2"},
				ordered:  false,
			},
			"filter matching one value": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"1"}},
				},
				expected: []string{"1"},
				ordered:  true,
			},
			"filter matching nothing": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"zzz"}},
				},
				expected: nil,
				ordered:  true,
			},
			// limit=1 with no sort: arrival order from concurrent blocks is non-deterministic,
			// so we only assert the count rather than a specific value.
			"limit applied after filter": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Limit: 1,
				},
				wantLen: 1,
			},
			// filter matches "1" and "2"; limit=1 keeps one but which one is non-deterministic.
			"filter then limit": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "b",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"1", "2"}},
					Limit:        1,
				},
				wantLen: 1,
			},
			"unknown label returns empty": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "unknown",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: nil,
				ordered:  true,
			},
			"outside time range": {
				req: &storepb.SearchLabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
				ordered:  true,
			},
			"sort alpha asc": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SortBy: storepb.SORT_BY_ALPHA, SortOrder: storepb.SORT_ORDER_ASC},
				},
				expected: []string{"1", "2"},
				ordered:  true,
			},
			"sort alpha desc": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SortBy: storepb.SORT_BY_ALPHA, SortOrder: storepb.SORT_ORDER_DESC},
				},
				expected: []string{"2", "1"},
				ordered:  true,
			},
			"case insensitive filter": {
				req: &storepb.SearchLabelValuesRequest{
					Label:        "a",
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"1"}, CaseInsensitive: true},
				},
				expected: []string{"1"},
				ordered:  true,
			},
			// Series with b=1 have labels {a,b} only — none carry label c.
			// Querying label "c" values under this matcher must return empty.
			"matchers exclude non-matching series values": {
				req: &storepb.SearchLabelValuesRequest{
					Label:    "c",
					Start:    timestamp.FromTime(minTime),
					End:      timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"}},
				},
				expected: nil,
				ordered:  true,
			},
		} {
			t.Run(name, func(t *testing.T) {
				srv := newTestSearchServer(ctx)
				require.NoError(t, s.store.SearchLabelValues(tc.req, srv))
				switch {
				case tc.wantLen > 0:
					require.Len(t, srv.results, tc.wantLen)
				case tc.ordered:
					require.Equal(t, tc.expected, srv.results)
				default:
					require.ElementsMatch(t, tc.expected, srv.results)
				}
			})
		}
	})
}

func TestBucketStore_SearchLabelValues_MissingLabel(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, defaultPrepareStoreConfig(t))

	ctx := context.Background()
	srv := newTestSearchServer(ctx)
	err := s.store.SearchLabelValues(&storepb.SearchLabelValuesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	}, srv)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing label name")
}

func TestBucketStore_SearchLabelNames_MultipleBlocks(t *testing.T) {
	// Use in-memory bucket so we can control block layout.
	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, defaultPrepareStoreConfig(t))

	ctx := context.Background()

	// All blocks are queried and results are merged/deduplicated.
	// Results arrive in goroutine completion order, so use ElementsMatch.
	srv := newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	}, srv))
	require.ElementsMatch(t, []string{"a", "b", "c"}, srv.results)

	// Filter applied across all blocks.
	srv = newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start:        timestamp.FromTime(minTime),
		End:          timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"a"}},
	}, srv))
	require.Equal(t, []string{"a"}, srv.results)
}

// TestBucketStore_SearchLabelValues_CaseInsensitive verifies case-insensitive matching against
// label values that have mixed-case variants. The default test data uses purely numeric values
// ("1","2") which cannot distinguish case-sensitive from case-insensitive logic.
func TestBucketStore_SearchLabelValues_CaseInsensitive(t *testing.T) {
	// Two blocks: one with mixed-case values, one with lowercase-only values.
	// This ensures deduplication across blocks works alongside case folding.
	customSeries := []labels.Labels{
		labels.FromStrings("env", "Prod"),
		labels.FromStrings("env", "Staging"),
		labels.FromStrings("env", "Dev"),
		labels.FromStrings("env", "Test"),
		labels.FromStrings("env", "prod"),
		labels.FromStrings("env", "staging"),
		labels.FromStrings("env", "dev"),
		labels.FromStrings("env", "test"),
	}
	cfg := defaultPrepareStoreConfig(t)
	cfg.series = customSeries

	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, cfg)

	ctx := context.Background()

	// Case-insensitive: "prod" matches both "Prod" and "prod".
	srv := newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelValues(&storepb.SearchLabelValuesRequest{
		Label:        "env",
		Start:        timestamp.FromTime(minTime),
		End:          timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"prod"}, CaseInsensitive: true},
	}, srv))
	require.ElementsMatch(t, []string{"Prod", "prod"}, srv.results)

	// Case-sensitive: "prod" matches only "prod", not "Prod".
	srv = newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelValues(&storepb.SearchLabelValuesRequest{
		Label:        "env",
		Start:        timestamp.FromTime(minTime),
		End:          timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"prod"}, CaseInsensitive: false},
	}, srv))
	require.Equal(t, []string{"prod"}, srv.results)
}

// TestBucketStore_SearchLabelNames_FuzzAndScoreSort verifies FuzzThreshold filtering and
// SORT_BY_SCORE ordering at the bucket store layer. The default test data has single-char
// labels ("a","b","c") that produce trivial JaroWinkler scores, so custom series are used.
func TestBucketStore_SearchLabelNames_FuzzAndScoreSort(t *testing.T) {
	// Labels: __name__, status, route — same as the ingester fuzz tests for comparable expectations.
	customSeries := []labels.Labels{
		labels.FromStrings("__name__", "test_1", "status", "200"),
		labels.FromStrings("__name__", "test_1", "status", "500"),
		labels.FromStrings("__name__", "test_2", "route", "get_user"),
		labels.FromStrings("__name__", "test_2"),
		labels.FromStrings("__name__", "test_3", "status", "200"),
		labels.FromStrings("__name__", "test_3", "status", "500"),
		labels.FromStrings("__name__", "test_4", "route", "post_user"),
		labels.FromStrings("__name__", "test_4"),
	}
	cfg := defaultPrepareStoreConfig(t)
	cfg.series = customSeries

	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, cfg)

	ctx := context.Background()

	// FuzzThreshold=0.4, term "ame": JaroWinkler("ame","__name__")≈0.792 and
	// JaroWinkler("ame","status")≈0.5 both exceed the threshold; "route" does not (~0).
	srv := newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{
			SearchTerms:   []string{"ame"},
			FuzzThreshold: 0.4,
		},
	}, srv))
	require.ElementsMatch(t, []string{"__name__", "status"}, srv.results)

	// SORT_BY_SCORE DESC: __name__ (score ~0.792) comes before status (score ~0.5).
	srv = newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelNames(&storepb.SearchLabelNamesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{
			SearchTerms:   []string{"ame"},
			FuzzThreshold: 0.4,
			SortBy:        storepb.SORT_BY_SCORE,
			SortOrder:     storepb.SORT_ORDER_DESC,
		},
	}, srv))
	require.Equal(t, []string{"__name__", "status"}, srv.results)
}

// TestBucketStore_SearchLabelValues_FuzzAndScoreSort verifies FuzzThreshold filtering and
// SORT_BY_SCORE ordering for label values at the bucket store layer.
func TestBucketStore_SearchLabelValues_FuzzAndScoreSort(t *testing.T) {
	// Same custom series as TestBucketStore_SearchLabelNames_FuzzAndScoreSort.
	customSeries := []labels.Labels{
		labels.FromStrings("__name__", "test_1", "status", "200"),
		labels.FromStrings("__name__", "test_1", "status", "500"),
		labels.FromStrings("__name__", "test_2", "route", "get_user"),
		labels.FromStrings("__name__", "test_2"),
		labels.FromStrings("__name__", "test_3", "status", "200"),
		labels.FromStrings("__name__", "test_3", "status", "500"),
		labels.FromStrings("__name__", "test_4", "route", "post_user"),
		labels.FromStrings("__name__", "test_4"),
	}
	cfg := defaultPrepareStoreConfig(t)
	cfg.series = customSeries

	bkt := objstore.NewInMemBucket()
	s := prepareStoreWithTestBlocks(t, bkt, cfg)

	ctx := context.Background()

	// FuzzThreshold=0.9, term "200": only the exact match "200" passes (JaroWinkler("200","500")≈0.78).
	srv := newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelValues(&storepb.SearchLabelValuesRequest{
		Label: "status",
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{
			SearchTerms:   []string{"200"},
			FuzzThreshold: 0.9,
		},
	}, srv))
	require.Equal(t, []string{"200"}, srv.results)

	// SORT_BY_SCORE DESC, FuzzThreshold=0.4: "200" (score=1.0) before "500" (score≈0.78).
	srv = newTestSearchServer(ctx)
	require.NoError(t, s.store.SearchLabelValues(&storepb.SearchLabelValuesRequest{
		Label: "status",
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{
			SearchTerms:   []string{"200"},
			FuzzThreshold: 0.4,
			SortBy:        storepb.SORT_BY_SCORE,
			SortOrder:     storepb.SORT_ORDER_DESC,
		},
	}, srv))
	require.Equal(t, []string{"200", "500"}, srv.results)
}
