// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/grafana/dskit/grpcutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type mockSearchLabelNamesServer struct {
	storegatewaypb.StoreGateway_SearchLabelNamesServer
	ctx  context.Context
	sent []*storepb.SearchResultBatch
}

func (m *mockSearchLabelNamesServer) Send(b *storepb.SearchResultBatch) error {
	m.sent = append(m.sent, b)
	return nil
}
func (m *mockSearchLabelNamesServer) Context() context.Context { return m.ctx }

type mockSearchLabelValuesServer struct {
	storegatewaypb.StoreGateway_SearchLabelValuesServer
	ctx  context.Context
	sent []*storepb.SearchResultBatch
}

func (m *mockSearchLabelValuesServer) Send(b *storepb.SearchResultBatch) error {
	m.sent = append(m.sent, b)
	return nil
}
func (m *mockSearchLabelValuesServer) Context() context.Context { return m.ctx }

// prepareSearchTestStore builds a BucketStore backed by a temporary filesystem
// bucket using the default test series: {a,b,c} + external label ext1=value1.
// External labels are not stored in block indexes, so label names visible at
// the BucketStore are {a, b, c} only.
func prepareSearchTestStore(t *testing.T) *BucketStore {
	t.Helper()
	tmpDir := t.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := defaultPrepareStoreConfig(t)
	cfg.tempDir = tmpDir
	s := prepareStoreWithTestBlocks(t, bkt, cfg)
	return s.store
}

// collectSearchNames collects all value strings from the sent batches.
func collectSearchNames(sent []*storepb.SearchResultBatch) []string {
	var got []string
	for _, b := range sent {
		for _, r := range b.Results {
			got = append(got, r.Value)
		}
	}
	return got
}

func TestBucketStoreSearchLabelNames(t *testing.T) {
	bs := prepareSearchTestStore(t)

	tests := []struct {
		name        string
		filterTerms []string
		caseInsens  bool
		ordering    storepb.SearchOrdering
		limit       int64
		want        []string
	}{
		{
			// External labels (ext1=value1) are not stored in the block index;
			// only the series labels {a, b, c} are visible at BucketStore level.
			name: "all label names no filter",
			want: []string{"a", "b", "c"},
		},
		{
			name:        "substring filter matches single name",
			filterTerms: []string{"b"},
			want:        []string{"b"},
		},
		{
			name:  "limit 2 returns first two in asc order",
			limit: 2,
			want:  []string{"a", "b"},
		},
		{
			name:     "desc ordering returns names in reverse",
			ordering: storepb.ORDER_BY_VALUE_DESC,
			want:     []string{"c", "b", "a"},
		},
		{
			name:        "no match returns empty",
			filterTerms: []string{"zzz"},
			want:        nil,
		},
		{
			name:        "filter matches label a only",
			filterTerms: []string{"a"},
			want:        []string{"a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &storepb.SearchLabelNamesRequest{
				Start: 0,
				End:   math.MaxInt64,
				Filter: &storepb.SearchFilter{
					Terms:           tc.filterTerms,
					CaseInsensitive: tc.caseInsens,
				},
				Ordering: tc.ordering,
				Limit:    tc.limit,
			}
			s := &mockSearchLabelNamesServer{ctx: context.Background()}
			require.NoError(t, bs.SearchLabelNames(req, s))
			assert.Equal(t, tc.want, collectSearchNames(s.sent))
		})
	}
}

func TestBucketStoreSearchLabelValues(t *testing.T) {
	bs := prepareSearchTestStore(t)

	tests := []struct {
		name        string
		labelName   string
		filterTerms []string
		caseInsens  bool
		ordering    storepb.SearchOrdering
		limit       int64
		want        []string
	}{
		{
			name:      "all values for label a",
			labelName: "a",
			want:      []string{"1", "2"},
		},
		{
			name:      "limit 1 on label a returns first value",
			labelName: "a",
			limit:     1,
			want:      []string{"1"},
		},
		{
			name:      "desc ordering on label a",
			labelName: "a",
			ordering:  storepb.ORDER_BY_VALUE_DESC,
			want:      []string{"2", "1"},
		},
		{
			name:      "missing label returns empty",
			labelName: "missing",
			want:      nil,
		},
		{
			name:        "filter value 1 returns only 1",
			labelName:   "a",
			filterTerms: []string{"1"},
			want:        []string{"1"},
		},
		{
			name:      "all values for label b",
			labelName: "b",
			want:      []string{"1", "2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &storepb.SearchLabelValuesRequest{
				Start: 0,
				End:   math.MaxInt64,
				Label: tc.labelName,
				Filter: &storepb.SearchFilter{
					Terms:           tc.filterTerms,
					CaseInsensitive: tc.caseInsens,
				},
				Ordering: tc.ordering,
				Limit:    tc.limit,
			}
			s := &mockSearchLabelValuesServer{ctx: context.Background()}
			require.NoError(t, bs.SearchLabelValues(req, s))
			assert.Equal(t, tc.want, collectSearchNames(s.sent))
		})
	}
}

// TestBucketStoreSearchLabelNamesHonoursCtxCancellation cancels the server
// context before invoking the RPC and asserts that the RPC returns
// codes.Canceled rather than draining the iterator.
func TestBucketStoreSearchLabelNamesHonoursCtxCancellation(t *testing.T) {
	bs := prepareSearchTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := &storepb.SearchLabelNamesRequest{
		Start:  0,
		End:    math.MaxInt64,
		Filter: &storepb.SearchFilter{},
	}
	s := &mockSearchLabelNamesServer{ctx: ctx}
	err := bs.SearchLabelNames(req, s)
	require.Error(t, err)
	st, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.Canceled, st.Code())
}

// TestStreamBucketSearchResultsBatching exercises streamBucketSearchResults
// in isolation against a synthetic iterator of >searchBatchSize results, and
// verifies the boundary splits into the expected batches with the header
// batch sent first (carrying response_hints.queried_blocks) and warnings
// riding on the trailer. The trailer must NOT carry response_hints — the
// querier reads them only from the header.
func TestStreamBucketSearchResultsBatching(t *testing.T) {
	const total = searchBatchSize + 1
	results := make([]storage.SearchResult, total)
	for i := 0; i < total; i++ {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%04d", i), Score: 1.0}
	}
	var warns annotations.Annotations
	warns.Add(errors.New("partial-block: index header missing"))
	rs := storage.NewSearchResultSetFromSlice(results, warns)

	var sent []*storepb.SearchResultBatch
	send := func(b *storepb.SearchResultBatch) error {
		copyResults := make([]storepb.SearchResultBatch_Result, len(b.Results))
		copy(copyResults, b.Results)
		sent = append(sent, &storepb.SearchResultBatch{Results: copyResults, Warnings: b.Warnings, ResponseHints: b.ResponseHints})
		return nil
	}
	queried := []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)}
	require.NoError(t, streamBucketSearchResults(context.Background(), rs, queried, send))

	require.Len(t, sent, 3, "header + 257 results split at the searchBatchSize=256 boundary = 3 messages")
	// Header batch: empty Results, empty Warnings, ResponseHints populated.
	assert.Empty(t, sent[0].Results, "header batch must carry no results")
	assert.Empty(t, sent[0].Warnings, "header batch must carry no warnings")
	require.NotNil(t, sent[0].ResponseHints, "header batch must carry response_hints")
	require.Len(t, sent[0].ResponseHints.QueriedBlocks, 2)
	assert.Equal(t, queried[0].String(), sent[0].ResponseHints.QueriedBlocks[0].Id)
	assert.Equal(t, queried[1].String(), sent[0].ResponseHints.QueriedBlocks[1].Id)
	// First full data batch.
	assert.Len(t, sent[1].Results, searchBatchSize)
	assert.Empty(t, sent[1].Warnings, "warnings must ride only on the trailer batch")
	assert.Nil(t, sent[1].ResponseHints, "response_hints must ride only on the header batch")
	// Trailer batch: residual results + warnings, no response_hints.
	assert.Len(t, sent[2].Results, 1)
	require.Len(t, sent[2].Warnings, 1)
	assert.Equal(t, "partial-block: index header missing", sent[2].Warnings[0])
	assert.Nil(t, sent[2].ResponseHints, "response_hints must NOT ride on the trailer batch")
}

// TestStreamBucketSearchResultsHeaderOnlyWhenNoResults asserts the SG still
// emits the header batch when the iterator produces zero results — the
// querier's consistency check needs the header even from SGs that found
// nothing for the requested blocks (so those blocks are correctly classified
// as queried-but-empty rather than still-missing).
func TestStreamBucketSearchResultsHeaderOnlyWhenNoResults(t *testing.T) {
	rs := storage.NewSearchResultSetFromSlice(nil, nil)
	var sent []*storepb.SearchResultBatch
	send := func(b *storepb.SearchResultBatch) error {
		sent = append(sent, b)
		return nil
	}
	queried := []ulid.ULID{ulid.MustNew(1, nil)}
	require.NoError(t, streamBucketSearchResults(context.Background(), rs, queried, send))

	require.Len(t, sent, 1, "no results and no warnings should yield header-only stream")
	require.NotNil(t, sent[0].ResponseHints)
	require.Len(t, sent[0].ResponseHints.QueriedBlocks, 1)
	assert.Equal(t, queried[0].String(), sent[0].ResponseHints.QueriedBlocks[0].Id)
	assert.Empty(t, sent[0].Results)
	assert.Empty(t, sent[0].Warnings)
}

// TestStreamBucketSearchResultsHeaderWhenNoQueriedBlocks asserts the SG
// always sends the header batch even when no blocks were queried — the
// querier-side header read would otherwise block waiting for a message that
// never arrives.
func TestStreamBucketSearchResultsHeaderWhenNoQueriedBlocks(t *testing.T) {
	rs := storage.NewSearchResultSetFromSlice(nil, nil)
	var sent []*storepb.SearchResultBatch
	send := func(b *storepb.SearchResultBatch) error {
		sent = append(sent, b)
		return nil
	}
	require.NoError(t, streamBucketSearchResults(context.Background(), rs, nil, send))

	require.Len(t, sent, 1, "empty queried-blocks set still requires a header batch")
	require.NotNil(t, sent[0].ResponseHints, "header must carry a (possibly empty) response_hints struct")
	assert.Empty(t, sent[0].ResponseHints.QueriedBlocks)
}

// TestStreamBucketSearchResultsHonoursCtxCancellation cancels ctx before
// streaming and asserts the loop returns ctx.Err().
func TestStreamBucketSearchResultsHonoursCtxCancellation(t *testing.T) {
	results := make([]storage.SearchResult, 10)
	for i := range results {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%d", i), Score: 1.0}
	}
	rs := storage.NewSearchResultSetFromSlice(results, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	send := func(_ *storepb.SearchResultBatch) error { return nil }
	err := streamBucketSearchResults(ctx, rs, nil, send)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
}
