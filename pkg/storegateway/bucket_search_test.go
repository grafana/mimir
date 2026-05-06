// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// TestBucketStoreSearchLabelValuesEmitsTruncationWarning checks that when
// ApplySearchHints clips the merged result set due to req.Limit, a
// human-readable warning rides on the trailer batch.
func TestBucketStoreSearchLabelValuesEmitsTruncationWarning(t *testing.T) {
	bs := prepareSearchTestStore(t)

	req := &storepb.SearchLabelValuesRequest{
		Start:  0,
		End:    math.MaxInt64,
		Label:  "a", // values {1, 2}; limit 1 forces truncation by 1.
		Filter: &storepb.SearchFilter{},
		Limit:  1,
	}
	s := &mockSearchLabelValuesServer{ctx: context.Background()}
	require.NoError(t, bs.SearchLabelValues(req, s))

	require.Len(t, s.sent, 1)
	assert.Equal(t, []string{"1"}, collectSearchNames(s.sent))
	require.Len(t, s.sent[0].Warnings, 1)
	assert.Contains(t, s.sent[0].Warnings[0], "truncated")
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
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got %T: %v", err, err)
	assert.Equal(t, codes.Canceled, st.Code())
}

// TestStreamBucketSearchResultsBatching exercises streamBucketSearchResults
// in isolation against a synthetic slice of >searchBatchSize results, and
// verifies the boundary splits into the expected batches with the warning
// riding on the trailer.
func TestStreamBucketSearchResultsBatching(t *testing.T) {
	const total = searchBatchSize + 1
	results := make([]storage.SearchResult, total)
	for i := 0; i < total; i++ {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%04d", i), Score: 1.0}
	}
	warnings := []string{"results truncated: 999 values matched, returning first 257"}

	var sent []*storepb.SearchResultBatch
	send := func(b *storepb.SearchResultBatch) error {
		copyResults := make([]storepb.SearchResultBatch_Result, len(b.Results))
		copy(copyResults, b.Results)
		sent = append(sent, &storepb.SearchResultBatch{Results: copyResults, Warnings: b.Warnings})
		return nil
	}
	require.NoError(t, streamBucketSearchResults(context.Background(), results, warnings, send))

	require.Len(t, sent, 2, "257 results must split into 2 batches at the searchBatchSize=256 boundary")
	assert.Len(t, sent[0].Results, searchBatchSize)
	assert.Empty(t, sent[0].Warnings, "warnings must ride only on the trailer batch")
	assert.Len(t, sent[1].Results, 1)
	assert.Equal(t, warnings, sent[1].Warnings)
}

// TestStreamBucketSearchResultsHonoursCtxCancellation cancels ctx before
// streaming and asserts the loop returns ctx.Err().
func TestStreamBucketSearchResultsHonoursCtxCancellation(t *testing.T) {
	results := make([]storage.SearchResult, 10)
	for i := range results {
		results[i] = storage.SearchResult{Value: fmt.Sprintf("v%d", i), Score: 1.0}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	send := func(_ *storepb.SearchResultBatch) error { return nil }
	err := streamBucketSearchResults(ctx, results, nil, send)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
}
