// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

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
		caseSens    bool
		ordering    storepb.SearchOrdering
		limit       int64
		want        []string
	}{
		{
			// External labels (ext1=value1) are not stored in the block index;
			// only the series labels {a, b, c} are visible at BucketStore level.
			name:     "all label names no filter",
			caseSens: true,
			want:     []string{"a", "b", "c"},
		},
		{
			name:        "substring filter matches single name",
			filterTerms: []string{"b"},
			caseSens:    true,
			want:        []string{"b"},
		},
		{
			name:     "limit 2 returns first two in asc order",
			caseSens: true,
			limit:    2,
			want:     []string{"a", "b"},
		},
		{
			name:     "desc ordering returns names in reverse",
			caseSens: true,
			ordering: storepb.ORDER_BY_VALUE_DESC,
			want:     []string{"c", "b", "a"},
		},
		{
			name:        "no match returns empty",
			filterTerms: []string{"zzz"},
			caseSens:    true,
			want:        nil,
		},
		{
			name:        "filter matches label a only",
			filterTerms: []string{"a"},
			caseSens:    true,
			want:        []string{"a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &storepb.SearchLabelNamesRequest{
				Start: 0,
				End:   math.MaxInt64,
				Filter: &storepb.SearchFilter{
					Terms:         tc.filterTerms,
					CaseSensitive: tc.caseSens,
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
		caseSens    bool
		ordering    storepb.SearchOrdering
		limit       int64
		want        []string
	}{
		{
			name:      "all values for label a",
			labelName: "a",
			caseSens:  true,
			want:      []string{"1", "2"},
		},
		{
			name:      "limit 1 on label a returns first value",
			labelName: "a",
			caseSens:  true,
			limit:     1,
			want:      []string{"1"},
		},
		{
			name:      "desc ordering on label a",
			labelName: "a",
			caseSens:  true,
			ordering:  storepb.ORDER_BY_VALUE_DESC,
			want:      []string{"2", "1"},
		},
		{
			name:      "missing label returns empty",
			labelName: "missing",
			caseSens:  true,
			want:      nil,
		},
		{
			name:        "filter value 1 returns only 1",
			labelName:   "a",
			filterTerms: []string{"1"},
			caseSens:    true,
			want:        []string{"1"},
		},
		{
			name:      "all values for label b",
			labelName: "b",
			caseSens:  true,
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
					Terms:         tc.filterTerms,
					CaseSensitive: tc.caseSens,
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
