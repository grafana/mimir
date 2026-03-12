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

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

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
		}{
			"no filter returns all names": {
				req: &storepb.SearchLabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"a", "b", "c"},
			},
			"filter matching subset": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"a"}},
				},
				expected: []string{"a"},
			},
			"filter matching multiple names": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"b", "c"}},
				},
				expected: []string{"b", "c"},
			},
			"filter matching nothing": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"zzz"}},
				},
				expected: nil,
			},
			"limit applied after filter": {
				req: &storepb.SearchLabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Limit: 1,
				},
				expected: []string{"a"},
			},
			"filter then limit": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"b", "c"}},
					Limit:        1,
				},
				expected: []string{"b"},
			},
			"outside time range": {
				req: &storepb.SearchLabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"case insensitive filter": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"A"}, CaseInsensitive: true},
				},
				expected: []string{"a"},
			},
			"case sensitive filter does not match lowercase": {
				req: &storepb.SearchLabelNamesRequest{
					Start:        timestamp.FromTime(minTime),
					End:          timestamp.FromTime(maxTime),
					SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"A"}, CaseInsensitive: false},
				},
				expected: nil,
			},
		} {
			t.Run(name, func(t *testing.T) {
				resp, err := s.store.SearchLabelNames(ctx, tc.req)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, resp.Names)
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
		} {
			t.Run(name, func(t *testing.T) {
				resp, err := s.store.SearchLabelValues(ctx, tc.req)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, resp.Values)
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
	resp, err := s.store.SearchLabelNames(ctx, &storepb.SearchLabelNamesRequest{
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, resp.Names)

	// Filter applied across all blocks.
	resp, err = s.store.SearchLabelNames(ctx, &storepb.SearchLabelNamesRequest{
		Start:        timestamp.FromTime(minTime),
		End:          timestamp.FromTime(maxTime),
		SearchFilter: &storepb.SearchFilter{SearchTerms: []string{"a"}},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, resp.Names)
}
