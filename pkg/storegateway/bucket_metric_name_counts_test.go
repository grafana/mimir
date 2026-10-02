// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestBucketStore_MetricNameCounts(t *testing.T) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	cfg := defaultPrepareStoreConfig(t)
	// Non-overlapping blocks: the first block range holds only the first
	// block, with series[:4].
	cfg.nonOverlappingBlocks = true
	cfg.numBlocks = 2
	cfg.series = []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "b"),
		labels.FromStrings("__name__", "up", "job", "c"),
		labels.FromStrings("__name__", "requests_total", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "d"),
		labels.FromStrings("__name__", "errors_total", "job", "a"),
		labels.FromStrings("__name__", "errors_total", "job", "b"),
		labels.FromStrings("__name__", "errors_total", "job", "c"),
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)

	var first string
	s.store.blockSet.forEach(func(b *bucketBlock) {
		if b.meta.MinTime == s.minTime {
			first = b.meta.ULID.String()
		}
	})
	require.NotEmpty(t, first)

	// It counts exactly the requested blocks it holds, and says which.
	unknown := ulid.MustNew(1, nil)
	res, err := s.store.MetricNameCounts(t.Context(), &storegatewaypb.MetricNameCountsRequest{BlockIds: []string{first, unknown.String()}})
	require.NoError(t, err)
	assert.Equal(t, []string{first}, res.BlockIds)
	counts := map[string]int64{}
	for _, c := range res.Counts {
		counts[c.Name] = c.Count
	}
	assert.Equal(t, map[string]int64{"up": 3, "requests_total": 1}, counts)

	_, err = s.store.MetricNameCounts(t.Context(), &storegatewaypb.MetricNameCountsRequest{BlockIds: []string{"not-a-ulid"}})
	require.Error(t, err)
}
