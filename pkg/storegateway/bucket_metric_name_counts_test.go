// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	srv := &mockMetricNameCountsServer{ctx: t.Context()}
	require.NoError(t, s.store.MetricNameCounts(&storegatewaypb.MetricNameCountsRequest{BlockIds: []string{first, unknown.String()}}, srv))
	require.NotEmpty(t, srv.sent)
	counts := map[string]int64{}
	for _, msg := range srv.sent {
		assert.Equal(t, []string{first}, msg.BlockIds, "every message names the counted blocks")
		for _, c := range msg.Counts {
			counts[c.Name] = c.Count
		}
	}
	assert.Equal(t, map[string]int64{"up": 3, "requests_total": 1}, counts)

	// max_names refuses before anything is sent.
	srv = &mockMetricNameCountsServer{ctx: t.Context()}
	err = s.store.MetricNameCounts(&storegatewaypb.MetricNameCountsRequest{BlockIds: []string{first}, MaxNames: 1}, srv)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.ErrorContains(t, err, "max_names 1")
	assert.Empty(t, srv.sent)

	// An unknown block is an empty stream that still says nothing was counted.
	srv = &mockMetricNameCountsServer{ctx: t.Context()}
	require.NoError(t, s.store.MetricNameCounts(&storegatewaypb.MetricNameCountsRequest{BlockIds: []string{unknown.String()}}, srv))
	require.Len(t, srv.sent, 1)
	assert.Empty(t, srv.sent[0].BlockIds)
	assert.Empty(t, srv.sent[0].Counts)

	err = s.store.MetricNameCounts(&storegatewaypb.MetricNameCountsRequest{BlockIds: []string{"not-a-ulid"}}, &mockMetricNameCountsServer{ctx: t.Context()})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

type mockMetricNameCountsServer struct {
	storegatewaypb.StoreGateway_MetricNameCountsServer
	ctx  context.Context
	sent []*storegatewaypb.MetricNameCountsResponse
}

func (m *mockMetricNameCountsServer) Send(r *storegatewaypb.MetricNameCountsResponse) error {
	m.sent = append(m.sent, r)
	return nil
}
func (m *mockMetricNameCountsServer) Context() context.Context { return m.ctx }
