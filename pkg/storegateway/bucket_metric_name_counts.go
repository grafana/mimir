// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// MetricNameCounts implements the storegatewaypb.StoreGatewayServer interface. It sums every metric name's
// series count over the requested blocks this store holds, from their index-headers, and reports which blocks
// it counted. Summing is exact only if the blocks share no series, which the caller has to ensure.
func (s *BucketStore) MetricNameCounts(ctx context.Context, req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
	want := make(map[ulid.ULID]bool, len(req.BlockIds))
	for _, id := range req.BlockIds {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "block ID %q: %v", id, err)
		}
		want[parsed] = true
	}

	counts := map[string]int64{}
	resp := &storegatewaypb.MetricNameCountsResponse{}
	var errOut error
	s.blockSet.forEach(func(b *bucketBlock) {
		if errOut != nil || !want[b.meta.ULID] {
			return
		}
		if err := addBlockMetricNameCounts(ctx, b, counts); err != nil {
			errOut = err
			return
		}
		resp.BlockIds = append(resp.BlockIds, b.meta.ULID.String())
	})
	if errOut != nil {
		return nil, errOut
	}
	resp.Counts = make([]*storegatewaypb.MetricNameCount, 0, len(counts))
	for name, n := range counts {
		resp.Counts = append(resp.Counts, &storegatewaypb.MetricNameCount{Name: name, Count: n})
	}
	return resp, nil
}

// addBlockMetricNameCounts adds one block's per-name counts from its index-header: a raw postings list is a
// 4-byte entry count followed by 4 bytes per series.
func addBlockMetricNameCounts(ctx context.Context, b *bucketBlock, counts map[string]int64) error {
	offsets, err := b.indexHeaderReader.LabelValuesOffsets(ctx, "__name__", "", nil)
	if err != nil {
		return fmt.Errorf("block %s: %w", b.meta.ULID, err)
	}
	for _, off := range offsets {
		counts[off.LabelValue] += (off.Off.End - off.Off.Start - 4) / 4
	}
	return nil
}
