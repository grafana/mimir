// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// metricNameCounts is every metric name's series count over one block time
// range, from the index-headers of the blocks this store holds.
type metricNameCounts struct {
	blocks []ulid.ULID
	counts map[string]int64
}

// metricNameCounts counts the series of every metric name over the blocks
// whose time range is exactly [minT, maxT), from each block's postings
// offset table in its index-header: a raw postings list is a 4-byte entry
// count followed by 4 bytes per series, so its byte span gives its length,
// and no index or chunk bytes are read from object storage.
//
// Adding the blocks' counts is exact only if no two of them hold the same
// series. It returns an error if a block's range cuts through the window, or
// if the window's blocks are not a single block or split-compactor shards
// with one shard count and distinct shard IDs.
func (s *BucketStore) metricNameCounts(ctx context.Context, minT, maxT int64) (metricNameCounts, error) {
	var (
		res    = metricNameCounts{counts: map[string]int64{}}
		metas  []*block.Meta
		errOut error
	)
	s.blockSet.forEach(func(b *bucketBlock) {
		if errOut != nil {
			return
		}
		m := b.meta
		if m.MaxTime <= minT || m.MinTime >= maxT {
			return
		}
		if m.MinTime >= minT && m.MaxTime <= maxT && (m.MinTime != minT || m.MaxTime != maxT) {
			errOut = fmt.Errorf("the window [%d, %d) spans more than one block range, and counts across block ranges need deduplication", minT, maxT)
			return
		}
		if m.MinTime != minT || m.MaxTime != maxT {
			errOut = fmt.Errorf("block %s covers [%d, %d), which cuts through the window [%d, %d)", m.ULID, m.MinTime, m.MaxTime, minT, maxT)
			return
		}
		if err := addBlockMetricNameCounts(ctx, b, res.counts); err != nil {
			errOut = err
			return
		}
		res.blocks = append(res.blocks, m.ULID)
		metas = append(metas, m)
	})
	if errOut != nil {
		return metricNameCounts{}, errOut
	}
	if err := checkSeriesDisjointShards(metas); err != nil {
		return metricNameCounts{}, err
	}
	return res, nil
}

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

// checkSeriesDisjointShards returns nil if metas is at most one block, or
// split-compactor shards with one shard count and no repeated shard ID,
// which hold disjoint series.
func checkSeriesDisjointShards(metas []*block.Meta) error {
	if len(metas) <= 1 {
		return nil
	}
	seen := map[uint64]bool{}
	var count uint64
	for _, m := range metas {
		id := m.Thanos.Labels[block.CompactorShardIDExternalLabel]
		if id == "" {
			return fmt.Errorf("block %s has no shard ID, so the %d blocks for this range may share series", m.ULID, len(metas))
		}
		var index, of uint64
		if _, err := fmt.Sscanf(id, "%d_of_%d", &index, &of); err != nil {
			return fmt.Errorf("block %s: shard ID %q: %w", m.ULID, id, err)
		}
		if count == 0 {
			count = of
		} else if of != count {
			return fmt.Errorf("blocks for this range use shard counts %d and %d", count, of)
		}
		if seen[index] {
			return fmt.Errorf("shard %s appears twice for this range", id)
		}
		seen[index] = true
	}
	return nil
}
