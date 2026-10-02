// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// MetricNameCountsResult is every metric name's series count over one block
// range, as counted by the store-gateways.
type MetricNameCountsResult struct {
	Counts        map[string]int64
	Blocks        []ulid.ULID
	StoreGateways int
}

// estimatePart is one compartment's blocks for a window.
type estimatePart struct {
	c         blocksStoreCompartment
	blocks    bucketindex.Blocks
	indexMeta *bucketindex.Metadata
}

// estimateParts finds every compartment's blocks for [minT, maxT).
func (q *BlocksStoreQueryable) estimateParts(ctx context.Context, tenantID string, minT, maxT int64) ([]estimatePart, bucketindex.Blocks, error) {
	var (
		parts []estimatePart
		all   bucketindex.Blocks
	)
	for _, c := range q.compartments {
		// GetBlocks takes both ends as included.
		blocks, indexMeta, err := c.finder.GetBlocks(ctx, tenantID, minT, maxT-1)
		if err != nil {
			return nil, nil, err
		}
		parts = append(parts, estimatePart{c, blocks, indexMeta})
		all = append(all, blocks...)
	}
	return parts, all, nil
}

// blocksMarkedForDeletionFinder is implemented by a finder that can say
// which blocks carry a deletion mark, whatever the mark's age.
type blocksMarkedForDeletionFinder interface {
	BlocksMarkedForDeletion(ctx context.Context, userID string) (map[ulid.ULID]struct{}, error)
}

// withoutMarkedForDeletion drops the blocks that are marked for deletion.
// For a while after a compaction the finder lists both the source blocks and
// their compacted output, which would make a one-range window look like
// several, so the cheap paths fall back to this view. The dropped blocks'
// series are in the blocks that replaced them.
func (q *BlocksStoreQueryable) withoutMarkedForDeletion(ctx context.Context, tenantID string, parts []estimatePart) ([]estimatePart, bucketindex.Blocks, int, error) {
	var (
		out     []estimatePart
		all     bucketindex.Blocks
		dropped int
	)
	for _, p := range parts {
		f, ok := p.c.finder.(blocksMarkedForDeletionFinder)
		if !ok {
			out = append(out, p)
			all = append(all, p.blocks...)
			continue
		}
		marked, err := f.BlocksMarkedForDeletion(ctx, tenantID)
		if err != nil {
			return nil, nil, 0, err
		}
		kept := make(bucketindex.Blocks, 0, len(p.blocks))
		for _, b := range p.blocks {
			if _, ok := marked[b.ID]; ok {
				dropped++
				continue
			}
			kept = append(kept, b)
		}
		out = append(out, estimatePart{p.c, kept, p.indexMeta})
		all = append(all, kept...)
	}
	return out, all, dropped, nil
}

// MetricNameCounts returns every metric name's series count for tenantID
// over [minT, maxT), counted by the store-gateways from the index-headers of
// the blocks they hold. Each block is sent to one store-gateway, chosen the
// same way as for a query, and the counts are added up, which is exact only
// when the blocks share no series. So the window must be exactly one block
// range, whose blocks are a single block or split-compactor shards with one
// shard count and distinct shard IDs. Counts across several block ranges
// need deduplication and are refused. Experimental: there is no retry on
// another store-gateway and no ingester side yet.
func (q *BlocksStoreQueryable) MetricNameCounts(ctx context.Context, tenantID string, minT, maxT int64) (MetricNameCountsResult, error) {
	parts, all, err := q.estimateParts(ctx, tenantID, minT, maxT)
	if err != nil {
		return MetricNameCountsResult{}, err
	}
	if err := checkMetricNameCountsBlocks(all, minT, maxT); err != nil {
		return MetricNameCountsResult{}, err
	}
	return q.metricNameCounts(ctx, tenantID, parts)
}

// metricNameCounts fans the index-header count out over parts, which the
// caller has checked to be one block range of series-disjoint blocks.
func (q *BlocksStoreQueryable) metricNameCounts(ctx context.Context, tenantID string, parts []estimatePart) (MetricNameCountsResult, error) {
	res := MetricNameCountsResult{Counts: map[string]int64{}}
	gateways := map[string]bool{}
	for _, p := range parts {
		if len(p.blocks) == 0 {
			continue
		}
		clients, err := p.c.stores.GetClientsFor(tenantID, p.blocks, nil)
		if err != nil {
			return MetricNameCountsResult{}, err
		}

		reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, p.indexMeta)
		var (
			g, gCtx = errgroup.WithContext(reqCtx)
			mtx     sync.Mutex
			counted = map[ulid.ULID]bool{}
		)
		for client, partitions := range clients {
			gateways[client.RemoteAddress()] = true
			for _, ids := range partitions {
				g.Go(func() error {
					req := &storegatewaypb.MetricNameCountsRequest{BlockIds: convertULIDsToString(ids), MaxNames: q.estimateCfg.MaxMetricNames}
					stream, err := client.MetricNameCounts(gCtx, req)
					if err != nil {
						return classifyStoreGatewayError(client.RemoteAddress(), err)
					}
					for {
						resp, err := stream.Recv()
						if errors.Is(err, io.EOF) {
							return nil
						}
						if err != nil {
							return classifyStoreGatewayError(client.RemoteAddress(), err)
						}
						mtx.Lock()
						for _, nc := range resp.Counts {
							res.Counts[nc.Name] += nc.Count
						}
						for _, id := range resp.BlockIds {
							parsed, err := ulid.Parse(id)
							if err != nil {
								mtx.Unlock()
								return fmt.Errorf("store-gateway %s returned block ID %q: %w", client.RemoteAddress(), id, err)
							}
							counted[parsed] = true
						}
						mtx.Unlock()
					}
				})
			}
		}
		if err := g.Wait(); err != nil {
			return MetricNameCountsResult{}, err
		}
		for _, b := range p.blocks {
			if !counted[b.ID] {
				return MetricNameCountsResult{}, fmt.Errorf("block %s was not counted by the store-gateway it was sent to", b.ID)
			}
			res.Blocks = append(res.Blocks, b.ID)
		}
	}
	res.StoreGateways = len(gateways)
	return res, nil
}

// checkMetricNameCountsBlocks returns nil if every block covers exactly
// [minT, maxT) and the blocks are a single block or split-compactor shards
// with one shard count and distinct shard IDs.
func checkMetricNameCountsBlocks(blocks bucketindex.Blocks, minT, maxT int64) error {
	for _, b := range blocks {
		if b.MinTime >= minT && b.MaxTime <= maxT && (b.MinTime != minT || b.MaxTime != maxT) {
			return badEstimateRequest("the window [%d, %d) spans more than one block range, and counts across block ranges need deduplication", minT, maxT)
		}
		if b.MinTime != minT || b.MaxTime != maxT {
			return badEstimateRequest("block %s covers [%d, %d), which cuts through the window [%d, %d)", b.ID, b.MinTime, b.MaxTime, minT, maxT)
		}
	}
	return checkDisjointShards(blocks)
}

// checkDisjointShards returns nil if blocks is at most one block, or
// split-compactor shards with one shard count and distinct shard IDs.
func checkDisjointShards(blocks bucketindex.Blocks) error {
	if len(blocks) <= 1 {
		return nil
	}
	seen := map[uint64]bool{}
	var count uint64
	for _, b := range blocks {
		if b.CompactorShardID == "" {
			return badEstimateRequest("block %s has no shard ID, so the %d blocks for this range may share series", b.ID, len(blocks))
		}
		index, of, err := sharding.ParseShardIDLabelValue(b.CompactorShardID)
		if err != nil {
			return badEstimateRequest("block %s: %v", b.ID, err)
		}
		if count == 0 {
			count = of
		} else if of != count {
			return badEstimateRequest("blocks for this range use shard counts %d and %d", count, of)
		}
		if seen[index] {
			return badEstimateRequest("shard %s appears twice for this range", b.CompactorShardID)
		}
		seen[index] = true
	}
	return nil
}
