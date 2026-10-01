// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"sync"

	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"

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

// MetricNameCounts returns every metric name's series count for tenantID
// over [minT, maxT), counted by the store-gateways from the index-headers of
// the blocks they hold. Each block is sent to one store-gateway, chosen the
// same way as for a query, and the counts are added up, which is exact only
// when the blocks share no series. So the window must be exactly one block
// range, whose blocks are a single block or split-compactor shards with one
// shard count and distinct shard IDs. Counts across several block ranges
// need deduplication and are refused. Experimental: there is no retry on
// another store-gateway, no limit and no ingester side yet.
func (q *BlocksStoreQueryable) MetricNameCounts(ctx context.Context, tenantID string, minT, maxT int64) (MetricNameCountsResult, error) {
	res := MetricNameCountsResult{Counts: map[string]int64{}}
	gateways := map[string]bool{}
	for _, c := range q.compartments {
		// GetBlocks takes both ends as included.
		blocks, indexMeta, err := c.finder.GetBlocks(ctx, tenantID, minT, maxT-1)
		if err != nil {
			return MetricNameCountsResult{}, err
		}
		if err := checkMetricNameCountsBlocks(blocks, minT, maxT); err != nil {
			return MetricNameCountsResult{}, err
		}
		if len(blocks) == 0 {
			continue
		}
		clients, err := c.stores.GetClientsFor(tenantID, blocks, nil)
		if err != nil {
			return MetricNameCountsResult{}, err
		}

		reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)
		var (
			g, gCtx = errgroup.WithContext(reqCtx)
			mtx     sync.Mutex
			counted = map[ulid.ULID]bool{}
		)
		for client, partitions := range clients {
			gateways[client.RemoteAddress()] = true
			for _, ids := range partitions {
				g.Go(func() error {
					req := &storegatewaypb.MetricNameCountsRequest{BlockIds: convertULIDsToString(ids)}
					resp, err := client.MetricNameCounts(gCtx, req)
					if err != nil {
						return fmt.Errorf("store-gateway %s: %w", client.RemoteAddress(), err)
					}
					mtx.Lock()
					defer mtx.Unlock()
					for _, nc := range resp.Counts {
						res.Counts[nc.Name] += nc.Count
					}
					for _, id := range resp.BlockIds {
						parsed, err := ulid.Parse(id)
						if err != nil {
							return fmt.Errorf("store-gateway %s returned block ID %q: %w", client.RemoteAddress(), id, err)
						}
						counted[parsed] = true
					}
					return nil
				})
			}
		}
		if err := g.Wait(); err != nil {
			return MetricNameCountsResult{}, err
		}
		for _, b := range blocks {
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
			return fmt.Errorf("the window [%d, %d) spans more than one block range, and counts across block ranges need deduplication", minT, maxT)
		}
		if b.MinTime != minT || b.MaxTime != maxT {
			return fmt.Errorf("block %s covers [%d, %d), which cuts through the window [%d, %d)", b.ID, b.MinTime, b.MaxTime, minT, maxT)
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
			return fmt.Errorf("block %s has no shard ID, so the %d blocks for this range may share series", b.ID, len(blocks))
		}
		var index, of uint64
		if _, err := fmt.Sscanf(b.CompactorShardID, "%d_of_%d", &index, &of); err != nil {
			return fmt.Errorf("block %s: shard ID %q: %w", b.ID, b.CompactorShardID, err)
		}
		if count == 0 {
			count = of
		} else if of != count {
			return fmt.Errorf("blocks for this range use shard counts %d and %d", count, of)
		}
		if seen[index] {
			return fmt.Errorf("shard %s appears twice for this range", b.CompactorShardID)
		}
		seen[index] = true
	}
	return nil
}
