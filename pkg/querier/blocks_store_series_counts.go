// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/oklog/ulid/v2"
)

// seriesCountsInWindow counts the matching series with a chunk in the window, from the store-gateways. In one block range of series-disjoint blocks their counts are added up, otherwise each returns label-set hashes and the counts are the size of their union. Per-bucket counts need the first case.
func (q *BlocksStoreQueryable) seriesCountsInWindow(ctx context.Context, tenantID string, req CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	parts, all, err := q.seriesCountsParts(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	return q.seriesCounts(ctx, tenantID, req, parts, all)
}

// seriesCounts answers req from the blocks in parts (all is the same blocks flattened).
func (q *BlocksStoreQueryable) seriesCounts(ctx context.Context, tenantID string, req CardinalityEstimateRequest, parts []estimatePart, all bucketindex.Blocks) (CardinalityEstimateResult, error) {
	dedup := !inOneRangeWithoutSharedSeries(all)
	m := newEstimateMerger(ctx, dedup)
	res, err := q.seriesCountsInto(ctx, tenantID, req, parts, all, dedup, m)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if err := m.finish(&res); err != nil {
		return CardinalityEstimateResult{}, err
	}
	return res, nil
}

// seriesCountsInto sends req to the store-gateways for the blocks in parts and adds every group they return to m. The result carries everything but the counts, which m.finish writes.
func (q *BlocksStoreQueryable) seriesCountsInto(ctx context.Context, tenantID string, req CardinalityEstimateRequest, parts []estimatePart, all bucketindex.Blocks, dedup bool, m *estimateMerger) (CardinalityEstimateResult, error) {
	res := CardinalityEstimateResult{Counts: map[string][]int64{}, Dedup: dedup}
	buckets, err := req.expectedBuckets()
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	m.buckets = buckets
	if res.Dedup && req.Step > 0 {
		return CardinalityEstimateResult{}, badEstimateRequest("%w: the window spans more than one block range, or blocks that may share series", errStepNeedsOneSource)
	}
	pbMatchers := cardinalitypb.ToLabelMatchers(req.Matchers)
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	var mtx sync.Mutex
	merge := func(addr string, resp *storegatewaypb.SeriesCountsResponse) error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, g := range resp.Groups {
			if err := m.add("store-gateway", g.Value, g.Counts, g.Hashes); err != nil {
				return withSourceAddr(addr, err)
			}
		}
		res.SeriesCounted += resp.SeriesCounted
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		res.IndexBytes += resp.IndexBytes
		res.PostingsOffsetsTableBytes += resp.PostingsOffsetsTableBytes
		res.BlocksFromPostings += resp.BlocksFromPostings
		res.BlocksFromOffsetsTable += resp.BlocksFromOffsetsTable
		return nil
	}

	heldBeforeAttempt := map[int]int64{}
	cov, err := q.runSeriesCountsCalls(ctx, tenantID, parts, req.MaxIndexBytes, func(ctx context.Context, c estimateCall, perCall int64, calls int) (seriesCountsCallResult, error) {
		// Each call gets a share of the querier's hashes limit, from what the hashes already merged leave of it. The held count is read once per attempt.
		var maxHashes int64
		if res.Dedup && req.MaxHashes > 0 {
			mtx.Lock()
			held, ok := heldBeforeAttempt[c.attempt]
			if !ok {
				held = m.held
				heldBeforeAttempt[c.attempt] = held
			}
			mtx.Unlock()
			left := req.MaxHashes - held
			if left <= 0 {
				return seriesCountsCallResult{}, refusedByLimit(refusedByMaxHashes, "the %d hashes the tenant's cardinality estimate memory limit allows are held before every block was counted", req.MaxHashes)
			}
			maxHashes = splitIndexBytes(left, calls)
		}
		stream, err := c.client.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{
			BlockIds:      convertULIDsToString(c.ids),
			Matchers:      pbMatchers,
			StartMs:       req.MinT,
			EndMs:         req.MaxT,
			StepMs:        req.Step,
			WindowMs:      req.windowMs(),
			GroupBy:       req.GroupBy,
			Hashes:        res.Dedup,
			MaxIndexBytes: perCall,
			MaxHashes:     maxHashes,
			MaxGroups:     req.MaxGroups,
		})
		// The whole answer is read before any of it is merged, so a retried call is not counted twice. What is held is charged message by message and given back once merged or on failure.
		var (
			resp    *storegatewaypb.SeriesCountsResponse
			release func()
		)
		if err == nil {
			resp, release, err = receiveSeriesCounts(stream, tracker)
		}
		if err != nil {
			// What this querier refuses while it reads the answer is not the store-gateway's failure.
			if own := (estimateError{}); errors.As(err, &own) {
				return seriesCountsCallResult{}, err
			}
			err = classifyStoreGatewayError(c.client.RemoteAddress(), err)
			switch estimateRefusalLimit(err) {
			case refusedByMaxIndexBytes:
				if req.MaxIndexBytes > 0 {
					return seriesCountsCallResult{}, refusedByLimit(refusedByMaxIndexBytes, "max_index_bytes %d, split into %d bytes for each of %d store-gateway calls: %w", req.MaxIndexBytes, perCall, calls, err)
				}
			case refusedByMaxHashes:
				return seriesCountsCallResult{}, refusedByLimit(refusedByMaxHashes, "the %d hashes the tenant's cardinality estimate memory limit allows, split into %d for each of %d store-gateway calls: %w", req.MaxHashes, maxHashes, calls, err)
			}
			return seriesCountsCallResult{}, err
		}
		counted, err := parseBlockIDs(c.client.RemoteAddress(), resp.BlockIds)
		if err != nil {
			return seriesCountsCallResult{}, err
		}
		err = merge(c.client.RemoteAddress(), resp)
		release()
		if err != nil {
			return seriesCountsCallResult{}, err
		}
		// max_index_bytes bounds the postings offsets table walked as well as index_bytes.
		return seriesCountsCallResult{counted: counted, indexBytes: resp.IndexBytes + resp.PostingsOffsetsTableBytes}, nil
	})
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	if err := res.setCoverage(all, cov); err != nil {
		return CardinalityEstimateResult{}, err
	}

	return res, nil
}

// receiveSeriesCounts reads every message of a SeriesCounts stream into one response, charging each message's groups to tracker (which may be nil) as they arrive. On success the caller gives the charge back with release once the groups are merged; on an error nothing stays charged.
func receiveSeriesCounts(s storegatewaypb.StoreGateway_SeriesCountsClient, tracker *limiter.MemoryConsumptionTracker) (*storegatewaypb.SeriesCountsResponse, func(), error) {
	var charged uint64
	release := func() {
		if tracker != nil && charged > 0 {
			tracker.DecreaseMemoryConsumption(charged, limiter.CardinalityEstimateGroups)
		}
		charged = 0
	}
	out, err := cardinalitypb.ReceiveGroups(s.Recv, func(msg *storegatewaypb.SeriesCountsResponse) error {
		if tracker == nil {
			return nil
		}
		var b uint64
		for _, g := range msg.Groups {
			b += uint64(estimateGroupBytes + len(g.Value) + 8*len(g.Counts) + 8*len(g.Hashes))
		}
		if err := tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
			return refusedByLimit(refusedByMemory, "the store-gateway replies this estimate holds in the querier: %w", err)
		}
		charged += b
		return nil
	})
	if err != nil {
		release()
		return nil, nil, err
	}
	return out, release, nil
}

// parseBlockIDs parses the block IDs a store-gateway reported as counted.
func parseBlockIDs(addr string, ids []string) ([]ulid.ULID, error) {
	out := make([]ulid.ULID, 0, len(ids))
	for _, id := range ids {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return nil, withSourceAddr(addr, fmt.Errorf("a store-gateway returned block ID %q: %w", id, err))
		}
		out = append(out, parsed)
	}
	return out, nil
}

// splitIndexBytes gives each of n calls an equal share of limit, at least one byte. 0 stays unlimited.
func splitIndexBytes(limit int64, n int) int64 {
	if limit <= 0 || n <= 1 {
		return limit
	}
	return max(1, limit/int64(n))
}

// inOneRangeWithoutSharedSeries reports whether every block covers the same
// range and the blocks are a single block or disjoint shards.
func inOneRangeWithoutSharedSeries(blocks bucketindex.Blocks) bool {
	for _, b := range blocks {
		if b.MinTime != blocks[0].MinTime || b.MaxTime != blocks[0].MaxTime {
			return false
		}
	}
	return checkDisjointShards(blocks) == nil
}

// cardinalityEstimate answers req from the store-gateways.
func (q *BlocksStoreQueryable) cardinalityEstimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	parts, blocks, err := q.seriesCountsBlocks(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	res, err := q.seriesCounts(ctx, tenantID, *req, parts, blocks)
	res.Read = estimateRead(res)
	return res, err
}

// estimateRead is the read the store-gateways made for res.
func estimateRead(res CardinalityEstimateResult) string {
	if len(res.Blocks) > 0 && res.BlocksFromOffsetsTable == int64(len(res.Blocks)) {
		return ReadIndexHeader
	}
	return ReadFullIndex
}

// seriesCountsBlocks finds the blocks for [minT, maxT). Blocks marked for deletion are dropped when that makes them one block range of series-disjoint blocks, since right after a compaction the finder lists both the sources and their output. While the store-gateways may not have loaded the output yet, the sources are kept and both are deduplicated.
func (q *BlocksStoreQueryable) seriesCountsBlocks(ctx context.Context, tenantID string, minT, maxT int64) ([]estimatePart, bucketindex.Blocks, error) {
	parts, blocks, err := q.seriesCountsParts(ctx, tenantID, minT, maxT)
	if err != nil {
		return nil, nil, err
	}
	if inOneRangeWithoutSharedSeries(blocks) {
		return parts, blocks, nil
	}
	kept, keptBlocks, dropped, err := q.withoutMarkedForDeletion(ctx, tenantID, parts)
	if err != nil {
		return nil, nil, err
	}
	if dropped > 0 && len(keptBlocks) > 0 && inOneRangeWithoutSharedSeries(keptBlocks) {
		awaited := map[ulid.ULID]struct{}{}
		recent := false
		for _, b := range keptBlocks {
			if !q.consistency.uploadedWithinGrace(b) {
				continue
			}
			recent = true
			// Only a compactor's output is sure to have its series in the sources.
			if b.CompactionLevel >= 2 {
				awaited[b.ID] = struct{}{}
			}
		}
		if !recent {
			return kept, keptBlocks, nil
		}
		for i := range parts {
			parts[i].awaited = awaited
		}
	}
	return parts, blocks, nil
}
