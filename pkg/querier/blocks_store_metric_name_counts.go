// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// MetricNameCountsResult is every metric name's series count over one block
// range, as counted by the store-gateways.
type MetricNameCountsResult struct {
	Counts map[string]int64
	Blocks []ulid.ULID
	// MissingBlocks are the blocks no store-gateway counted after every
	// retry, so the counts are a lower bound when it is not empty.
	MissingBlocks []ulid.ULID
	// MissingShards are the split-compactor shards no listed block holds,
	// so the counts are a lower bound when it is not empty.
	MissingShards []MissingShard
	Retries       int
	StoreGateways int
	// PostingsOffsetsTableBytes is the __name__ postings offsets table the
	// store-gateways walked, as they priced it for max_index_bytes.
	PostingsOffsetsTableBytes int64
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
			return nil, nil, bucketIndexLookupError(err)
		}
		parts = append(parts, estimatePart{c, blocks, indexMeta})
		all = append(all, blocks...)
	}
	return parts, all, nil
}

// bucketIndexLookupError is the error for a failed bucket index lookup. The
// loader's error can name bucket paths or carry object storage's error text,
// which is not the caller's to see, so it is hidden. The finder's own errors,
// a stale bucket index or a finder not running, name no path and keep their
// text.
func bucketIndexLookupError(err error) error {
	var tooOld bucketIndexTooOldError
	if errors.As(err, &tooOld) || errors.Is(err, errBucketIndexBlocksFinderNotRunning) {
		return fmt.Errorf("the bucket index lookup: %w", err)
	}
	return redactFailure("the bucket index lookup", err)
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
			return nil, nil, 0, bucketIndexLookupError(err)
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
// need deduplication and are refused. It reads the store-gateways only: a
// window the ingesters hold is answered by CardinalityEstimator, which asks
// them. Experimental.
func (q *BlocksStoreQueryable) MetricNameCounts(ctx context.Context, tenantID string, minT, maxT int64) (MetricNameCountsResult, error) {
	parts, all, err := q.estimateParts(ctx, tenantID, minT, maxT)
	if err != nil {
		return MetricNameCountsResult{}, err
	}
	if err := checkMetricNameCountsBlocks(all, minT, maxT); err != nil {
		return MetricNameCountsResult{}, err
	}
	// No max_index_bytes: each store-gateway applies only the tenant's limit
	// to its own call.
	return q.metricNameCounts(ctx, tenantID, parts, 0)
}

// metricNameCounts fans the index-header count out over parts, which the
// caller has checked to be one block range of series-disjoint blocks. Each
// call's stream is collected before it is added, so a call that fails
// part-way and is retried elsewhere is not counted twice. maxIndexBytes, 0
// for none, is split across the calls as for series counts.
func (q *BlocksStoreQueryable) metricNameCounts(ctx context.Context, tenantID string, parts []estimatePart, maxIndexBytes int64) (MetricNameCountsResult, error) {
	res := MetricNameCountsResult{Counts: map[string]int64{}}
	var mtx sync.Mutex
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	cov, err := q.runEstimateCalls(ctx, tenantID, parts, 1, maxIndexBytes, func(ctx context.Context, c estimateCall, perCall int64, calls int) (estimateCallResult, error) {
		addr := c.client.RemoteAddress()
		req := &storegatewaypb.MetricNameCountsRequest{BlockIds: convertULIDsToString(c.ids), MaxNames: q.estimateCfg.MaxMetricNames, MaxIndexBytes: perCall}
		stream, err := c.client.MetricNameCounts(ctx, req)
		if err != nil {
			return estimateCallResult{}, nameTableCallError(addr, err, maxIndexBytes, perCall, calls)
		}
		var (
			batches [][]*storegatewaypb.MetricNameCount
			ids     []string
			table   int64
		)
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return estimateCallResult{}, nameTableCallError(addr, err, maxIndexBytes, perCall, calls)
			}
			batches = append(batches, resp.Counts)
			ids = resp.BlockIds
			table = max(table, resp.PostingsOffsetsTableBytes)
		}
		counted, err := parseBlockIDs(addr, ids)
		if err != nil {
			return estimateCallResult{}, err
		}
		mtx.Lock()
		defer mtx.Unlock()
		for _, batch := range batches {
			for _, nc := range batch {
				if _, ok := res.Counts[nc.Name]; !ok {
					if err := chargeEstimateGroup(tracker, nc.Name); err != nil {
						return estimateCallResult{}, err
					}
				}
				res.Counts[nc.Name] += nc.Count
			}
		}
		res.PostingsOffsetsTableBytes += table
		return estimateCallResult{counted: counted, indexBytes: table}, nil
	})
	if err != nil {
		return MetricNameCountsResult{}, err
	}
	if err := res.setCoverage(parts, cov); err != nil {
		return MetricNameCountsResult{}, err
	}
	return res, nil
}

// nameTableCallError classifies a name-table call's error, saying how
// max_index_bytes was split when a store-gateway refused on it. A refusal on
// max_names is left as it is.
func nameTableCallError(addr string, err error, maxIndexBytes, perCall int64, calls int) error {
	onIndexBytes := strings.Contains(err.Error(), "max_index_bytes")
	err = classifyStoreGatewayError(addr, err)
	if onIndexBytes && estimateHTTPStatus(err) == http.StatusUnprocessableEntity && maxIndexBytes > 0 {
		return refusedByLimit(refusedByMaxIndexBytes, "max_index_bytes %d, split into %d bytes for each of %d store-gateway calls: %w", maxIndexBytes, perCall, calls, err)
	}
	return err
}

// setCoverage records which of the blocks in parts were counted. When no
// block was counted at all there is nothing to bound, and the request fails.
func (r *MetricNameCountsResult) setCoverage(parts []estimatePart, cov estimateCoverage) error {
	var all bucketindex.Blocks
	for _, p := range parts {
		all = append(all, p.blocks...)
	}
	missing := make(map[ulid.ULID]bool, len(cov.Missing))
	for _, id := range cov.Missing {
		missing[id] = true
	}
	if len(all) > 0 && len(missing) == len(all) {
		return errNothingCounted(len(all), cov)
	}
	for _, b := range all {
		if !missing[b.ID] {
			r.Blocks = append(r.Blocks, b.ID)
		}
	}
	r.MissingBlocks = cov.Missing
	r.MissingShards = missingShards(all)
	r.StoreGateways = cov.StoreGateways
	r.Retries = cov.Retries
	return nil
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

// MissingShard is a split-compactor shard that no listed block holds for
// the block range [MinTime, MaxTime), such as a shard block that was lost
// or deleted. Its series are in no block that was counted. The compactor
// also uploads no block for a shard with no series, which only happens to
// a range with very few series; the querier can't tell the two apart, so
// either makes the counts a lower bound.
type MissingShard struct {
	Shard            string
	MinTime, MaxTime int64
}

// maxCheckedShardCount is the largest shard count missingShards checks, so a
// corrupt shard ID can't make it list billions of shards.
const maxCheckedShardCount = 1 << 16

// missingShards returns the shards that no block holds, for each block
// range whose blocks all carry a shard ID of one shard count and that no
// block of another range overlaps. Other ranges are left out: their blocks
// may share series and are deduplicated, so a shard missing from one set of
// blocks may be held by another. That is the case while a range is being
// compacted, since each shard's merge finishes on its own and its sources
// stay listed until they are deleted.
func missingShards(blocks bucketindex.Blocks) []MissingShard {
	type blockRange struct{ minT, maxT int64 }
	var ranges []blockRange
	byRange := map[blockRange]bucketindex.Blocks{}
	for _, b := range blocks {
		r := blockRange{b.MinTime, b.MaxTime}
		if _, ok := byRange[r]; !ok {
			ranges = append(ranges, r)
		}
		byRange[r] = append(byRange[r], b)
	}
	var out []MissingShard
	for _, r := range ranges {
		overlapped := slices.ContainsFunc(blocks, func(b *bucketindex.Block) bool {
			return (b.MinTime != r.minT || b.MaxTime != r.maxT) && b.MinTime < r.maxT && b.MaxTime > r.minT
		})
		if overlapped {
			continue
		}
		seen := map[uint64]bool{}
		var count uint64
		for _, b := range byRange[r] {
			index, of, err := sharding.ParseShardIDLabelValue(b.CompactorShardID)
			if err != nil || (count != 0 && of != count) || of > maxCheckedShardCount {
				count = 0
				break
			}
			count = of
			seen[index] = true
		}
		for i := uint64(0); i < count; i++ {
			if !seen[i] {
				out = append(out, MissingShard{Shard: sharding.FormatShardIDLabelValue(i, count), MinTime: r.minT, MaxTime: r.maxT})
			}
		}
	}
	return out
}
