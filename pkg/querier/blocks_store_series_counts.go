// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// SeriesCountsRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type SeriesCountsRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	GroupBy    string
	// MaxSeries is applied by each store-gateway on its own, so the total
	// can reach MaxSeries times the number of store-gateways.
	MaxSeries int64
	// Snap widens a window inside one block range to that range.
	Snap bool
}

// SeriesCountsResult holds one count per bucket for each group.
type SeriesCountsResult struct {
	// Read is readIndexHeader or readFullIndex.
	Read    string
	Snapped bool
	Counts  map[string][]int64
	// Dedup is set when the blocks may share series, so the counts come
	// from a union of label-set hashes rather than a sum.
	Dedup                bool
	Blocks               []ulid.ULID
	StoreGateways        int
	LowerBound           bool
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
}

// SeriesCounts counts the matching series with a chunk in the window, from
// the store-gateways. When every block covering the window is in one block
// range and the blocks share no series, the store-gateways' counts are
// added up. Otherwise, for example over several days, each store-gateway
// returns label-set hashes and the counts are the size of their union.
// Per-bucket counts need the first case.
func (q *BlocksStoreQueryable) SeriesCounts(ctx context.Context, tenantID string, req SeriesCountsRequest) (SeriesCountsResult, error) {
	type found struct {
		c         blocksStoreCompartment
		blocks    bucketindex.Blocks
		indexMeta *bucketindex.Metadata
	}
	var (
		all   bucketindex.Blocks
		parts []found
	)
	for _, c := range q.compartments {
		blocks, indexMeta, err := c.finder.GetBlocks(ctx, tenantID, req.MinT, req.MaxT-1)
		if err != nil {
			return SeriesCountsResult{}, err
		}
		all = append(all, blocks...)
		parts = append(parts, found{c, blocks, indexMeta})
	}

	res := SeriesCountsResult{Counts: map[string][]int64{}, Dedup: !inOneRangeWithoutSharedSeries(all)}
	if res.Dedup && req.Step > 0 {
		return SeriesCountsResult{}, errors.New("per-bucket counts need a window inside one block range whose blocks share no series")
	}
	matchers, err := storepb.PromMatchersToMatchers(req.Matchers...)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	pbMatchers := make([]*storepb.LabelMatcher, len(matchers))
	for i := range matchers {
		pbMatchers[i] = &matchers[i]
	}

	var (
		mtx      sync.Mutex
		gateways = map[string]bool{}
		hashes   = map[string]map[uint64]struct{}{}
		counted  = map[ulid.ULID]bool{}
	)
	merge := func(addr string, resp *storegatewaypb.SeriesCountsResponse) error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, g := range resp.Groups {
			if res.Dedup {
				set := hashes[g.Value]
				if set == nil {
					set = map[uint64]struct{}{}
					hashes[g.Value] = set
				}
				for _, h := range g.Hashes {
					set[h] = struct{}{}
				}
				continue
			}
			sum := res.Counts[g.Value]
			if sum == nil {
				sum = make([]int64, len(g.Counts))
				res.Counts[g.Value] = sum
			}
			if len(sum) != len(g.Counts) {
				return fmt.Errorf("store-gateway %s returned %d buckets for %q, expected %d", addr, len(g.Counts), g.Value, len(sum))
			}
			for i, n := range g.Counts {
				sum[i] += n
			}
		}
		for _, id := range resp.BlockIds {
			parsed, err := ulid.Parse(id)
			if err != nil {
				return fmt.Errorf("store-gateway %s returned block ID %q: %w", addr, id, err)
			}
			counted[parsed] = true
		}
		res.LowerBound = res.LowerBound || resp.LowerBound
		res.SeriesCounted += resp.SeriesCounted
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		res.IndexBytes += resp.IndexBytes
		return nil
	}

	for _, p := range parts {
		if len(p.blocks) == 0 {
			continue
		}
		clients, err := p.c.stores.GetClientsFor(tenantID, p.blocks, nil)
		if err != nil {
			return SeriesCountsResult{}, err
		}
		g, gCtx := errgroup.WithContext(grpcContextWithBucketStoreRequestMeta(ctx, tenantID, p.indexMeta))
		for client, partitions := range clients {
			gateways[client.RemoteAddress()] = true
			for _, ids := range partitions {
				g.Go(func() error {
					resp, err := client.SeriesCounts(gCtx, &storegatewaypb.SeriesCountsRequest{
						BlockIds:  convertULIDsToString(ids),
						Matchers:  pbMatchers,
						MinTime:   req.MinT,
						MaxTime:   req.MaxT,
						StepMs:    req.Step,
						GroupBy:   req.GroupBy,
						Hashes:    res.Dedup,
						MaxSeries: req.MaxSeries,
					})
					if err != nil {
						return fmt.Errorf("store-gateway %s: %w", client.RemoteAddress(), err)
					}
					return merge(client.RemoteAddress(), resp)
				})
			}
		}
		if err := g.Wait(); err != nil {
			return SeriesCountsResult{}, err
		}
	}

	for _, b := range all {
		// A store-gateway that stopped at max_series leaves blocks unfinished.
		if !counted[b.ID] && !res.LowerBound {
			return SeriesCountsResult{}, fmt.Errorf("block %s was not counted by the store-gateway it was sent to", b.ID)
		}
		res.Blocks = append(res.Blocks, b.ID)
	}
	for v, set := range hashes {
		res.Counts[v] = []int64{int64(len(set))}
	}
	res.StoreGateways = len(gateways)
	return res, nil
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

// Reads a cardinality estimate can use, as reported in the response and in
// the duration metric's path label.
const (
	readIndexHeader = "index_header"
	readFullIndex   = "full_index"
	readChunks      = "chunks" // the compare=true reference only
)

// CardinalityEstimate answers req with the cheapest read. With Snap, a
// window inside one block range is first widened to that range, and req is
// updated to say so. A window of exactly one block range whose blocks share
// no series, asked for every metric name with no matchers and no step, is
// counted from the index-headers. Anything else reads the full index.
func (q *BlocksStoreQueryable) CardinalityEstimate(ctx context.Context, tenantID string, req *SeriesCountsRequest) (SeriesCountsResult, error) {
	blocks, err := q.blocksFor(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	snapped := false
	if req.Snap {
		lo, hi, err := blockRangeHolding(blocks, req.MinT, req.MaxT)
		if err != nil {
			return SeriesCountsResult{}, err
		}
		snapped = lo != req.MinT || hi != req.MaxT
		req.MinT, req.MaxT = lo, hi
		if blocks, err = q.blocksFor(ctx, tenantID, lo, hi); err != nil {
			return SeriesCountsResult{}, err
		}
	}
	byName := req.GroupBy == "" || req.GroupBy == labels.MetricName
	if byName && len(req.Matchers) == 0 && req.Step == 0 && len(blocks) > 0 && checkMetricNameCountsBlocks(blocks, req.MinT, req.MaxT) == nil {
		mc, err := q.MetricNameCounts(ctx, tenantID, req.MinT, req.MaxT)
		if err != nil {
			return SeriesCountsResult{}, err
		}
		res := SeriesCountsResult{Read: readIndexHeader, Snapped: snapped, Counts: make(map[string][]int64, len(mc.Counts)), Blocks: mc.Blocks, StoreGateways: mc.StoreGateways}
		for name, n := range mc.Counts {
			res.Counts[name] = []int64{n}
		}
		return res, nil
	}
	res, err := q.SeriesCounts(ctx, tenantID, *req)
	res.Read, res.Snapped = readFullIndex, snapped
	return res, err
}

// blocksFor returns every compartment's blocks for [minT, maxT).
func (q *BlocksStoreQueryable) blocksFor(ctx context.Context, tenantID string, minT, maxT int64) (bucketindex.Blocks, error) {
	var all bucketindex.Blocks
	for _, c := range q.compartments {
		blocks, _, err := c.finder.GetBlocks(ctx, tenantID, minT, maxT-1)
		if err != nil {
			return nil, err
		}
		all = append(all, blocks...)
	}
	return all, nil
}

func blockRangeHolding(blocks bucketindex.Blocks, minT, maxT int64) (int64, int64, error) {
	if len(blocks) == 0 {
		return minT, maxT, nil
	}
	lo, hi := blocks[0].MinTime, blocks[0].MaxTime
	for _, b := range blocks {
		if b.MinTime != lo || b.MaxTime != hi {
			return 0, 0, fmt.Errorf("the window [%d, %d) touches more than one block range, so it can't be snapped to one", minT, maxT)
		}
	}
	if lo > minT || hi < maxT {
		return 0, 0, fmt.Errorf("the blocks cover [%d, %d), which doesn't hold the whole window [%d, %d)", lo, hi, minT, maxT)
	}
	return lo, hi, nil
}
