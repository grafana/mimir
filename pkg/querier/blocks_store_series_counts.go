// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/axiomhq/hyperloglog"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// CardinalityEstimateRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	GroupBy    string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for
	// none. It is split evenly across the store-gateway calls.
	MaxIndexBytes int64
	// SketchAbove has each store-gateway send an HLL sketch instead of
	// hashes for a group with more series than this, when deduplicating.
	// 0 keeps every group exact.
	SketchAbove int64
	// Snap widens a window inside one block range to that range.
	Snap bool
}

// CardinalityEstimateResult holds one count per bucket for each group.
type CardinalityEstimateResult struct {
	// Read is readIndexHeader or readFullIndex.
	Read    string
	Snapped bool
	Counts  map[string][]int64
	// Dedup is set when the blocks may share series, so the counts come
	// from a union of label-set hashes rather than a sum.
	Dedup                bool
	Blocks               []ulid.ULID
	StoreGateways        int
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
	// Estimated holds the groups counted from an HLL sketch, not exactly.
	Estimated map[string]bool
	// HashBytes and SketchBytes are the dedup payload the store-gateways sent.
	HashBytes, SketchBytes int64
}

// SeriesCounts counts the matching series with a chunk in the window, from
// the store-gateways. When every block covering the window is in one block
// range and the blocks share no series, the store-gateways' counts are
// added up. Otherwise, for example over several days, each store-gateway
// returns label-set hashes and the counts are the size of their union.
// Per-bucket counts need the first case.
func (q *BlocksStoreQueryable) SeriesCounts(ctx context.Context, tenantID string, req CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
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
			return CardinalityEstimateResult{}, err
		}
		all = append(all, blocks...)
		parts = append(parts, found{c, blocks, indexMeta})
	}

	res := CardinalityEstimateResult{Counts: map[string][]int64{}, Dedup: !inOneRangeWithoutSharedSeries(all)}
	if res.Dedup && req.Step > 0 {
		return CardinalityEstimateResult{}, errors.New("per-bucket counts need a window inside one block range whose blocks share no series")
	}
	matchers, err := storepb.PromMatchersToMatchers(req.Matchers...)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	pbMatchers := make([]*storepb.LabelMatcher, len(matchers))
	for i := range matchers {
		pbMatchers[i] = &matchers[i]
	}

	var (
		mtx      sync.Mutex
		gateways = map[string]bool{}
		hashes   = map[string]map[uint64]struct{}{}
		sketches = map[string]*hyperloglog.Sketch{}
		counted  = map[ulid.ULID]bool{}
	)
	merge := func(addr string, resp *storegatewaypb.SeriesCountsResponse) error {
		mtx.Lock()
		defer mtx.Unlock()
		for _, g := range resp.Groups {
			if res.Dedup && len(g.Sketch) > 0 {
				sk, err := hyperloglog.NewSketch(storegateway.SeriesCountsSketchPrecision, false)
				if err != nil {
					return err
				}
				if err := sk.UnmarshalBinary(g.Sketch); err != nil {
					return fmt.Errorf("store-gateway %s returned a bad sketch for %q: %w", addr, g.Value, err)
				}
				if into := sketches[g.Value]; into != nil {
					if err := into.Merge(sk); err != nil {
						return err
					}
				} else {
					sketches[g.Value] = sk
				}
				res.SketchBytes += int64(len(g.Sketch))
				continue
			}
			if res.Dedup {
				res.HashBytes += 8 * int64(len(g.Hashes))
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
		res.SeriesCounted += resp.SeriesCounted
		res.PostingsFetchedBytes += resp.PostingsFetchedBytes
		res.SeriesFetchedBytes += resp.SeriesFetchedBytes
		res.IndexBytes += resp.IndexBytes
		return nil
	}

	// Find every call first, so max_index_bytes can be split across them.
	type call struct {
		p      found
		client BlocksStoreClient
		ids    []ulid.ULID
	}
	var calls []call
	for _, p := range parts {
		if len(p.blocks) == 0 {
			continue
		}
		clients, err := p.c.stores.GetClientsFor(tenantID, p.blocks, nil)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		for client, partitions := range clients {
			gateways[client.RemoteAddress()] = true
			for _, ids := range partitions {
				calls = append(calls, call{p, client, ids})
			}
		}
	}
	perCall := splitIndexBytes(req.MaxIndexBytes, len(calls))

	for _, p := range parts {
		g, gCtx := errgroup.WithContext(grpcContextWithBucketStoreRequestMeta(ctx, tenantID, p.indexMeta))
		for _, cl := range calls {
			if cl.p.c != p.c {
				continue
			}
			g.Go(func() error {
				resp, err := cl.client.SeriesCounts(gCtx, &storegatewaypb.SeriesCountsRequest{
					BlockIds:      convertULIDsToString(cl.ids),
					Matchers:      pbMatchers,
					MinTime:       req.MinT,
					MaxTime:       req.MaxT,
					StepMs:        req.Step,
					GroupBy:       req.GroupBy,
					Hashes:        res.Dedup,
					MaxIndexBytes: perCall,
					SketchAbove:   req.SketchAbove,
				})
				if status.Code(err) == codes.ResourceExhausted {
					return fmt.Errorf("max_index_bytes %d, split into %d bytes for each of %d store-gateway calls: store-gateway %s: %s", req.MaxIndexBytes, perCall, len(calls), cl.client.RemoteAddress(), status.Convert(err).Message())
				}
				if err != nil {
					return fmt.Errorf("store-gateway %s: %w", cl.client.RemoteAddress(), err)
				}
				return merge(cl.client.RemoteAddress(), resp)
			})
		}
		if err := g.Wait(); err != nil {
			return CardinalityEstimateResult{}, err
		}
	}

	for _, b := range all {
		if !counted[b.ID] {
			return CardinalityEstimateResult{}, fmt.Errorf("block %s was not counted by the store-gateway it was sent to", b.ID)
		}
		res.Blocks = append(res.Blocks, b.ID)
	}
	// A group that any store-gateway sent as a sketch is estimated: the other
	// store-gateways' hashes for it go into the same sketch.
	for v, set := range hashes {
		if sk := sketches[v]; sk != nil {
			for h := range set {
				sk.InsertHash(h)
			}
			continue
		}
		res.Counts[v] = []int64{int64(len(set))}
	}
	for v, sk := range sketches {
		if res.Estimated == nil {
			res.Estimated = map[string]bool{}
		}
		res.Estimated[v] = true
		res.Counts[v] = []int64{int64(sk.Estimate())}
	}
	res.StoreGateways = len(gateways)
	return res, nil
}

// splitIndexBytes gives each of n calls an equal share of limit, at least one
// byte, so the request's total stays under limit. 0 stays unlimited.
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
func (q *BlocksStoreQueryable) CardinalityEstimate(ctx context.Context, tenantID string, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
	blocks, err := q.blocksFor(ctx, tenantID, req.MinT, req.MaxT)
	if err != nil {
		return CardinalityEstimateResult{}, err
	}
	snapped := false
	if req.Snap {
		lo, hi, err := blockRangeHolding(blocks, req.MinT, req.MaxT)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		snapped = lo != req.MinT || hi != req.MaxT
		req.MinT, req.MaxT = lo, hi
		if blocks, err = q.blocksFor(ctx, tenantID, lo, hi); err != nil {
			return CardinalityEstimateResult{}, err
		}
	}
	byName := req.GroupBy == "" || req.GroupBy == labels.MetricName
	if byName && len(req.Matchers) == 0 && req.Step == 0 && len(blocks) > 0 && checkMetricNameCountsBlocks(blocks, req.MinT, req.MaxT) == nil {
		mc, err := q.MetricNameCounts(ctx, tenantID, req.MinT, req.MaxT)
		if err != nil {
			return CardinalityEstimateResult{}, err
		}
		res := CardinalityEstimateResult{Read: readIndexHeader, Snapped: snapped, Counts: make(map[string][]int64, len(mc.Counts)), Blocks: mc.Blocks, StoreGateways: mc.StoreGateways}
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
