// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// SampleQueryCost samples chunk data only after applying server-side budgets.
func (s *BucketStore) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (res *cost.SamplingResponse, err error) {
	matchers, err := req.Validate()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer func() { err = mapSeriesError(err) }()
	stats := newSafeQueryStats()
	defer s.recordSeriesCallResult(grpcRoute(ctx), stats)
	defer s.recordRequestAmbientTime(stats, time.Now())
	done, err := s.limitConcurrentQueries(ctx, stats)
	if err != nil {
		return nil, err
	}
	defer done()

	res = &cost.SamplingResponse{}
	assigned := make(map[string]struct{}, len(req.BlockIds))
	for _, id := range req.BlockIds {
		assigned[id] = struct{}{}
	}
	type sampledBlock struct {
		block *bucketBlock
		index *bucketIndexReader
	}
	selected := make([]sampledBlock, 0, cost.MaxBlocks+1)
	// Pin selected blocks while holding the block-set lock. Keep at most MaxBlocks
	// readers, preferring recent data within this selector's own time window.
	s.blockSet.filter(req.Start, req.End, nil, func(b *bucketBlock) {
		id := b.meta.ULID.String()
		if _, ok := assigned[id]; len(assigned) > 0 && !ok {
			return
		}
		res.AvailableBlocks = append(res.AvailableBlocks, id)
		selected = append(selected, sampledBlock{b, b.indexReader(s.postingsStrategy)})
		slices.SortFunc(selected, func(a, b sampledBlock) int {
			return cmp.Or(cmp.Compare(b.block.meta.MaxTime, a.block.meta.MaxTime), b.block.meta.ULID.Compare(a.block.meta.ULID))
		})
		if len(selected) > cost.MaxBlocks {
			err = errors.Join(err, selected[cost.MaxBlocks].index.Close())
			selected = selected[:cost.MaxBlocks]
		}
	})
	defer func() {
		for _, b := range selected {
			err = errors.Join(err, b.index.Close())
		}
	}()
	if err != nil {
		return nil, err
	}
	for _, b := range selected {
		if !res.HasBudget() || res.SeriesExamined >= cost.MaxSeries {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.block.ensureIndexHeaderLoaded(ctx, stats)
		postings, pendingMatchers, err := b.index.ExpandedPostings(ctx, matchers, stats)
		if err != nil {
			return nil, err
		}
		// Cap candidate series before loading index records or applying lazy matchers.
		// A selector with no matches in this bounded prefix falls back to configured estimates.
		postings = postings[:min(len(postings), cost.MaxSeries-int(res.SeriesExamined))]
		res.SeriesExamined += uint64(len(postings))
		it := openBlockSeriesChunkRefsSetsIteratorFromPostings(ctx, s.userID, b.index, s.indexCache, b.block.meta, nil, cachedSeriesHasher{}, overlapMintMaxt, req.Start, req.End, stats, newPostingsSetsIterator(postings, cost.MaxSeries), pendingMatchers, s.logger)
		for res.HasBudget() && it.Next() {
			set := it.At()
			stop, sampleErr := sampleCostChunkRefs(ctx, b.block, set, req, res, stats)
			set.release()
			if sampleErr != nil {
				return nil, sampleErr
			}
			if stop {
				return res, nil
			}
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	return res, ctx.Err()
}

func sampleCostChunkRefs(ctx context.Context, b *bucketBlock, set seriesChunkRefsSet, req *cost.SamplingRequest, res *cost.SamplingResponse, stats *safeQueryStats) (bool, error) {
	for _, series := range set.series {
		for _, ref := range series.refs {
			if !res.HasBudget() {
				return true, nil
			}
			chk, err := readCostSampleChunk(ctx, b, ref, cost.MaxChunkBytes-res.ChunkBytes, stats)
			if err != nil {
				return false, err
			}
			if chk == nil {
				return true, nil
			}
			ok, err := res.Observe(ctx, chunks.Meta{MinTime: ref.minTime, MaxTime: ref.maxTime, Chunk: chk}, req)
			if err != nil || !ok {
				return true, err
			}
		}
	}
	return false, nil
}

// readCostSampleChunk checks the encoded size before fetching the payload.
// It requests only the length prefix and the admitted chunk, without prefetching.
func readCostSampleChunk(ctx context.Context, b *bucketBlock, ref seriesChunkRef, remaining uint64, stats *safeQueryStats) (chunkenc.Chunk, error) {
	header, err := readCostSampleRange(ctx, b, int(ref.segmentFile), int64(ref.segFileOffset), binary.MaxVarintLen32)
	if err != nil {
		return nil, err
	}
	size, n := binary.Uvarint(header)
	if n <= 0 || size < 2 {
		return nil, fmt.Errorf("invalid sampling chunk length")
	}
	if size > remaining {
		return nil, nil
	}
	data, err := readCostSampleRange(ctx, b, int(ref.segmentFile), int64(ref.segFileOffset)+int64(n), int64(size)+1+crc32.Size)
	if err != nil {
		return nil, err
	}
	var checked storepb.AggrChunk
	if err := populateChecksummedChunk(&checked, data); err != nil {
		return nil, err
	}
	stats.update(func(s *queryStats) {
		s.chunksFetched++
		s.chunksFetchedSizeSum += len(header) + len(data)
	})
	return chunkenc.FromData(chunkenc.Encoding(data[0]), data[1:len(data)-crc32.Size])
}

func readCostSampleRange(ctx context.Context, b *bucketBlock, segment int, offset, length int64) (data []byte, err error) {
	reader, err := b.chunkRangeReader(ctx, segment, offset, length)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	data = make([]byte, int(length))
	_, err = io.ReadFull(reader, data)
	return data, err
}

// SampleQueryCost routes a sampling request to the authenticated tenant's store.
func (u *BucketStores) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	userID := getUserIDFromGRPCContext(ctx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}
	store := u.getStore(userID)
	if store == nil {
		return &cost.SamplingResponse{}, nil
	}
	return store.SampleQueryCost(ctx, req)
}

// SampleQueryCost implements the bounded storage sampling RPC.
func (g *StoreGateway) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	ix := g.tracker.Insert(func() string { return requestActivity(ctx, "StoreGateway/SampleQueryCost", req) })
	defer g.tracker.Delete(ix)
	return g.stores.SampleQueryCost(ctx, req)
}
