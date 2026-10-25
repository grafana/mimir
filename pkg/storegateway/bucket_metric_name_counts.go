// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// metricNameCountsBatch is how many names go into one streamed message. At
// about 50 bytes a name that is about 500 KB per message, well under the
// gRPC message size limits on both sides.
const metricNameCountsBatch = 10_000

// MetricNameCounts implements the storegatewaypb.StoreGatewayServer interface. It sums every metric name's
// series count over the requested blocks this store holds, from their index-headers, and streams the table in
// batches, each message naming the blocks that were counted. Summing is exact only if the blocks share no
// series, which the caller has to ensure. With max_names set, more distinct names than that is refused with
// ResourceExhausted before anything is sent: before any postings offset table is walked when the sampled
// offsets the index-header holds in memory already show one block over it, and after the walk otherwise. The
// tenant's max_index_bytes limit bounds the postings offsets table bytes walked, which index-header version 2
// reads from object storage, priced from the same sampled offsets before any walk.
func (s *BucketStore) MetricNameCounts(req *storegatewaypb.MetricNameCountsRequest, srv storegatewaypb.StoreGateway_MetricNameCountsServer) error {
	ctx := srv.Context()
	want := make(map[ulid.ULID]bool, len(req.BlockIds))
	for _, id := range req.BlockIds {
		parsed, err := ulid.Parse(id)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "block ID %q: %v", id, err)
		}
		want[parsed] = true
	}
	if req.MaxNames < 0 {
		return status.Error(codes.InvalidArgument, "max_names can't be negative")
	}
	done, err := s.startCount(ctx)
	if err != nil {
		return err
	}
	defer done()

	// Take an index reader for each block inside the lock, which keeps the block open, and do the
	// index-header walk outside it so block removal is not held up by a long header read.
	type openBlock struct {
		b      *bucketBlock
		indexr *bucketIndexReader
	}
	var blocks []openBlock
	s.blockSet.forEach(func(b *bucketBlock) {
		if want[b.meta.ULID] {
			blocks = append(blocks, openBlock{b: b, indexr: b.indexReader(s.postingsStrategy)})
		}
	})
	defer func() {
		for _, o := range blocks {
			runutil.CloseWithLogOnErr(o.b.logger, o.indexr, "close block index reader")
		}
	}()
	slices.SortFunc(blocks, func(x, y openBlock) int { return x.b.meta.ULID.Compare(y.b.meta.ULID) })

	// Refuse before walking any postings offset table when one block alone is known to hold more names than
	// max_names: the distinct names over the blocks are at least as many as any one block holds.
	if req.MaxNames > 0 {
		for _, o := range blocks {
			n, err := o.b.indexHeaderReader.LabelValuesCountLowerBound(ctx, labels.MetricName)
			if err != nil {
				return status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
			}
			if int64(n) > req.MaxNames {
				return status.Errorf(codes.ResourceExhausted, "block %s holds at least %d distinct metric names, more than max_names %d", o.b.meta.ULID, n, req.MaxNames)
			}
		}
	}

	if s.cardinalityEstimateMaxIndexBytes != nil {
		if limit := s.cardinalityEstimateMaxIndexBytes(); limit > 0 {
			var table int64
			for _, o := range blocks {
				n, err := o.b.indexHeaderReader.LabelValuesOffsetsTableBytes(ctx, labels.MetricName)
				if err != nil {
					return status.Errorf(codes.Internal, "block %s: %v", o.b.meta.ULID, err)
				}
				table += n
			}
			if table > limit {
				return status.Errorf(codes.ResourceExhausted, "the request would read about %d bytes of the %s postings offsets table over %d blocks, more than max_index_bytes %d", table, labels.MetricName, len(blocks), limit)
			}
		}
	}

	counts := map[string]int64{}
	blockIDs := make([]string, 0, len(blocks))
	var mtx sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.countBlockConcurrency)
	for _, o := range blocks {
		g.Go(func() error {
			// Each block is read into its own map, then added under the lock.
			own := map[string]int64{}
			if err := addBlockMetricNameCounts(gctx, o.b, own); err != nil {
				if _, ok := status.FromError(err); ok {
					return err
				}
				return status.Error(codes.Internal, err.Error())
			}
			mtx.Lock()
			for name, n := range own {
				counts[name] += n
			}
			mtx.Unlock()
			return nil
		})
		blockIDs = append(blockIDs, o.b.meta.ULID.String())
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if req.MaxNames > 0 && int64(len(counts)) > req.MaxNames {
		return status.Errorf(codes.ResourceExhausted, "the blocks hold %d distinct metric names, more than max_names %d", len(counts), req.MaxNames)
	}

	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	slices.Sort(names)
	// Always send at least one message, so the caller learns which blocks were counted even when they are empty.
	for start := 0; start == 0 || start < len(names); start += metricNameCountsBatch {
		end := min(start+metricNameCountsBatch, len(names))
		msg := &storegatewaypb.MetricNameCountsResponse{BlockIds: blockIDs, Counts: make([]*storegatewaypb.MetricNameCount, 0, end-start)}
		for _, name := range names[start:end] {
			msg.Counts = append(msg.Counts, &storegatewaypb.MetricNameCount{Name: name, Count: counts[name]})
		}
		if err := srv.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

// addBlockMetricNameCounts adds one block's per-name counts from its index-header: a postings offset table
// range starts at the list's 4-byte entry count and ends before its CRC, so the range holds 4 bytes per series
// after the count field.
func addBlockMetricNameCounts(ctx context.Context, b *bucketBlock, counts map[string]int64) error {
	offsets, err := b.indexHeaderReader.LabelValuesOffsets(ctx, labels.MetricName, "", nil)
	if err != nil {
		return fmt.Errorf("block %s: %w", b.meta.ULID, err)
	}
	for _, off := range offsets {
		size := off.Off.End - off.Off.Start - 4
		if size < 0 || size%4 != 0 {
			return status.Errorf(codes.Internal, "block %s: postings range for %s=%q spans %d bytes, which is not a count field plus 4-byte entries",
				b.meta.ULID, labels.MetricName, off.LabelValue, off.Off.End-off.Off.Start)
		}
		counts[off.LabelValue] += size / 4
	}
	return nil
}
