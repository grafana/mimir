// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

var errNautilusDisabled = status.Error(codes.Unimplemented, "nautilus is not enabled on this ingester")

// hashRangeSeriesWalkInterval is how often the ingester walks every
// tenant's TSDB head to recount active series per owned hash range.
// Chosen to match the default ownedSeriesService interval; the CPU cost
// of the two walks is comparable (~O(total head series)), and aligning
// them lets operators reason about "stats freshness" uniformly.
const hashRangeSeriesWalkInterval = 15 * time.Second

// HashRangeStats returns per-range active-series counts for the hash
// ranges this ingester has been told it owns (via SetHashRanges), plus
// the total in-memory series count across all tenants. The rebalancer
// uses TotalActiveSeries (L_i) to rank source/destination partitions
// and the per-range counts (R_r) to pick specific ranges to move.
//
// Also returns per-partition query-load EWMAs (samples-per-second
// scanned by named queries) and a per-ingester unnamed bucket EWMA
// (samples-per-second scanned by full-fanout queries that arrived
// without a partition hint). The rebalancer surfaces both as
// observability signals; named loads will drive the Phase 2 actuator.
func (i *Ingester) HashRangeStats(_ context.Context, _ *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	if i.hashRangeSeries == nil {
		return nil, errNautilusDisabled
	}

	snap := i.hashRangeSeries.Snapshot()

	resp := &client.HashRangeStatsResponse{
		Rates:             make([]client.HashRangeRate, len(snap.Ranges)),
		TotalActiveSeries: i.seriesCount.Load(),
	}
	for j, r := range snap.Ranges {
		resp.Rates[j] = client.HashRangeRate{
			Lo:           r.Lo,
			Hi:           r.Hi,
			ActiveSeries: snap.Counts[j],
		}
	}

	if i.queryLoad != nil {
		ql := i.queryLoad.Snapshot()
		resp.UnnamedQuerySamplesEwma = ql.Unnamed
		resp.PartitionQueryLoads = make([]client.PartitionQueryLoad, len(ql.PerPartition))
		for j, p := range ql.PerPartition {
			resp.PartitionQueryLoads[j] = client.PartitionQueryLoad{
				PartitionId: p.PartitionID,
				SamplesEwma: p.SamplesEWMA,
			}
		}
	}

	return resp, nil
}

// SetHashRanges tells this ingester which hash ranges it owns.
// Called by the nautilus rebalancer after each rebalance round.
func (i *Ingester) SetHashRanges(_ context.Context, req *client.SetHashRangesRequest) (*client.SetHashRangesResponse, error) {
	if i.hashRangeSeries == nil {
		return nil, errNautilusDisabled
	}

	ranges := make([]assignment.HashRange, len(req.Ranges))
	for j, r := range req.Ranges {
		ranges[j] = assignment.HashRange{Lo: r.Lo, Hi: r.Hi}
	}
	i.hashRangeSeries.SetRanges(ranges)
	return &client.SetHashRangesResponse{}, nil
}

// GetHashRanges returns the set of hash ranges this ingester currently
// believes it owns (i.e. whatever was most recently set via
// SetHashRanges). The rebalancer uses this on cold start to reconstruct
// the existing fleet-wide assignment from whatever each ingester
// locally remembers, rather than destroying rebalanced state with a
// fresh even-split. An ingester that has never been told ranges
// (newly-joined or fresh boot before the rebalancer's first round)
// returns an empty list; nautilus-disabled ingesters return the
// existing errNautilusDisabled so the caller can distinguish the two.
func (i *Ingester) GetHashRanges(_ context.Context, _ *client.GetHashRangesRequest) (*client.GetHashRangesResponse, error) {
	if i.hashRangeSeries == nil {
		return nil, errNautilusDisabled
	}

	ranges := i.hashRangeSeries.Ranges()
	resp := &client.GetHashRangesResponse{
		Ranges: make([]client.HashRangeEntry, len(ranges)),
	}
	for j, r := range ranges {
		resp.Ranges[j] = client.HashRangeEntry{Lo: r.Lo, Hi: r.Hi}
	}
	return resp, nil
}

// updateHashRangeSeriesCounts walks every tenant's TSDB head, hashes
// each series with mimirpb.ShardByMetricNameLocalityLabels (the same
// function distributors use to route by hash), tallies the hits per
// currently-owned range, and publishes the counts back to
// hashRangeSeries.
//
// Doing it via a walk (rather than atomic inc/dec on series
// create/delete) has three benefits:
//
//  1. Self-correcting: any drift from missed lifecycle callbacks or
//     ownership changes is washed out on the next walk, rather than
//     accumulating forever in a counter.
//  2. Stateless SetHashRanges: when owned ranges change we just zero
//     the counts and let the next walk re-derive them. No rebucketing
//     logic needed.
//  3. Removes the per-write atomic op from the push hot path.
//
// Cost scales O(total head series across all tenants) per walk. At
// 5M series per ingester and hashRangeSeriesWalkInterval = 15s, that's
// roughly 1–2% of a core continuously — comparable to the existing
// ownedSeriesService walk.
func (i *Ingester) updateHashRangeSeriesCounts(ctx context.Context) {
	ranges := i.hashRangeSeries.Ranges()
	if len(ranges) == 0 {
		return
	}

	counts := make([]int64, len(ranges))
	start := time.Now()
	var walked int64

	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return
		}
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}
		n, err := countSeriesByHashRange(ctx, userID, userDB.Head(), ranges, counts)
		if err != nil {
			level.Warn(i.logger).Log(
				"msg", "failed to count series by hash range",
				"user", userID, "err", err,
			)
			continue
		}
		walked += n
	}

	if !i.hashRangeSeries.SetCountsFor(ranges, counts) {
		level.Debug(i.logger).Log(
			"msg", "hash range series walk discarded because ranges changed mid-walk",
		)
		return
	}

	level.Debug(i.logger).Log(
		"msg", "updated hash range series counts",
		"num_ranges", len(ranges),
		"series_walked", walked,
		"duration", time.Since(start),
	)
}

// countSeriesByHashRange walks all series in head, computes the
// locality hash for each, looks up which of the provided sorted,
// non-overlapping ranges (if any) contains it, and increments counts
// at that index. counts must be the same length as ranges and is
// mutated in place.
//
// Returns the number of series it observed (regardless of whether
// they landed in an owned range).
func countSeriesByHashRange(ctx context.Context, userID string, head *tsdb.Head, ranges []assignment.HashRange, counts []int64) (int64, error) {
	idx, err := head.Index()
	if err != nil {
		return 0, err
	}
	defer idx.Close()

	name, value := index.AllPostingsKey()
	postings, err := idx.Postings(ctx, name, value)
	if err != nil {
		return 0, err
	}

	builder := labels.NewScratchBuilder(16)
	var walked int64

	for postings.Next() {
		if walked&0xffff == 0 && ctx.Err() != nil {
			return walked, ctx.Err()
		}

		ref := postings.At()
		builder.Reset()
		if err := idx.Series(ref, &builder, nil); err != nil {
			// Series may be deleted between Postings() and Series();
			// treat as absent.
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return walked, err
		}
		walked++

		lset := builder.Labels()
		metricName := lset.Get(labels.MetricName)
		hash := mimirpb.ShardByMetricNameLocalityLabels(userID, metricName, lset)

		// Binary search: find largest i such that ranges[i].Lo <= hash.
		ri := sort.Search(len(ranges), func(i int) bool {
			return ranges[i].Lo > hash
		}) - 1
		if ri >= 0 && ranges[ri].Contains(hash) {
			counts[ri]++
		}
	}
	return walked, postings.Err()
}
