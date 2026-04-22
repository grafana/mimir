// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

var errNautilusDisabled = status.Error(codes.Unimplemented, "nautilus is not enabled on this ingester")

// HashRangeStats returns per-range active-series counts for the hash
// ranges this ingester has been told it owns (via SetHashRanges), plus
// the total in-memory series count across all tenants. The rebalancer
// uses TotalActiveSeries (L_i) to rank source/destination partitions
// and the per-range counts (R_r) to pick specific ranges to move.
//
// SamplesPerSecond is no longer populated on the wire: the series-only
// reframed load model does not consume it. The proto field is kept for
// ABI compatibility with older rebalancers.
func (i *Ingester) HashRangeStats(_ context.Context, _ *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	if i.hashRangeRates == nil {
		return nil, errNautilusDisabled
	}

	snap := i.hashRangeRates.Snapshot()

	resp := &client.HashRangeStatsResponse{
		Rates:             make([]client.HashRangeRate, len(snap.Ranges)),
		TotalActiveSeries: i.seriesCount.Load(),
	}
	for j, r := range snap.Ranges {
		resp.Rates[j] = client.HashRangeRate{
			Lo:           r.Lo,
			Hi:           r.Hi,
			ActiveSeries: snap.ActiveSeries[j],
		}
	}
	return resp, nil
}

// SetHashRanges tells this ingester which hash ranges it owns.
// Called by the nautilus rebalancer after each rebalance round.
func (i *Ingester) SetHashRanges(_ context.Context, req *client.SetHashRangesRequest) (*client.SetHashRangesResponse, error) {
	if i.hashRangeRates == nil {
		return nil, errNautilusDisabled
	}

	ranges := make([]assignment.HashRange, len(req.Ranges))
	for j, r := range req.Ranges {
		ranges[j] = assignment.HashRange{Lo: r.Lo, Hi: r.Hi}
	}
	i.hashRangeRates.SetRanges(ranges)
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
	if i.hashRangeRates == nil {
		return nil, errNautilusDisabled
	}

	ranges := i.hashRangeRates.Ranges()
	resp := &client.GetHashRangesResponse{
		Ranges: make([]client.HashRangeEntry, len(ranges)),
	}
	for j, r := range ranges {
		resp.Ranges[j] = client.HashRangeEntry{Lo: r.Lo, Hi: r.Hi}
	}
	return resp, nil
}
