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

// HashRangeStats returns per-range ingestion rates for the hash ranges
// this ingester has been told it owns (via SetHashRanges), plus the
// total in-memory series count across all tenants. The rebalancer
// compares the total against the sum of per-range counts to detect
// "orphan" series left behind by a recent move that haven't been GC'd
// by head compaction yet.
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
			Lo:               r.Lo,
			Hi:               r.Hi,
			SamplesPerSecond: snap.SamplesPerSecond[j],
			ActiveSeries:     snap.ActiveSeries[j],
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
