// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// HashRangeStats returns a fixed-resolution histogram of ingestion rates
// across the 32-bit hash space. Each bucket covers an equal portion of the
// hash space, and the ingester reports samples-per-second per bucket since
// the last snapshot.
func (i *Ingester) HashRangeStats(_ context.Context, _ *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	snap := i.hashBucketRates.Snapshot()

	resp := &client.HashRangeStatsResponse{
		NumBuckets:       uint32(snap.NumBuckets),
		SamplesPerSecond: snap.SamplesPerSecond[:],
	}
	return resp, nil
}
