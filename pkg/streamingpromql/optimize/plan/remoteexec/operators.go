// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func finalize(ctx context.Context, resp RemoteExecutionResponse, annos *annotations.Annotations, queryStats *types.QueryStats) error {
	newAnnos, remoteStats, err := resp.Finalize(ctx)
	if err != nil {
		return err
	}

	if newAnnos != nil {
		annos.Merge(*newAnnos)
	}

	queryStats.IncrementSamples(int64(remoteStats.SamplesProcessed))

	if localStats := stats.FromContext(ctx); localStats != nil {
		// We need to remove the samples processed from the remote stats before merging them into the local stats, as we already added them to MQE's queryStats above.
		// MQE's queryStats will be added to the local stats in engineQueryRequestRoundTripperHandler.Do when the query completes.
		remoteStats.SamplesProcessed = 0
		remoteStats.SamplesProcessedPerStep = nil
		localStats.Merge(&stats.SafeStats{Stats: remoteStats})
	}

	return nil
}
