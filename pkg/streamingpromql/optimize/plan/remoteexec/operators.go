// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func finalize(ctx context.Context, resp RemoteExecutionResponse, annos *annotations.Annotations, queryStats *types.QueryStats) error {
	newAnnos, remoteStats, err := resp.GetEvaluationInfo(ctx)
	if err != nil {
		return err
	}

	annos.Merge(*newAnnos)

	if queryStats != nil {
		// FIXME: once we support evaluating multiple nodes at once, only do this once per request, not once per requested node
		if len(remoteStats.SamplesProcessedPerStep) > 0 {
			for _, step := range remoteStats.SamplesProcessedPerStep {
				queryStats.IncrementSamplesAtTimestamp(step.Timestamp, step.Value)
			}

			// IncrementSamplesAtTimestamp updates TotalSamples, so there's nothing more to do.
		} else {
			queryStats.TotalSamples += int64(remoteStats.SamplesProcessed)
		}
	}

	if localStats := stats.FromContext(ctx); localStats != nil {
		remoteStats.SamplesProcessed = 0
		remoteStats.SamplesProcessedPerStep = nil
		localStats.Merge(&stats.SafeStats{Stats: remoteStats})
	}

	return nil
}
