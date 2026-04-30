// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func finalize(ctx context.Context, resp RemoteExecutionResponse, annos *annotations.Annotations) error {
	newAnnos, remoteStats, err := resp.Finalize(ctx)
	if err != nil {
		return err
	}

	if newAnnos != nil {
		annos.Merge(*newAnnos)
	}

	if localStats := stats.FromContext(ctx); localStats != nil {
		// We need to remove the samples processed from the remote stats before merging them into the local stats, as the total samples
		// processed count will be computed from the overall evaluation stats when the query completes.
		remoteStats.SamplesProcessed = 0
		localStats.Merge(&stats.SafeStats{Stats: remoteStats})
	}

	return nil
}
