// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func finalise(ctx context.Context, resp RemoteExecutionResponse, annos *annotations.Annotations, queryStats *types.QueryStats) error {
	newAnnos, totalSamples, err := resp.GetEvaluationInfo(ctx)
	if err != nil {
		return err
	}

	annos.Merge(*newAnnos)

	if queryStats != nil {
		// FIXME: once we support evaluating multiple nodes at once, only do this once per request, not once per requested node
		queryStats.TotalSamples += totalSamples
	}

	return nil
}
