// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func finalise(ctx context.Context, resp RemoteExecutionResponse, annos *annotations.Annotations, queryStats *types.QueryStats) error {
	newAnnos, totalSamples, err := resp.GetEvaluationInfo(ctx)
	if err != nil {
		return err
	}

	annos.Merge(newAnnos)

	if queryStats != nil {
		// FIXME: once we support evaluating multiple nodes at once, only do this once per request, not once per requested node
		queryStats.TotalSamples += totalSamples
	}

	return nil
}

func accountForSeriesMetadataMemoryConsumption(series []types.SeriesMetadata, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(series))*types.SeriesMetadataSize, limiter.SeriesMetadataSlices); err != nil {
		return err
	}

	for _, s := range series {
		if err := memoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(s.Labels); err != nil {
			return err
		}
	}

	return nil
}

func accountForFPointMemoryConsumption(d []promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d))*types.FPointSize, limiter.FPointSlices); err != nil {
		return err
	}

	return nil
}

func accountForHPointMemoryConsumption(d []promql.HPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d))*types.HPointSize, limiter.HPointSlices); err != nil {
		return err
	}

	return nil
}
