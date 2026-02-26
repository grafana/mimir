// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func computeFilterBitmap(unfilteredSeries []types.SeriesMetadata, filters []*labels.Matcher, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (nextUnfilteredSeriesIndex int, unfilteredSeriesBitmap []bool, filteredSeriesCount int, err error) {
	if len(filters) == 0 {
		return 0, nil, len(unfilteredSeries), nil
	}

	unfilteredSeriesBitmap, err = types.BoolSlicePool.Get(len(unfilteredSeries), memoryConsumptionTracker)
	if err != nil {
		return 0, nil, 0, err
	}

	nextUnfilteredSeriesIndex = len(unfilteredSeries)
	unfilteredSeriesBitmap = unfilteredSeriesBitmap[:len(unfilteredSeries)]

	for unfilteredSeriesIndex, series := range unfilteredSeries {
		if !matchesSeries(filters, series.Labels) {
			continue
		}

		unfilteredSeriesBitmap[unfilteredSeriesIndex] = true
		filteredSeriesCount++

		if nextUnfilteredSeriesIndex == len(unfilteredSeries) {
			// First unfiltered series that matches.
			nextUnfilteredSeriesIndex = unfilteredSeriesIndex
		}
	}

	return nextUnfilteredSeriesIndex, unfilteredSeriesBitmap, filteredSeriesCount, nil
}

func applyFiltering(unfilteredSeries []types.SeriesMetadata, bitmap []bool, filteredSeriesCount int, isLastConsumer bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	if bitmap == nil {
		// Fast path: no filters to apply.

		if isLastConsumer {
			return unfilteredSeries, nil
		}

		// Return a copy of the original series metadata.
		// This is a shallow copy, which is sufficient while we're using stringlabels for labels.Labels given these are immutable.
		metadata, err := types.SeriesMetadataSlicePool.Get(len(unfilteredSeries), memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		metadata, err = types.AppendSeriesMetadata(memoryConsumptionTracker, metadata, unfilteredSeries...)
		if err != nil {
			return nil, err
		}

		return metadata, nil
	}

	filteredSeries, err := types.SeriesMetadataSlicePool.Get(filteredSeriesCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for idx, matchesFilter := range bitmap {
		if !matchesFilter {
			continue
		}

		var err error
		filteredSeries, err = types.AppendSeriesMetadata(memoryConsumptionTracker, filteredSeries, unfilteredSeries[idx])
		if err != nil {
			return nil, err
		}
	}

	if isLastConsumer {
		// It's simpler not to reuse the slice when filtering is involved, so return it to the pool now that we're done with it.
		types.SeriesMetadataSlicePool.Put(&unfilteredSeries, memoryConsumptionTracker)
	}

	return filteredSeries, nil
}
