// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type subset struct {
	// filters are the matchers that define this subset. If empty, this subset performs no filtering.
	filters []*labels.Matcher

	// subsetIndex is the index of the subset in the inner operator's stats.
	subsetIndex int

	// unfilteredSeriesBitmap contains one entry per unfiltered input series, where true indicates that it passes this consumer's filters.
	// If the subset has no filters (unfiltered), this is nil.
	unfilteredSeriesBitmap []bool

	filteredSeriesCount int
}

// applicable returns true if this subset performs any filtering.
func (s *subset) applicable() bool {
	return len(s.filters) > 0
}

func (s *subset) computeFilterBitmap(unfilteredSeries []types.SeriesMetadata, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (nextUnfilteredSeriesIndex int, err error) {
	if !s.applicable() {
		s.filteredSeriesCount = len(unfilteredSeries)

		return 0, nil
	}

	s.unfilteredSeriesBitmap, err = types.BoolSlicePool.Get(len(unfilteredSeries), memoryConsumptionTracker)
	if err != nil {
		return 0, err
	}

	nextUnfilteredSeriesIndex = len(unfilteredSeries)
	s.unfilteredSeriesBitmap = s.unfilteredSeriesBitmap[:len(unfilteredSeries)]

	for unfilteredSeriesIndex, series := range unfilteredSeries {
		if !types.MatchersMatch(s.filters, series.Labels) {
			continue
		}

		s.unfilteredSeriesBitmap[unfilteredSeriesIndex] = true
		s.filteredSeriesCount++

		if nextUnfilteredSeriesIndex == len(unfilteredSeries) {
			// First unfiltered series that matches.
			nextUnfilteredSeriesIndex = unfilteredSeriesIndex
		}
	}

	return nextUnfilteredSeriesIndex, nil
}

func (s *subset) close(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.BoolSlicePool.Put(&s.unfilteredSeriesBitmap, memoryConsumptionTracker)
}

func applyFiltering(unfilteredSeries []types.SeriesMetadata, subset subset, isLastConsumer bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	if !subset.applicable() {
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

	filteredSeries, err := types.SeriesMetadataSlicePool.Get(subset.filteredSeriesCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for idx, matchesFilter := range subset.unfilteredSeriesBitmap {
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
