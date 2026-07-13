// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// seriesMetadataSource provides the series metadata for a single source (a range for TimeRangeSplitOperator,
// or an extent for CacheOperator) that is to be merged with the other sources.
type seriesMetadataSource interface {
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)
}

// mergeSeriesMetadataFromMultipleSources merges the series metadata from each of sources into a single deduplicated slice, returning that
// slice along with a mapping from each output series to the corresponding series index in each source.
//
// The provided matchers are passed through to each source's SeriesMetadata method.
//
// If there is only a single source, the fast path is taken: that source's series metadata is returned directly and the
// returned []splitOrCacheOutputSeries is nil, as there is nothing to merge. Callers must handle this case when reading
// the series' data.
//
// sources must not be empty.
func mergeSeriesMetadataFromMultipleSources[S seriesMetadataSource](ctx context.Context, sources []S, matchers types.Matchers, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, []splitOrCacheOutputSeries, error) {
	// Use the slice from the first source as the base for the returned series metadata.
	allSeries, err := sources[0].SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}

	if len(sources) == 1 {
		// There are no other sources to merge, so just return the series metadata from the first source.
		return allSeries, nil, nil
	}

	var outputSeries []splitOrCacheOutputSeries

	addNewOutputSeries := func(sourceIdx int, sourceSeriesIdx int) error {
		sourceSeriesIndices, err := types.IntSlicePool.Get(len(sources), memoryConsumptionTracker)
		if err != nil {
			return err
		}

		sourceSeriesIndices = sourceSeriesIndices[:len(sources)]

		for idx := range sources {
			if idx == sourceIdx {
				sourceSeriesIndices[idx] = sourceSeriesIdx
			} else {
				sourceSeriesIndices[idx] = -1
			}
		}

		outputSeries = append(outputSeries, splitOrCacheOutputSeries{sourceSeriesIndices: sourceSeriesIndices})
		return nil
	}

	// Build up a map of the series labels to their index in the output.
	seriesIndices := make(map[string]int, len(allSeries))
	labelBytesBuf := make([]byte, 0, types.LabelBytesBufferSize)
	for seriesIdx, series := range allSeries {
		labelBytesBuf = series.Labels.Bytes(labelBytesBuf)
		seriesIndices[string(labelBytesBuf)] = seriesIdx // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if err := addNewOutputSeries(0, seriesIdx); err != nil {
			return nil, nil, err
		}
	}

	// Now go through the remaining sources.
	for sourceIdx := 1; sourceIdx < len(sources); sourceIdx++ {
		sourceSeries, err := sources[sourceIdx].SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		for seriesIdx, series := range sourceSeries {
			labelBytesBuf = series.Labels.Bytes(labelBytesBuf)

			// Important: don't extract the string(...) call in the map lookup below - passing it directly allows us to avoid allocating it.
			if outputSeriesIdx, seenAlready := seriesIndices[string(labelBytesBuf)]; seenAlready {
				if series.DropName != allSeries[outputSeriesIdx].DropName {
					return nil, nil, fmt.Errorf("series with labels %s has conflicting drop name values in different ranges / extents", series.Labels.String())
				}

				outputSeries[outputSeriesIdx].sourceSeriesIndices[sourceIdx] = seriesIdx

				// We're not going to keep this labels instance (we already have it from a previous source), so
				// decrease memory consumption now.
				memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(series.Labels)
			} else {
				seriesIndices[string(labelBytesBuf)] = len(allSeries)
				allSeries, err = types.SeriesMetadataSlicePool.AppendToSlice(allSeries, memoryConsumptionTracker, series)
				if err != nil {
					return nil, nil, err
				}

				if err := addNewOutputSeries(sourceIdx, seriesIdx); err != nil {
					return nil, nil, err
				}
			}

			// We've already accounted for the memory consumption of this series' labels with the DecreaseMemoryConsumptionForLabels
			// or AppendToSlice calls above, so clear the labels now so they're not double-decremented when we return the series
			// slice to the pool below.
			sourceSeries[seriesIdx] = types.SeriesMetadata{}
		}

		types.SeriesMetadataSlicePool.Put(&sourceSeries, memoryConsumptionTracker)
	}

	return allSeries, outputSeries, nil
}
