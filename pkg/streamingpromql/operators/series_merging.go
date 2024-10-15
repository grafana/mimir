// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"math"
	"sort"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// MergeSeries merges the series in data into a single InstantVectorSeriesData, or returns information about a conflict between series.
//
// For example, MergeSeries would return a single series with points [1, 2, 3] given the two input series:
//
//	1 2 _
//	_ _ 3
//
// MergeSeries is optimised for the case where there is only one source series, or the source series do not overlap, as in the example above.
//
// sourceSeriesIndices should contain the indices of the original series in data. For example, if data contains the first, fourth and tenth
// series, then sourceSeriesIndices would be [0, 3, 9]. These indices are used to include source series information when a conflict occurs.
//
// MergeSeries re-orders both data and sourceSeriesIndices.
func MergeSeries(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, *MergeConflict, error) {
	if len(data) == 1 {
		// Fast path: if there's only one series on this side, there's no merging required.
		return data[0], nil, nil
	}

	if len(data) == 0 {
		return types.InstantVectorSeriesData{}, nil, nil
	}

	// Merge floats and histograms individually.
	// After which we check if there are any duplicate points in either the floats or histograms.

	floats, conflict, err := mergeOneSideFloats(data, sourceSeriesIndices, memoryConsumptionTracker)
	if err != nil || conflict != nil {
		return types.InstantVectorSeriesData{}, conflict, err
	}

	histograms, conflict, err := mergeOneSideHistograms(data, sourceSeriesIndices, memoryConsumptionTracker)
	if err != nil || conflict != nil {
		return types.InstantVectorSeriesData{}, conflict, err
	}

	// Check for any conflicts between floats and histograms
	idxFloats, idxHistograms := 0, 0
	for idxFloats < len(floats) && idxHistograms < len(histograms) {
		if floats[idxFloats].T == histograms[idxHistograms].T {
			// Histogram and float at the same timestamp: we have a conflict.
			conflict := &MergeConflict{
				firstConflictingSeriesIndex:  sourceSeriesIndices[0],
				secondConflictingSeriesIndex: -1,
				description:                  "both float and histogram samples",
				timestamp:                    floats[idxFloats].T,
			}

			return types.InstantVectorSeriesData{}, conflict, nil
		}

		if floats[idxFloats].T < histograms[idxHistograms].T {
			idxFloats++
		} else {
			idxHistograms++
		}
	}

	return types.InstantVectorSeriesData{Floats: floats, Histograms: histograms}, nil, nil
}

func mergeOneSideFloats(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) ([]promql.FPoint, *MergeConflict, error) {
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) == 1 {
		return data[0].Floats, nil, nil
	}

	sort.Sort(floatSideSorter{data, sourceSeriesIndices})

	// After sorting, if the first series has no floats, we're done, as that means there are no floats in any series
	if len(data[0].Floats) == 0 {
		return nil, nil, nil
	}

	mergedSize := len(data[0].Floats)
	remainingSeriesWithFloats := 1
	haveOverlaps := false

	for i := 0; i < len(data)-1; i++ {
		first := data[i]
		second := data[i+1]

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Floats.
		// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer types.FPointSlicePool.Put(second.Floats, memoryConsumptionTracker)

		if len(second.Floats) == 0 {
			// We've reached the end of all series with floats.
			// However, continue iterating so we can return all of the slices.
			// (As they may have length 0, but a non-zero capacity).
			continue
		}

		mergedSize += len(second.Floats)
		remainingSeriesWithFloats++

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Floats is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Floats[len(first.Floats)-1].T >= second.Floats[0].T {
			haveOverlaps = true
		}
	}

	if remainingSeriesWithFloats == 1 {
		// No other series had any floats
		return data[0].Floats, nil, nil
	}

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Floats.
	// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer types.FPointSlicePool.Put(data[0].Floats, memoryConsumptionTracker)

	// Re-slice the data with just the series with floats to make the rest of our job easier
	// Because we aren't re-sorting here it doesn't matter that sourceSeriesIndices remains longer.
	data = data[:remainingSeriesWithFloats]

	output, err := types.FPointSlicePool.Get(mergedSize, memoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Floats...)
		}

		return output, nil, nil
	}

	// Slow path: there are overlaps, so we need to merge slices together and check for conflicts as we go.
	// We don't expect to have many series here, so something like a loser tree is likely unnecessary.
	for {
		if remainingSeriesWithFloats == 1 {
			// Only one series left, just copy remaining points.
			for _, d := range data {
				if len(d.Floats) > 0 {
					output = append(output, d.Floats...)
					return output, nil, nil
				}
			}
		}

		nextT := int64(math.MaxInt64)
		sourceSeriesIndexInData := -1

		for seriesIndexInData, d := range data {
			if len(d.Floats) == 0 {
				continue
			}

			nextPointInSeries := d.Floats[0]
			if nextPointInSeries.T == nextT {
				// Another series has a point with the same timestamp. We have a conflict.
				conflict := &MergeConflict{
					firstConflictingSeriesIndex:  sourceSeriesIndices[sourceSeriesIndexInData],
					secondConflictingSeriesIndex: sourceSeriesIndices[seriesIndexInData],
					description:                  "duplicate series",
					timestamp:                    nextT,
				}

				return nil, conflict, nil
			}

			if d.Floats[0].T < nextT {
				nextT = d.Floats[0].T
				sourceSeriesIndexInData = seriesIndexInData
			}
		}

		output = append(output, data[sourceSeriesIndexInData].Floats[0])
		data[sourceSeriesIndexInData].Floats = data[sourceSeriesIndexInData].Floats[1:]

		if len(data[sourceSeriesIndexInData].Floats) == 0 {
			remainingSeriesWithFloats--
		}
	}
}

func mergeOneSideHistograms(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) ([]promql.HPoint, *MergeConflict, error) {
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) == 1 {
		return data[0].Histograms, nil, nil
	}

	sort.Sort(histogramSideSorter{data, sourceSeriesIndices})

	// After sorting, if the first series has no histograms, we're done, as that means there are no histograms in any series
	if len(data[0].Histograms) == 0 {
		return nil, nil, nil
	}

	mergedSize := len(data[0].Histograms)
	remainingSeriesWithHistograms := 1
	haveOverlaps := false

	for i := 0; i < len(data)-1; i++ {
		first := data[i]
		second := data[i+1]

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Histograms.
		// We deliberately want this to happen at the end of mergeOneSideHistograms, so calling defer like this in a loop is fine.
		// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer clearAndReturnHPointSlice(second.Histograms, memoryConsumptionTracker) // We're going to retain all the FloatHistogram instances, so don't leave them in the slice to be reused.

		if len(second.Histograms) == 0 {
			// We've reached the end of all series with histograms.
			// However, continue iterating so we can return all of the slices to the pool.
			// (As they may have length 0, but a non-zero capacity and so still need to be returned to the pool).
			continue
		}
		mergedSize += len(second.Histograms)
		remainingSeriesWithHistograms++

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Histograms is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Histograms[len(first.Histograms)-1].T >= second.Histograms[0].T {
			haveOverlaps = true
		}
	}

	if remainingSeriesWithHistograms == 1 {
		// No other series had any histograms
		return data[0].Histograms, nil, nil
	}

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Histograms.
	// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer clearAndReturnHPointSlice(data[0].Histograms, memoryConsumptionTracker) // We're going to retain all the FloatHistogram instances, so don't leave them in the slice to be reused.

	// Re-slice data with just the series with histograms to make the rest of our job easier
	// Because we aren't re-sorting here it doesn't matter that sourceSeriesIndices remains longer.
	data = data[:remainingSeriesWithHistograms]

	output, err := types.HPointSlicePool.Get(mergedSize, memoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Histograms...)
		}

		return output, nil, nil
	}

	// Slow path: there are overlaps, so we need to merge slices together and check for conflicts as we go.
	// We don't expect to have many series here, so something like a loser tree is likely unnecessary.
	for {
		if remainingSeriesWithHistograms == 1 {
			// Only one series left, just copy remaining points.
			for _, d := range data {
				if len(d.Histograms) > 0 {
					output = append(output, d.Histograms...)
					return output, nil, nil
				}
			}
		}

		nextT := int64(math.MaxInt64)
		sourceSeriesIndexInData := -1

		for seriesIndexInData, d := range data {
			if len(d.Histograms) == 0 {
				continue
			}

			nextPointInSeries := d.Histograms[0]
			if nextPointInSeries.T == nextT {
				// Another series has a point with the same timestamp. We have a conflict.
				conflict := &MergeConflict{
					firstConflictingSeriesIndex:  sourceSeriesIndices[sourceSeriesIndexInData],
					secondConflictingSeriesIndex: sourceSeriesIndices[seriesIndexInData],
					description:                  "duplicate series",
					timestamp:                    nextT,
				}

				return nil, conflict, nil
			}

			if d.Histograms[0].T < nextT {
				nextT = d.Histograms[0].T
				sourceSeriesIndexInData = seriesIndexInData
			}
		}

		output = append(output, data[sourceSeriesIndexInData].Histograms[0])
		data[sourceSeriesIndexInData].Histograms = data[sourceSeriesIndexInData].Histograms[1:]

		if len(data[sourceSeriesIndexInData].Histograms) == 0 {
			remainingSeriesWithHistograms--
		}
	}
}

func clearAndReturnHPointSlice(s []promql.HPoint, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) {
	clear(s)
	types.HPointSlicePool.Put(s, memoryConsumptionTracker)
}

type MergeConflict struct {
	firstConflictingSeriesIndex  int // Will be the index of any input series in the case of a mixed float / histogram conflict.
	secondConflictingSeriesIndex int // Will be -1 in the case of a mixed float / histogram conflict.
	description                  string
	timestamp                    int64
}

// floatSideSorter sorts side by the timestamp of the first float point in each series.
// It maintains the order of both data and sourceSeriesIndices.
type floatSideSorter struct {
	data                []types.InstantVectorSeriesData
	sourceSeriesIndices []int
}

func (f floatSideSorter) Len() int {
	return len(f.data)
}

func (f floatSideSorter) Swap(i, j int) {
	f.data[i], f.data[j] = f.data[j], f.data[i]
	f.sourceSeriesIndices[i], f.sourceSeriesIndices[j] = f.sourceSeriesIndices[j], f.sourceSeriesIndices[i]
}

func (f floatSideSorter) Less(i, j int) bool {
	if len(f.data[i].Floats) == 0 {
		return false
	}
	if len(f.data[j].Floats) == 0 {
		return true
	}
	return f.data[i].Floats[0].T < f.data[j].Floats[0].T
}

// histogramSideSorter sorts side by the timestamp of the first histogram point in each series.
// It maintains the order of both data and sourceSeriesIndices.
type histogramSideSorter struct {
	data                []types.InstantVectorSeriesData
	sourceSeriesIndices []int
}

func (h histogramSideSorter) Len() int {
	return len(h.data)
}

func (h histogramSideSorter) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
	h.sourceSeriesIndices[i], h.sourceSeriesIndices[j] = h.sourceSeriesIndices[j], h.sourceSeriesIndices[i]
}

func (h histogramSideSorter) Less(i, j int) bool {
	if len(h.data[i].Histograms) == 0 {
		return false
	}
	if len(h.data[j].Histograms) == 0 {
		return true
	}
	return h.data[i].Histograms[0].T < h.data[j].Histograms[0].T
}
