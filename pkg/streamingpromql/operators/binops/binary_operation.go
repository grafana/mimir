// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// vectorMatchingGroupKeyFunc returns a function that computes the grouping key of the output group a series belongs to.
//
// The return value from the function is valid until it is called again.
func vectorMatchingGroupKeyFunc(vectorMatching parser.VectorMatching) func(labels.Labels) []byte {
	buf := make([]byte, 0, 1024)

	if vectorMatching.On {
		slices.Sort(vectorMatching.MatchingLabels)

		return func(l labels.Labels) []byte {
			return l.BytesWithLabels(buf, vectorMatching.MatchingLabels...)
		}
	}

	if len(vectorMatching.MatchingLabels) == 0 {
		// Fast path for common case for expressions like "a + b" with no 'on' or 'without' labels.
		return func(l labels.Labels) []byte {
			return l.BytesWithoutLabels(buf, labels.MetricName)
		}
	}

	lbls := make([]string, 0, len(vectorMatching.MatchingLabels)+1)
	lbls = append(lbls, labels.MetricName)
	lbls = append(lbls, vectorMatching.MatchingLabels...)
	slices.Sort(lbls)

	return func(l labels.Labels) []byte {
		return l.BytesWithoutLabels(buf, lbls...)
	}
}

// filterSeries returns data filtered based on the mask provided.
//
// mask is expected to contain one value for each time step in the query time range.
// Samples in data where mask has value desiredMaskValue are returned.
//
// The return value reuses the slices from data, and returns any unused slices to the pool.
func filterSeries(data types.InstantVectorSeriesData, mask []bool, desiredMaskValue bool, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {
	filteredData := types.InstantVectorSeriesData{}
	nextOutputFloatIndex := 0

	for _, p := range data.Floats {
		if mask[timeRange.PointIndex(p.T)] != desiredMaskValue {
			continue
		}

		data.Floats[nextOutputFloatIndex] = p
		nextOutputFloatIndex++
	}

	if nextOutputFloatIndex > 0 {
		// We have at least one output float point to return.
		filteredData.Floats = data.Floats[:nextOutputFloatIndex]
	} else {
		// We don't have any float points to return, return the original slice to the pool.
		types.FPointSlicePool.Put(data.Floats, memoryConsumptionTracker)
	}

	nextOutputHistogramIndex := 0

	for idx, p := range data.Histograms {
		if mask[timeRange.PointIndex(p.T)] != desiredMaskValue {
			continue
		}

		data.Histograms[nextOutputHistogramIndex] = p

		if idx > nextOutputHistogramIndex {
			// Remove the histogram from the original point to ensure that it's not mutated unexpectedly when the HPoint slice is reused.
			data.Histograms[idx].H = nil
		}

		nextOutputHistogramIndex++
	}

	if nextOutputHistogramIndex > 0 {
		// We have at least one output histogram point to return.
		filteredData.Histograms = data.Histograms[:nextOutputHistogramIndex]
	} else {
		// We don't have any histogram points to return, return the original slice to the pool.
		types.HPointSlicePool.Put(data.Histograms, memoryConsumptionTracker)
	}

	return filteredData, nil
}

func emitIncompatibleTypesAnnotation(a *annotations.Annotations, op parser.ItemType, lH *histogram.FloatHistogram, rH *histogram.FloatHistogram, expressionPosition posrange.PositionRange) {
	a.Add(annotations.NewIncompatibleTypesInBinOpInfo(sampleTypeDescription(lH), op.String(), sampleTypeDescription(rH), expressionPosition))
}

func sampleTypeDescription(h *histogram.FloatHistogram) string {
	if h == nil {
		return "float"
	}

	return "histogram"
}
