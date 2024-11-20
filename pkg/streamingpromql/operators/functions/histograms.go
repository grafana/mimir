// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// HistogramFunctionOverInstantVector performs a function over each series in an instant vector,
// with special handling for classic and native histograms.
// At the moment, it only supports histogram_quantile
type HistogramFunctionOverInstantVector struct {
	phArg                    types.ScalarOperator // for histogram_quantile
	inner                    types.InstantVectorOperator
	currentInnerSeriesIndex  int
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	timeRange                types.QueryTimeRange

	annotations            *annotations.Annotations
	phValues               types.ScalarData
	expressionPosition     posrange.PositionRange
	innerSeriesMetricNames *operators.MetricNames // We need to keep track of the metric names for annotations.NewBadBucketLabelWarning

	seriesGroups    [][]*bucketGroup // Each series belongs to 2 groups. One with the `le` label, and one without. Sometimes, these are the same group.
	remainingGroups []*bucketGroup   // One entry per group, in the order we want to return them.
	bucketValues    []string         // One entry per series produced by inner. Contains the value of `le` for that series. An empty string ("") may mean no `le` label was present.
}

var _ types.InstantVectorOperator = &HistogramFunctionOverInstantVector{}

type groupWithLabels struct {
	labels labels.Labels
	group  *bucketGroup
}

type bucketGroup struct {
	pointBuckets         []buckets       // Buckets for the grouped series at each step
	nativeHistograms     []promql.HPoint // Histograms should only ever exist once per group
	remainingSeriesCount uint            // The number of series remaining before this group is fully collated.
	firstInputSeriesIdx  int             // All the input series should have the same Metric name (from innerSeriesMetricNames). We just need one index to determine the group name, so we take the first series.
}

var bucketGroupPool = zeropool.New(func() *bucketGroup {
	return &bucketGroup{}
})

func NewHistogramFunctionOverInstantVector(
	phArg types.ScalarOperator,
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) *HistogramFunctionOverInstantVector {
	return &HistogramFunctionOverInstantVector{
		phArg:                    phArg,
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations,
		innerSeriesMetricNames:   &operators.MetricNames{},
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
	}
}

func (h *HistogramFunctionOverInstantVector) ExpressionPosition() posrange.PositionRange {
	return h.expressionPosition
}

func (h *HistogramFunctionOverInstantVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Pre-process any Scalar arguments
	var err error
	h.phValues, err = h.phArg.GetValues(ctx)
	if err != nil {
		return nil, err
	}

	innerSeries, err := h.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		types.PutSeriesMetadataSlice(innerSeries)
	}()

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	h.innerSeriesMetricNames.CaptureMetricNames(innerSeries)
	groups := map[string]groupWithLabels{}
	h.seriesGroups = make([][]*bucketGroup, 0, len(innerSeries))
	h.bucketValues, err = types.StringSlicePool.Get(len(innerSeries), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, 1024)
	lb := labels.NewBuilder(nil)

	for innerIdx, series := range innerSeries {
		// Each series belongs to two groups
		seriesGroupPair := make([]*bucketGroup, 2)

		// Store the le label. If it doesn't exist, it'll be an empty string
		le := series.Labels.Get(labels.BucketLabel)
		h.bucketValues = append(h.bucketValues, le)

		// First get the group with all labels
		b = series.Labels.Bytes(b)
		g, groupExists := groups[string(b)]
		if !groupExists {
			g.labels = series.Labels
			g.group = bucketGroupPool.Get()
			g.group.firstInputSeriesIdx = innerIdx
			groups[string(b)] = g
		}
		g.group.remainingSeriesCount++
		seriesGroupPair[0] = g.group

		// Now, ignore `le` and create a group with the rest of the labels.
		// We do this even if we don't have an `le` label. This is so we can detect
		// where native histograms exist alongside classic histograms.
		// So seriesGroupPair[0] could be the same as seriesGroupPair[1]
		b = series.Labels.BytesWithoutLabels(b, labels.BucketLabel)
		g, groupExists = groups[string(b)]

		if !groupExists {
			lb.Reset(series.Labels)
			lb.Del(labels.BucketLabel)
			g.labels = lb.Labels()
			g.group = bucketGroupPool.Get()
			g.group.firstInputSeriesIdx = innerIdx
			groups[string(b)] = g
		}
		g.group.remainingSeriesCount++
		seriesGroupPair[1] = g.group
		h.seriesGroups = append(h.seriesGroups, seriesGroupPair)
	}

	seriesMetadata := types.GetSeriesMetadataSlice(len(groups))
	h.remainingGroups = make([]*bucketGroup, 0, len(groups))
	for _, g := range groups {
		seriesMetadata = append(seriesMetadata, types.SeriesMetadata{Labels: g.labels.DropMetricName()})
		h.remainingGroups = append(h.remainingGroups, g.group)
	}

	return seriesMetadata, nil
}

func (h *HistogramFunctionOverInstantVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(h.remainingGroups) == 0 {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Determine next group to return
	thisGroup := h.remainingGroups[0]
	h.remainingGroups = h.remainingGroups[1:]
	defer func() {
		// Reset the group before returning to the pool
		//thisGroup.groupedMetricName = ""
		thisGroup.firstInputSeriesIdx = 0
		thisGroup.pointBuckets = nil
		thisGroup.nativeHistograms = nil
		thisGroup.remainingSeriesCount = 0
		bucketGroupPool.Put(thisGroup)
	}()

	// Iterate through inner series until the desired group is complete
	if err := h.accumulateUntilGroupComplete(ctx, thisGroup); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if len(h.remainingGroups) == 0 {
		// We no longer need the bucketValues strings
		types.StringSlicePool.Put(h.bucketValues, h.memoryConsumptionTracker)
	}

	return h.computeOutputSeriesForGroup(thisGroup)
}

// accumulateUntilGroupComplete gathers all the series associated with the given bucketGroup
// As each inner series is selected, it is added into its respective groups.
// This means a group other than the one we are focused on may get completed first, but we
// continue until the desired group is ready.
func (h *HistogramFunctionOverInstantVector) accumulateUntilGroupComplete(ctx context.Context, g *bucketGroup) error {
	for g.remainingSeriesCount > 0 {
		s, err := h.inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}
			return err
		}
		thisSeriesGroups := h.seriesGroups[h.currentInnerSeriesIndex]

		// Native histograms only ever go to their original labelset.
		// Floats only ever go to their group.
		// It is possible that a series has both floats and histograms.
		// It is also possible that both series groups are the same.
		// The conflict in points is then detected in computeOutputSeriesForGroup.
		h.saveNativeHistogramsToGroup(s.Histograms, thisSeriesGroups[0])
		h.saveFloatsToGroup(s.Floats, thisSeriesGroups[1])
		h.currentInnerSeriesIndex++
	}
	return nil
}

// saveFloatsToGroup places each fPoint into a bucket with the upperBound set by the input series.
func (h *HistogramFunctionOverInstantVector) saveFloatsToGroup(fPoints []promql.FPoint, g *bucketGroup) {
	if len(fPoints) > 0 {
		upperBound, err := strconv.ParseFloat(h.bucketValues[h.currentInnerSeriesIndex], 64)
		if err != nil {
			// The le label was invalid. Record it:
			h.annotations.Add(annotations.NewBadBucketLabelWarning(
				h.innerSeriesMetricNames.GetMetricNameForSeries(h.currentInnerSeriesIndex),
				h.bucketValues[h.currentInnerSeriesIndex],
				h.inner.ExpressionPosition(),
			))
		} else {
			if g.pointBuckets == nil {
				g.pointBuckets = make([]buckets, h.timeRange.StepCount)
			}
			for _, f := range fPoints {
				pointIdx := h.timeRange.PointIndex(f.T)
				g.pointBuckets[pointIdx] = append(
					g.pointBuckets[pointIdx],
					bucket{
						upperBound: upperBound,
						count:      f.F,
					},
				)
			}
		}
	}
	// We are done with the fPoints, so return these now
	types.FPointSlicePool.Put(fPoints, h.memoryConsumptionTracker)
	g.remainingSeriesCount--
}

// saveNativeHistogramsToGroup stores the given native histograms onto the given group.
// There should only ever be one native histogram per a series or series group. In general,
// they are per-series, but we handle them as parts of groups because they could hypothetically
// have the same series name as a classic histogram.
func (h *HistogramFunctionOverInstantVector) saveNativeHistogramsToGroup(hPoints []promql.HPoint, g *bucketGroup) {
	if len(hPoints) > 0 {
		// We should only ever see one set of native histograms per group.
		if g.nativeHistograms != nil {
			panic("We should never see more than one native histogram per group")
		}
		g.nativeHistograms = hPoints
	}
	// hPoint's are returned to the pool after computeOutputSeriesForGroup is finished with them
	g.remainingSeriesCount--
}

func (h *HistogramFunctionOverInstantVector) computeOutputSeriesForGroup(g *bucketGroup) (types.InstantVectorSeriesData, error) {
	floatPoints, err := types.FPointSlicePool.Get(h.timeRange.StepCount, h.memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	histogramIndex := 0
	var currentHistogram *histogram.FloatHistogram
	var currentHistogramPointIdx int64

	for pointIdx := range h.timeRange.StepCount {
		ph := h.phValues.Samples[pointIdx].F
		if math.IsNaN(ph) || ph < 0 || ph > 1 {
			// Even when ph is invalid we still return a series as BucketQuantile will return +/-Inf.
			// So don't skip/continue the series, just emit a warning.
			h.annotations.Add(annotations.NewInvalidQuantileWarning(ph, h.phArg.ExpressionPosition()))
		}

		// Get the HPoint if it exists at this step
		if g.nativeHistograms != nil && histogramIndex < len(g.nativeHistograms) {
			nextHPoint := g.nativeHistograms[histogramIndex]
			if h.timeRange.PointIndex(nextHPoint.T) == int64(pointIdx) {
				currentHistogram = nextHPoint.H
				currentHistogramPointIdx = h.timeRange.PointIndex(nextHPoint.T)
				histogramIndex++
			}
		}

		if g.pointBuckets != nil && len(g.pointBuckets[pointIdx]) > 0 && currentHistogram != nil && currentHistogramPointIdx == int64(pointIdx) {
			// At this data point, we have classic histogram buckets and a native histogram with the same name and labels.
			// No value is returned, so emit an annotation and continue.
			h.annotations.Add(annotations.NewMixedClassicNativeHistogramsWarning(
				h.innerSeriesMetricNames.GetMetricNameForSeries(g.firstInputSeriesIdx), h.inner.ExpressionPosition(),
			))
			continue
		}

		if g.pointBuckets != nil && len(g.pointBuckets[pointIdx]) > 0 {
			res, forcedMonotonicity, _ := bucketQuantile(ph, g.pointBuckets[pointIdx])
			floatPoints = append(floatPoints, promql.FPoint{
				T: h.timeRange.IndexTime(int64(pointIdx)),
				F: res,
			})
			if forcedMonotonicity {
				// Currently Prometheus does not correctly place the metric name onto this annotation.
				// See: https://github.com/prometheus/prometheus/issues/15411
				h.annotations.Add(annotations.NewHistogramQuantileForcedMonotonicityInfo(
					h.innerSeriesMetricNames.GetMetricNameForSeries(g.firstInputSeriesIdx), h.inner.ExpressionPosition(),
				))
			}
			continue
		}

		if currentHistogram != nil && currentHistogramPointIdx == int64(pointIdx) {
			res := histogramQuantile(ph, currentHistogram)

			floatPoints = append(floatPoints, promql.FPoint{
				T: h.timeRange.IndexTime(int64(pointIdx)),
				F: res,
			})
		}
	}

	// Return any retained native histogram to the pool
	if g.nativeHistograms != nil {
		types.HPointSlicePool.Put(g.nativeHistograms, h.memoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{Floats: floatPoints}, nil
}

func (h *HistogramFunctionOverInstantVector) Close() {
	h.inner.Close()
	h.phArg.Close()
	types.FPointSlicePool.Put(h.phValues.Samples, h.memoryConsumptionTracker)
}
