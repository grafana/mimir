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
	"sort"
	"strconv"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// intentionallyEmptyMetricName exists for annotations compatibility with prometheus.
	// This is only used for backwards compatibility when delayed __name__ removal is not enabled.
	intentionallyEmptyMetricName = ""
)

// HistogramFunction performs a function over each series in an instant vector,
// with special handling for classic and native histograms.
// At the moment, it supports only histogram_quantile and histogram_fraction.
type HistogramFunction struct {
	f                        histogramFunction
	inner                    types.InstantVectorOperator
	currentInnerSeriesIndex  int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool

	annotations            *annotations.Annotations
	expressionPosition     posrange.PositionRange
	innerSeriesMetricNames *operators.MetricNames // We need to keep track of the metric names for annotations.NewBadBucketLabelWarning

	seriesGroupPairs []seriesGroupPair // Each series belongs to 2 groups. One with the `le` label, and one without. Sometimes, these are the same group.
	remainingGroups  []*bucketGroup    // One entry per group, in the order we want to return them.
}

var _ types.InstantVectorOperator = &HistogramFunction{}

type groupWithLabels struct {
	labels labels.Labels
	group  *bucketGroup
}

type bucketGroup struct {
	pointBuckets         []promql.Buckets // Buckets for the grouped series at each step
	nativeHistograms     []promql.HPoint  // Histograms should only ever exist once per group
	remainingSeriesCount uint             // The number of series remaining before this group is fully collated.

	// All the input series should have the same Metric name (from innerSeriesMetricNames).
	// We just need one index to determine the group name, so we take the last series, as we also use that to sort the groups by.
	lastInputSeriesIdx      int
	isClassicHistogramGroup bool // Denotes if it is the group where `le` has been dropped or not. Used to sort output series.
}

// Each series belongs to 2 groups. One with the `le` label, and one without. Sometimes, these are the same group.
type seriesGroupPair struct {
	bucketValue           string       // Contains the value of `le` for that series. An empty string ("") may mean no `le` label was present.
	classicHistogramGroup *bucketGroup // The group for the input series without an `le` label
	nativeHistogramGroup  *bucketGroup // The group for the input series with all labels
}

var bucketGroupPool = zeropool.New(func() *bucketGroup {
	return &bucketGroup{}
})

var pointBucketPool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedPointsPerSeries, func(size int) []promql.Buckets {
		return make([]promql.Buckets, 0, size)
	}),
	limiter.BucketsSlices,
	uint64(unsafe.Sizeof(promql.Buckets{})),
	true,
	mangleBuckets,
	nil,
)

func mangleBuckets(b promql.Buckets) promql.Buckets {
	for i := range b {
		b[i].UpperBound = 12345678
		b[i].Count = 12345678
	}
	return b
}

const maxExpectedBucketsPerHistogram = 64 // There isn't much science to this

var bucketSliceBucketedPool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(maxExpectedBucketsPerHistogram, func(size int) promql.Buckets {
		return make([]promql.Bucket, 0, size)
	}),
	limiter.BucketSlices,
	uint64(unsafe.Sizeof(promql.Bucket{})),
	true,
	nil,
	nil,
)

func NewHistogramQuantileFunction(
	phArg types.ScalarOperator,
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *HistogramFunction {
	innerSeriesMetricNames := &operators.MetricNames{}

	return &HistogramFunction{
		f: &histogramQuantile{
			phArg:                    phArg,
			memoryConsumptionTracker: memoryConsumptionTracker,
			annotations:              annotations,
			innerSeriesMetricNames:   innerSeriesMetricNames,
			innerExpressionPosition:  inner.ExpressionPosition(),
			enableDelayedNameRemoval: enableDelayedNameRemoval,
		},
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations,
		innerSeriesMetricNames:   innerSeriesMetricNames,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}
}

func NewHistogramFractionFunction(
	lower types.ScalarOperator,
	upper types.ScalarOperator,
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *HistogramFunction {
	innerSeriesMetricNames := &operators.MetricNames{}

	return &HistogramFunction{
		f: &histogramFraction{
			upperArg:                 upper,
			lowerArg:                 lower,
			memoryConsumptionTracker: memoryConsumptionTracker,
			innerSeriesMetricNames:   innerSeriesMetricNames,
			innerExpressionPosition:  inner.ExpressionPosition(),
		},
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations,
		innerSeriesMetricNames:   innerSeriesMetricNames,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}
}

func (h *HistogramFunction) ExpressionPosition() posrange.PositionRange {
	return h.expressionPosition
}

func (h *HistogramFunction) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if err := h.f.LoadArguments(ctx); err != nil {
		return nil, err
	}

	innerSeries, err := h.inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerSeries, h.memoryConsumptionTracker)

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	h.innerSeriesMetricNames.CaptureMetricNames(innerSeries)
	groups := map[string]groupWithLabels{}
	h.seriesGroupPairs = make([]seriesGroupPair, len(innerSeries))
	b := make([]byte, 0, 1024)
	lb := labels.NewBuilder(labels.EmptyLabels())

	for innerIdx, series := range innerSeries {
		// Each series belongs to two groups, one without the `le` label, and one with all labels.
		// Sometimes these are the same group.

		// Store the le label. If it doesn't exist, it'll be an empty string
		le := series.Labels.Get(labels.BucketLabel)
		h.seriesGroupPairs[innerIdx].bucketValue = le

		// First get the group with all labels
		b = series.Labels.Bytes(b)
		g, groupExists := groups[string(b)]
		if !groupExists {
			g.labels = series.Labels
			g.group = bucketGroupPool.Get()
			groups[string(b)] = g
		}
		g.group.lastInputSeriesIdx = innerIdx
		g.group.remainingSeriesCount++
		h.seriesGroupPairs[innerIdx].nativeHistogramGroup = g.group

		// Then get the group without the `le` label. This may be the same
		// as the previous group if no le label exists.
		// We still need to do this (rather than set it to nil) so that we know when to emit
		// NewBadBucketLabelWarning when the series are processed.
		b = series.Labels.BytesWithoutLabels(b, labels.BucketLabel)
		g, groupExists = groups[string(b)]

		if !groupExists {
			lb.Reset(series.Labels)
			lb.Del(labels.BucketLabel)
			g.labels = lb.Labels()
			g.group = bucketGroupPool.Get()
			g.group.isClassicHistogramGroup = true
			groups[string(b)] = g
		}
		g.group.lastInputSeriesIdx = innerIdx
		g.group.remainingSeriesCount++
		h.seriesGroupPairs[innerIdx].classicHistogramGroup = g.group
	}

	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(groups), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	h.remainingGroups = make([]*bucketGroup, 0, len(groups))
	for _, g := range groups {
		var labelsMetadata types.SeriesMetadata
		if h.enableDelayedNameRemoval {
			labelsMetadata = types.SeriesMetadata{Labels: g.labels, DropName: true}
		} else {
			//nolint:staticcheck // SA1019: DropMetricName is deprecated.
			labelsMetadata = types.SeriesMetadata{Labels: g.labels.DropMetricName()}
		}
		seriesMetadata, err = types.AppendSeriesMetadata(h.memoryConsumptionTracker, seriesMetadata, labelsMetadata)
		if err != nil {
			return nil, err
		}

		h.remainingGroups = append(h.remainingGroups, g.group)
	}

	// Sort the groups by the last series within them. This helps finish a group
	// as soon as possible when accumulating them.
	sort.Sort(bucketGroupSorter{seriesMetadata, h.remainingGroups})

	return seriesMetadata, nil
}

func (h *HistogramFunction) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(h.remainingGroups) == 0 {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Determine next group to return
	thisGroup := h.remainingGroups[0]
	h.remainingGroups = h.remainingGroups[1:]
	defer func() {
		// Reset the group before returning to the pool
		thisGroup.lastInputSeriesIdx = 0
		pointBucketPool.Put(&thisGroup.pointBuckets, h.memoryConsumptionTracker)
		thisGroup.nativeHistograms = nil
		thisGroup.remainingSeriesCount = 0
		bucketGroupPool.Put(thisGroup)
	}()

	// Iterate through inner series until the desired group is complete
	if err := h.accumulateUntilGroupComplete(ctx, thisGroup); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return h.computeOutputSeriesForGroup(thisGroup)
}

// getMetricNameForSeries returns the metric name from innerSeriesMetricNames for the given series index.
// If enableDelayedNameRemoval is not enabled, this func will return "" to maintain compatibility with Prometheus.
func (h *HistogramFunction) getMetricNameForSeries(seriesIndex int) string {
	if h.enableDelayedNameRemoval {
		return h.innerSeriesMetricNames.GetMetricNameForSeries(seriesIndex)
	} else {
		return intentionallyEmptyMetricName
	}
}

// accumulateUntilGroupComplete gathers all the series associated with the given bucketGroup
// As each inner series is selected, it is added into its respective groups.
// This means a group other than the one we are focused on may get completed first, but we
// continue until the desired group is ready.
func (h *HistogramFunction) accumulateUntilGroupComplete(ctx context.Context, g *bucketGroup) error {
	for g.remainingSeriesCount > 0 {
		s, err := h.inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}
			return err
		}
		thisSeriesGroups := h.seriesGroupPairs[h.currentInnerSeriesIndex]

		// Native histograms only ever go to their original labelset.
		// Floats only ever go to their group.
		// It is possible that a series has both floats and histograms.
		// It is also possible that both series groups are the same.
		// The conflict in points is then detected in computeOutputSeriesForGroup.
		err = h.saveNativeHistogramsToGroup(s.Histograms, thisSeriesGroups.nativeHistogramGroup)
		if err != nil {
			return err
		}
		err = h.saveFloatsToGroup(s.Floats, thisSeriesGroups.bucketValue, thisSeriesGroups.classicHistogramGroup)
		if err != nil {
			return err
		}

		// We are done with the FPoints, so return these now
		// HPoints are returned to the pool after computeOutputSeriesForGroup is finished with them as they may be copied to a group.
		types.FPointSlicePool.Put(&s.Floats, h.memoryConsumptionTracker)
		h.currentInnerSeriesIndex++
	}
	return nil
}

// saveFloatsToGroup places each FPoint into a bucket with the upperBound set by the input series.
func (h *HistogramFunction) saveFloatsToGroup(fPoints []promql.FPoint, le string, g *bucketGroup) error {
	g.remainingSeriesCount--
	if len(fPoints) == 0 {
		return nil
	}

	upperBound, err := strconv.ParseFloat(le, 64)
	if err != nil {
		// The le label was invalid. Record it:
		h.annotations.Add(annotations.NewBadBucketLabelWarning(
			h.getMetricNameForSeries(g.lastInputSeriesIdx),
			le,
			h.inner.ExpressionPosition(),
		))
		return nil
	}

	if g.pointBuckets == nil {
		g.pointBuckets, err = pointBucketPool.Get(h.timeRange.StepCount, h.memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.pointBuckets = g.pointBuckets[:h.timeRange.StepCount]
	}
	for _, f := range fPoints {
		pointIdx := h.timeRange.PointIndex(f.T)

		if g.pointBuckets[pointIdx] == nil {
			// Remaining series count + 1 since we decrement the series count early to simplify each return point.
			maxBuckets := int(g.remainingSeriesCount) + 1
			g.pointBuckets[pointIdx], err = bucketSliceBucketedPool.Get(maxBuckets, h.memoryConsumptionTracker)
			if err != nil {
				return err
			}
			g.pointBuckets[pointIdx] = g.pointBuckets[pointIdx][:]
		}

		bucketIdx := len(g.pointBuckets[pointIdx])
		g.pointBuckets[pointIdx] = g.pointBuckets[pointIdx][:bucketIdx+1]
		g.pointBuckets[pointIdx][bucketIdx].UpperBound = upperBound
		g.pointBuckets[pointIdx][bucketIdx].Count = f.F
	}

	return nil
}

// saveNativeHistogramsToGroup stores the given native histograms onto the given group.
// There should only ever be one native histogram per step for a series or series group.
// In general, they are per-series, but we handle them as parts of groups because they
// could hypothetically have the same series name as a classic histogram.
func (h *HistogramFunction) saveNativeHistogramsToGroup(hPoints []promql.HPoint, g *bucketGroup) error {
	g.remainingSeriesCount--
	if len(hPoints) == 0 {
		return nil
	}

	// We should only ever see one set of native histograms per group.
	if g.nativeHistograms != nil {
		return fmt.Errorf("we should never see more than one native histogram per group")
	}
	g.nativeHistograms = hPoints
	return nil
}

func (h *HistogramFunction) computeOutputSeriesForGroup(g *bucketGroup) (types.InstantVectorSeriesData, error) {
	// We only allocate floatPoints from the pool once we know for certain we'll return some points.
	// We also then only need to get a length for the remaining points.
	var floatPoints []promql.FPoint
	histogramIndex := 0

	for pointIdx := range h.timeRange.StepCount {
		var currentHistogram *histogram.FloatHistogram
		var thisPointBuckets promql.Buckets

		if g.pointBuckets != nil && len(g.pointBuckets[pointIdx]) > 0 {
			thisPointBuckets = g.pointBuckets[pointIdx]
		}

		// Get the HPoint if it exists at this step
		if g.nativeHistograms != nil && histogramIndex < len(g.nativeHistograms) {
			nextHPoint := g.nativeHistograms[histogramIndex]
			if h.timeRange.PointIndex(nextHPoint.T) == int64(pointIdx) {
				currentHistogram = nextHPoint.H
				histogramIndex++
			}
		}

		if thisPointBuckets != nil && currentHistogram != nil {
			// At this data point, we have classic histogram buckets and a native histogram with the same name and labels.
			// No value is returned, so emit an annotation and continue.
			h.annotations.Add(annotations.NewMixedClassicNativeHistogramsWarning(
				h.getMetricNameForSeries(g.lastInputSeriesIdx), h.inner.ExpressionPosition(),
			))
			continue
		}

		if thisPointBuckets != nil {
			res := h.f.ComputeClassicHistogramResult(pointIdx, g.lastInputSeriesIdx, thisPointBuckets)

			if floatPoints == nil {
				var err error
				floatPoints, err = types.FPointSlicePool.Get(h.timeRange.StepCount-pointIdx, h.memoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			floatPoints = append(floatPoints, promql.FPoint{
				T: h.timeRange.IndexTime(int64(pointIdx)),
				F: res,
			})

			continue
		}

		if currentHistogram != nil {
			if floatPoints == nil {
				var err error
				floatPoints, err = types.FPointSlicePool.Get(h.timeRange.StepCount-pointIdx, h.memoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			res, annos := h.f.ComputeNativeHistogramResult(pointIdx, g.lastInputSeriesIdx, currentHistogram)

			if annos != nil {
				h.annotations.Merge(annos)
			}

			floatPoints = append(floatPoints, promql.FPoint{
				T: h.timeRange.IndexTime(int64(pointIdx)),
				F: res,
			})
		}
	}

	// Return any retained native histogram to the pool
	if g.nativeHistograms != nil {
		types.HPointSlicePool.Put(&g.nativeHistograms, h.memoryConsumptionTracker)
	}

	// We are done with all the point buckets, so return all those to the pool too
	for _, b := range g.pointBuckets {
		bucketSliceBucketedPool.Put(&b, h.memoryConsumptionTracker)
	}

	return types.InstantVectorSeriesData{Floats: floatPoints}, nil
}

func (h *HistogramFunction) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := h.f.Prepare(ctx, params); err != nil {
		return err
	}

	return h.inner.Prepare(ctx, params)
}

func (h *HistogramFunction) AfterPrepare(ctx context.Context) error {
	if err := h.f.AfterPrepare(ctx); err != nil {
		return err
	}

	return h.inner.AfterPrepare(ctx)
}

func (h *HistogramFunction) Finalize(ctx context.Context) error {
	if err := h.f.Finalize(ctx); err != nil {
		return err
	}

	return h.inner.Finalize(ctx)
}

func (h *HistogramFunction) Close() {
	h.inner.Close()
	h.f.Close()
}

type bucketGroupSorter struct {
	metadata []types.SeriesMetadata
	groups   []*bucketGroup
}

func (g bucketGroupSorter) Len() int {
	return len(g.metadata)
}

func (g bucketGroupSorter) Less(i, j int) bool {
	if g.groups[i].lastInputSeriesIdx != g.groups[j].lastInputSeriesIdx {
		return g.groups[i].lastInputSeriesIdx < g.groups[j].lastInputSeriesIdx
	}
	// return classic histogram groups first
	return g.groups[i].isClassicHistogramGroup && !g.groups[j].isClassicHistogramGroup
}

func (g bucketGroupSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.groups[i], g.groups[j] = g.groups[j], g.groups[i]
}

type histogramFunction interface {
	LoadArguments(ctx context.Context) error
	ComputeClassicHistogramResult(pointIndex int, seriesIndex int, buckets promql.Buckets) float64
	ComputeNativeHistogramResult(pointIndex int, seriesIndex int, h *histogram.FloatHistogram) (float64, annotations.Annotations)
	Prepare(ctx context.Context, params *types.PrepareParams) error
	AfterPrepare(ctx context.Context) error
	Finalize(ctx context.Context) error
	Close()
}

type histogramQuantile struct {
	phArg    types.ScalarOperator
	phValues types.ScalarData

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	annotations              *annotations.Annotations
	innerSeriesMetricNames   *operators.MetricNames
	innerExpressionPosition  posrange.PositionRange
	enableDelayedNameRemoval bool
}

func (q *histogramQuantile) LoadArguments(ctx context.Context) error {
	var err error
	q.phValues, err = q.phArg.GetValues(ctx)
	if err != nil {
		return err
	}

	for _, s := range q.phValues.Samples {
		ph := s.F
		if math.IsNaN(ph) || ph < 0 || ph > 1 {
			// Even when ph is invalid we still return a series as BucketQuantile will return +/-Inf.
			// Additionally, even if a point isn't returned, an annotation is emitted if ph is invalid
			// at any step.
			q.annotations.Add(annotations.NewInvalidQuantileWarning(ph, q.phArg.ExpressionPosition()))
		}
	}

	return nil
}

// getMetricNameForSeries returns the metric name from innerSeriesMetricNames for the given series index.
// If enableDelayedNameRemoval is not enabled, this func will return "" to maintain compatibility with Prometheus.
func (q *histogramQuantile) getMetricNameForSeries(seriesIndex int) string {
	if q.enableDelayedNameRemoval {
		return q.innerSeriesMetricNames.GetMetricNameForSeries(seriesIndex)
	} else {
		return intentionallyEmptyMetricName
	}
}

func (q *histogramQuantile) ComputeClassicHistogramResult(pointIndex int, seriesIndex int, buckets promql.Buckets) float64 {
	ph := q.phValues.Samples[pointIndex].F
	res, forcedMonotonicity, _ := promql.BucketQuantile(ph, buckets)

	if forcedMonotonicity {
		q.annotations.Add(annotations.NewHistogramQuantileForcedMonotonicityInfo(
			q.getMetricNameForSeries(seriesIndex),
			q.innerExpressionPosition,
		))
	}

	return res
}

func (q *histogramQuantile) ComputeNativeHistogramResult(pointIndex int, seriesIndex int, h *histogram.FloatHistogram) (float64, annotations.Annotations) {
	ph := q.phValues.Samples[pointIndex].F
	return promql.HistogramQuantile(ph, h, q.getMetricNameForSeries(seriesIndex), q.innerExpressionPosition)
}

func (q *histogramQuantile) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return q.phArg.Prepare(ctx, params)
}

func (q *histogramQuantile) AfterPrepare(ctx context.Context) error {
	return q.phArg.AfterPrepare(ctx)
}

func (q *histogramQuantile) Finalize(ctx context.Context) error {
	return q.phArg.Finalize(ctx)
}

func (q *histogramQuantile) Close() {
	q.phArg.Close()

	types.FPointSlicePool.Put(&q.phValues.Samples, q.memoryConsumptionTracker)
}

type histogramFraction struct {
	lowerArg    types.ScalarOperator
	upperArg    types.ScalarOperator
	lowerValues types.ScalarData
	upperValues types.ScalarData

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	innerSeriesMetricNames   *operators.MetricNames
	innerExpressionPosition  posrange.PositionRange
}

func (f *histogramFraction) LoadArguments(ctx context.Context) error {
	var err error
	f.lowerValues, err = f.lowerArg.GetValues(ctx)
	if err != nil {
		return err
	}

	f.upperValues, err = f.upperArg.GetValues(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (f *histogramFraction) ComputeClassicHistogramResult(pointIndex int, _ int, buckets promql.Buckets) float64 {
	lower := f.lowerValues.Samples[pointIndex].F
	upper := f.upperValues.Samples[pointIndex].F

	return promql.BucketFraction(lower, upper, buckets)
}

func (f *histogramFraction) ComputeNativeHistogramResult(pointIndex int, seriesIndex int, h *histogram.FloatHistogram) (float64, annotations.Annotations) {
	lower := f.lowerValues.Samples[pointIndex].F
	upper := f.upperValues.Samples[pointIndex].F

	return promql.HistogramFraction(lower, upper, h, f.innerSeriesMetricNames.GetMetricNameForSeries(seriesIndex), f.innerExpressionPosition)
}

func (f *histogramFraction) Prepare(ctx context.Context, params *types.PrepareParams) error {
	err := f.lowerArg.Prepare(ctx, params)
	if err != nil {
		return err
	}
	return f.upperArg.Prepare(ctx, params)
}

func (f *histogramFraction) AfterPrepare(ctx context.Context) error {
	err := f.lowerArg.AfterPrepare(ctx)
	if err != nil {
		return err
	}
	return f.upperArg.AfterPrepare(ctx)
}

func (f *histogramFraction) Finalize(ctx context.Context) error {
	err := f.lowerArg.Finalize(ctx)
	if err != nil {
		return err
	}
	return f.upperArg.Finalize(ctx)
}

func (f *histogramFraction) Close() {
	f.lowerArg.Close()
	f.upperArg.Close()

	types.FPointSlicePool.Put(&f.lowerValues.Samples, f.memoryConsumptionTracker)
	types.FPointSlicePool.Put(&f.upperValues.Samples, f.memoryConsumptionTracker)
}
