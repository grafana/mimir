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

// histogramGrouper holds the state and logic shared by HistogramFunction and
// HistogramQuantilesFunction for collating an instant vector's classic and native histogram series
// into bucketGroups and accumulating their points.
type histogramGrouper struct {
	inner                    types.InstantVectorOperator
	currentInnerSeriesIndex  int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool

	annotations            annotations.Annotations
	innerSeriesMetricNames *operators.MetricNames // We need to keep track of the metric names for annotations.NewBadBucketLabelWarning

	seriesGroupPairs []seriesGroupPair // Each series belongs to 2 groups. One with the `le` label, and one without. Sometimes, these are the same group.
}

// HistogramFunction performs a function over each series in an instant vector,
// with special handling for classic and native histograms.
// At the moment, it supports only histogram_quantile and histogram_fraction.
type HistogramFunction struct {
	histogramGrouper

	f                  histogramFunction
	expressionPosition posrange.PositionRange

	remainingGroups []*bucketGroup // One entry per group, in the order we want to return them.
	nextGroupIdx    int            // Index into remainingGroups for the next group to return.
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

const (
	seriesGroupPairSize    = uint64(unsafe.Sizeof(seriesGroupPair{}))
	bucketGroupPointerSize = uint64(unsafe.Sizeof((*bucketGroup)(nil)))
)

// seriesGroupPairPool is defined locally and not added to the collection of pools provided by the types package
// because it is being used for seriesGroupPair which is not an exported type.
// If seriesGroupPair were to be exported then this pool should be moved into limiting_pool.go
var seriesGroupPairPool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []seriesGroupPair {
		return make([]seriesGroupPair, 0, size)
	}),
	limiter.SeriesGroupPairSlices,
	seriesGroupPairSize,
	true, // clearOnGet: zero out stale pointers and strings from previous use
	nil,
	nil,
)

// bucketGroupPointerSlicePool is defined locally for the same reason as seriesGroupPairPool above.
var bucketGroupPointerSlicePool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []*bucketGroup {
		return make([]*bucketGroup, 0, size)
	}),
	limiter.BucketGroupPointerSlices,
	bucketGroupPointerSize,
	true, // clearOnGet: zero out stale pointers from previous use
	nil,
	nil,
)

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
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *HistogramFunction {
	innerSeriesMetricNames := &operators.MetricNames{}

	return &HistogramFunction{
		f: &histogramQuantile{
			phArg:                    phArg,
			memoryConsumptionTracker: memoryConsumptionTracker,
			innerSeriesMetricNames:   innerSeriesMetricNames,
			innerExpressionPosition:  inner.ExpressionPosition(),
			enableDelayedNameRemoval: enableDelayedNameRemoval,
		},
		histogramGrouper: histogramGrouper{
			inner:                    inner,
			memoryConsumptionTracker: memoryConsumptionTracker,
			innerSeriesMetricNames:   innerSeriesMetricNames,
			timeRange:                timeRange,
			enableDelayedNameRemoval: enableDelayedNameRemoval,
		},
		expressionPosition: expressionPosition,
	}
}

func NewHistogramFractionFunction(
	lower types.ScalarOperator,
	upper types.ScalarOperator,
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
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
		histogramGrouper: histogramGrouper{
			inner:                    inner,
			memoryConsumptionTracker: memoryConsumptionTracker,
			innerSeriesMetricNames:   innerSeriesMetricNames,
			timeRange:                timeRange,
			enableDelayedNameRemoval: enableDelayedNameRemoval,
		},
		expressionPosition: expressionPosition,
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
	groups, err := h.buildGroups(innerSeries)
	if err != nil {
		return nil, err
	}

	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(groups), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	h.remainingGroups, err = bucketGroupPointerSlicePool.Get(len(groups), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
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
	if h.nextGroupIdx >= len(h.remainingGroups) {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisGroup := h.remainingGroups[h.nextGroupIdx]
	h.nextGroupIdx++
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

// buildGroups collates the inner series into bucketGroups: each series belongs to a group keyed by
// its full label set (used for native histograms) and a group with the `le` label removed (used for
// classic histogram buckets), which may be the same group. It allocates and populates
// g.seriesGroupPairs and returns the groups keyed by their label bytes.
func (g *histogramGrouper) buildGroups(innerSeries []types.SeriesMetadata) (map[string]groupWithLabels, error) {
	var err error
	g.seriesGroupPairs, err = seriesGroupPairPool.Get(len(innerSeries), g.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	g.seriesGroupPairs = g.seriesGroupPairs[:len(innerSeries)]

	groups := map[string]groupWithLabels{}
	b := make([]byte, 0, 1024)
	lb := labels.NewBuilder(labels.EmptyLabels())

	for innerIdx, series := range innerSeries {
		// Each series belongs to two groups, one without the `le` label, and one with all labels.
		// Sometimes these are the same group.

		// Store the le label. If it doesn't exist, it'll be an empty string
		le := series.Labels.Get(labels.BucketLabel)
		g.seriesGroupPairs[innerIdx].bucketValue = le

		// First get the group with all labels
		b = series.Labels.Bytes(b)
		grp, groupExists := groups[string(b)]
		if !groupExists {
			grp.labels = series.Labels
			grp.group = bucketGroupPool.Get()
			groups[string(b)] = grp
		}
		grp.group.lastInputSeriesIdx = innerIdx
		grp.group.remainingSeriesCount++
		g.seriesGroupPairs[innerIdx].nativeHistogramGroup = grp.group

		// Then get the group without the `le` label. This may be the same
		// as the previous group if no le label exists.
		// We still need to do this (rather than set it to nil) so that we know when to emit
		// NewBadBucketLabelWarning when the series are processed.
		b = series.Labels.BytesWithoutLabels(b, labels.BucketLabel)
		grp, groupExists = groups[string(b)]

		if !groupExists {
			lb.Reset(series.Labels)
			lb.Del(labels.BucketLabel)
			grp.labels = lb.Labels()
			grp.group = bucketGroupPool.Get()
			grp.group.isClassicHistogramGroup = true
			groups[string(b)] = grp
		}
		grp.group.lastInputSeriesIdx = innerIdx
		grp.group.remainingSeriesCount++
		g.seriesGroupPairs[innerIdx].classicHistogramGroup = grp.group
	}

	return groups, nil
}

// getMetricNameForSeries returns the metric name from innerSeriesMetricNames for the given series index.
// If enableDelayedNameRemoval is not enabled, this func will return "" to maintain compatibility with Prometheus.
func (g *histogramGrouper) getMetricNameForSeries(seriesIndex int) string {
	if g.enableDelayedNameRemoval {
		return g.innerSeriesMetricNames.GetMetricNameForSeries(seriesIndex)
	} else {
		return intentionallyEmptyMetricName
	}
}

// accumulateUntilGroupComplete gathers all the series associated with the given bucketGroup
// As each inner series is selected, it is added into its respective groups.
// This means a group other than the one we are focused on may get completed first, but we
// continue until the desired group is ready.
func (g *histogramGrouper) accumulateUntilGroupComplete(ctx context.Context, group *bucketGroup) error {
	for group.remainingSeriesCount > 0 {
		s, err := g.inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}
			return err
		}
		thisSeriesGroups := g.seriesGroupPairs[g.currentInnerSeriesIndex]

		// Native histograms only ever go to their original labelset.
		// Floats only ever go to their group.
		// It is possible that a series has both floats and histograms.
		// It is also possible that both series groups are the same.
		// The conflict in points is then detected when computing output series
		// (computeOutputSeriesForGroup / computeOutputSeriesForQuantile).
		err = g.saveNativeHistogramsToGroup(s.Histograms, thisSeriesGroups.nativeHistogramGroup)
		if err != nil {
			return err
		}
		err = g.saveFloatsToGroup(s.Floats, thisSeriesGroups.bucketValue, thisSeriesGroups.classicHistogramGroup)
		if err != nil {
			return err
		}

		// We are done with the FPoints, so return these now.
		// HPoints are not returned here: they may be copied to a group and are still needed while
		// computing output series (and, for histogram_quantiles, reused across quantiles), so the
		// owning operator returns them to the pool once it is finished with them.
		types.FPointSlicePool.Put(&s.Floats, g.memoryConsumptionTracker)
		g.currentInnerSeriesIndex++
	}
	return nil
}

// saveFloatsToGroup places each FPoint into a bucket with the upperBound set by the input series.
func (g *histogramGrouper) saveFloatsToGroup(fPoints []promql.FPoint, le string, group *bucketGroup) error {
	group.remainingSeriesCount--
	if len(fPoints) == 0 {
		return nil
	}

	upperBound, err := strconv.ParseFloat(le, 64)
	if err != nil {
		// The le label was invalid. Record it:
		g.annotations.Add(annotations.NewBadBucketLabelWarning(
			g.getMetricNameForSeries(group.lastInputSeriesIdx),
			le,
			g.inner.ExpressionPosition(),
		))
		return nil
	}

	if group.pointBuckets == nil {
		group.pointBuckets, err = pointBucketPool.Get(g.timeRange.StepCount, g.memoryConsumptionTracker)
		if err != nil {
			return err
		}
		group.pointBuckets = group.pointBuckets[:g.timeRange.StepCount]
	}
	for _, f := range fPoints {
		pointIdx := g.timeRange.PointIndex(f.T)

		if group.pointBuckets[pointIdx] == nil {
			// Remaining series count + 1 since we decrement the series count early to simplify each return point.
			maxBuckets := int(group.remainingSeriesCount) + 1
			group.pointBuckets[pointIdx], err = bucketSliceBucketedPool.Get(maxBuckets, g.memoryConsumptionTracker)
			if err != nil {
				return err
			}
			group.pointBuckets[pointIdx] = group.pointBuckets[pointIdx][:]
		}

		bucketIdx := len(group.pointBuckets[pointIdx])
		group.pointBuckets[pointIdx] = group.pointBuckets[pointIdx][:bucketIdx+1]
		group.pointBuckets[pointIdx][bucketIdx].UpperBound = upperBound
		group.pointBuckets[pointIdx][bucketIdx].Count = f.F
	}

	return nil
}

// saveNativeHistogramsToGroup stores the given native histograms onto the given group.
// There should only ever be one native histogram per step for a series or series group.
// In general, they are per-series, but we handle them as parts of groups because they
// could hypothetically have the same series name as a classic histogram.
func (g *histogramGrouper) saveNativeHistogramsToGroup(hPoints []promql.HPoint, group *bucketGroup) error {
	group.remainingSeriesCount--
	if len(hPoints) == 0 {
		return nil
	}

	// We should only ever see one set of native histograms per group.
	if group.nativeHistograms != nil {
		return fmt.Errorf("we should never see more than one native histogram per group")
	}
	group.nativeHistograms = hPoints
	return nil
}

// appendOutputPoint appends a single output point at pointIdx to floatPoints, allocating the slice
// from the pool on first use. The slice is only allocated once we know we'll return at least one
// point, and is sized for the remaining steps. Shared by the computeOutputSeries* methods.
func (g *histogramGrouper) appendOutputPoint(floatPoints []promql.FPoint, pointIdx int, f float64) ([]promql.FPoint, error) {
	if floatPoints == nil {
		var err error
		floatPoints, err = types.FPointSlicePool.Get(g.timeRange.StepCount-pointIdx, g.memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
	}

	return append(floatPoints, promql.FPoint{
		T: g.timeRange.IndexTime(int64(pointIdx)),
		F: f,
	}), nil
}

// computeOutputPoints walks the group's points step by step, selecting the classic histogram buckets
// or the native histogram present at each step. When both are present it emits a mixed-histogram
// warning and skips the step; otherwise it calls computeClassic or computeNative to obtain the output
// value and appends it. Shared by the computeOutputSeries* methods, which differ only in how they
// compute each value and how they clean up afterwards.
func (g *histogramGrouper) computeOutputPoints(
	group *bucketGroup,
	computeClassic func(pointIdx int, buckets promql.Buckets) (float64, error),
	computeNative func(pointIdx int, h *histogram.FloatHistogram) (float64, error),
) ([]promql.FPoint, error) {
	// We only allocate floatPoints from the pool once we know for certain we'll return some points.
	var floatPoints []promql.FPoint
	histogramIndex := 0

	for pointIdx := range g.timeRange.StepCount {
		var currentHistogram *histogram.FloatHistogram
		var thisPointBuckets promql.Buckets

		if group.pointBuckets != nil && len(group.pointBuckets[pointIdx]) > 0 {
			thisPointBuckets = group.pointBuckets[pointIdx]
		}

		// Get the HPoint if it exists at this step
		if group.nativeHistograms != nil && histogramIndex < len(group.nativeHistograms) {
			nextHPoint := group.nativeHistograms[histogramIndex]
			if g.timeRange.PointIndex(nextHPoint.T) == int64(pointIdx) {
				currentHistogram = nextHPoint.H
				histogramIndex++
			}
		}

		if thisPointBuckets != nil && currentHistogram != nil {
			// At this data point, we have classic histogram buckets and a native histogram with the same name and labels.
			// No value is returned, so emit an annotation and continue.
			g.annotations.Add(annotations.NewMixedClassicNativeHistogramsWarning(
				g.getMetricNameForSeries(group.lastInputSeriesIdx), g.inner.ExpressionPosition(),
			))
			continue
		}

		var (
			res float64
			err error
		)
		switch {
		case thisPointBuckets != nil:
			res, err = computeClassic(pointIdx, thisPointBuckets)
		case currentHistogram != nil:
			res, err = computeNative(pointIdx, currentHistogram)
		default:
			continue
		}
		if err != nil {
			return nil, err
		}

		if floatPoints, err = g.appendOutputPoint(floatPoints, pointIdx, res); err != nil {
			return nil, err
		}
	}

	return floatPoints, nil
}

func (h *HistogramFunction) computeOutputSeriesForGroup(g *bucketGroup) (types.InstantVectorSeriesData, error) {
	floatPoints, err := h.computeOutputPoints(g,
		func(pointIdx int, buckets promql.Buckets) (float64, error) {
			return h.f.ComputeClassicHistogramResult(pointIdx, g.lastInputSeriesIdx, buckets), nil
		},
		func(pointIdx int, hist *histogram.FloatHistogram) (float64, error) {
			res, annos := h.f.ComputeNativeHistogramResult(pointIdx, g.lastInputSeriesIdx, hist)
			if annos != nil {
				h.annotations.Merge(annos)
			}
			return res, nil
		},
	)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
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

func (h *HistogramFunction) FinishedReading(ctx context.Context) error {
	seriesGroupPairPool.Put(&h.seriesGroupPairs, h.memoryConsumptionTracker)
	bucketGroupPointerSlicePool.Put(&h.remainingGroups, h.memoryConsumptionTracker)
	h.nextGroupIdx = 0

	if err := h.f.FinishedReading(ctx); err != nil {
		return err
	}

	return h.inner.FinishedReading(ctx)
}

func (h *HistogramFunction) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := types.FinalizeAndCombine[types.Finalizer](ctx, h.f, h.inner)
	if err != nil {
		return nil, nil, err
	}

	h.annotations.Merge(childAnnos)

	return stats, h.annotations, nil
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
	FinishedReading(ctx context.Context) error
	Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error)
	Close()
}

type histogramQuantile struct {
	phArg    types.ScalarOperator
	phValues types.ScalarData

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	annotations              annotations.Annotations
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
	quantile, forcedMonotonicity, _, _, _, _ := promql.BucketQuantile(ph, buckets)

	if forcedMonotonicity {
		// Set the last few values to 0 to use original version of histogram quantile info
		// annotations for now until merging annotations is fully supported in MQE.
		q.annotations.Add(annotations.NewHistogramQuantileForcedMonotonicityInfo(
			q.getMetricNameForSeries(seriesIndex),
			q.innerExpressionPosition,
			0, 0, 0, 0,
		))
	}

	return quantile
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

func (q *histogramQuantile) FinishedReading(ctx context.Context) error {
	types.FPointSlicePool.Put(&q.phValues.Samples, q.memoryConsumptionTracker)
	return q.phArg.FinishedReading(ctx)
}

func (q *histogramQuantile) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := q.phArg.Finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	q.annotations.Merge(childAnnos)

	return stats, q.annotations, nil
}

func (q *histogramQuantile) Close() {
	q.phArg.Close()
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

func (f *histogramFraction) FinishedReading(ctx context.Context) error {
	types.FPointSlicePool.Put(&f.lowerValues.Samples, f.memoryConsumptionTracker)
	types.FPointSlicePool.Put(&f.upperValues.Samples, f.memoryConsumptionTracker)

	err := f.lowerArg.FinishedReading(ctx)
	if err != nil {
		return err
	}

	return f.upperArg.FinishedReading(ctx)
}

func (f *histogramFraction) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return types.FinalizeAndCombine(ctx, f.lowerArg, f.upperArg)
}

func (f *histogramFraction) Close() {
	f.lowerArg.Close()
	f.upperArg.Close()
}

// HistogramQuantilesFunction performs histogram_quantiles over each series in an instant vector.
// It outputs multiple series per input group (one per quantile).
type HistogramQuantilesFunction struct {
	histogramGrouper

	quantileArgs    []types.ScalarOperator
	quantileLabelOp types.StringOperator
	quantileLabel   string
	quantileValues  [][]promql.FPoint // Indexed by quantile, then by point index
	quantileLabels  []string          // Formatted quantile strings

	// Tracking state for series output
	remainingGroups    []*bucketGroup
	nextGroupIdx       int
	currentQuantileIdx int // Which quantile we're currently returning

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &HistogramQuantilesFunction{}

func NewHistogramQuantilesFunction(
	quantileArgs []types.ScalarOperator,
	quantileLabelOp types.StringOperator,
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *HistogramQuantilesFunction {
	return &HistogramQuantilesFunction{
		histogramGrouper: histogramGrouper{
			inner:                    inner,
			memoryConsumptionTracker: memoryConsumptionTracker,
			timeRange:                timeRange,
			enableDelayedNameRemoval: enableDelayedNameRemoval,
			innerSeriesMetricNames:   &operators.MetricNames{},
		},
		quantileArgs:       quantileArgs,
		quantileLabelOp:    quantileLabelOp,
		expressionPosition: expressionPosition,
	}
}

func (h *HistogramQuantilesFunction) ExpressionPosition() posrange.PositionRange {
	return h.expressionPosition
}

func (h *HistogramQuantilesFunction) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Load quantile label
	h.quantileLabel = h.quantileLabelOp.GetValue()

	// Load quantile arguments once for the entire query
	if h.quantileValues == nil {
		h.quantileValues = make([][]promql.FPoint, len(h.quantileArgs))
		h.quantileLabels = make([]string, len(h.quantileArgs))
		for i, arg := range h.quantileArgs {
			values, err := arg.GetValues(ctx)
			if err != nil {
				return nil, err
			}
			// Store the samples directly from the pool
			h.quantileValues[i] = values.Samples

			// Validate quantiles and format labels
			for _, s := range values.Samples {
				ph := s.F
				if math.IsNaN(ph) || ph < 0 || ph > 1 {
					h.annotations.Add(annotations.NewInvalidQuantileWarning(ph, arg.ExpressionPosition()))
				}
			}
			// The quantile label value is derived from the first sample only. For the common case where
			// the quantile is a literal (or any step-invariant scalar) this is constant across all steps
			// and matches Prometheus. For a quantile that varies per step (e.g. a non-constant scalar
			// subexpression in a range query), Prometheus would emit a different label value per step,
			// potentially splitting a single output series into several. MQE cannot do that because output
			// series labels are fixed here in SeriesMetadata before any values are read, so we keep the
			// step-0 value. This is a known, minor divergence for an unusual usage.
			if len(values.Samples) > 0 {
				h.quantileLabels[i] = labels.FormatOpenMetricsFloat(values.Samples[0].F)
			}
		}
	}

	innerSeries, err := h.inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerSeries, h.memoryConsumptionTracker)

	if len(innerSeries) == 0 {
		return nil, nil
	}

	h.innerSeriesMetricNames.CaptureMetricNames(innerSeries)

	groups, err := h.buildGroups(innerSeries)
	if err != nil {
		return nil, err
	}
	lb := labels.NewBuilder(labels.EmptyLabels())

	// Create output series: one per group per quantile
	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(groups)*len(h.quantileArgs), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	h.remainingGroups, err = bucketGroupPointerSlicePool.Get(len(groups), h.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	groupList := make([]*groupWithLabels, 0, len(groups))
	for _, g := range groups {
		g := g
		groupList = append(groupList, &g)
	}

	// Sort groups by last input series index
	sort.Slice(groupList, func(i, j int) bool {
		if groupList[i].group.lastInputSeriesIdx != groupList[j].group.lastInputSeriesIdx {
			return groupList[i].group.lastInputSeriesIdx < groupList[j].group.lastInputSeriesIdx
		}
		return groupList[i].group.isClassicHistogramGroup && !groupList[j].group.isClassicHistogramGroup
	})

	// For each group, create output series for each quantile
	for _, g := range groupList {
		h.remainingGroups = append(h.remainingGroups, g.group)
		for i := range h.quantileArgs {
			var labelsMetadata types.SeriesMetadata
			if h.enableDelayedNameRemoval {
				lb.Reset(g.labels)
				lb.Set(h.quantileLabel, h.quantileLabels[i])
				labelsMetadata = types.SeriesMetadata{Labels: lb.Labels(), DropName: true}
			} else {
				lb.Reset(g.labels.DropMetricName())
				lb.Set(h.quantileLabel, h.quantileLabels[i])
				labelsMetadata = types.SeriesMetadata{Labels: lb.Labels()}
			}
			seriesMetadata, err = types.AppendSeriesMetadata(h.memoryConsumptionTracker, seriesMetadata, labelsMetadata)
			if err != nil {
				return nil, err
			}
		}
	}

	return seriesMetadata, nil
}

func (h *HistogramQuantilesFunction) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	groupIdx := h.nextGroupIdx / len(h.quantileArgs)
	h.currentQuantileIdx = h.nextGroupIdx % len(h.quantileArgs)

	if groupIdx >= len(h.remainingGroups) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisGroup := h.remainingGroups[groupIdx]
	h.nextGroupIdx++

	// For the first quantile of each group, accumulate the data
	if h.currentQuantileIdx == 0 {
		if err := h.accumulateUntilGroupComplete(ctx, thisGroup); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	// Compute output for current quantile
	result, err := h.computeOutputSeriesForQuantile(thisGroup, h.currentQuantileIdx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Clean up after the last quantile for this group
	if h.currentQuantileIdx == len(h.quantileArgs)-1 {
		thisGroup.lastInputSeriesIdx = 0
		pointBucketPool.Put(&thisGroup.pointBuckets, h.memoryConsumptionTracker)
		if thisGroup.nativeHistograms != nil {
			types.HPointSlicePool.Put(&thisGroup.nativeHistograms, h.memoryConsumptionTracker)
			thisGroup.nativeHistograms = nil
		}
		thisGroup.remainingSeriesCount = 0
		bucketGroupPool.Put(thisGroup)
	}

	return result, nil
}

func (h *HistogramQuantilesFunction) computeOutputSeriesForQuantile(g *bucketGroup, quantileIdx int) (types.InstantVectorSeriesData, error) {
	floatPoints, err := h.computeOutputPoints(g,
		func(pointIdx int, buckets promql.Buckets) (float64, error) {
			phValue := h.quantileValues[quantileIdx][pointIdx].F
			quantile, forcedMonotonicity, _, _, _, _ := promql.BucketQuantile(phValue, buckets)
			if forcedMonotonicity {
				h.annotations.Add(annotations.NewHistogramQuantileForcedMonotonicityInfo(
					h.getMetricNameForSeries(g.lastInputSeriesIdx),
					h.inner.ExpressionPosition(),
					0, 0, 0, 0,
				))
			}
			return quantile, nil
		},
		func(pointIdx int, hist *histogram.FloatHistogram) (float64, error) {
			phValue := h.quantileValues[quantileIdx][pointIdx].F
			hq, annos := promql.HistogramQuantile(phValue, hist, h.getMetricNameForSeries(g.lastInputSeriesIdx), h.inner.ExpressionPosition())
			if annos != nil {
				h.annotations.Merge(annos)
			}
			return hq, nil
		},
	)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Note: native histograms are NOT returned to the pool here because we only process them
	// for the first quantile and need them for subsequent quantiles. They're returned in NextSeries
	// after the last quantile for this group is processed.

	// Clean up point buckets for the last quantile of this group
	if quantileIdx == len(h.quantileArgs)-1 {
		for _, b := range g.pointBuckets {
			bucketSliceBucketedPool.Put(&b, h.memoryConsumptionTracker)
		}
	}

	return types.InstantVectorSeriesData{Floats: floatPoints}, nil
}

func (h *HistogramQuantilesFunction) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := h.quantileLabelOp.Prepare(ctx, params); err != nil {
		return err
	}
	for _, arg := range h.quantileArgs {
		if err := arg.Prepare(ctx, params); err != nil {
			return err
		}
	}
	return h.inner.Prepare(ctx, params)
}

func (h *HistogramQuantilesFunction) AfterPrepare(ctx context.Context) error {
	if err := h.quantileLabelOp.AfterPrepare(ctx); err != nil {
		return err
	}
	for _, arg := range h.quantileArgs {
		if err := arg.AfterPrepare(ctx); err != nil {
			return err
		}
	}
	return h.inner.AfterPrepare(ctx)
}

func (h *HistogramQuantilesFunction) FinishedReading(ctx context.Context) error {
	seriesGroupPairPool.Put(&h.seriesGroupPairs, h.memoryConsumptionTracker)
	bucketGroupPointerSlicePool.Put(&h.remainingGroups, h.memoryConsumptionTracker)
	h.nextGroupIdx = 0
	h.currentQuantileIdx = 0

	// Return the scalar argument samples to the pool before signalling the scalar operators
	// that produced them are done.
	for _, samples := range h.quantileValues {
		types.FPointSlicePool.Put(&samples, h.memoryConsumptionTracker)
	}
	h.quantileValues = nil
	h.quantileLabels = nil

	if err := h.quantileLabelOp.FinishedReading(ctx); err != nil {
		return err
	}

	for _, arg := range h.quantileArgs {
		if err := arg.FinishedReading(ctx); err != nil {
			return err
		}
	}

	return h.inner.FinishedReading(ctx)
}

func (h *HistogramQuantilesFunction) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	finalizers := make([]types.Finalizer, 0, len(h.quantileArgs)+2)
	finalizers = append(finalizers, h.inner, h.quantileLabelOp)
	for _, arg := range h.quantileArgs {
		finalizers = append(finalizers, arg)
	}

	stats, childAnnos, err := types.FinalizeAndCombine(ctx, finalizers...)
	if err != nil {
		return nil, nil, err
	}

	h.annotations.Merge(childAnnos)

	return stats, h.annotations, nil
}

func (h *HistogramQuantilesFunction) Close() {
	h.inner.Close()
	h.quantileLabelOp.Close()
	for _, arg := range h.quantileArgs {
		arg.Close()
	}
}
