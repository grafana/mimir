// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type TopKBottomK struct {
	Inner                    types.InstantVectorOperator
	Param                    types.ScalarOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, NewTopKBottomK will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	IsTopK                   bool // If false, this is operator is for bottomk().

	expressionPosition posrange.PositionRange
	limit              []int64 // Maximum number of values to return at each time step for each group

	remainingInnerSeriesToGroup []*topKBottomKGroup // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*topKBottomKGroup // One entry per group, in the order we want to return them

	currentGroup              *topKBottomKGroup
	seriesIndexInCurrentGroup int

	annotations                            *annotations.Annotations
	haveEmittedIgnoredHistogramsAnnotation bool

	// Reuse the same heap instance to allow us to avoid allocating a new one every time.
	heap topKBottomKHeap
}

var _ types.InstantVectorOperator = &TopKBottomK{}

func NewTopKBottomK(
	inner types.InstantVectorOperator,
	param types.ScalarOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	isTopK bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) *TopKBottomK {
	if without {
		labelsToDrop := make([]string, 0, len(grouping)+1)
		labelsToDrop = append(labelsToDrop, labels.MetricName)
		labelsToDrop = append(labelsToDrop, grouping...)
		grouping = labelsToDrop
	}

	slices.Sort(grouping)

	var h topKBottomKHeap

	if isTopK {
		h = &topKHeap{}
	} else {
		h = &bottomKHeap{}
	}

	return &TopKBottomK{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		IsTopK:                   isTopK,

		expressionPosition: expressionPosition,
		annotations:        annotations,
		heap:               h,
	}
}

func (t *TopKBottomK) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if err := t.loadLimit(ctx); err != nil {
		return nil, err
	}

	innerSeries, err := t.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	groups := map[string]*topKBottomKGroup{}
	groupLabelsBytesFunc := t.groupLabelsBytesFunc()
	t.remainingInnerSeriesToGroup = make([]*topKBottomKGroup, 0, len(innerSeries))

	for seriesIdx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g = &topKBottomKGroup{}
			groups[string(groupLabelsString)] = g
		}

		g.totalSeries++
		g.lastSeriesIndex = seriesIdx
		t.remainingInnerSeriesToGroup = append(t.remainingInnerSeriesToGroup, g)
	}

	t.remainingGroups = make([]*topKBottomKGroup, 0, len(groups))

	for _, g := range groups {
		t.remainingGroups = append(t.remainingGroups, g)
	}

	// Sort the groups in the order we'll return them.
	slices.SortFunc(t.remainingGroups, func(a, b *topKBottomKGroup) int {
		return a.lastSeriesIndex - b.lastSeriesIndex
	})

	// Sort the series in the order we'll return them.
	// It's important that we use a stable sort here, so that the order we receive series (and therefore accumulate series into their groups)
	// matches the order we'll return those series in.
	sort.Stable(topKBottomKOutputSorter{
		metadata:       innerSeries,
		seriesToGroups: slices.Clone(t.remainingInnerSeriesToGroup), // We don't want to reorder remainingInnerSeriesToGroup, but we need to keep it in sync with the list of output series during sorting.
	})

	return innerSeries, nil
}

func (t *TopKBottomK) loadLimit(ctx context.Context) error {
	paramValues, err := t.Param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(paramValues.Samples, t.MemoryConsumptionTracker)

	t.limit, err = types.Int64SlicePool.Get(t.TimeRange.StepCount, t.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	t.limit = t.limit[:t.TimeRange.StepCount]

	for stepIdx := range t.TimeRange.StepCount {
		v := paramValues.Samples[stepIdx].F

		if !convertibleToInt64(v) {
			return fmt.Errorf("scalar value %v overflows int64", v)
		}

		t.limit[stepIdx] = max(int64(v), 0) // Ignore any negative values.
	}

	return nil
}

func convertibleToInt64(v float64) bool {
	return v <= math.MaxInt64 && v >= math.MinInt64
}

func (t *TopKBottomK) groupLabelsBytesFunc() seriesToGroupLabelsBytesFunc {
	return groupLabelsBytesFunc(t.Grouping, t.Without)
}

type topKBottomKOutputSorter struct {
	metadata       []types.SeriesMetadata
	seriesToGroups []*topKBottomKGroup
}

func (s topKBottomKOutputSorter) Len() int {
	return len(s.metadata)
}

func (s topKBottomKOutputSorter) Less(i, j int) bool {
	return s.seriesToGroups[i].lastSeriesIndex < s.seriesToGroups[j].lastSeriesIndex
}

func (s topKBottomKOutputSorter) Swap(i, j int) {
	s.seriesToGroups[i], s.seriesToGroups[j] = s.seriesToGroups[j], s.seriesToGroups[i]
	s.metadata[i], s.metadata[j] = s.metadata[j], s.metadata[i]
}

func (t *TopKBottomK) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if err := t.ensureCurrentGroupPopulated(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	series := t.currentGroup.series[t.seriesIndexInCurrentGroup]

	data, err := t.constructOutputSeries(series)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	t.returnSeriesToPool(series)
	t.seriesIndexInCurrentGroup++

	if t.seriesIndexInCurrentGroup >= t.currentGroup.totalSeries {
		for _, ts := range t.currentGroup.seriesForTimestamps {
			types.IntSlicePool.Put(ts, t.MemoryConsumptionTracker)
		}

		t.currentGroup = nil
		t.seriesIndexInCurrentGroup = 0
	}

	return data, nil
}

func (t *TopKBottomK) ensureCurrentGroupPopulated(ctx context.Context) error {
	if t.currentGroup != nil {
		return nil
	}

	if len(t.remainingGroups) == 0 {
		return types.EOS
	}

	t.currentGroup = t.remainingGroups[0]
	t.remainingGroups = t.remainingGroups[1:]

	for t.currentGroup.seriesRead() < t.currentGroup.totalSeries {
		if err := t.readNextSeries(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (t *TopKBottomK) constructOutputSeries(series topKBottomKSeries) (types.InstantVectorSeriesData, error) {
	data := types.InstantVectorSeriesData{}

	if series.pointCount == 0 {
		return data, nil
	}

	var err error
	data.Floats, err = types.FPointSlicePool.Get(series.pointCount, t.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	for idx, shouldReturn := range series.shouldReturnPoint {
		if !shouldReturn {
			continue
		}

		data.Floats = append(data.Floats, promql.FPoint{T: t.TimeRange.IndexTime(int64(idx)), F: series.values[idx]})
	}

	return data, nil
}

func (t *TopKBottomK) readNextSeries(ctx context.Context) error {
	nextSeries, err := t.Inner.NextSeries(ctx)
	if err != nil {
		return err
	}

	if len(nextSeries.Histograms) > 0 {
		t.emitIgnoredHistogramsAnnotation()
	}

	// topk() and bottomk() ignore histograms, so return the HPoint slice to the pool now.
	types.HPointSlicePool.Put(nextSeries.Histograms, t.MemoryConsumptionTracker)

	g := t.remainingInnerSeriesToGroup[0]
	t.remainingInnerSeriesToGroup = t.remainingInnerSeriesToGroup[1:]
	if err := t.accumulateIntoGroup(nextSeries, g); err != nil {
		return err
	}

	types.FPointSlicePool.Put(nextSeries.Floats, t.MemoryConsumptionTracker)

	return nil
}

func (t *TopKBottomK) emitIgnoredHistogramsAnnotation() {
	if t.haveEmittedIgnoredHistogramsAnnotation {
		return
	}

	if t.IsTopK {
		t.annotations.Add(annotations.NewHistogramIgnoredInAggregationInfo("topk", t.expressionPosition))
	} else {
		t.annotations.Add(annotations.NewHistogramIgnoredInAggregationInfo("bottomk", t.expressionPosition))
	}

	t.haveEmittedIgnoredHistogramsAnnotation = true
}

func (t *TopKBottomK) accumulateIntoGroup(data types.InstantVectorSeriesData, g *topKBottomKGroup) error {
	groupSeriesIndex := g.seriesRead()

	if g.seriesForTimestamps == nil {
		g.seriesForTimestamps = make([][]int, t.TimeRange.StepCount)
	}

	if g.series == nil {
		g.series = make([]topKBottomKSeries, 0, g.totalSeries)
	}

	g.series = g.series[:groupSeriesIndex+1]
	thisSeries := &g.series[groupSeriesIndex]

	for _, p := range data.Floats {
		timestampIndex := t.TimeRange.PointIndex(p.T)
		limit := t.limit[timestampIndex]

		if limit == 0 {
			continue
		}

		if g.seriesForTimestamps[timestampIndex] == nil {
			// This is the first time we've seen a point for this timestamp, create the list of source series.
			maximumPossibleSeries := min(limit, int64(g.totalSeries))

			var err error
			g.seriesForTimestamps[timestampIndex], err = types.IntSlicePool.Get(int(maximumPossibleSeries), t.MemoryConsumptionTracker)
			if err != nil {
				return err
			}
		}

		populateSeriesSlices := func() error {
			lastPointIndex := t.TimeRange.PointIndex(data.Floats[len(data.Floats)-1].T)
			sliceLength := int(lastPointIndex + 1)

			var err error
			thisSeries.shouldReturnPoint, err = types.BoolSlicePool.Get(sliceLength, t.MemoryConsumptionTracker)
			if err != nil {
				return err
			}

			thisSeries.shouldReturnPoint = thisSeries.shouldReturnPoint[:sliceLength]

			thisSeries.values, err = types.Float64SlicePool.Get(sliceLength, t.MemoryConsumptionTracker)
			if err != nil {
				return err
			}

			thisSeries.values = thisSeries.values[:sliceLength]

			return nil
		}

		recordUsedPoint := func() error {
			thisSeries.pointCount++

			if thisSeries.shouldReturnPoint == nil {
				if err := populateSeriesSlices(); err != nil {
					return err
				}
			}

			thisSeries.shouldReturnPoint[timestampIndex] = true
			thisSeries.values[timestampIndex] = p.F

			return nil
		}

		if len(g.seriesForTimestamps[timestampIndex]) == int(limit) {
			// Already have a full set of values for this timestamp, see if the one from this series is better than the current worst.
			// (ie. larger for topk / smaller for bottomk)
			currentWorstSeriesIndex := g.seriesForTimestamps[timestampIndex][0]
			currentWorstSeries := &g.series[currentWorstSeriesIndex]
			currentWorstValue := currentWorstSeries.values[timestampIndex]

			if math.IsNaN(p.F) {
				// A NaN is never better than any existing value.
				continue
			} else if t.IsTopK && p.F <= currentWorstValue && !math.IsNaN(currentWorstValue) {
				// Value is not larger than the nth biggest value we've already seen. Continue.
				continue
			} else if !t.IsTopK && p.F >= currentWorstValue && !math.IsNaN(currentWorstValue) {
				// Value is not smaller than the nth smallest value we've already seen. Continue.
				continue
			}

			if err := recordUsedPoint(); err != nil {
				return err
			}

			g.seriesForTimestamps[timestampIndex][0] = groupSeriesIndex
			t.heap.Reset(g, timestampIndex)

			if limit != 1 {
				// We only need to bother to fix the heap if there's more than one element.
				// This optimises for the common case of topk(1, xxx) or bottomk(1, xxx).
				heap.Fix(t.heap, 0)
			}

			currentWorstSeries.pointCount--

			if currentWorstSeries.pointCount == 0 {
				// We just replaced the last possible point the previous worst series could return.
				// Return its slices to the pool.
				t.returnSeriesToPool(*currentWorstSeries)

				// Make sure we can't return the slices to the pool a second time when we emit this series later.
				currentWorstSeries.shouldReturnPoint = nil
				currentWorstSeries.values = nil
			} else {
				currentWorstSeries.shouldReturnPoint[timestampIndex] = false
			}
		} else {
			// We don't have a full set of values for this timestamp. Add this series to the list.
			if err := recordUsedPoint(); err != nil {
				return err
			}

			t.heap.Reset(g, timestampIndex)
			heap.Push(t.heap, groupSeriesIndex)
		}
	}

	return nil
}

func (t *TopKBottomK) returnSeriesToPool(series topKBottomKSeries) {
	types.BoolSlicePool.Put(series.shouldReturnPoint, t.MemoryConsumptionTracker)
	types.Float64SlicePool.Put(series.values, t.MemoryConsumptionTracker)
}

func (t *TopKBottomK) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *TopKBottomK) Close() {
	t.Inner.Close()
	t.Param.Close()

	types.Int64SlicePool.Put(t.limit, t.MemoryConsumptionTracker)
}

type topKBottomKGroup struct {
	lastSeriesIndex int // The index (from the inner operator) of the last series that will contribute to this group
	totalSeries     int // The total number of series that will contribute to this group

	series []topKBottomKSeries

	seriesForTimestamps [][]int // One entry per timestamp, each entry contains a slice of the series indices (from `series` above) used as a min-/max-heap for the current 'best' values seen (highest for topk / lowest for bottomk)
}

func (g *topKBottomKGroup) seriesRead() int {
	return len(g.series)
}

type topKBottomKHeap interface {
	heap.Interface
	Reset(group *topKBottomKGroup, timestampIndex int64)
}

type topKBottomKSeries struct {
	pointCount        int       // Number of points that will be returned (should equal the number of true elements in shouldReturnPoint)
	shouldReturnPoint []bool    // One entry per timestamp, true means the value should be returned
	values            []float64 // One entry per timestamp with value for that timestamp (entry only guaranteed to be populated if value at that timestamp might be returned)
}

type topKHeap struct {
	timestampIndex int64
	group          *topKBottomKGroup
}

func (h *topKHeap) Reset(group *topKBottomKGroup, timestampIndex int64) {
	h.group = group
	h.timestampIndex = timestampIndex
}

func (h *topKHeap) Len() int {
	return len(h.group.seriesForTimestamps[h.timestampIndex])
}

func (h *topKHeap) Less(i, j int) bool {
	iSeries := h.group.seriesForTimestamps[h.timestampIndex][i]
	jSeries := h.group.seriesForTimestamps[h.timestampIndex][j]

	iValue := h.group.series[iSeries].values[h.timestampIndex]
	jValue := h.group.series[jSeries].values[h.timestampIndex]

	if math.IsNaN(iValue) {
		return true
	}

	if math.IsNaN(jValue) {
		return false
	}

	return iValue < jValue
}

func (h *topKHeap) Swap(i, j int) {
	h.group.seriesForTimestamps[h.timestampIndex][i], h.group.seriesForTimestamps[h.timestampIndex][j] = h.group.seriesForTimestamps[h.timestampIndex][j], h.group.seriesForTimestamps[h.timestampIndex][i]
}

func (h *topKHeap) Push(x any) {
	h.group.seriesForTimestamps[h.timestampIndex] = append(h.group.seriesForTimestamps[h.timestampIndex], x.(int))
}

func (h *topKHeap) Pop() any {
	panic("not supported")
}

type bottomKHeap struct {
	timestampIndex int64
	group          *topKBottomKGroup
}

func (h *bottomKHeap) Reset(group *topKBottomKGroup, timestampIndex int64) {
	h.group = group
	h.timestampIndex = timestampIndex
}

func (h *bottomKHeap) Len() int {
	return len(h.group.seriesForTimestamps[h.timestampIndex])
}

func (h *bottomKHeap) Less(i, j int) bool {
	iSeries := h.group.seriesForTimestamps[h.timestampIndex][i]
	jSeries := h.group.seriesForTimestamps[h.timestampIndex][j]

	iValue := h.group.series[iSeries].values[h.timestampIndex]
	jValue := h.group.series[jSeries].values[h.timestampIndex]

	if math.IsNaN(iValue) {
		return false
	}

	if math.IsNaN(jValue) {
		return true
	}

	return iValue > jValue
}

func (h *bottomKHeap) Swap(i, j int) {
	h.group.seriesForTimestamps[h.timestampIndex][i], h.group.seriesForTimestamps[h.timestampIndex][j] = h.group.seriesForTimestamps[h.timestampIndex][j], h.group.seriesForTimestamps[h.timestampIndex][i]
}

func (h *bottomKHeap) Push(x any) {
	h.group.seriesForTimestamps[h.timestampIndex] = append(h.group.seriesForTimestamps[h.timestampIndex], x.(int))
}

func (h *bottomKHeap) Pop() any {
	panic("not supported")
}
