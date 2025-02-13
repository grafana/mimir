// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package topkbottomk

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"unsafe"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/pool"
)

// RangeQuery implements topk() and bottomk() for range queries.
type RangeQuery struct {
	Inner                    types.InstantVectorOperator
	Param                    types.ScalarOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, New will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	IsTopK                   bool // If false, this is operator is for bottomk().

	expressionPosition posrange.PositionRange
	k                  []int64 // Maximum number of values to return at each time step for each group

	remainingInnerSeriesToGroup []*rangeQueryGroup // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*rangeQueryGroup // One entry per group, in the order we want to return them

	currentGroup              *rangeQueryGroup
	seriesIndexInCurrentGroup int

	annotations                            *annotations.Annotations
	haveEmittedIgnoredHistogramsAnnotation bool

	// Reuse the same heap instance to allow us to avoid allocating a new one every time.
	heap *rangeQueryHeap
}

var _ types.InstantVectorOperator = &RangeQuery{}

func (t *RangeQuery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if err := t.getK(ctx); err != nil {
		return nil, err
	}

	innerSeries, err := t.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	groups := map[string]*rangeQueryGroup{}
	groupLabelsBytesFunc := aggregations.GroupLabelsBytesFunc(t.Grouping, t.Without)
	t.remainingInnerSeriesToGroup = make([]*rangeQueryGroup, 0, len(innerSeries))

	for seriesIdx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g = &rangeQueryGroup{}
			groups[string(groupLabelsString)] = g
		}

		g.totalSeries++
		g.lastSeriesIndex = seriesIdx
		t.remainingInnerSeriesToGroup = append(t.remainingInnerSeriesToGroup, g)
	}

	t.remainingGroups = make([]*rangeQueryGroup, 0, len(groups))

	for _, g := range groups {
		t.remainingGroups = append(t.remainingGroups, g)
	}

	// Sort the groups in the order we'll return them.
	slices.SortFunc(t.remainingGroups, func(a, b *rangeQueryGroup) int {
		return a.lastSeriesIndex - b.lastSeriesIndex
	})

	// Sort the series in the order we'll return them.
	// It's important that we use a stable sort here, so that the order we receive series (and therefore accumulate series into their groups)
	// matches the order we'll return those series in.
	sort.Stable(topKBottomKOutputSorter{
		metadata: innerSeries,

		// Why do we clone this?
		// We need to know which series belongs to which group, so that we can sort the series in the order groups will be completed.
		// The easiest way to do this is to maintain a slice where each element contains the group for the corresponding labels in `metadata`
		// above, and swap elements in `seriesToGroups` as we swap them in `metadata` during sorting.
		// However, we don't want to modify remainingInnerSeriesToGroup, as we need to keep it in the same order as the input series so we can
		// know which input series maps to which output group when reading series data later on.
		// So we make a copy for use only during sorting.
		seriesToGroups: slices.Clone(t.remainingInnerSeriesToGroup),
	})

	return innerSeries, nil
}

func (t *RangeQuery) getK(ctx context.Context) error {
	paramValues, err := t.Param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(paramValues.Samples, t.MemoryConsumptionTracker)

	t.k, err = types.Int64SlicePool.Get(t.TimeRange.StepCount, t.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	t.k = t.k[:t.TimeRange.StepCount]

	for stepIdx := range t.TimeRange.StepCount {
		v := paramValues.Samples[stepIdx].F

		if !convertibleToInt64(v) {
			return fmt.Errorf("scalar value %v overflows int64", v)
		}

		t.k[stepIdx] = max(int64(v), 0) // Ignore any negative values.
	}

	return nil
}

type topKBottomKOutputSorter struct {
	metadata       []types.SeriesMetadata
	seriesToGroups []*rangeQueryGroup
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

func (t *RangeQuery) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
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
		t.returnGroupToPool(t.currentGroup)
		t.currentGroup = nil
		t.seriesIndexInCurrentGroup = 0
	}

	return data, nil
}

func (t *RangeQuery) ensureCurrentGroupPopulated(ctx context.Context) error {
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

func (t *RangeQuery) constructOutputSeries(series topKBottomKSeries) (types.InstantVectorSeriesData, error) {
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

func (t *RangeQuery) readNextSeries(ctx context.Context) error {
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

func (t *RangeQuery) emitIgnoredHistogramsAnnotation() {
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

func (t *RangeQuery) accumulateIntoGroup(data types.InstantVectorSeriesData, g *rangeQueryGroup) error {
	groupSeriesIndex := g.seriesRead()

	if g.seriesForTimestamps == nil {
		var err error
		g.seriesForTimestamps, err = intSliceSlicePool.Get(t.TimeRange.StepCount, t.MemoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.seriesForTimestamps = g.seriesForTimestamps[:t.TimeRange.StepCount]
	}

	if g.series == nil {
		var err error
		g.series, err = topKBottomKSeriesSlicePool.Get(g.totalSeries, t.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
	}

	g.series = g.series[:groupSeriesIndex+1]
	thisSeries := &g.series[groupSeriesIndex]

	for _, p := range data.Floats {
		timestampIndex := t.TimeRange.PointIndex(p.T)
		limit := t.k[timestampIndex]

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

			if limit != 1 {
				// We only need to bother to fix the heap if there's more than one element.
				// This optimises for the common case of topk(1, xxx) or bottomk(1, xxx).
				t.heap.Reset(g, timestampIndex)
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

func (t *RangeQuery) returnGroupToPool(g *rangeQueryGroup) {
	for _, ts := range g.seriesForTimestamps {
		types.IntSlicePool.Put(ts, t.MemoryConsumptionTracker)
	}

	intSliceSlicePool.Put(g.seriesForTimestamps, t.MemoryConsumptionTracker)
	topKBottomKSeriesSlicePool.Put(g.series, t.MemoryConsumptionTracker)
}

func (t *RangeQuery) returnSeriesToPool(series topKBottomKSeries) {
	types.BoolSlicePool.Put(series.shouldReturnPoint, t.MemoryConsumptionTracker)
	types.Float64SlicePool.Put(series.values, t.MemoryConsumptionTracker)
}

func (t *RangeQuery) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *RangeQuery) Close() {
	t.Inner.Close()
	t.Param.Close()

	types.Int64SlicePool.Put(t.k, t.MemoryConsumptionTracker)
}

type rangeQueryGroup struct {
	lastSeriesIndex int // The index (from the inner operator) of the last series that will contribute to this group
	totalSeries     int // The total number of series that will contribute to this group

	series []topKBottomKSeries

	seriesForTimestamps [][]int // One entry per timestamp, each entry contains a slice of the series indices (from `series` above) used as a min-/max-heap for the current 'best' values seen (highest for topk / lowest for bottomk)
}

func (g *rangeQueryGroup) seriesRead() int {
	return len(g.series)
}

type topKBottomKSeries struct {
	pointCount        int       // Number of points that will be returned (should equal the number of true elements in shouldReturnPoint)
	shouldReturnPoint []bool    // One entry per timestamp, true means the value should be returned
	values            []float64 // One entry per timestamp with value for that timestamp (entry only guaranteed to be populated if value at that timestamp might be returned)
}

var topKBottomKSeriesSlicePool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []topKBottomKSeries {
		return make([]topKBottomKSeries, 0, size)
	}),
	uint64(unsafe.Sizeof(topKBottomKSeries{})),
	true,
	nil,
)

var intSliceSlicePool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedPointsPerSeries, func(size int) [][]int {
		return make([][]int, 0, size)
	}),
	uint64(unsafe.Sizeof([][]int{})),
	true,
	nil,
)

type rangeQueryHeap struct {
	timestampIndex int64
	group          *rangeQueryGroup
	less           func(i, j float64) bool
}

func (h *rangeQueryHeap) Reset(group *rangeQueryGroup, timestampIndex int64) {
	h.group = group
	h.timestampIndex = timestampIndex
}

func (h *rangeQueryHeap) Len() int {
	return len(h.group.seriesForTimestamps[h.timestampIndex])
}

func (h *rangeQueryHeap) Less(i, j int) bool {
	iSeries := h.group.seriesForTimestamps[h.timestampIndex][i]
	jSeries := h.group.seriesForTimestamps[h.timestampIndex][j]

	iValue := h.group.series[iSeries].values[h.timestampIndex]
	jValue := h.group.series[jSeries].values[h.timestampIndex]

	return h.less(iValue, jValue)
}

func (h *rangeQueryHeap) Swap(i, j int) {
	h.group.seriesForTimestamps[h.timestampIndex][i], h.group.seriesForTimestamps[h.timestampIndex][j] = h.group.seriesForTimestamps[h.timestampIndex][j], h.group.seriesForTimestamps[h.timestampIndex][i]
}

func (h *rangeQueryHeap) Push(x any) {
	h.group.seriesForTimestamps[h.timestampIndex] = append(h.group.seriesForTimestamps[h.timestampIndex], x.(int))
}

func (h *rangeQueryHeap) Pop() any {
	panic("not supported")
}
