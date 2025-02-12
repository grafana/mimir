// SPDX-License-Identifier: AGPL-3.0-only

package topkbottomk

import (
	"container/heap"
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// InstantQuery implements topk() and bottomk() for range queries.
type InstantQuery struct {
	Inner                    types.InstantVectorOperator
	Param                    types.ScalarOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, New will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	IsTopK                   bool // If false, this is operator is for bottomk().

	expressionPosition posrange.PositionRange
	limit              int64 // Maximum number of values to return for each group.

	values          []float64
	nextSeriesIndex int

	annotations                            *annotations.Annotations
	haveEmittedIgnoredHistogramsAnnotation bool

	// Reuse the same heap instance to allow us to avoid allocating a new one every time.
	heap *instantQueryHeap
}

var _ types.InstantVectorOperator = &InstantQuery{}

func (t *InstantQuery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if err := t.loadLimit(ctx); err != nil {
		return nil, err
	}

	if t.limit == 0 {
		// We can't return any series, so stop now.
		return nil, nil
	}

	innerSeries, err := t.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(innerSeries)

	groupLabelsBytesFunc := t.groupLabelsBytesFunc()
	groups := map[string]*instantQueryGroup{}
	seriesToGroups := make([]*instantQueryGroup, 0, len(innerSeries))

	for _, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g = &instantQueryGroup{}
			groups[string(groupLabelsString)] = g
		}

		g.seriesCount++
		seriesToGroups = append(seriesToGroups, g)
	}

	outputSeriesCount := 0

	for idx, series := range innerSeries {
		g := seriesToGroups[idx]

		data, err := t.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		if len(data.Histograms) > 0 {
			t.emitIgnoredHistogramsAnnotation()
		}

		if len(data.Floats) == 0 {
			// No float values, nothing to do.
			g.seriesCount-- // Avoid allocating space for this series if we can.
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
			continue
		}

		if t.accumulateValue(series, data.Floats[0].F, g) {
			outputSeriesCount++
		}

		types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
	}

	outputSeries := types.GetSeriesMetadataSlice(outputSeriesCount)
	t.values, err = types.Float64SlicePool.Get(outputSeriesCount, t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	outputSeries = outputSeries[:outputSeriesCount]
	t.values = t.values[:outputSeriesCount]
	firstOutputSeriesIndexForNextGroup := 0

	for _, g := range groups {
		t.heap.Reset(g)

		lastOutputSeriesIndexForGroup := firstOutputSeriesIndexForNextGroup + len(g.series) - 1
		nextOutputSeriesIndex := lastOutputSeriesIndexForGroup
		firstOutputSeriesIndexForNextGroup += len(g.series)

		for len(g.series) > 0 {
			next := heap.Pop(t.heap).(instantQuerySeries)

			// Pop returns the next lowest value (topk) or next highest value (bottomk), but we want to return values
			// in descending order for topk / ascending order for bottomk. The only exception is NaN values, which
			// should always be last.
			outputSeries[nextOutputSeriesIndex] = next.metadata
			t.values[nextOutputSeriesIndex] = next.value
			nextOutputSeriesIndex--
		}
	}

	return outputSeries, nil
}

func (t *InstantQuery) loadLimit(ctx context.Context) error {
	paramValues, err := t.Param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(paramValues.Samples, t.MemoryConsumptionTracker)

	v := paramValues.Samples[0].F

	if !convertibleToInt64(v) {
		return fmt.Errorf("scalar value %v overflows int64", v)
	}

	t.limit = max(int64(v), 0) // Ignore any negative values.

	return nil
}

func (t *InstantQuery) groupLabelsBytesFunc() aggregations.SeriesToGroupLabelsBytesFunc {
	return aggregations.GroupLabelsBytesFunc(t.Grouping, t.Without)
}

func (t *InstantQuery) emitIgnoredHistogramsAnnotation() {
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

// Returns true if accumulating this value means that the group will return an additional series.
func (t *InstantQuery) accumulateValue(metadata types.SeriesMetadata, value float64, g *instantQueryGroup) bool {
	if int64(len(g.series)) < t.limit {
		// We don't have a full set of values for this group yet. Add this series to the list.

		if g.series == nil {
			// This is the first time we've seen a series for this group, create the list of values.
			maximumPossibleSeries := min(t.limit, int64(g.seriesCount))

			g.series = make([]instantQuerySeries, 0, maximumPossibleSeries) // TODO: pool this?
		}

		t.heap.Reset(g)
		heap.Push(t.heap, instantQuerySeries{metadata, value})

		return true
	}

	// Already have a full set of values for this timestamp, see if the one from this series is better than the current worst.
	// (ie. larger for topk / smaller for bottomk)

	currentWorstValue := g.series[0].value

	if math.IsNaN(value) {
		// A NaN is never better than any existing value.
		return false
	} else if t.IsTopK && value <= currentWorstValue && !math.IsNaN(currentWorstValue) {
		// Value is not larger than the nth biggest value we've already seen. Continue.
		return false
	} else if !t.IsTopK && value >= currentWorstValue && !math.IsNaN(currentWorstValue) {
		// Value is not smaller than the nth smallest value we've already seen. Continue.
		return false
	}

	g.series[0].metadata = metadata
	g.series[0].value = value

	if t.limit != 1 {
		// We only need to bother to fix the heap if there's more than one element.
		// This optimises for the common case of topk(1, xxx) or bottomk(1, xxx).
		t.heap.Reset(g)
		heap.Fix(t.heap, 0)
	}

	return false
}

func (t *InstantQuery) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if t.nextSeriesIndex >= len(t.values) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := types.InstantVectorSeriesData{}

	var err error
	data.Floats, err = types.FPointSlicePool.Get(1, t.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data.Floats = append(data.Floats, promql.FPoint{
		T: t.TimeRange.StartT,
		F: t.values[t.nextSeriesIndex],
	})

	t.nextSeriesIndex++

	return data, nil
}

func (t *InstantQuery) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *InstantQuery) Close() {
	t.Inner.Close()
	t.Param.Close()

	types.Float64SlicePool.Put(t.values, t.MemoryConsumptionTracker)
}

type instantQueryGroup struct {
	seriesCount int

	// Min-/max-heap for the current 'best' values we've seen (highest for topk / lowest for bottomk)
	series []instantQuerySeries
}

type instantQuerySeries struct {
	metadata types.SeriesMetadata
	value    float64
}

type instantQueryHeap struct {
	group *instantQueryGroup
	less  func(i, j float64) bool
}

func (h *instantQueryHeap) Reset(group *instantQueryGroup) {
	h.group = group
}

func (h *instantQueryHeap) Len() int {
	return len(h.group.series)
}

func (h *instantQueryHeap) Less(i, j int) bool {
	iValue := h.group.series[i].value
	jValue := h.group.series[j].value

	return h.less(iValue, jValue)
}

func (h *instantQueryHeap) Swap(i, j int) {
	h.group.series[i], h.group.series[j] = h.group.series[j], h.group.series[i]
}

func (h *instantQueryHeap) Push(x any) {
	h.group.series = append(h.group.series, x.(instantQuerySeries))
}

func (h *instantQueryHeap) Pop() any {
	i := len(h.group.series) - 1
	v := h.group.series[i]
	h.group.series = h.group.series[:i]
	return v
}
