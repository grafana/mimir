package topkbottomk

import (
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func New(
	inner types.InstantVectorOperator,
	param types.ScalarOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	isTopK bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) types.InstantVectorOperator {
	if without {
		labelsToDrop := make([]string, 0, len(grouping)+1)
		labelsToDrop = append(labelsToDrop, labels.MetricName)
		labelsToDrop = append(labelsToDrop, grouping...)
		grouping = labelsToDrop
	}

	slices.Sort(grouping)

	// Why do we have separate implementations for instant queries and range queries?
	// For instant queries, we need to return series sorted by their value.
	// This requires us to hold the entire output result in memory.
	// For range queries, no such requirement exists, and so we don't need to hold the
	// entire output result in memory.
	// This is a significant enough difference that it is easier and clearer to have separate
	// operators for the two cases rather than try to satisfy both in the one implementation.
	if timeRange.StepCount == 1 {
		h := &instantQueryHeap{}

		if isTopK {
			h.less = topKLess
		} else {
			h.less = bottomKLess
		}

		return &InstantQuery{
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

	h := &rangeQueryHeap{}

	if isTopK {
		h.less = topKLess
	} else {
		h.less = bottomKLess
	}

	return &RangeQuery{
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

func convertibleToInt64(v float64) bool {
	return v <= math.MaxInt64 && v >= math.MinInt64
}

func topKLess(i float64, j float64) bool {
	if math.IsNaN(i) {
		return true
	}

	if math.IsNaN(j) {
		return false
	}

	return i < j
}

func bottomKLess(i float64, j float64) bool {
	if math.IsNaN(i) {
		return false
	}

	if math.IsNaN(j) {
		return true
	}

	return i > j
}
