// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/querysplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

// Generic function types for type-safe splittable function implementations

// Range represents a time range within a query split.
// Start is exclusive (points with timestamp > Start are included).
// End is inclusive (points with timestamp <= End are included).
type Range struct {
	Start     int64
	End       int64
	Cacheable bool
}

// SplitGenerateFunc generates an intermediate result for a single time range split.
type SplitGenerateFunc[T any] func(
	step *types.RangeVectorStepData,
	scalarArgsData []types.ScalarData,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (T, error)

// SplitCombineFunc combines intermediate results from multiple time range splits.
type SplitCombineFunc[T any] func(
	pieces []T,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

type SplittableOperatorFactory func(
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []Range,
	cacheKey string,
	irCache *cache.Cache,
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
	logger log.Logger,
) (types.Operator, error)

func NewSplitOperatorFactory[T any](
	generate SplitGenerateFunc[T],
	combine SplitCombineFunc[T],
	codec cache.SplitCodec[T],
	funcDef FunctionOverRangeVectorDefinition,
	funcId Function,
) SplittableOperatorFactory {
	return func(
		innerNode planning.Node,
		materializer *planning.Materializer,
		timeRange types.QueryTimeRange,
		ranges []Range,
		cacheKey string,
		irCache *cache.Cache,
		expressionPosition posrange.PositionRange,
		annotations *annotations.Annotations,
		memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
		enableDelayedNameRemoval bool,
		logger log.Logger,
	) (types.Operator, error) {
		return NewSplittingFunctionOverRangeVector[T](
			innerNode,
			materializer,
			timeRange,
			ranges,
			cacheKey,
			irCache,
			funcId,
			funcDef,
			generate,
			combine,
			codec,
			expressionPosition,
			annotations,
			memoryConsumptionTracker,
			enableDelayedNameRemoval,
			logger,
		)
	}
}
