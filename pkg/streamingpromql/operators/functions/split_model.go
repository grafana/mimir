// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

// Range represents a time range within a query split.
// Start is exclusive, End is inclusive.
type Range struct {
	Start     int64
	End       int64
	Cacheable bool
}

// SplitGenerateFunc generates an intermediate result for a single split range.
type SplitGenerateFunc[T any] func(
	step *types.RangeVectorStepData,
	scalarArgsData []types.ScalarData,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (T, error)

// SplitCombineFunc combines intermediate results from multiple split ranges.
// Histograms within the input ranges must not be modified in place. These histograms can share references with
// histogram protos that are waiting to be cached.
type SplitCombineFunc[T any] func(
	ranges []T,
	scalarArgsData []types.ScalarData,
	rangeStart int64,
	rangeEnd int64,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

type SplitOperatorFactory func(
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []Range,
	cacheKey string,
	irCache *cache.CacheFactory,
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
) SplitOperatorFactory {
	return func(
		innerNode planning.Node,
		materializer *planning.Materializer,
		timeRange types.QueryTimeRange,
		ranges []Range,
		cacheKey string,
		irCache *cache.CacheFactory,
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
