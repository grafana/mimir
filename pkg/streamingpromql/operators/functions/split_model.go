// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"io"

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

// SplittableGenerateFunc generates an intermediate result for a single time range split.
type SplittableGenerateFunc[T any] func(
	step *types.RangeVectorStepData,
	scalarArgsData []types.ScalarData,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (T, error)

// SplittableCombineFunc combines intermediate results from multiple time range splits.
type SplittableCombineFunc[T any] func(
	pieces []T,
	emitAnnotation types.EmitAnnotationFunc,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error)

type SplittableWriterFactory[T any] func(writer io.Writer) (SplittableWriter[T], error)

type SplittableWriter[T any] interface {
	WriteNextResult(T) error
	// Flush writes all buffered results to the underlying writer.
	// Must be called before the underlying writer is closed/finalized.
	Flush() error
}

type SplittableReaderFactory[T any] func(reader io.Reader) (SplittableReader[T], error)

type SplittableReader[T any] interface {
	ReadNextResult() (T, error)
	// TODO: Push result buffering etc lower if possible? Otherwise in non-streaming case we might be buffering once in splits and once in reader
	//ReadResultAt(idx int64) (T, error)
}

type SplittableOperatorFactory func(
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []Range,
	cacheKey string,
	irCache cache.IntermediateResultsCache,
	funcDef *FunctionOverRangeVectorDefinition,
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
) (types.Operator, error)

func NewSplitOperatorDefinition[T any](
	generate SplittableGenerateFunc[T],
	combine SplittableCombineFunc[T],
	write SplittableWriterFactory[T],
	read SplittableReaderFactory[T],
) SplittableOperatorFactory {
	return func(
		innerNode planning.Node,
		materializer *planning.Materializer,
		timeRange types.QueryTimeRange,
		ranges []Range,
		cacheKey string,
		irCache cache.IntermediateResultsCache,
		funcDef *FunctionOverRangeVectorDefinition,
		expressionPosition posrange.PositionRange,
		annotations *annotations.Annotations,
		memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
		enableDelayedNameRemoval bool,
	) (types.Operator, error) {
		// Create the fully-typed operator with all type information preserved via closure
		return NewSplittingFunctionOverRangeVector[T](
			innerNode,
			materializer,
			timeRange,
			ranges,
			cacheKey,
			irCache,
			*funcDef,
			generate,
			combine,
			write,
			read,
			expressionPosition,
			annotations,
			memoryConsumptionTracker,
			enableDelayedNameRemoval,
		)
	}
}
