// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type FunctionOperatorFactory func(
	args []types.Operator,
	absentLabels labels.Labels, // Only used by absent and absent_over_time.
	opParams *planning.OperatorParameters,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) (types.Operator, error)

// SingleInputVectorFunctionOperatorFactory creates an InstantVectorFunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector).
//
// Parameters:
//   - name: The name of the function
//   - f: The function implementation
func SingleInputVectorFunctionOperatorFactory(name string, f FunctionOverInstantVectorDefinition) FunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
		}

		return NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
	}
}

// InstantVectorTransformationFunctionOperatorFactory creates an FunctionOperatorFactory for functions
// that have exactly 1 argument (v instant-vector), and drop the series __name__ label.
//
// Parameters:
//   - name: The name of the function
//   - seriesDataFunc: The function to handle series data
func InstantVectorTransformationFunctionOperatorFactory(name string, seriesDataFunc InstantVectorSeriesFunction) FunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: DropSeriesName,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
}

func TimeTransformationFunctionOperatorFactory(name string, seriesDataFunc InstantVectorSeriesFunction) FunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         seriesDataFunc,
		SeriesMetadataFunction: DropSeriesName,
	}

	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		var inner types.InstantVectorOperator
		if len(args) == 0 {
			// if the argument is not provided, it will default to vector(time())
			inner = scalars.NewScalarToInstantVector(operators.NewTime(timeRange, opParams.MemoryConsumptionTracker, expressionPosition), expressionPosition, opParams.MemoryConsumptionTracker)
		} else if len(args) == 1 {
			// if one argument is provided, it must be an instant vector
			var ok bool
			inner, ok = args[0].(types.InstantVectorOperator)
			if !ok {
				// Should be caught by the PromQL parser, but we check here for safety.
				return nil, fmt.Errorf("expected an instant vector argument for %s, got %T", name, args[0])
			}
		} else {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected 0 or 1 argument for %s, got %v", name, len(args))
		}

		return NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
	}
}

// InstantVectorLabelManipulationFunctionOperatorFactory creates an InstantVectorFunctionOperator for functions
// that have exactly 1 argument (v instant-vector), and need to manipulate the labels of
// each series without manipulating the returned samples.
// The values of v are passed through.
//
// Parameters:
//   - name: The name of the function
//   - metadataFunc: The function for handling metadata
func InstantVectorLabelManipulationFunctionOperatorFactory(name string, metadataFunc SeriesMetadataFunctionDefinition) FunctionOperatorFactory {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         PassthroughData,
		SeriesMetadataFunction: metadataFunc,
	}

	return SingleInputVectorFunctionOperatorFactory(name, f)
}

// FunctionOverRangeVectorOperatorFactory creates an FunctionOperatorFactory for functions
// that have exactly 1 argument (v range-vector).
//
// Parameters:
//   - name: The name of the function
//   - f: The function implementation
func FunctionOverRangeVectorOperatorFactory(
	name string,
	f FunctionOverRangeVectorDefinition,
) FunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", name, len(args))
		}

		inner, ok := args[0].(types.RangeVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a range vector argument for %s, got %T", name, args[0])
		}

		return NewFunctionOverRangeVector(inner, nil, opParams.MemoryConsumptionTracker, f, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
	}
}

func PredictLinearFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	f := PredictLinear

	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for predict_linear, got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected first argument for predict_linear to be a range vector, got %T", args[0])
	}

	arg, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for predict_linear to be a scalar, got %T", args[1])
	}

	var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, []types.ScalarOperator{arg}, opParams.MemoryConsumptionTracker, f, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval)

	return o, nil
}

func QuantileOverTimeFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	f := QuantileOverTime

	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for quantile_over_time, got %v", len(args))
	}

	arg, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected first argument for quantile_over_time to be a scalar, got %T", args[1])
	}

	inner, ok := args[1].(types.RangeVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for quantile_over_time to be a range vector, got %T", args[0])
	}

	var o types.InstantVectorOperator = NewFunctionOverRangeVector(inner, []types.ScalarOperator{arg}, opParams.MemoryConsumptionTracker, f, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval)

	return o, nil
}

func scalarToInstantVectorOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, _ types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for vector, got %v", len(args))
	}

	inner, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar argument for vector, got %T", args[0])
	}

	return scalars.NewScalarToInstantVector(inner, expressionPosition, opParams.MemoryConsumptionTracker), nil
}

func LabelJoinFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	// It is valid for label_join to have no source label names. ie, only 3 arguments are actually required.
	if len(args) < 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected 3 or more arguments for label_join, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for label_join, got %T", args[0])
	}

	dstLabel, ok := args[1].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 2nd argument for label_join, got %T", args[1])
	}

	separator, ok := args[2].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 3rd argument for label_join, got %T", args[2])
	}

	srcLabels := make([]types.StringOperator, len(args)-3)
	for i := 3; i < len(args); i++ {
		srcLabel, ok := args[i].(types.StringOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a string for %dth argument for label_join, got %T", i+1, args[i])
		}
		srcLabels[i-3] = srcLabel
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc: PassthroughData,
		SeriesMetadataFunction: SeriesMetadataFunctionDefinition{
			Func: LabelJoinFactory(dstLabel, separator, srcLabels),
		},
	}

	return NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func LabelReplaceFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 5 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 5 arguments for label_replace, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for label_replace, got %T", args[0])
	}

	dstLabel, ok := args[1].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 2nd argument for label_replace, got %T", args[1])
	}

	replacement, ok := args[2].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 3rd argument for label_replace, got %T", args[2])
	}

	srcLabel, ok := args[3].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 4th argument for label_replace, got %T", args[3])
	}

	regex, ok := args[4].(types.StringOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a string for 5th argument for label_replace, got %T", args[4])
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc: PassthroughData,
		SeriesMetadataFunction: SeriesMetadataFunctionDefinition{
			Func: LabelReplaceFactory(dstLabel, replacement, srcLabel, regex),
		},
	}

	return NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func AbsentOperatorFactory(args []types.Operator, labels labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected exactly 1 parameter for 'absent', got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator as parameter of 'absent' function call, got %T", args[0])
	}

	return NewAbsent(inner, labels, timeRange, opParams.MemoryConsumptionTracker, expressionPosition), nil
}

func AbsentOverTimeOperatorFactory(args []types.Operator, labels labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected exactly 1 parameter for 'absent_over_time', got %v", len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected RangeVectorOperator as parameter of 'absent_over_time' function call, got %T", args[0])
	}

	return NewAbsentOverTime(inner, labels, timeRange, opParams.MemoryConsumptionTracker, expressionPosition), nil
}

func ClampFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for clamp, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for clamp, got %T", args[0])
	}

	min, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 2nd argument for clamp, got %T", args[1])
	}

	max, ok := args[2].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 3rd argument for clamp, got %T", args[2])
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         Clamp,
		SeriesMetadataFunction: DropSeriesName,
	}

	return NewFunctionOverInstantVector(inner, []types.ScalarOperator{min, max}, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func ClampMinMaxFunctionOperatorFactory(functionName string, isMin bool) FunctionOperatorFactory {
	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		if len(args) != 2 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 2 arguments for %s, got %v", functionName, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector for 1st argument for %s, got %T", functionName, args[0])
		}

		clampTo, ok := args[1].(types.ScalarOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a scalar for 2nd argument for %s, got %T", functionName, args[1])
		}

		f := FunctionOverInstantVectorDefinition{
			SeriesDataFunc:         ClampMinMaxFactory(isMin),
			SeriesMetadataFunction: DropSeriesName,
		}

		return NewFunctionOverInstantVector(inner, []types.ScalarOperator{clampTo}, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
	}
}

func RoundFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 && len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected 1 or 2 arguments for round, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for round, got %T", args[0])
	}

	var toNearest types.ScalarOperator
	if len(args) == 2 {
		toNearest, ok = args[1].(types.ScalarOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a scalar for 2nd argument for round, got %T", args[1])
		}
	} else {
		toNearest = scalars.NewScalarConstant(float64(1), timeRange, opParams.MemoryConsumptionTracker, expressionPosition)
	}

	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         Round,
		SeriesMetadataFunction: DropSeriesName,
	}

	return NewFunctionOverInstantVector(inner, []types.ScalarOperator{toNearest}, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func HistogramQuantileFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 2 arguments for histogram_quantile, got %v", len(args))
	}

	ph, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 1st argument for histogram_quantile, got %T", args[0])
	}

	inner, ok := args[1].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 2nd argument for histogram_quantile, got %T", args[1])
	}

	return NewHistogramQuantileFunction(ph, inner, opParams.MemoryConsumptionTracker, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func HistogramFractionFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for histogram_fraction, got %v", len(args))
	}

	lower, ok := args[0].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 1st argument for histogram_fraction, got %T", args[0])
	}

	upper, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected a scalar for 2nd argument for histogram_fraction, got %T", args[1])
	}

	inner, ok := args[2].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 3rd argument for histogram_fraction, got %T", args[2])
	}

	return NewHistogramFractionFunction(lower, upper, inner, opParams.MemoryConsumptionTracker, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func TimestampFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for timestamp, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for timestamp, got %T", args[0])
	}

	f := Timestamp
	_, isSelector := args[0].(*selectors.InstantVectorSelector)

	if isSelector {
		// We'll have already set ReturnSampleTimestamps on the InstantVectorSelector during the planning process, so we don't need to do that here.
		f.SeriesDataFunc = PassthroughData
	}

	o := NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval)
	return o, nil
}

func SortByLabelOperatorFactory(descending bool) FunctionOperatorFactory {
	functionName := "sort_by_label"
	if descending {
		functionName = "sort_by_label_desc"
	}

	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		if len(args) < 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected at least 1 argument for %s, got %v", functionName, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector for 1st argument for %s, got %T", functionName, args[0])
		}

		var labels []string
		for i := 1; i < len(args); i++ {
			l, ok := args[i].(types.StringOperator)
			if !ok {
				// Should be caught by the PromQL parser, but we check here for safety.
				return nil, fmt.Errorf("expected a string for argument %d, got %T", i+1, args[i])
			}

			labels = append(labels, l.GetValue())
		}

		// sort_by_labels and sort_by_labels_desc only affect the results of instant queries
		// since range query results have a fixed output ordering. However, we still validate
		// all the arguments as if we were going to sort for consistency.
		if !timeRange.IsInstant {
			return inner, nil
		}

		return NewSortByLabel(inner, descending, labels, opParams.MemoryConsumptionTracker, expressionPosition), nil
	}
}

func SortOperatorFactory(descending bool) FunctionOperatorFactory {
	functionName := "sort"

	if descending {
		functionName = "sort_desc"
	}

	return func(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
		if len(args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly 1 argument for %s, got %v", functionName, len(args))
		}

		inner, ok := args[0].(types.InstantVectorOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected an instant vector for 1st argument for %s, got %T", functionName, args[0])
		}

		if timeRange.StepCount != 1 {
			// If this is a range query, sort / sort_desc does not reorder series, but does drop all histograms like it would for an instant query.
			f := FunctionOverInstantVectorDefinition{SeriesDataFunc: DropHistograms}
			return NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
		}

		return NewSort(inner, descending, opParams.MemoryConsumptionTracker, expressionPosition), nil
	}
}

func InfoFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) < 2 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected at least 2 arguments for info, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector for 1st argument for info, got %T", args[0])
	}

	info, ok := args[1].(*selectors.InstantVectorSelector)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector selector for 2nd argument for info, got %T", args[1])
	}

	var identifyingLabels []string
	for i := 2; i < len(args); i++ {
		l, ok := args[i].(types.StringOperator)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected a string for argument %d for info, got %T", i+1, args[i])
		}

		identifyingLabels = append(identifyingLabels, l.GetValue())
	}

	if len(identifyingLabels) == 0 {
		identifyingLabels = []string{"instance", "job"}
	}

	return NewInfoFunction(inner, info, opParams.MemoryConsumptionTracker, timeRange, expressionPosition, identifyingLabels), nil
}

// RegisteredFunctions contains information for each registered function.
//
// Do not modify this map directly. Instead, call RegisterFunction.
var RegisteredFunctions = map[Function]FunctionMetadata{}

type FunctionMetadata struct {
	Name            string
	OperatorFactory FunctionOperatorFactory
	ReturnType      parser.ValueType
}

func RegisterFunction(function Function, name string, returnType parser.ValueType, factory FunctionOperatorFactory) error {
	if _, exists := RegisteredFunctions[function]; exists {
		return fmt.Errorf("function with ID %d has already been registered", function)
	}

	if existing, exists := promQLNamesToFunctions[name]; exists {
		return fmt.Errorf("function with name '%s' has already been registered with a different ID: %d", name, existing)
	}

	RegisteredFunctions[function] = FunctionMetadata{
		Name:            name,
		ReturnType:      returnType,
		OperatorFactory: factory,
	}
	promQLNamesToFunctions[name] = function

	return nil
}

func piOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for pi, got %v", len(args))
	}

	return scalars.NewScalarConstant(math.Pi, timeRange, opParams.MemoryConsumptionTracker, expressionPosition), nil
}

func timeOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 0 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 0 arguments for time, got %v", len(args))
	}

	return operators.NewTime(timeRange, opParams.MemoryConsumptionTracker, expressionPosition), nil
}

func instantVectorToScalarOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 1 argument for scalar, got %v", len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected an instant vector argument for scalar, got %T", args[0])
	}

	return scalars.NewInstantVectorToScalar(inner, timeRange, opParams.MemoryConsumptionTracker, expressionPosition), nil
}

func UnaryNegationOfInstantVectorOperatorFactory(inner types.InstantVectorOperator, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) types.Operator {
	f := FunctionOverInstantVectorDefinition{
		SeriesDataFunc:         UnaryNegation,
		SeriesMetadataFunction: DropSeriesName,
	}

	o := NewFunctionOverInstantVector(inner, nil, opParams.MemoryConsumptionTracker, f, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval)
	return o
}

func DoubleExponentialSmoothingFunctionOperatorFactory(args []types.Operator, _ labels.Labels, opParams *planning.OperatorParameters, expressionPosition posrange.PositionRange, timeRange types.QueryTimeRange) (types.Operator, error) {
	f := DoubleExponentialSmoothing

	functionName := "double_exponential_smoothing"
	if len(args) != 3 {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected exactly 3 arguments for %s, got %v", functionName, len(args))
	}

	inner, ok := args[0].(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected a range vector argument for %s, got %T", functionName, args[0])
	}

	smoothingFactor, ok := args[1].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected second argument for %s to be a scalar, got %T", functionName, args[1])
	}

	trendFactor, ok := args[2].(types.ScalarOperator)
	if !ok {
		// Should be caught by the PromQL parser, but we check here for safety.
		return nil, fmt.Errorf("expected third argument for %s to be a scalar, got %T", functionName, args[2])
	}

	return NewFunctionOverRangeVector(inner, []types.ScalarOperator{smoothingFactor, trendFactor}, opParams.MemoryConsumptionTracker, f, opParams.Annotations, expressionPosition, timeRange, opParams.QueryParameters.EnableDelayedNameRemoval), nil
}

func init() {
	//lint:sorted
	must(RegisterFunction(FUNCTION_ABS, "abs", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("abs", Abs)))
	must(RegisterFunction(FUNCTION_ABSENT, "absent", parser.ValueTypeVector, AbsentOperatorFactory))
	must(RegisterFunction(FUNCTION_ABSENT_OVER_TIME, "absent_over_time", parser.ValueTypeVector, AbsentOverTimeOperatorFactory))
	must(RegisterFunction(FUNCTION_ACOS, "acos", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("acos", Acos)))
	must(RegisterFunction(FUNCTION_ACOSH, "acosh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("acosh", Acosh)))
	must(RegisterFunction(FUNCTION_ASIN, "asin", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("asin", Asin)))
	must(RegisterFunction(FUNCTION_ASINH, "asinh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("asinh", Asinh)))
	must(RegisterFunction(FUNCTION_ATAN, "atan", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("atan", Atan)))
	must(RegisterFunction(FUNCTION_ATANH, "atanh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("atanh", Atanh)))
	must(RegisterFunction(FUNCTION_AVG_OVER_TIME, "avg_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("avg_over_time", AvgOverTime)))
	must(RegisterFunction(FUNCTION_CEIL, "ceil", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("ceil", Ceil)))
	must(RegisterFunction(FUNCTION_CHANGES, "changes", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("changes", Changes)))
	must(RegisterFunction(FUNCTION_CLAMP, "clamp", parser.ValueTypeVector, ClampFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_CLAMP_MAX, "clamp_max", parser.ValueTypeVector, ClampMinMaxFunctionOperatorFactory("clamp_max", false)))
	must(RegisterFunction(FUNCTION_CLAMP_MIN, "clamp_min", parser.ValueTypeVector, ClampMinMaxFunctionOperatorFactory("clamp_min", true)))
	must(RegisterFunction(FUNCTION_COS, "cos", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("cos", Cos)))
	must(RegisterFunction(FUNCTION_COSH, "cosh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("cosh", Cosh)))
	must(RegisterFunction(FUNCTION_COUNT_OVER_TIME, "count_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("count_over_time", CountOverTime)))
	must(RegisterFunction(FUNCTION_DAYS_IN_MONTH, "days_in_month", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("days_in_month", DaysInMonth)))
	must(RegisterFunction(FUNCTION_DAY_OF_MONTH, "day_of_month", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("day_of_month", DayOfMonth)))
	must(RegisterFunction(FUNCTION_DAY_OF_WEEK, "day_of_week", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("day_of_week", DayOfWeek)))
	must(RegisterFunction(FUNCTION_DAY_OF_YEAR, "day_of_year", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("day_of_year", DayOfYear)))
	must(RegisterFunction(FUNCTION_DEG, "deg", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("deg", Deg)))
	must(RegisterFunction(FUNCTION_DELTA, "delta", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("delta", Delta)))
	must(RegisterFunction(FUNCTION_DERIV, "deriv", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("deriv", Deriv)))
	must(RegisterFunction(FUNCTION_DOUBLE_EXPONENTIAL_SMOOTHING, "double_exponential_smoothing", parser.ValueTypeVector, DoubleExponentialSmoothingFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_EXP, "exp", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("exp", Exp)))
	must(RegisterFunction(FUNCTION_FIRST_OVER_TIME, "first_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("first_over_time", FirstOverTime)))
	must(RegisterFunction(FUNCTION_FLOOR, "floor", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("floor", Floor)))
	must(RegisterFunction(FUNCTION_HISTOGRAM_AVG, "histogram_avg", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("histogram_avg", HistogramAvg)))
	must(RegisterFunction(FUNCTION_HISTOGRAM_COUNT, "histogram_count", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("histogram_count", HistogramCount)))
	must(RegisterFunction(FUNCTION_HISTOGRAM_FRACTION, "histogram_fraction", parser.ValueTypeVector, HistogramFractionFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_HISTOGRAM_QUANTILE, "histogram_quantile", parser.ValueTypeVector, HistogramQuantileFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_HISTOGRAM_STDDEV, "histogram_stddev", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("histogram_stddev", HistogramStdDevStdVar(true))))
	must(RegisterFunction(FUNCTION_HISTOGRAM_STDVAR, "histogram_stdvar", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("histogram_stdvar", HistogramStdDevStdVar(false))))
	must(RegisterFunction(FUNCTION_HISTOGRAM_SUM, "histogram_sum", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("histogram_sum", HistogramSum)))
	must(RegisterFunction(FUNCTION_HOUR, "hour", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("hour", Hour)))
	must(RegisterFunction(FUNCTION_IDELTA, "idelta", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("idelta", Idelta)))
	must(RegisterFunction(FUNCTION_INCREASE, "increase", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("increase", Increase)))
	must(RegisterFunction(FUNCTION_INFO, "info", parser.ValueTypeVector, InfoFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_IRATE, "irate", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("irate", Irate)))
	must(RegisterFunction(FUNCTION_LABEL_JOIN, "label_join", parser.ValueTypeVector, LabelJoinFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_LABEL_REPLACE, "label_replace", parser.ValueTypeVector, LabelReplaceFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_LAST_OVER_TIME, "last_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("last_over_time", LastOverTime)))
	must(RegisterFunction(FUNCTION_LN, "ln", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("ln", Ln)))
	must(RegisterFunction(FUNCTION_LOG10, "log10", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("log10", Log10)))
	must(RegisterFunction(FUNCTION_LOG2, "log2", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("log2", Log2)))
	must(RegisterFunction(FUNCTION_MAD_OVER_TIME, "mad_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("mad_over_time", MadOverTime)))
	must(RegisterFunction(FUNCTION_MAX_OVER_TIME, "max_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("max_over_time", MaxOverTime)))
	must(RegisterFunction(FUNCTION_MINUTE, "minute", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("minute", Minute)))
	must(RegisterFunction(FUNCTION_MIN_OVER_TIME, "min_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("min_over_time", MinOverTime)))
	must(RegisterFunction(FUNCTION_MONTH, "month", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("month", Month)))
	must(RegisterFunction(FUNCTION_PI, "pi", parser.ValueTypeScalar, piOperatorFactory))
	must(RegisterFunction(FUNCTION_PREDICT_LINEAR, "predict_linear", parser.ValueTypeVector, PredictLinearFactory))
	must(RegisterFunction(FUNCTION_PRESENT_OVER_TIME, "present_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("present_over_time", PresentOverTime)))
	must(RegisterFunction(FUNCTION_QUANTILE_OVER_TIME, "quantile_over_time", parser.ValueTypeVector, QuantileOverTimeFactory))
	must(RegisterFunction(FUNCTION_RAD, "rad", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("rad", Rad)))
	must(RegisterFunction(FUNCTION_RATE, "rate", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("rate", Rate)))
	must(RegisterFunction(FUNCTION_RESETS, "resets", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("resets", Resets)))
	must(RegisterFunction(FUNCTION_ROUND, "round", parser.ValueTypeVector, RoundFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_SCALAR, "scalar", parser.ValueTypeScalar, instantVectorToScalarOperatorFactory))
	must(RegisterFunction(FUNCTION_SGN, "sgn", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("sgn", Sgn)))
	must(RegisterFunction(FUNCTION_SIN, "sin", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("sin", Sin)))
	must(RegisterFunction(FUNCTION_SINH, "sinh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("sinh", Sinh)))
	must(RegisterFunction(FUNCTION_SORT, "sort", parser.ValueTypeVector, SortOperatorFactory(false)))
	must(RegisterFunction(FUNCTION_SORT_BY_LABEL, "sort_by_label", parser.ValueTypeVector, SortByLabelOperatorFactory(false)))
	must(RegisterFunction(FUNCTION_SORT_BY_LABEL_DESC, "sort_by_label_desc", parser.ValueTypeVector, SortByLabelOperatorFactory(true)))
	must(RegisterFunction(FUNCTION_SORT_DESC, "sort_desc", parser.ValueTypeVector, SortOperatorFactory(true)))
	must(RegisterFunction(FUNCTION_SQRT, "sqrt", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("sqrt", Sqrt)))
	must(RegisterFunction(FUNCTION_STDDEV_OVER_TIME, "stddev_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("stddev_over_time", StddevOverTime)))
	must(RegisterFunction(FUNCTION_STDVAR_OVER_TIME, "stdvar_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("stdvar_over_time", StdvarOverTime)))
	must(RegisterFunction(FUNCTION_SUM_OVER_TIME, "sum_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("sum_over_time", SumOverTime)))
	must(RegisterFunction(FUNCTION_TAN, "tan", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("tan", Tan)))
	must(RegisterFunction(FUNCTION_TANH, "tanh", parser.ValueTypeVector, InstantVectorTransformationFunctionOperatorFactory("tanh", Tanh)))
	must(RegisterFunction(FUNCTION_TIME, "time", parser.ValueTypeScalar, timeOperatorFactory))
	must(RegisterFunction(FUNCTION_TIMESTAMP, "timestamp", parser.ValueTypeVector, TimestampFunctionOperatorFactory))
	must(RegisterFunction(FUNCTION_TS_OF_FIRST_OVER_TIME, "ts_of_first_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("ts_of_first_over_time", TsOfFirstOverTime)))
	must(RegisterFunction(FUNCTION_TS_OF_LAST_OVER_TIME, "ts_of_last_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("ts_of_last_over_time", TsOfLastOverTime)))
	must(RegisterFunction(FUNCTION_TS_OF_MAX_OVER_TIME, "ts_of_max_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("ts_of_max_over_time", TsOfMaxOverTime)))
	must(RegisterFunction(FUNCTION_TS_OF_MIN_OVER_TIME, "ts_of_min_over_time", parser.ValueTypeVector, FunctionOverRangeVectorOperatorFactory("ts_of_min_over_time", TsOfMinOverTime)))
	must(RegisterFunction(FUNCTION_VECTOR, "vector", parser.ValueTypeVector, scalarToInstantVectorOperatorFactory))
	must(RegisterFunction(FUNCTION_YEAR, "year", parser.ValueTypeVector, TimeTransformationFunctionOperatorFactory("year", Year)))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
