// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type FunctionCall struct {
	*FunctionCallDetails
	Args []planning.Node `json:"-"`
}

func (f *FunctionCall) Describe() string {
	if len(f.AbsentLabels) == 0 {
		return fmt.Sprintf("%v(...)", f.FunctionName)
	}

	lbls := mimirpb.FromLabelAdaptersToString(f.AbsentLabels)

	return fmt.Sprintf("%v(...) with labels %v", f.FunctionName, lbls)
}

func (f *FunctionCall) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (f *FunctionCall) Details() proto.Message {
	return f.FunctionCallDetails
}

func (f *FunctionCall) NodeType() planning.NodeType {
	return planning.NODE_TYPE_FUNCTION_CALL
}

func (f *FunctionCall) Children() []planning.Node {
	return f.Args
}

func (f *FunctionCall) SetChildren(children []planning.Node) error {
	f.Args = children
	return nil
}

func (f *FunctionCall) EquivalentTo(other planning.Node) bool {
	otherFunctionCall, ok := other.(*FunctionCall)

	return ok &&
		f.FunctionName == otherFunctionCall.FunctionName &&
		slices.EqualFunc(f.Args, otherFunctionCall.Args, func(a, b planning.Node) bool {
			return a.EquivalentTo(b)
		}) &&
		slices.Equal(f.AbsentLabels, otherFunctionCall.AbsentLabels)
}

func (f *FunctionCall) ChildrenLabels() []string {
	if len(f.Args) == 0 {
		return nil
	}

	if len(f.Args) == 1 {
		return []string{""}
	}

	l := make([]string, len(f.Args))

	for i := range l {
		l[i] = fmt.Sprintf("param %v", i)
	}

	return l
}

func (f *FunctionCall) OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	instantVectorFactory, ok := functions.InstantVectorFunctionOperatorFactories[f.FunctionName]
	if ok {
		o, err := instantVectorFactory(children, params.MemoryConsumptionTracker, params.Annotations, f.ExpressionPosition.ToPrometheusType(), timeRange)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(o), nil
	}

	scalarFactory, ok := functions.ScalarFunctionOperatorFactories[f.FunctionName]
	if ok {
		o, err := scalarFactory(children, params.MemoryConsumptionTracker, params.Annotations, f.ExpressionPosition.ToPrometheusType(), timeRange)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(o), nil
	}

	// absent and absent_over_time need special handling because we need to pass the series labels to their operators.
	switch f.FunctionName {
	case "absent":
		if len(children) != 1 {
			return nil, fmt.Errorf("expected exactly 1 parameter for '%v', got %v", f.FunctionName, len(children))
		}

		inner, ok := children[0].(types.InstantVectorOperator)
		if !ok {
			return nil, fmt.Errorf("expected InstantVectorOperator as parameter of '%v' function call, got %T", f.FunctionName, children[0])
		}

		o := functions.NewAbsent(inner, mimirpb.FromLabelAdaptersToLabels(f.AbsentLabels), timeRange, params.MemoryConsumptionTracker, f.ExpressionPosition.ToPrometheusType())
		return planning.NewSingleUseOperatorFactory(o), nil

	case "absent_over_time":
		if len(children) != 1 {
			return nil, fmt.Errorf("expected exactly 1 parameter for '%v', got %v", f.FunctionName, len(children))
		}

		inner, ok := children[0].(types.RangeVectorOperator)
		if !ok {
			return nil, fmt.Errorf("expected InstantVectorOperator as parameter of '%v' function call, got %T", f.FunctionName, children[0])
		}

		o := functions.NewAbsentOverTime(inner, mimirpb.FromLabelAdaptersToLabels(f.AbsentLabels), timeRange, params.MemoryConsumptionTracker, f.ExpressionPosition.ToPrometheusType())
		return planning.NewSingleUseOperatorFactory(o), nil

	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%v' function", f.FunctionName))
	}
}

func (f *FunctionCall) ResultType() (parser.ValueType, error) {
	// absent and absent_over_time are special cases that aren't present in InstantVectorFunctionOperatorFactories,
	// so we have to handle them specifically.
	if _, ok := functions.InstantVectorFunctionOperatorFactories[f.FunctionName]; ok || f.FunctionName == "absent" || f.FunctionName == "absent_over_time" {
		return parser.ValueTypeVector, nil
	}

	if _, ok := functions.ScalarFunctionOperatorFactories[f.FunctionName]; ok {
		return parser.ValueTypeScalar, nil
	}

	return parser.ValueTypeNone, compat.NewNotSupportedError(fmt.Sprintf("'%v' function", f.FunctionName))
}
