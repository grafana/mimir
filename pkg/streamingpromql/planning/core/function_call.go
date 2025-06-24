// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
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
		return fmt.Sprintf("%v(...)", f.Function.PromQLName())
	}

	lbls := mimirpb.FromLabelAdaptersToString(f.AbsentLabels)

	return fmt.Sprintf("%v(...) with labels %v", f.Function.PromQLName(), lbls)
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
		f.Function == otherFunctionCall.Function &&
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
		functionParams := functions.InstantVectorFunctionOperatorParams{
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			NameValidationScheme:     params.NameValidationScheme,
			ExpressionPosition:       f.ExpressionPosition.ToPrometheusType(),
			TimeRange:                timeRange,
		}
		o, err := instantVectorFactory(children, functionParams)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(o), nil
	}

	var absentLabels labels.Labels

	if f.Function == functions.FUNCTION_ABSENT || f.Function == functions.FUNCTION_ABSENT_OVER_TIME {
		absentLabels = mimirpb.FromLabelAdaptersToLabels(f.AbsentLabels)
	}

	o, err := fnc.OperatorFactory(children, absentLabels, params.MemoryConsumptionTracker, params.Annotations, f.ExpressionPosition.ToPrometheusType(), timeRange)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (f *FunctionCall) ResultType() (parser.ValueType, error) {
	if fnc, ok := functions.RegisteredFunctions[f.Function]; ok {
		return fnc.ReturnType, nil
	}

	return parser.ValueTypeNone, compat.NewNotSupportedError(fmt.Sprintf("'%v' function", f.Function.PromQLName()))
}
