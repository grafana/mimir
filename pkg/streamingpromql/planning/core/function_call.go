// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

//node:generate
type FunctionCall struct {
	*FunctionCallDetails
	Args []planning.Node `json:"-" node:"children,labelfmt=param %d"`
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

func (f *FunctionCall) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func MaterializeFunctionCall(ctx context.Context, f *FunctionCall, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	fnc, ok := functions.RegisteredFunctions[f.Function]
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%v' function", f.Function.PromQLName()))
	}

	children := make([]types.Operator, 0, len(f.Args))
	for _, arg := range f.Args {
		o, err := materializer.ConvertNodeToOperator(ctx, arg, timeRange)
		if err != nil {
			return nil, err
		}

		children = append(children, o)
	}

	var absentLabels labels.Labels

	if f.Function == functions.FUNCTION_ABSENT || f.Function == functions.FUNCTION_ABSENT_OVER_TIME {
		absentLabels = mimirpb.FromLabelAdaptersToLabels(f.AbsentLabels)
	}

	if f.Function == functions.FUNCTION_INFO && len(f.Args) == 2 {
		// Propagate the @/offset modifiers of the first selector in the first argument to the
		// data label selector, mirroring Prometheus's infoSelectHints, so that info series are
		// selected at the same (shifted) time as the samples they enrich. This is derived from
		// the plan here rather than in the operator factory because operators do not support
		// generic child traversal, so nested selectors would not be reachable there.
		if dataLabelSelector, ok := children[1].(*functions.DataLabelSelector); ok {
			if ts, offset, found := infoSelectTimestampAndOffset(f.Args[0]); found {
				dataLabelSelector.Selector.Timestamp = TimestampFromTime(ts)
				dataLabelSelector.Selector.Offset = offset.Milliseconds()
			}
		}
	}

	o, err := fnc.OperatorFactory(children, absentLabels, params, f.GetExpressionPosition().ToPrometheusType(), timeRange)
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

func (f *FunctionCall) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	timeRange := planning.NoDataQueried()

	for _, arg := range f.Args {
		argTimeRange, err := arg.QueriedTimeRange(queryTimeRange, lookbackDelta)
		if err != nil {
			return planning.NoDataQueried(), err
		}

		timeRange = timeRange.Union(argTimeRange)
	}

	return timeRange, nil
}

func (f *FunctionCall) ExpressionPosition() (posrange.PositionRange, error) {
	return f.GetExpressionPosition().ToPrometheusType(), nil
}

func (f *FunctionCall) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanVersionZero, nil
}
