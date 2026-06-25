// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// EvaluationRootFunctionName is the name of the internal marker function used to denote the root of a
// query that should be evaluated independently. It is injected when spinning off subqueries from
// instant queries: the outer downstream query parts and each spun-off subquery are each wrapped in a
// call to this function so that sharding, splitting, caching and remote execution can treat each one as
// a separate query.
//
// It is not a real function: it is intercepted while converting the AST to a query plan and replaced
// with an EvaluationRoot node, and it is blocked from appearing in user queries.
const EvaluationRootFunctionName = "__evaluation_root__"

// EvaluationRootFunction is the PromQL function definition for EvaluationRootFunctionName.
var EvaluationRootFunction = &parser.Function{
	Name:       EvaluationRootFunctionName,
	ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
	ReturnType: parser.ValueTypeVector,
}

func init() {
	parser.Functions[EvaluationRootFunction.Name] = EvaluationRootFunction

	planning.RegisterNodeFactory(func() planning.Node {
		return &EvaluationRoot{EvaluationRootDetails: &EvaluationRootDetails{}}
	})
}

// EvaluationRoot marks the root of a query that should be evaluated independently of the rest of the
// plan, emulating the behaviour of the subquery spin-off query-frontend middleware. It is a
// transparent pass-through at evaluation time: it never alters the result of its child.
//
//node:generate
type EvaluationRoot struct {
	*EvaluationRootDetails
	Inner planning.Node `node:"child"`
}

func (e *EvaluationRoot) Details() proto.Message {
	return e.EvaluationRootDetails
}

func (e *EvaluationRoot) NodeType() planning.NodeType {
	return planning.NODE_TYPE_EVALUATION_ROOT
}

func (e *EvaluationRoot) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*EvaluationRoot)

	return ok
}

func (e *EvaluationRoot) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (e *EvaluationRoot) Describe() string {
	return ""
}

func (e *EvaluationRoot) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (e *EvaluationRoot) ResultType() (parser.ValueType, error) {
	return e.Inner.ResultType()
}

func (e *EvaluationRoot) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return e.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (e *EvaluationRoot) ExpressionPosition() (posrange.PositionRange, error) {
	return e.Inner.ExpressionPosition()
}

func (e *EvaluationRoot) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV17, nil
}

// MaterializeEvaluationRoot returns the inner operator unchanged: EvaluationRoot is a planning-time
// marker only and has no effect at evaluation time.
func MaterializeEvaluationRoot(ctx context.Context, e *EvaluationRoot, materializer *planning.Materializer, timeRange types.QueryTimeRange, _ *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToOperator(ctx, e.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(inner), nil
}
