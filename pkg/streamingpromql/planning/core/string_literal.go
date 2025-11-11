// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type StringLiteral struct {
	*StringLiteralDetails
}

func (s *StringLiteral) Describe() string {
	return strconv.QuoteToASCII(s.Value)
}

func (s *StringLiteral) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (s *StringLiteral) Details() proto.Message {
	return s.StringLiteralDetails
}

func (s *StringLiteral) NodeType() planning.NodeType {
	return planning.NODE_TYPE_STRING_LITERAL
}

func (s *StringLiteral) Child(idx int) planning.Node {
	panic(fmt.Sprintf("node of type StringLiteral has no children, but attempted to get child at index %d", idx))
}

func (s *StringLiteral) ChildCount() int {
	return 0
}

func (s *StringLiteral) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type StringLiteral expects 0 children, but got %d", len(children))
	}

	return nil
}

func (s *StringLiteral) ReplaceChild(idx int, node planning.Node) error {
	return fmt.Errorf("node of type StringLiteral supports no children, but attempted to replace child at index %d", idx)
}

func (s *StringLiteral) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherLiteral, ok := other.(*StringLiteral)

	return ok && s.Value == otherLiteral.Value
}

func (s *StringLiteral) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (s *StringLiteral) ChildrenLabels() []string {
	return nil
}

func MaterializeStringLiteral(s *StringLiteral, _ *planning.Materializer, _ types.QueryTimeRange, _ *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := operators.NewStringLiteral(s.Value, s.ExpressionPosition())

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (s *StringLiteral) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeString, nil
}

func (s *StringLiteral) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return planning.NoDataQueried()
}

func (s *StringLiteral) ExpressionPosition() posrange.PositionRange {
	return s.GetExpressionPosition().ToPrometheusType()
}

func (s *StringLiteral) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}
