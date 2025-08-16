// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

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

func (s *StringLiteral) Children() []planning.Node {
	return nil
}

func (s *StringLiteral) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type StringLiteral expects 0 children, but got %d", len(children))
	}

	return nil
}

func (s *StringLiteral) EquivalentTo(other planning.Node) bool {
	otherLiteral, ok := other.(*StringLiteral)

	return ok && s.Value == otherLiteral.Value
}

func (s *StringLiteral) ChildrenLabels() []string {
	return nil
}

func MaterializeStringLiteral(s *StringLiteral, _ *planning.Materializer, _ types.QueryTimeRange, _ *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := operators.NewStringLiteral(s.Value, s.ExpressionPosition.ToPrometheusType())

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (s *StringLiteral) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeString, nil
}

func (s *StringLiteral) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return planning.NoDataQueried()
}
