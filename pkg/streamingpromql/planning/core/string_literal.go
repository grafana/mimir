// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"strconv"
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

func (s *StringLiteral) Children() []planning.Node {
	return nil
}

func (s *StringLiteral) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type StringLiteral supports 0 children, but got %d", len(children))
	}

	return nil
}

func (s *StringLiteral) Equals(other planning.Node) bool {
	otherLiteral, ok := other.(*StringLiteral)

	return ok && s.Value == otherLiteral.Value
}

func (s *StringLiteral) ChildrenLabels() []string {
	return nil
}

func (s *StringLiteral) OperatorFactory(_ []types.Operator, _ types.QueryTimeRange, _ *planning.OperatorParameters) (planning.OperatorFactory, error) {
	o := operators.NewStringLiteral(s.Value, s.ExpressionPosition.ToPrometheusType())

	return planning.NewSingleUseOperatorFactory(o), nil
}
