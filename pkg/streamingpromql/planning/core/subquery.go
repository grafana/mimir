// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Subquery struct {
	*SubqueryDetails
	Inner planning.Node `json:"-"`
}

func (s *Subquery) Describe() string {
	builder := &strings.Builder{}

	builder.WriteRune('[')
	builder.WriteString(s.Range.String())
	builder.WriteRune(':')
	builder.WriteString(s.Step.String())
	builder.WriteRune(']')

	if s.Timestamp != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(timestamp.FromTime(*s.Timestamp), 10))
		builder.WriteString(" (")
		builder.WriteString(s.Timestamp.Format(time.RFC3339Nano))
		builder.WriteRune(')')
	}

	if s.Offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(s.Offset.String())
	}

	return builder.String()
}

func (s *Subquery) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	// Subqueries are evaluated as a single range query with steps aligned to Unix epoch time 0.
	// They are not evaluated as queries aligned to the individual step timestamps.
	// See https://www.robustperception.io/promql-subqueries-and-alignment/ for an explanation.
	// Subquery evaluation aligned to step timestamps is not supported by Prometheus, but may be
	// introduced in the future in https://github.com/prometheus/prometheus/pull/9114.
	//
	// While this makes subqueries simpler to implement and more efficient in most cases, it does
	// mean we could waste time evaluating steps that won't be used if the subquery range is less
	// than the parent query step. For example, if the parent query is running with a step of 1h,
	// and the subquery is for a 10m range with 1m steps, then we'll evaluate ~50m of steps that
	// won't be used.
	// This is relatively uncommon, and Prometheus' engine does the same thing. In the future, we
	// could be smarter about this if it turns out to be a big problem.

	start := timeRange.StartT
	end := timeRange.EndT
	stepMilliseconds := s.Step.Milliseconds()

	if s.Timestamp != nil {
		start = timestamp.FromTime(*s.Timestamp)
		end = start
	}

	// Find the first timestamp inside the subquery range that is aligned to the step.
	// +1 because the query time range is inclusive of the start timestamp, but the subquery range is exclusive of the start.
	alignedStart := stepMilliseconds * ((start - s.Offset.Milliseconds() - s.Range.Milliseconds() + 1) / stepMilliseconds)
	if alignedStart < start-s.Offset.Milliseconds()-s.Range.Milliseconds()+1 {
		alignedStart += stepMilliseconds
	}

	end = end - s.Offset.Milliseconds()
	return types.NewRangeQueryTimeRange(timestamp.Time(alignedStart), timestamp.Time(end), s.Step)
}

func (s *Subquery) Details() proto.Message {
	return s.SubqueryDetails
}

func (s *Subquery) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SUBQUERY
}

func (s *Subquery) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type Subquery supports 1 child, but attempted to get child at index %d", idx))
	}

	return s.Inner
}

func (s *Subquery) ChildCount() int {
	return 1
}

func (s *Subquery) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Subquery expects 1 child, but got %d", len(children))
	}

	s.Inner = children[0]

	return nil
}

func (s *Subquery) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type Subquery supports 1 child, but attempted to replace child at index %d", idx)
	}

	s.Inner = node
	return nil
}

func (s *Subquery) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherSubquery, ok := other.(*Subquery)

	return ok &&
		((s.Timestamp == nil && otherSubquery.Timestamp == nil) || (s.Timestamp != nil && otherSubquery.Timestamp != nil && s.Timestamp.Equal(*otherSubquery.Timestamp))) &&
		s.Offset == otherSubquery.Offset &&
		s.Range == otherSubquery.Range &&
		s.Step == otherSubquery.Step
}

func (s *Subquery) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (s *Subquery) ChildrenLabels() []string {
	return []string{""}
}

func MaterializeSubquery(s *Subquery, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	innerTimeRange := s.ChildrenTimeRange(timeRange)
	inner, err := materializer.ConvertNodeToInstantVectorOperator(s.Inner, innerTimeRange)
	if err != nil {
		return nil, fmt.Errorf("could not create inner operator for Subquery: %w", err)
	}

	o, err := operators.NewSubquery(inner, timeRange, innerTimeRange, TimestampFromTime(s.Timestamp), s.Offset, s.Range, s.ExpressionPosition(), params.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (s *Subquery) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeMatrix, nil
}

func (s *Subquery) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return s.Inner.QueriedTimeRange(s.ChildrenTimeRange(queryTimeRange), lookbackDelta)
}

func (s *Subquery) ExpressionPosition() posrange.PositionRange {
	return s.GetExpressionPosition().ToPrometheusType()
}

func (s *Subquery) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}
