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
	return SubqueryTimeRange(timeRange, s.Range, s.Step, s.Timestamp, s.Offset)
}

// FIXME: inline this into the method above once we no longer support directly converting from PromQL expressions to operators
func SubqueryTimeRange(parentTimeRange types.QueryTimeRange, rng time.Duration, step time.Duration, ts *time.Time, offset time.Duration) types.QueryTimeRange {
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

	start := parentTimeRange.StartT
	end := parentTimeRange.EndT
	stepMilliseconds := step.Milliseconds()

	if ts != nil {
		start = timestamp.FromTime(*ts)
		end = start
	}

	// Find the first timestamp inside the subquery range that is aligned to the step.
	alignedStart := stepMilliseconds * ((start - offset.Milliseconds() - rng.Milliseconds()) / stepMilliseconds)
	if alignedStart < start-offset.Milliseconds()-rng.Milliseconds() {
		alignedStart += stepMilliseconds
	}

	end = end - offset.Milliseconds()

	return types.NewRangeQueryTimeRange(timestamp.Time(alignedStart), timestamp.Time(end), step)
}

func (s *Subquery) Details() proto.Message {
	return s.SubqueryDetails
}

func (s *Subquery) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SUBQUERY
}

func (s *Subquery) Children() []planning.Node {
	return []planning.Node{s.Inner}
}

func (s *Subquery) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Subquery expects 1 child, but got %d", len(children))
	}

	s.Inner = children[0]

	return nil
}

func (s *Subquery) EquivalentTo(other planning.Node) bool {
	otherSubquery, ok := other.(*Subquery)

	return ok &&
		((s.Timestamp == nil && otherSubquery.Timestamp == nil) || (s.Timestamp != nil && otherSubquery.Timestamp != nil && s.Timestamp.Equal(*otherSubquery.Timestamp))) &&
		s.Offset == otherSubquery.Offset &&
		s.Range == otherSubquery.Range &&
		s.Step == otherSubquery.Step &&
		s.Inner.EquivalentTo(otherSubquery.Inner)
}

func (s *Subquery) ChildrenLabels() []string {
	return []string{""}
}

func (s *Subquery) OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for Subquery, got %v", len(children))
	}

	inner, ok := children[0].(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator as child of Subquery, got %T", children[0])
	}
	o, err := operators.NewSubquery(inner, timeRange, s.ChildrenTimeRange(timeRange), TimestampFromTime(s.Timestamp), s.Offset, s.Range, s.ExpressionPosition.ToPrometheusType(), params.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (s *Subquery) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeMatrix, nil
}
