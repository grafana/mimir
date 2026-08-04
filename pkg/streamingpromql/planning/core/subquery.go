// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
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

//node:generate
type Subquery struct {
	*SubqueryDetails
	Inner planning.Node `json:"-" node:"child"`
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
	return SubqueryChildrenTimeRange(timeRange, s.Range, s.Step, s.Offset, s.Timestamp)
}

// SubqueryChildrenTimeRange computes the time range used by the children of a subquery with the given
// range, step, offset and @ timestamp (ts, nil if the subquery does not use the @ modifier), when the
// subquery is evaluated over parentTimeRange.
func SubqueryChildrenTimeRange(parentTimeRange types.QueryTimeRange, subqueryRange, step, offset time.Duration, ts *time.Time) types.QueryTimeRange {
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
	} else if !parentTimeRange.IsInstant {
		// Align the parent end timestamp down to the parent's step grid before applying the
		// subquery offset.
		// This ensures the subquery does not evaluate past the parent's last actual step if the
		// parent's end time isn't aligned to its step.
		// For example, if the step is 1h, and the parent time range is 09:00 to 11:30, then the last
		// parent step is 11:00, and the subquery should not evaluate past that.
		end = start + ((end-start)/parentTimeRange.IntervalMilliseconds)*parentTimeRange.IntervalMilliseconds
	}

	// Find the first timestamp inside the subquery range that is aligned to the step.
	// +1 because the query time range is inclusive of the start timestamp, but the subquery range is exclusive of the start.
	alignedStart := stepMilliseconds * ((start - offset.Milliseconds() - subqueryRange.Milliseconds() + 1) / stepMilliseconds)
	if alignedStart < start-offset.Milliseconds()-subqueryRange.Milliseconds()+1 {
		alignedStart += stepMilliseconds
	}

	// Note that this timestamp may not be aligned to the subquery's step grid, but this isn't an issue:
	// the subquery will be evaluated up to the last step within the range, just like the behaviour for top-level queries.
	// For example, if the start of the range is 09:00 and the subquery step is 1h, it doesn't matter if
	// the end is 11:00, 11:01 or 11:59, the last evaluated step will be 11:00, as expected.
	end = end - offset.Milliseconds()

	return types.NewRangeQueryTimeRange(timestamp.Time(alignedStart), timestamp.Time(end), step)
}

func (s *Subquery) Details() proto.Message {
	return s.SubqueryDetails
}

func (s *Subquery) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SUBQUERY
}

func (s *Subquery) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func MaterializeSubquery(ctx context.Context, s *Subquery, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	innerTimeRange := s.ChildrenTimeRange(timeRange)
	inner, err := materializer.ConvertNodeToInstantVectorOperator(ctx, s.Inner, innerTimeRange)
	if err != nil {
		return nil, fmt.Errorf("could not create inner operator for Subquery: %w", err)
	}

	o, err := operators.NewSubquery(inner, timeRange, innerTimeRange, TimestampFromTime(s.Timestamp), s.Offset, s.Range, s.GetExpressionPosition().ToPrometheusType(), params.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (s *Subquery) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeMatrix, nil
}

func (s *Subquery) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(s.ChildrenTimeRange(queryTimeRange), lookbackDelta)
}

func (s *Subquery) ExpressionPosition() (posrange.PositionRange, error) {
	return s.GetExpressionPosition().ToPrometheusType(), nil
}

func (s *Subquery) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanVersionZero, nil
}
