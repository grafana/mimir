// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

//node:generate
type DataLabelSelector struct {
	*DataLabelSelectorDetails
}

// infoSelectTimestampAndOffset returns the @ timestamp and offset the info data label selector
// should use, taken from the first vector or matrix selector in a pre-order traversal of the info
// function's first argument (mirroring Prometheus's infoSelectHints; enclosing subqueries compose
// their offsets until an @ timestamp anchors the reference). uniform is false when the vector paths
// don't all share one reference (mixed references, selector-free vectors, or only scalar selectors),
// in which case the caller must not pin the lookup so enrichment stays step-dependent.
func infoSelectTimestampAndOffset(node planning.Node) (ts *time.Time, offset time.Duration, uniform bool) {
	w := infoReferenceWalk{uniform: true}
	w.inspect(node, nil)
	if !w.found {
		return nil, 0, false
	}
	return w.first.timestamp, w.first.offset, w.uniform && !w.referenceFree
}

type infoSeriesReference struct {
	timestamp *time.Time
	offset    time.Duration
}

func (r infoSeriesReference) equal(other infoSeriesReference) bool {
	if r.timestamp == nil || other.timestamp == nil {
		return r.timestamp == nil && other.timestamp == nil && r.offset == other.offset
	}
	return r.timestamp.Add(-r.offset).Equal(other.timestamp.Add(-other.offset))
}

type infoReferenceWalk struct {
	first         infoSeriesReference
	found         bool
	referenceFree bool
	uniform       bool
}

// inspect records the effective reference of each selector in a vector-producing subtree and
// returns whether it contains any. enclosingSubqueries are those it's nested within, outermost first.
func (w *infoReferenceWalk) inspect(node planning.Node, enclosingSubqueries []*Subquery) bool {
	if valueType, err := node.ResultType(); err != nil || (valueType != parser.ValueTypeVector && valueType != parser.ValueTypeMatrix) {
		// Not a vector-producing data source for enrichment.
		return false
	}

	switch n := node.(type) {
	case *VectorSelector:
		w.addSelector(n.Timestamp, n.Offset, enclosingSubqueries)
		return true
	case *MatrixSelector:
		w.addSelector(n.Timestamp, n.Offset, enclosingSubqueries)
		return true
	case *DataLabelSelector:
		// A nested info's data label selector is selector syntax, not an evaluated vector.
		return false
	}

	if sq, ok := node.(*Subquery); ok {
		enclosingSubqueries = append(enclosingSubqueries, sq)
	}

	hasSelector := false
	for child := range planning.ChildrenIter(node) {
		if w.inspect(child, enclosingSubqueries) {
			hasSelector = true
		}
	}
	if !hasSelector {
		// A selector-free vector follows evaluation time; it can't be pinned by another path.
		w.referenceFree = true
	}
	return hasSelector
}

// addSelector records a selector's reference, composing its enclosing subqueries' offsets and
// innermost @ timestamp.
func (w *infoReferenceWalk) addSelector(ts *time.Time, offset time.Duration, enclosingSubqueries []*Subquery) {
	ref := infoSeriesReference{timestamp: ts, offset: offset}
	for i := len(enclosingSubqueries) - 1; ref.timestamp == nil && i >= 0; i-- {
		ref.offset += enclosingSubqueries[i].Offset
		ref.timestamp = enclosingSubqueries[i].Timestamp
	}

	if !w.found {
		w.first = ref
		w.found = true
	} else if !w.first.equal(ref) {
		w.uniform = false
	}
}

func (t *DataLabelSelector) Details() proto.Message {
	return t.DataLabelSelectorDetails
}

func (t *DataLabelSelector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DATA_LABEL_SELECTOR
}

func (t *DataLabelSelector) MergeHints(other planning.Node) error {
	return nil
}

func (t *DataLabelSelector) Describe() string {
	return describeSelector(t.Matchers, nil, 0, nil, false, false, false, false, nil)
}

func (t *DataLabelSelector) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (t *DataLabelSelector) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}

func (t *DataLabelSelector) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	// The data label selector for info functions is evaluated using a vector selector
	// at query time so we need to use the same logic as a vector selector when determining
	// the time range here.
	minT, maxT := selectors.ComputeQueriedTimeRange(queryTimeRange, nil, 0, 0, lookbackDelta, false, false)
	return planning.NewQueriedTimeRange(timestamp.Time(minT), timestamp.Time(maxT)), nil
}

func (t *DataLabelSelector) ExpressionPosition() (posrange.PositionRange, error) {
	return t.DataLabelSelectorDetails.ExpressionPosition.ToPrometheusType(), nil
}

func (t *DataLabelSelector) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV12, nil
}

func MaterializeDataLabelSelector(_ context.Context, t *DataLabelSelector, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	selector := &selectors.Selector{
		Queryable:                params.Queryable,
		TimeRange:                timeRange,
		LookbackDelta:            params.QueryParameters.LookbackDelta,
		Matchers:                 LabelMatchersToOperatorType(t.Matchers),
		EagerLoad:                params.EagerLoadSelectors,
		ExpressionPosition:       t.GetExpressionPosition().ToPrometheusType(),
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
	}

	vectorSelector := selectors.NewInstantVectorSelector(
		selector,
		params.MemoryConsumptionTracker,
		false, // returnSampleTimestamps
		true,  // returnSampleTimestampsPreserveHistograms
	)

	return planning.NewSingleUseOperatorFactory(&functions.DataLabelSelector{InstantVectorSelector: vectorSelector}), nil

}
