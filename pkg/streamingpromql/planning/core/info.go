// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
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
	NodeIdentifier
	*DataLabelSelectorDetails
}

// OwnResourcesMatchers returns the matchers of the one selector whose series info()'s first argument, node, maps one
// to one onto its own series, with the filters of a shared selector on the way, and whether there is one: info()'s own
// mode looks up the inner series' own native resources by them. lookback is the query's.
//
// Its series map one to one through a vector selector; a range function over a matrix selector, with a range of at
// most lookback, since a longer one's output reaches past the series' newest sample plus the lookback, where the join
// still enriches from the target's later samples; an element-wise function; and a binary operation with a scalar. The
// plan's own nodes on the way are transparent: @'s step invariance, deduplication and name dropping, and common
// subexpression elimination's shared selectors. Anything else maps its series otherwise, or not from a selector.
func OwnResourcesMatchers(node planning.Node, lookback time.Duration) ([]*labels.Matcher, bool) {
	var filters []LabelMatcher
	// Whether a range function's matrix selector is next.
	wantMatrix := false
	for {
		switch n := node.(type) {
		case *VectorSelector:
			if wantMatrix {
				return nil, false
			}
			return withFilters(n.Matchers, filters)
		case *MatrixSelector:
			if !wantMatrix || n.Range > lookback {
				return nil, false
			}
			return withFilters(n.Matchers, filters)
		case *FunctionCall:
			if wantMatrix {
				return nil, false
			}
			next, isRange, ok := ownResourcesFunctionArg(n)
			if !ok {
				return nil, false
			}
			node, wantMatrix = next, isRange
		case *BinaryExpression:
			lhsScalar, rhsScalar := isScalarNode(n.LHS), isScalarNode(n.RHS)
			switch {
			case wantMatrix:
				return nil, false
			case lhsScalar && !rhsScalar:
				node = n.RHS
			case rhsScalar && !lhsScalar:
				node = n.LHS
			default:
				return nil, false
			}
		case *StepInvariantExpression:
			node = n.Inner
		case *DeduplicateAndMerge:
			node = n.Inner
		case *DropName:
			node = n.Inner
		default:
			// Common subexpression elimination's nodes, which this package can't name: a shared selector, and a
			// filter of it that keeps its consumer's series.
			switch node.NodeType() {
			case planning.NODE_TYPE_DUPLICATE:
				node = node.Child(0)
			case planning.NODE_TYPE_DUPLICATE_FILTER:
				details, ok := node.Details().(interface{ GetFilters() []LabelMatcher })
				if !ok {
					return nil, false
				}
				filters = append(filters, details.GetFilters()...)
				node = node.Child(0)
			default:
				return nil, false
			}
		}
	}
}

// ownResourcesRangeFunctions keep each series of their matrix selector apart, as one output series of its labels.
// absent_over_time's output isn't a stored series.
var ownResourcesRangeFunctions = map[functions.Function]struct{}{
	functions.FUNCTION_RATE: {}, functions.FUNCTION_IRATE: {}, functions.FUNCTION_INCREASE: {},
	functions.FUNCTION_DELTA: {}, functions.FUNCTION_IDELTA: {}, functions.FUNCTION_DERIV: {},
	functions.FUNCTION_AVG_OVER_TIME: {}, functions.FUNCTION_COUNT_OVER_TIME: {}, functions.FUNCTION_FIRST_OVER_TIME: {},
	functions.FUNCTION_LAST_OVER_TIME: {}, functions.FUNCTION_MAD_OVER_TIME: {}, functions.FUNCTION_MAX_OVER_TIME: {},
	functions.FUNCTION_MIN_OVER_TIME: {}, functions.FUNCTION_PRESENT_OVER_TIME: {}, functions.FUNCTION_QUANTILE_OVER_TIME: {},
	functions.FUNCTION_STDDEV_OVER_TIME: {}, functions.FUNCTION_STDVAR_OVER_TIME: {}, functions.FUNCTION_SUM_OVER_TIME: {},
	functions.FUNCTION_TS_OF_FIRST_OVER_TIME: {}, functions.FUNCTION_TS_OF_LAST_OVER_TIME: {},
	functions.FUNCTION_TS_OF_MAX_OVER_TIME: {}, functions.FUNCTION_TS_OF_MIN_OVER_TIME: {},
}

// ownResourcesElementWiseFunctions change each sample of their vector argument on its own.
var ownResourcesElementWiseFunctions = map[functions.Function]struct{}{
	functions.FUNCTION_ABS: {}, functions.FUNCTION_CEIL: {}, functions.FUNCTION_FLOOR: {}, functions.FUNCTION_ROUND: {},
	functions.FUNCTION_CLAMP: {}, functions.FUNCTION_CLAMP_MIN: {}, functions.FUNCTION_CLAMP_MAX: {},
	functions.FUNCTION_EXP: {}, functions.FUNCTION_LN: {}, functions.FUNCTION_LOG2: {}, functions.FUNCTION_LOG10: {},
	functions.FUNCTION_SQRT: {}, functions.FUNCTION_SGN: {},
}

// ownResourcesFunctionArg returns the argument of f whose series f maps one to one onto its own, whether it's a range
// function's, and whether f maps its series so: its other arguments are scalars.
func ownResourcesFunctionArg(f *FunctionCall) (planning.Node, bool, bool) {
	_, isRange := ownResourcesRangeFunctions[f.Function]
	_, isElementWise := ownResourcesElementWiseFunctions[f.Function]
	if !isRange && !isElementWise {
		return nil, false, false
	}
	var arg planning.Node
	for _, a := range f.Args {
		if isScalarNode(a) {
			continue
		}
		if arg != nil {
			return nil, false, false
		}
		arg = a
	}
	return arg, isRange, arg != nil
}

func isScalarNode(node planning.Node) bool {
	valueType, err := node.ResultType()
	return err == nil && valueType == parser.ValueTypeScalar
}

func withFilters(matchers, filters []LabelMatcher) ([]*labels.Matcher, bool) {
	out, err := LabelMatchersToPrometheusType(slices.Concat(matchers, filters))
	if err != nil {
		return nil, false
	}
	return out, true
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
	queryable := params.Queryable
	if params.InfoQueryable != nil {
		queryable = params.InfoQueryable
	}
	selector := &selectors.Selector{
		Queryable:                queryable,
		TimeRange:                timeRange,
		LookbackDelta:            params.QueryParameters.LookbackDelta,
		Matchers:                 LabelMatchersToOperatorType(t.Matchers),
		EagerLoad:                params.EagerLoadSelectors,
		ExpressionPosition:       t.GetExpressionPosition().ToPrometheusType(),
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
	}

	// Only collect per-node stats if we've been asked to explain the cost of the query,
	// otherwise pass a node ID of 0 which disables collecting per-node stats.
	var planningNodeId int64
	if slices.Contains(params.QueryParameters.Explain, types.EXPLAIN_VALUE_COST) {
		planningNodeId = t.GetNodeId()
	}

	vectorSelector := selectors.NewInstantVectorSelector(
		selector,
		params.MemoryConsumptionTracker,
		false, // returnSampleTimestamps
		true,  // returnSampleTimestampsPreserveHistograms
		planningNodeId,
	)

	return planning.NewSingleUseOperatorFactory(&functions.DataLabelSelector{InstantVectorSelector: vectorSelector}), nil

}
