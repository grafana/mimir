// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type MatrixSelector struct {
	*MatrixSelectorDetails
}

func (m *MatrixSelector) IsSplittable() bool {
	// TODO: it should be possible to add support for smoothed and anchored, but that will be left for later
	return !(m.Smoothed || m.Anchored)
}

var _ planning.SplitNode = &MatrixSelector{}

func (m *MatrixSelector) Describe() string {
	return describeSelector(m.Matchers, m.Timestamp, m.Offset, &m.Range, m.SkipHistogramBuckets, m.Anchored, m.Smoothed, m.CounterAware, m.ProjectionLabels, m.ProjectionInclude)
}

// RangeVectorSplittingCacheKey returns the cache key for the matrix selector.
// The range is not part of the cache key as range vector splitting means that matrix selectors which only differ by
// the range can share cache entries.
// The offset and @ modifiers are not part of the cache key as they are adjusted for when calculating split ranges.
// TODO: when subquery splitting is supported, the logic will have to change - if the matrix selector is not the root
//
//	inner node, the range plus the offset and @ modifiers will have to be retained.
//
// TODO: investigate codegen to keep the cache key up to date when new fields are added to the node.
func (m *MatrixSelector) RangeVectorSplittingCacheKey() string {
	builder := &strings.Builder{}
	builder.WriteRune('{')
	for i, m := range m.Matchers {
		if i > 0 {
			builder.WriteString(", ")
		}

		// Convert to the Prometheus type so we can use its String().
		promMatcher := labels.Matcher{Type: m.Type, Name: m.Name, Value: m.Value}
		builder.WriteString(promMatcher.String())
	}
	builder.WriteRune('}')

	if m.SkipHistogramBuckets {
		// This needs to be kept in the cache key, as the returned results will differ depending on if buckets are skipped or not
		builder.WriteString(", skip histogram buckets")
	}

	return builder.String()
}

func (m *MatrixSelector) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (m *MatrixSelector) Details() proto.Message {
	return m.MatrixSelectorDetails
}

func (m *MatrixSelector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MATRIX_SELECTOR
}

func (m *MatrixSelector) Child(idx int) planning.Node {
	panic(fmt.Sprintf("node of type MatrixSelector has no children, but attempted to get child at index %d", idx))
}

func (m *MatrixSelector) ChildCount() int {
	return 0
}

func (m *MatrixSelector) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type MatrixSelector expects 0 children, but got %d", len(children))
	}

	return nil
}

func (m *MatrixSelector) ReplaceChild(idx int, node planning.Node) error {
	return fmt.Errorf("node of type MatrixSelector supports no children, but attempted to replace child at index %d", idx)
}

func (m *MatrixSelector) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherMatrixSelector, ok := other.(*MatrixSelector)

	return ok &&
		slices.EqualFunc(m.Matchers, otherMatrixSelector.Matchers, matchersEqual) &&
		((m.Timestamp == nil && otherMatrixSelector.Timestamp == nil) || (m.Timestamp != nil && otherMatrixSelector.Timestamp != nil && m.Timestamp.Equal(*otherMatrixSelector.Timestamp))) &&
		m.Offset == otherMatrixSelector.Offset &&
		m.Range == otherMatrixSelector.Range &&
		m.Anchored == otherMatrixSelector.Anchored &&
		m.Smoothed == otherMatrixSelector.Smoothed &&
		m.CounterAware == otherMatrixSelector.CounterAware
}

func (m *MatrixSelector) MergeHints(other planning.Node) error {
	otherMatrixSelector, ok := other.(*MatrixSelector)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, m)
	}

	m.SkipHistogramBuckets = m.SkipHistogramBuckets && otherMatrixSelector.SkipHistogramBuckets
	m.ProjectionInclude, m.ProjectionLabels = mergeProjectionLabels(
		m.ProjectionInclude,
		m.ProjectionLabels,
		otherMatrixSelector.ProjectionInclude,
		otherMatrixSelector.ProjectionLabels,
	)

	return nil
}

func (m *MatrixSelector) ChildrenLabels() []string {
	return nil
}

func MaterializeMatrixSelector(m *MatrixSelector, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideTimeParams planning.RangeParams) (planning.OperatorFactory, error) {
	selectorRange := m.Range
	selectorTs := m.Timestamp
	selectorOffset := m.Offset.Milliseconds()
	if overrideTimeParams.IsSet {
		selectorOffset = overrideTimeParams.Offset.Milliseconds()
		if overrideTimeParams.HasTimestamp {
			selectorTs = &overrideTimeParams.Timestamp
		} else {
			selectorTs = nil
		}
		selectorRange = overrideTimeParams.Range
	}

	selector := &selectors.Selector{
		Queryable:                params.Queryable,
		TimeRange:                timeRange,
		Timestamp:                TimestampFromTime(selectorTs),
		Offset:                   selectorOffset,
		Range:                    selectorRange,
		Matchers:                 LabelMatchersToOperatorType(m.Matchers),
		EagerLoad:                params.EagerLoadSelectors,
		SkipHistogramBuckets:     m.SkipHistogramBuckets,
		ExpressionPosition:       m.GetExpressionPosition().ToPrometheusType(),
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
		Anchored:                 m.Anchored,
		Smoothed:                 m.Smoothed,
		CounterAware:             m.CounterAware,
		ProjectionInclude:        m.ProjectionInclude,
		ProjectionLabels:         m.ProjectionLabels,
	}

	if m.Anchored || m.Smoothed {
		selector.LookbackDelta = params.LookbackDelta
	}

	o := selectors.NewRangeVectorSelector(selector, params.MemoryConsumptionTracker, params.QueryStats)

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (m *MatrixSelector) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeMatrix, nil
}

func (m *MatrixSelector) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookback time.Duration) (planning.QueriedTimeRange, error) {
	if !m.Anchored && !m.Smoothed {
		// Normal matrix selectors do not use the lookback delta, so we don't pass it below.
		lookback = 0
	}
	minT, maxT := selectors.ComputeQueriedTimeRange(queryTimeRange, TimestampFromTime(m.Timestamp), m.Range, m.Offset.Milliseconds(), lookback, m.Anchored, m.Smoothed)
	return planning.NewQueriedTimeRange(timestamp.Time(minT), timestamp.Time(maxT)), nil
}

func (m *MatrixSelector) ExpressionPosition() (posrange.PositionRange, error) {
	return m.GetExpressionPosition().ToPrometheusType(), nil
}

func (m *MatrixSelector) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	if m.Anchored || m.Smoothed {
		return planning.QueryPlanV4
	}
	return planning.QueryPlanVersionZero
}

func (m *MatrixSelector) GetRange() time.Duration {
	return m.Range
}

func (m *MatrixSelector) GetRangeParams() planning.RangeParams {
	params := planning.RangeParams{
		IsSet:  true,
		Range:  m.Range,
		Offset: m.Offset,
	}
	if m.Timestamp != nil {
		params.HasTimestamp = true
		params.Timestamp = *m.Timestamp
	}
	return params
}
