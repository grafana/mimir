// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"slices"
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

func (t *DataLabelSelector) Details() proto.Message {
	return t.DataLabelSelectorDetails
}

func (t *DataLabelSelector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DATA_LABEL_SELECTOR
}

func (t *DataLabelSelector) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherTargetInfo, ok := other.(*DataLabelSelector)
	return ok && slices.EqualFunc(t.Matchers, otherTargetInfo.Matchers, matchersEqual)
}

func (t *DataLabelSelector) MergeHints(other planning.Node) error {
	return nil
}

func (t *DataLabelSelector) Describe() string {
	return describeSelector(t.Matchers, nil, 0, nil, false, false, false, false, nil, false, nil)
}

func (t *DataLabelSelector) ChildrenLabels() []string {
	return nil
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

func MaterializeDataLabelSelector(t *DataLabelSelector, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
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
