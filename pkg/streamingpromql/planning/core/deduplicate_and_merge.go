// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type DeduplicateAndMerge struct {
	*DeduplicateAndMergeDetails
	Inner planning.Node
}

func (d *DeduplicateAndMerge) Details() proto.Message {
	return d.DeduplicateAndMergeDetails
}

func (d *DeduplicateAndMerge) NodeType() planning.NodeType {
	return planning.NODE_TYPE_DEDUPLICATE_AND_MERGE
}

func (d *DeduplicateAndMerge) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type DeduplicateAndMerge supports 1 child, but attempted to get child at index %d", idx))
	}

	return d.Inner
}

func (d *DeduplicateAndMerge) ChildCount() int {
	return 1
}

func (d *DeduplicateAndMerge) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type DeduplicateAndMerge supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *DeduplicateAndMerge) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type DeduplicateAndMerge supports 1 child, but attempted to replace child at index %d", idx)
	}

	d.Inner = node
	return nil
}

func (d *DeduplicateAndMerge) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*DeduplicateAndMerge)

	return ok
}

func (d *DeduplicateAndMerge) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
}

func (d *DeduplicateAndMerge) Describe() string {
	return ""
}

func (d *DeduplicateAndMerge) ChildrenLabels() []string {
	return []string{""}
}

func (d *DeduplicateAndMerge) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (d *DeduplicateAndMerge) ResultType() (parser.ValueType, error) {
	return d.Inner.ResultType()
}

func (d *DeduplicateAndMerge) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return d.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (d *DeduplicateAndMerge) ExpressionPosition() posrange.PositionRange {
	return d.Inner.ExpressionPosition()
}

func (d *DeduplicateAndMerge) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}

func MaterializeDeduplicateAndMerge(d *DeduplicateAndMerge, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(d.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	o := operators.NewDeduplicateAndMerge(inner, params.MemoryConsumptionTracker)

	return planning.NewSingleUseOperatorFactory(o), nil
}
