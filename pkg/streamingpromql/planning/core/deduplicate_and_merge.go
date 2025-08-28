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

func (d *DeduplicateAndMerge) Children() []planning.Node {
	return []planning.Node{d.Inner}
}

func (d *DeduplicateAndMerge) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type DeduplicateAndMerge supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *DeduplicateAndMerge) EquivalentTo(other planning.Node) bool {
	otherDedup, ok := other.(*DeduplicateAndMerge)

	return ok && d.Inner.EquivalentTo(otherDedup.Inner)
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

func MaterializeDeduplicateAndMerge(d *DeduplicateAndMerge, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(d.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	o := operators.NewDeduplicateAndMerge(inner, params.MemoryConsumptionTracker, d.RunDelayedNameRemoval)

	return planning.NewSingleUseOperatorFactory(o), nil
}
