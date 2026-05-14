// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &Split{SplitDetails: &SplitDetails{}}
	})
}

type Split struct {
	*SplitDetails
	Inner planning.Node
}

func (s *Split) Details() proto.Message {
	return s.SplitDetails
}

func (s *Split) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SPLIT
}

func (s *Split) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type Split supports 1 child, but attempted to get child at index %d", idx))
	}

	return s.Inner
}

func (s *Split) ChildCount() int {
	return 1
}

func (s *Split) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Split requires 1 child, but got %d", len(children))
	}

	s.Inner = children[0]
	return nil
}

func (s *Split) ReplaceChild(idx int, child planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type Split supports 1 child, but attempted to replace child at index %d", idx)
	}

	s.Inner = child
	return nil
}

func (s *Split) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherConsumer, ok := other.(*Split)

	return ok && s.SplitInterval == otherConsumer.SplitInterval
}

func (s *Split) MergeHints(other planning.Node) error {
	return nil
}

func (s *Split) Describe() string {
	return fmt.Sprintf("interval %s", s.SplitInterval.String())
}

func (s *Split) ChildrenLabels() []string {
	return []string{""}
}

func (s *Split) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (s *Split) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *Split) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *Split) ExpressionPosition() (posrange.PositionRange, error) {
	return s.Inner.ExpressionPosition()
}

func (s *Split) MinimumRequiredPlanVersion(timeRange types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV14, nil
}
