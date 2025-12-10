// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"errors"
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
		return &RemoteExecutionGroup{RemoteExecutionGroupDetails: &RemoteExecutionGroupDetails{}}
	})
}

type RemoteExecutionGroup struct {
	*RemoteExecutionGroupDetails
	Nodes []planning.Node
}

func (r *RemoteExecutionGroup) Details() proto.Message {
	return r.RemoteExecutionGroupDetails
}

func (r *RemoteExecutionGroup) NodeType() planning.NodeType {
	return planning.NODE_TYPE_REMOTE_EXEC_GROUP
}

func (r *RemoteExecutionGroup) Child(idx int) planning.Node {
	if idx >= len(r.Nodes) {
		panic(fmt.Sprintf("this RemoteExecutionGroup node has %d children, but attempted to get child at index %d", len(r.Nodes), idx))
	}

	return r.Nodes[idx]
}

func (r *RemoteExecutionGroup) ChildCount() int {
	return len(r.Nodes)
}

func (r *RemoteExecutionGroup) SetChildren(children []planning.Node) error {
	if len(children) < 1 {
		return fmt.Errorf("node of type RemoteExecutionGroup requires at least one child, but got %d", len(children))
	}

	r.Nodes = children

	return nil
}

func (r *RemoteExecutionGroup) ReplaceChild(idx int, node planning.Node) error {
	if idx >= len(r.Nodes) {
		panic(fmt.Sprintf("this RemoteExecutionGroup node has %d children, but attempted to replace child at index %d", len(r.Nodes), idx))
	}

	r.Nodes[idx] = node
	return nil
}

func (r *RemoteExecutionGroup) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*RemoteExecutionGroup)

	return ok
}

func (r *RemoteExecutionGroup) MergeHints(other planning.Node) error {
	otherRemoteExec, ok := other.(*RemoteExecutionGroup)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, r)
	}

	if r.EagerLoad != otherRemoteExec.EagerLoad {
		return errors.New("cannot merge RemoteExecutionGroup nodes with different eager load values")
	}

	return nil
}

func (r *RemoteExecutionGroup) Describe() string {
	if r.EagerLoad {
		return "eager load"
	}

	return ""
}

func (r *RemoteExecutionGroup) ChildrenLabels() []string {
	lbls := make([]string, 0, len(r.Nodes))

	for idx := range r.Nodes {
		lbls = append(lbls, fmt.Sprintf("node %d", idx))
	}

	return lbls
}

func (r *RemoteExecutionGroup) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (r *RemoteExecutionGroup) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeNone, errors.New("cannot call ResultType on RemoteExecutionGroup node directly, call ResultType on consumer node instead")
}

func (r *RemoteExecutionGroup) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return planning.NoDataQueried(), errors.New("cannot call QueriedTimeRange on RemoteExecutionGroup node directly, call ResultType on consumer node instead")
}

func (r *RemoteExecutionGroup) ExpressionPosition() (posrange.PositionRange, error) {
	return posrange.PositionRange{}, errors.New("cannot call ExpressionPosition on RemoteExecutionGroup node directly, call ExpressionPosition on consumer node instead")
}

func (r *RemoteExecutionGroup) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	if len(r.Nodes) > 1 {
		return planning.QueryPlanV3
	}

	return planning.QueryPlanVersionZero
}

type RemoteExecutionMaterializer struct {
	executor RemoteExecutor
}

func NewRemoteExecutionMaterializer(executor RemoteExecutor) *RemoteExecutionMaterializer {
	return &RemoteExecutionMaterializer{executor: executor}
}

var _ planning.NodeMaterializer = &RemoteExecutionMaterializer{}

func (m *RemoteExecutionMaterializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	r, ok := n.(*RemoteExecutionGroup)
	if !ok {
		return nil, fmt.Errorf("expected node of type RemoteExecutionGroup, got %T", n)
	}

	if len(r.Nodes) != 1 {
		return nil, fmt.Errorf("attempted to materialize node of type RemoteExecutionGroup with %d nodes", len(r.Nodes))
	}

	node := r.Nodes[0]
	expressionPosition, err := node.ExpressionPosition()
	if err != nil {
		return nil, err
	}

	resultType, err := node.ResultType()
	if err != nil {
		return nil, err
	}

	switch resultType {
	case parser.ValueTypeScalar:
		return planning.NewSingleUseOperatorFactory(&ScalarRemoteExec{
			QueryParameters:          params.QueryParameters,
			Node:                     node,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			QueryStats:               params.QueryStats,
			EagerLoad:                r.EagerLoad,
			expressionPosition:       expressionPosition,
		}), nil

	case parser.ValueTypeVector:
		return planning.NewSingleUseOperatorFactory(&InstantVectorRemoteExec{
			QueryParameters:          params.QueryParameters,
			Node:                     node,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			QueryStats:               params.QueryStats,
			EagerLoad:                r.EagerLoad,
			expressionPosition:       expressionPosition,
		}), nil

	case parser.ValueTypeMatrix:
		return planning.NewSingleUseOperatorFactory(&RangeVectorRemoteExec{
			QueryParameters:          params.QueryParameters,
			Node:                     node,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			QueryStats:               params.QueryStats,
			EagerLoad:                r.EagerLoad,
			expressionPosition:       expressionPosition,
		}), nil

	default:
		return nil, fmt.Errorf("unsupported child result type for RemoteExecutionGroup: got %v", resultType)
	}
}
