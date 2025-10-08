// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

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
		return &RemoteExecution{RemoteExecutionDetails: &RemoteExecutionDetails{}}
	})
}

type RemoteExecution struct {
	*RemoteExecutionDetails
	Inner planning.Node
}

func (r *RemoteExecution) Details() proto.Message {
	return r.RemoteExecutionDetails
}

func (r *RemoteExecution) NodeType() planning.NodeType {
	return planning.NODE_TYPE_REMOTE_EXEC
}

func (r *RemoteExecution) Children() []planning.Node {
	return []planning.Node{r.Inner}
}

func (r *RemoteExecution) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type RemoteExecution supports 1 child, but got %d", len(children))
	}

	r.Inner = children[0]

	return nil
}

func (r *RemoteExecution) EquivalentTo(other planning.Node) bool {
	otherDuplicate, ok := other.(*RemoteExecution)

	return ok && r.Inner.EquivalentTo(otherDuplicate.Inner)
}

func (r *RemoteExecution) Describe() string {
	if r.EagerLoad {
		return "eager load"
	}

	return ""
}

func (r *RemoteExecution) ChildrenLabels() []string {
	return []string{""}
}

func (r *RemoteExecution) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (r *RemoteExecution) ResultType() (parser.ValueType, error) {
	return r.Inner.ResultType()
}

func (r *RemoteExecution) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return r.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (r *RemoteExecution) ExpressionPosition() posrange.PositionRange {
	return r.Inner.ExpressionPosition()
}

type RemoteExecutionMaterializer struct {
	executor RemoteExecutor
}

func NewRemoteExecutionMaterializer(executor RemoteExecutor) *RemoteExecutionMaterializer {
	return &RemoteExecutionMaterializer{executor: executor}
}

var _ planning.NodeMaterializer = &RemoteExecutionMaterializer{}

func (m *RemoteExecutionMaterializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	r, ok := n.(*RemoteExecution)
	if !ok {
		return nil, fmt.Errorf("expected node of type RemoteExecution, got %T", n)
	}

	resultType, err := r.Inner.ResultType()
	if err != nil {
		return nil, err
	}

	switch resultType {
	case parser.ValueTypeScalar:
		return planning.NewSingleUseOperatorFactory(&ScalarRemoteExec{
			RootPlan:                 params.Plan,
			Node:                     r.Inner,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			EagerLoad:                r.EagerLoad,
		}), nil

	case parser.ValueTypeVector:
		return planning.NewSingleUseOperatorFactory(&InstantVectorRemoteExec{
			RootPlan:                 params.Plan,
			Node:                     r.Inner,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			EagerLoad:                r.EagerLoad,
		}), nil

	case parser.ValueTypeMatrix:
		return planning.NewSingleUseOperatorFactory(&RangeVectorRemoteExec{
			RootPlan:                 params.Plan,
			Node:                     r.Inner,
			TimeRange:                timeRange,
			RemoteExecutor:           m.executor,
			MemoryConsumptionTracker: params.MemoryConsumptionTracker,
			Annotations:              params.Annotations,
			EagerLoad:                r.EagerLoad,
		}), nil

	default:
		return nil, fmt.Errorf("unsupported child result type for RemoteExecution: got %v", resultType)
	}
}
