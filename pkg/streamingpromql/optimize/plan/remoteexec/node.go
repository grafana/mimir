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
	"github.com/grafana/mimir/pkg/util/limiter"
)

var errShouldNotCallProduceDirectly = errors.New("should not call Produce() directly on RemoteExecutionGroupOperatorFactory: call ProduceForNode() instead")

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &RemoteExecutionGroup{RemoteExecutionGroupDetails: &RemoteExecutionGroupDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &RemoteExecutionConsumer{RemoteExecutionConsumerDetails: &RemoteExecutionConsumerDetails{}}
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

type RemoteExecutionConsumer struct {
	*RemoteExecutionConsumerDetails
	Group *RemoteExecutionGroup
}

func (c *RemoteExecutionConsumer) Details() proto.Message {
	return c.RemoteExecutionConsumerDetails
}

func (c *RemoteExecutionConsumer) NodeType() planning.NodeType {
	return planning.NODE_TYPE_REMOTE_EXEC_CONSUMER
}

func (c *RemoteExecutionConsumer) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type RemoteExecutionConsumer supports 1 child, but attempted to get child at index %d", idx))
	}

	return c.Group
}

func (c *RemoteExecutionConsumer) ChildCount() int {
	return 1
}

func (c *RemoteExecutionConsumer) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type RemoteExecutionConsumer requires 1 child, but got %d", len(children))
	}

	group, ok := children[0].(*RemoteExecutionGroup)
	if !ok {
		return fmt.Errorf("node of type RemoteExecutionConsumer requires child of type RemoteExecutionGroup, but got %T", children[0])
	}

	c.Group = group
	return nil
}

func (c *RemoteExecutionConsumer) ReplaceChild(idx int, child planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type RemoteExecutionConsumer supports 1 child, but attempted to replace child at index %d", idx)
	}

	group, ok := child.(*RemoteExecutionGroup)
	if !ok {
		return fmt.Errorf("node of type RemoteExecutionConsumer requires child of type RemoteExecutionGroup, but got %T", child)
	}

	c.Group = group
	return nil
}

func (c *RemoteExecutionConsumer) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherConsumer, ok := other.(*RemoteExecutionConsumer)

	return ok && c.NodeIndex == otherConsumer.NodeIndex
}

func (c *RemoteExecutionConsumer) MergeHints(other planning.Node) error {
	otherConsumer, ok := other.(*RemoteExecutionConsumer)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, c)
	}

	if c.NodeIndex != otherConsumer.NodeIndex {
		return errors.New("cannot merge RemoteExecutionConsumer nodes with different node indices")
	}

	return nil
}

func (c *RemoteExecutionConsumer) Describe() string {
	return fmt.Sprintf("node %d", c.NodeIndex)
}

func (c *RemoteExecutionConsumer) ChildrenLabels() []string {
	return []string{""}
}

func (c *RemoteExecutionConsumer) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (c *RemoteExecutionConsumer) ResultType() (parser.ValueType, error) {
	node, err := c.getEvaluatedNode()
	if err != nil {
		return parser.ValueTypeNone, err
	}

	return node.ResultType()
}

func (c *RemoteExecutionConsumer) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	node, err := c.getEvaluatedNode()
	if err != nil {
		return planning.NoDataQueried(), err
	}

	return node.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (c *RemoteExecutionConsumer) ExpressionPosition() (posrange.PositionRange, error) {
	node, err := c.getEvaluatedNode()
	if err != nil {
		return posrange.PositionRange{}, err
	}

	return node.ExpressionPosition()
}

func (c *RemoteExecutionConsumer) getEvaluatedNode() (planning.Node, error) {
	if c.NodeIndex >= uint64(len(c.Group.Nodes)) {
		return nil, fmt.Errorf("remote execution group has %d nodes, but attempted to get node at index %d", len(c.Group.Nodes), c.NodeIndex)
	}

	return c.Group.Nodes[c.NodeIndex], nil
}

func (c *RemoteExecutionConsumer) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	// Even though this node type was introduced around the time of query plan v3, this node type is only
	// ever used in query-frontends, and is needed to support remote execution of single nodes against
	// queriers supporting v2 or earlier.
	// So we return v0 here and rely on the RemoteExecutionGroup's MinimumRequiredPlanVersion() to
	// return the correct version required based on whether one or many nodes are being evaluated.
	return planning.QueryPlanVersionZero
}

type RemoteExecutionGroupMaterializer struct {
	groupEvaluatorFactory GroupEvaluatorFactory
}

type GroupEvaluatorFactory func(eagerLoad bool, queryParameters *planning.QueryParameters, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) GroupEvaluator

func NewRemoteExecutionGroupMaterializer(groupEvaluatorFactory GroupEvaluatorFactory) planning.NodeMaterializer {
	return &RemoteExecutionGroupMaterializer{groupEvaluatorFactory: groupEvaluatorFactory}
}

func (m *RemoteExecutionGroupMaterializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, _ planning.RangeParams) (planning.OperatorFactory, error) {
	g, ok := n.(*RemoteExecutionGroup)
	if !ok {
		return nil, fmt.Errorf("expected node of type RemoteExecutionGroup, got %T", n)
	}

	evaluator := m.groupEvaluatorFactory(g.EagerLoad, params.QueryParameters, params.MemoryConsumptionTracker)
	return &RemoteExecutionGroupOperatorFactory{GroupEvaluator: evaluator}, nil
}

type RemoteExecutionGroupOperatorFactory struct {
	GroupEvaluator GroupEvaluator
}

func (f *RemoteExecutionGroupOperatorFactory) Produce() (types.Operator, error) {
	return nil, errShouldNotCallProduceDirectly
}

func (f *RemoteExecutionGroupOperatorFactory) ProduceOperatorForConsumingNode(c *RemoteExecutionConsumer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (types.Operator, error) {
	if c.NodeIndex >= uint64(len(c.Group.Nodes)) {
		return nil, fmt.Errorf("tried to produce an operator for a RemoteExecutionConsumer with node index %v, but the RemoteExecutionGroup only has %v children", c.NodeIndex, len(c.Group.Nodes))
	}

	node := c.Group.Nodes[c.NodeIndex]
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
		return &ScalarRemoteExec{
			Node:               node,
			TimeRange:          timeRange,
			GroupEvaluator:     f.GroupEvaluator,
			Annotations:        params.Annotations,
			QueryStats:         params.QueryStats,
			expressionPosition: expressionPosition,
		}, nil

	case parser.ValueTypeVector:
		return &InstantVectorRemoteExec{
			Node:               node,
			TimeRange:          timeRange,
			GroupEvaluator:     f.GroupEvaluator,
			Annotations:        params.Annotations,
			QueryStats:         params.QueryStats,
			expressionPosition: expressionPosition,
		}, nil

	case parser.ValueTypeMatrix:
		return &RangeVectorRemoteExec{
			Node:               node,
			TimeRange:          timeRange,
			GroupEvaluator:     f.GroupEvaluator,
			Annotations:        params.Annotations,
			QueryStats:         params.QueryStats,
			expressionPosition: expressionPosition,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported child result type for RemoteExecutionGroup: got %v", resultType)
	}
}

type RemoteExecutionConsumerMaterializer struct {
}

func NewRemoteExecutionConsumerMaterializer() planning.NodeMaterializer {
	return &RemoteExecutionConsumerMaterializer{}
}

func (m *RemoteExecutionConsumerMaterializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, _ planning.RangeParams) (planning.OperatorFactory, error) {
	c, ok := n.(*RemoteExecutionConsumer)
	if !ok {
		return nil, fmt.Errorf("expected node of type RemoteExecutionConsumer, got %T", n)
	}

	f, err := materializer.FactoryForNode(c.Group, timeRange)
	if err != nil {
		return nil, err
	}

	groupFactory, ok := f.(*RemoteExecutionGroupOperatorFactory)
	if !ok {
		return nil, fmt.Errorf("expected factory of type RemoteExecutionGroupOperatorFactory, got %T", f)
	}

	o, err := groupFactory.ProduceOperatorForConsumingNode(c, timeRange, params)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}
