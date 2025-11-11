// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type operatorCacheKey struct {
	node      Node
	timeRange types.QueryTimeRange
}

// Materializer is responsible for converting query plan nodes to operators for a single query plan.
// This type is not thread safe.
type Materializer struct {
	operatorFactories map[operatorCacheKey]OperatorFactory
	operatorParams    *OperatorParameters

	nodeMaterializers map[NodeType]NodeMaterializer
}

func NewMaterializer(params *OperatorParameters, nodeMaterializers map[NodeType]NodeMaterializer) *Materializer {
	return &Materializer{
		operatorFactories: make(map[operatorCacheKey]OperatorFactory),
		operatorParams:    params,
		nodeMaterializers: nodeMaterializers,
	}
}

func (m *Materializer) ConvertNodeToOperator(node Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	key := operatorCacheKey{node: node, timeRange: timeRange}
	if f, ok := m.operatorFactories[key]; ok {
		return f.Produce()
	}

	nm, ok := m.nodeMaterializers[node.NodeType()]
	if !ok {
		return nil, fmt.Errorf("no registered node materializer for node of type %s", node.NodeType())
	}

	f, err := nm.Materialize(node, m, timeRange, m.operatorParams)
	if err != nil {
		return nil, err
	}

	m.operatorFactories[key] = f

	return f.Produce()
}

func (m *Materializer) ConvertNodeToInstantVectorOperator(node Node, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	o, err := m.ConvertNodeToOperator(node, timeRange)
	if err != nil {
		return nil, err
	}

	ivo, ok := o.(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator, got %T", o)
	}

	return ivo, nil
}

func (m *Materializer) ConvertNodeToScalarOperator(node Node, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	o, err := m.ConvertNodeToOperator(node, timeRange)
	if err != nil {
		return nil, err
	}

	so, ok := o.(types.ScalarOperator)
	if !ok {
		return nil, fmt.Errorf("expected ScalarOperator, got %T", o)
	}

	return so, nil
}

func (m *Materializer) ConvertNodeToStringOperator(node Node, timeRange types.QueryTimeRange) (types.StringOperator, error) {
	o, err := m.ConvertNodeToOperator(node, timeRange)
	if err != nil {
		return nil, err
	}

	so, ok := o.(types.StringOperator)
	if !ok {
		return nil, fmt.Errorf("expected StringOperator, got %T", o)
	}

	return so, nil
}

// NodeMaterializer is responsible for converting nodes to OperatorFactory instances.
type NodeMaterializer interface {
	// Materialize returns a factory that produces operators for the given node.
	//
	// Implementations may retain the provided Materializer for later use.
	Materialize(n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)
}

type NodeMaterializerFunc[T Node] func(n T, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)

func (f NodeMaterializerFunc[T]) Materialize(n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error) {
	node, ok := n.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to node materializer: expected %T, got %T", new(T), n)
	}

	return f(node, materializer, timeRange, params)
}
