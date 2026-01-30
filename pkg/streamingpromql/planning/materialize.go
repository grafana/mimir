// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Materializer is responsible for converting query plan nodes to operators for a single query plan.
// This type is not thread safe.
type Materializer struct {
	operatorFactories map[OperatorFactoryKey]OperatorFactory
	operatorParams    *OperatorParameters

	nodeMaterializers map[NodeType]NodeMaterializer
}

type OperatorFactoryKey struct {
	node               Node
	timeRange          types.QueryTimeRange
	subRange           time.Duration
	overrideTimeParams RangeParams
}

func NewMaterializer(params *OperatorParameters, nodeMaterializers map[NodeType]NodeMaterializer) *Materializer {
	return &Materializer{
		operatorFactories: make(map[OperatorFactoryKey]OperatorFactory),
		operatorParams:    params,
		nodeMaterializers: nodeMaterializers,
	}
}

func (m *Materializer) FactoryForNode(node Node, timeRange types.QueryTimeRange) (OperatorFactory, error) {
	return m.FactoryForNodeWithSubRange(node, timeRange, RangeParams{IsSet: false})
}

// FactoryForNodeWithSubRange returns a factory for the given node with optional sub-range parameters.
func (m *Materializer) FactoryForNodeWithSubRange(node Node, timeRange types.QueryTimeRange, overrideTimeParams RangeParams) (OperatorFactory, error) {
	key := OperatorFactoryKey{
		node:               node,
		timeRange:          timeRange,
		overrideTimeParams: overrideTimeParams,
	}
	if f, ok := m.operatorFactories[key]; ok {
		return f, nil
	}

	nm, ok := m.nodeMaterializers[node.NodeType()]
	if !ok {
		return nil, fmt.Errorf("no registered node materializer for node of type %s", node.NodeType())
	}

	f, err := nm.Materialize(node, m, timeRange, m.operatorParams, overrideTimeParams)
	if err != nil {
		return nil, err
	}

	m.operatorFactories[key] = f

	return f, nil
}

func (m *Materializer) ConvertNodeToOperator(node Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	return m.ConvertNodeToOperatorWithSubRange(node, timeRange, RangeParams{IsSet: false})
}

// ConvertNodeToOperatorWithSubRange will call materialize with the selected subrange.
func (m *Materializer) ConvertNodeToOperatorWithSubRange(node Node, timeRange types.QueryTimeRange, overrideTimeParams RangeParams) (types.Operator, error) {
	f, err := m.FactoryForNodeWithSubRange(node, timeRange, overrideTimeParams)
	if err != nil {
		return nil, err
	}

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
	Materialize(n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error)
}

type NodeMaterializerFunc[T Node] func(n T, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)

func (f NodeMaterializerFunc[T]) Materialize(n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, _ RangeParams) (OperatorFactory, error) {
	node, ok := n.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to node materializer: expected %T, got %T", new(T), n)
	}

	return f(node, materializer, timeRange, params)
}

type RangeAwareNodeMaterializerFunc[T Node] func(n T, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error)

func (f RangeAwareNodeMaterializerFunc[T]) Materialize(n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error) {
	node, ok := n.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to range aware node materializer: expected %T, got %T", new(T), n)
	}

	return f(node, materializer, timeRange, params, overrideRangeParams)
}
