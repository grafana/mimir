// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"context"
	"fmt"

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
	overrideTimeParams RangeParams
}

func NewMaterializer(params *OperatorParameters, nodeMaterializers map[NodeType]NodeMaterializer) *Materializer {
	return &Materializer{
		operatorFactories: make(map[OperatorFactoryKey]OperatorFactory),
		operatorParams:    params,
		nodeMaterializers: nodeMaterializers,
	}
}

func (m *Materializer) FactoryForNode(ctx context.Context, node Node, timeRange types.QueryTimeRange) (OperatorFactory, error) {
	return m.FactoryForNodeWithSubRange(ctx, node, timeRange, RangeParams{IsSet: false})
}

// FactoryForNodeWithSubRange returns a factory for the given node with optional sub-range parameters.
func (m *Materializer) FactoryForNodeWithSubRange(ctx context.Context, node Node, timeRange types.QueryTimeRange, overrideTimeParams RangeParams) (OperatorFactory, error) {
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

	f, err := nm.Materialize(ctx, node, m, timeRange, m.operatorParams, overrideTimeParams)
	if err != nil {
		return nil, err
	}

	m.operatorFactories[key] = f

	return f, nil
}

func (m *Materializer) ConvertNodeToOperator(ctx context.Context, node Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	return m.ConvertNodeToOperatorWithSubRange(ctx, node, timeRange, RangeParams{IsSet: false})
}

func (m *Materializer) ConvertNodeToOperatorWithSubRange(ctx context.Context, node Node, timeRange types.QueryTimeRange, overrideTimeParams RangeParams) (types.Operator, error) {
	f, err := m.FactoryForNodeWithSubRange(ctx, node, timeRange, overrideTimeParams)
	if err != nil {
		return nil, err
	}

	return f.Produce()
}

func (m *Materializer) ConvertNodeToInstantVectorOperator(ctx context.Context, node Node, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	o, err := m.ConvertNodeToOperator(ctx, node, timeRange)
	if err != nil {
		return nil, err
	}

	ivo, ok := o.(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator, got %T", o)
	}

	return ivo, nil
}

func (m *Materializer) ConvertNodeToRangeVectorOperator(ctx context.Context, node Node, timeRange types.QueryTimeRange) (types.RangeVectorOperator, error) {
	o, err := m.ConvertNodeToOperator(ctx, node, timeRange)
	if err != nil {
		return nil, err
	}

	rvo, ok := o.(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected RangeVectorOperator, got %T", o)
	}

	return rvo, nil
}

func (m *Materializer) ConvertNodeToScalarOperator(ctx context.Context, node Node, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	o, err := m.ConvertNodeToOperator(ctx, node, timeRange)
	if err != nil {
		return nil, err
	}

	so, ok := o.(types.ScalarOperator)
	if !ok {
		return nil, fmt.Errorf("expected ScalarOperator, got %T", o)
	}

	return so, nil
}

func (m *Materializer) ConvertNodeToStringOperator(ctx context.Context, node Node, timeRange types.QueryTimeRange) (types.StringOperator, error) {
	o, err := m.ConvertNodeToOperator(ctx, node, timeRange)
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
	Materialize(ctx context.Context, n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error)
}

type NodeMaterializerFunc[T Node] func(ctx context.Context, n T, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)

func (f NodeMaterializerFunc[T]) Materialize(ctx context.Context, n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error) {
	if overrideRangeParams.IsSet {
		return nil, fmt.Errorf("overrideRangeParams is not supported for NodeMaterializerFunc")
	}

	node, ok := n.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to node materializer: expected %T, got %T", new(T), n)
	}

	return f(ctx, node, materializer, timeRange, params)
}

type RangeAwareNodeMaterializerFunc[T Node] func(ctx context.Context, n T, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error)

func (f RangeAwareNodeMaterializerFunc[T]) Materialize(ctx context.Context, n Node, materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters, overrideRangeParams RangeParams) (OperatorFactory, error) {
	node, ok := n.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to range aware node materializer: expected %T, got %T", new(T), n)
	}

	return f(ctx, node, materializer, timeRange, params, overrideRangeParams)
}
