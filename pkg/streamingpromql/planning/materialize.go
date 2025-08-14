// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Materializer is responsible for converting query plan nodes to operators.
// This type is not thread safe.
type Materializer struct {
	operatorFactories map[OperatorFactoryMapKey]OperatorFactory
	operatorParams    *OperatorParameters
}

type OperatorFactoryMapKey struct {
	node      Node
	timeRange types.QueryTimeRange
}

func NewMaterializer(params *OperatorParameters) *Materializer {
	return &Materializer{
		operatorFactories: make(map[OperatorFactoryMapKey]OperatorFactory),
		operatorParams:    params,
	}
}

func (m *Materializer) ConvertNodeToOperator(node Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	key := OperatorFactoryMapKey{node: node, timeRange: timeRange}
	if f, ok := m.operatorFactories[key]; ok {
		return f.Produce()
	}

	f, err := node.OperatorFactory(m, timeRange, m.operatorParams)
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
