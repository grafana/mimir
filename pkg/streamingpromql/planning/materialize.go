// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Materializer is responsible for converting query plan nodes to operators.
// This type is not thread safe.
type Materializer struct {
	operatorFactories map[Node]OperatorFactory
	operatorParams    *OperatorParameters
}

func NewMaterializer(params *OperatorParameters) *Materializer {
	return &Materializer{
		operatorFactories: make(map[Node]OperatorFactory),
		operatorParams:    params,
	}
}

func (m *Materializer) ConvertNodeToOperator(node Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	// FIXME: we should check that we're not trying to get the same operator but with a different time range
	if f, ok := m.operatorFactories[node]; ok {
		return f.Produce()
	}

	f, err := node.OperatorFactory(m, timeRange, m.operatorParams)
	if err != nil {
		return nil, err
	}

	m.operatorFactories[node] = f

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
