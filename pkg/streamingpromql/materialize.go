// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Materializer is responsible for converting query plan nodes to operators.
// This type is not thread safe.
type Materializer struct {
	operatorFactories map[planning.Node]planning.OperatorFactory
	operatorParams    *planning.OperatorParameters
}

func NewMaterializer(params *planning.OperatorParameters) *Materializer {
	return &Materializer{
		operatorFactories: make(map[planning.Node]planning.OperatorFactory),
		operatorParams:    params,
	}
}

func (m *Materializer) ConvertNodeToOperator(node planning.Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	// FIXME: we should check that we're not trying to get the same operator but with a different time range
	if f, ok := m.operatorFactories[node]; ok {
		return f.Produce()
	}

	childTimeRange := node.ChildrenTimeRange(timeRange)
	childrenNodes := node.Children()
	childrenOperators := make([]types.Operator, 0, len(childrenNodes))
	for _, child := range childrenNodes {
		o, err := m.ConvertNodeToOperator(child, childTimeRange)
		if err != nil {
			return nil, err
		}

		childrenOperators = append(childrenOperators, o)
	}

	f, err := node.OperatorFactory(childrenOperators, timeRange, m.operatorParams)
	if err != nil {
		return nil, err
	}

	m.operatorFactories[node] = f

	return f.Produce()
}
