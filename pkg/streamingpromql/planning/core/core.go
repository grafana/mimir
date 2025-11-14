// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &AggregateExpression{AggregateExpressionDetails: &AggregateExpressionDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &BinaryExpression{BinaryExpressionDetails: &BinaryExpressionDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &FunctionCall{FunctionCallDetails: &FunctionCallDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &NumberLiteral{NumberLiteralDetails: &NumberLiteralDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &StringLiteral{StringLiteralDetails: &StringLiteralDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &UnaryExpression{UnaryExpressionDetails: &UnaryExpressionDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &VectorSelector{VectorSelectorDetails: &VectorSelectorDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &MatrixSelector{MatrixSelectorDetails: &MatrixSelectorDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &Subquery{SubqueryDetails: &SubqueryDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &DeduplicateAndMerge{DeduplicateAndMergeDetails: &DeduplicateAndMergeDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &StepInvariantExpression{StepInvariantExpressionDetails: &StepInvariantExpressionDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &DropName{DropNameDetails: &DropNameDetails{}}
	})
}

// Why do we have this slightly odd structure with some fields declared directly on the node type and some in the Protobuf message types?
// When marshalling a query plan to Protobuf (or any other format), we need to handle references to nodes separately,
// as multiple parent nodes can refer to the same child node.
// If we declared these fields as ordinary Protobuf message fields, then these references would be lost, and each parent
// would end up with a unique copy of the child node on unmarshalling.
// So the rule is:
// - if it's a reference to a Node instance, it needs to be declared here
// - if it's anything else, it can go in the Protobuf message definition
