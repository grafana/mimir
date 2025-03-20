// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"github.com/prometheus/prometheus/promql/parser"
	"slices"
)

// Why do we have this slightly odd structure with some fields declared below and some in the Protobuf message types?
// When marshalling a query plan to Protobuf (or any other format), we need to handle references to nodes separately,
// as multiple parent nodes can refer to the same child node.
// If we declared these fields as ordinary Protobuf message fields, then these references would be lost, and each parent
// would end up with a unique copy of the child node on unmarshalling.
// So the rule is:
// - if it's a reference to a Node instance, it needs to be declared here
// - if it's anything else, it can go in the Protobuf message definition

type AggregateExpression struct {
	AggregateExpressionDetails
	Inner Node
	Param Node
}

type BinaryExpression struct {
	BinaryExpressionDetails
	LHS Node
	RHS Node
}

func (v *VectorMatching) Equals(other *VectorMatching) bool {
	if v == nil && other == nil {
		// Both are nil.
		return true
	}

	return v != nil && other != nil &&
		v.On == other.On &&
		v.Card == other.Card &&
		slices.Equal(v.MatchingLabels, other.MatchingLabels) &&
		slices.Equal(v.Include, other.Include)
}

func (v *VectorMatching) AsParserType() *parser.VectorMatching {
	return (*parser.VectorMatching)(v)
}

func VectorMatchingFromParserType(v *parser.VectorMatching) *VectorMatching {
	return (*VectorMatching)(v)
}

type FunctionCall struct {
	FunctionCallDetails
	Args []Node `json:"-"`
}

type NumberLiteral struct {
	NumberLiteralDetails
}

type StringLiteral struct {
	StringLiteralDetails
}

type UnaryExpression struct {
	UnaryExpressionDetails
	Inner Node
}

type VectorSelector struct {
	VectorSelectorDetails
}

type MatrixSelector struct {
	MatrixSelectorDetails
}

type Subquery struct {
	SubqueryDetails
	Inner Node `json:"-"`
}

// TODO: []LabelMatcher -> []*labels.Matcher conversion through labels.NewMatcher() to ensure internal regexp instance is populated
// TODO: conversion helper methods for PositionRange
