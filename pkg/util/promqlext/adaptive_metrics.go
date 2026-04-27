// SPDX-License-Identifier: AGPL-3.0-only

// Temporary stubs for Adaptive Metrics custom PromQL functions aggregated_wrapper and not_aggregated_wrapper.
// These are registered here so that mimir's parser and standard engine accept them in queries and tests.
// The real implementations (including streaming engine operators) live in backend-enterprise.

package promqlext

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

var aggregatedWrapperFunction = &parser.Function{
	Name:       "aggregated_wrapper",
	ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
	ReturnType: parser.ValueTypeVector,
}

var notAggregatedWrapperFunction = &parser.Function{
	Name:       "not_aggregated_wrapper",
	ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
	ReturnType: parser.ValueTypeVector,
}

func init() {
	// Register with Prometheus parser so the lexer accepts these as valid function names.
	parser.Functions[aggregatedWrapperFunction.Name] = aggregatedWrapperFunction
	parser.Functions[notAggregatedWrapperFunction.Name] = notAggregatedWrapperFunction

	// Register no-op FunctionCalls entries so the standard Prometheus engine accepts evaluation.
	// Both functions are pass-through: they return the input vector unchanged.
	promql.FunctionCalls[aggregatedWrapperFunction.Name] = passthroughFunctionCall
	promql.FunctionCalls[notAggregatedWrapperFunction.Name] = passthroughFunctionCall
}

// passthroughFunctionCall is a no-op FunctionCall that returns the input instant vector unchanged.
func passthroughFunctionCall(vectorVals []promql.Vector, _ promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
	if len(vectorVals) == 0 {
		return enh.Out, nil
	}
	return append(enh.Out, vectorVals[0]...), nil
}
