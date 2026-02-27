// SPDX-License-Identifier: AGPL-3.0-only

package promqlext

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// ExtendPromQL enriches PromQL with Mimir extensions.
func ExtendPromQL() {
	// Keep an alias for users using holt_winters, even though dropped in Prometheus v3.
	promql.FunctionCalls["holt_winters"] = promql.FunctionCalls["double_exponential_smoothing"]
	parser.Functions["holt_winters"] = parser.Functions["double_exponential_smoothing"]
	parser.Functions["holt_winters"].Experimental = false
}

// NewPromQLParser returns a new parser with the default PromQL parser options used in Mimir
// (both query-frontend and querier).
func NewPromQLParser() parser.Parser {
	return parser.NewParser(NewPromQLParserOptions())
}

// NewPromQLParserOptions returns the default PromQL parser options used in Mimir (both query-frontend and querier).
func NewPromQLParserOptions() parser.Options {
	return parser.Options{
		// Experimental functions are always enabled globally for all engines. Access to them
		// is controlled by an experimental functions query-frontend middleware that reads per-tenant settings.
		EnableExperimentalFunctions: true,

		// This enables duration arithmetic https://github.com/prometheus/prometheus/pull/16249.
		ExperimentalDurationExpr: true,

		// This enables the anchored and smoothed selector modifiers.
		EnableExtendedRangeSelectors: true,

		// Disabled by default.
		EnableBinopFillModifiers: false,
	}
}
