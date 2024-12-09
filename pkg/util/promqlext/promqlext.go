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
