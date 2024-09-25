// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"

	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts     promql.EngineOpts
	FeatureToggles FeatureToggles

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	Pedantic bool
}

type FeatureToggles struct {
	EnableAggregationOperations      bool `yaml:"enable_aggregation_operations" category:"experimental"`
	EnableBinaryOperations           bool `yaml:"enable_binary_operations" category:"experimental"`
	EnableBinaryComparisonOperations bool `yaml:"enable_binary_comparison_operations" category:"experimental"`
	EnableOffsetModifier             bool `yaml:"enable_offset_modifier" category:"experimental"`
	EnableOverTimeFunctions          bool `yaml:"enable_over_time_functions" category:"experimental"`
	EnableScalars                    bool `yaml:"enable_scalars" category:"experimental"`
	EnableUnaryNegation              bool `yaml:"enable_unary_negation" category:"experimental"`
}

var overTimeFunctionNames = []string{
	"avg_over_time",
	"count_over_time",
	"last_over_time",
	"max_over_time",
	"min_over_time",
	"present_over_time",
	"sum_over_time",
}

// EnableAllFeatures enables all features supported by MQE, including experimental or incomplete features.
var EnableAllFeatures = FeatureToggles{
	// Note that we deliberately use a keyless literal here to force a compilation error if we don't keep this in sync with new fields added to FeatureToggles.
	true,
	true,
	true,
	true,
	true,
	true,
	true,
}

func (t *FeatureToggles) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&t.EnableAggregationOperations, "querier.mimir-query-engine.enable-aggregation-operations", true, "Enable support for aggregation operations in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableBinaryOperations, "querier.mimir-query-engine.enable-binary-operations", true, "Enable support for binary operations in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableBinaryComparisonOperations, "querier.mimir-query-engine.enable-binary-comparison-operations", true, "Enable support for binary comparison operations in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableOffsetModifier, "querier.mimir-query-engine.enable-offset-modifier", true, "Enable support for offset modifier in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableOverTimeFunctions, "querier.mimir-query-engine.enable-over-time-functions", true, "Enable support for ..._over_time functions in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableScalars, "querier.mimir-query-engine.enable-scalars", true, "Enable support for scalars in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableUnaryNegation, "querier.mimir-query-engine.enable-unary-negation", true, "Enable support for unary negation in Mimir's query engine. Only applies if the Mimir query engine is in use.")
}
