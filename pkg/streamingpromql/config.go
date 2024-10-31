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
	EnableAggregationOperations                  bool `yaml:"enable_aggregation_operations" category:"experimental"`
	EnableVectorVectorBinaryComparisonOperations bool `yaml:"enable_vector_vector_binary_comparison_operations" category:"experimental"`
	EnableVectorScalarBinaryComparisonOperations bool `yaml:"enable_vector_scalar_binary_comparison_operations" category:"experimental"`
	EnableScalarScalarBinaryComparisonOperations bool `yaml:"enable_scalar_scalar_binary_comparison_operations" category:"experimental"`
	EnableBinaryLogicalOperations                bool `yaml:"enable_binary_logical_operations" category:"experimental"`
	EnableScalars                                bool `yaml:"enable_scalars" category:"experimental"`
	EnableSubqueries                             bool `yaml:"enable_subqueries" category:"experimental"`
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
	f.BoolVar(&t.EnableVectorVectorBinaryComparisonOperations, "querier.mimir-query-engine.enable-vector-vector-binary-comparison-operations", true, "Enable support for binary comparison operations between two vectors in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableVectorScalarBinaryComparisonOperations, "querier.mimir-query-engine.enable-vector-scalar-binary-comparison-operations", true, "Enable support for binary comparison operations between a vector and a scalar in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableScalarScalarBinaryComparisonOperations, "querier.mimir-query-engine.enable-scalar-scalar-binary-comparison-operations", true, "Enable support for binary comparison operations between two scalars in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableBinaryLogicalOperations, "querier.mimir-query-engine.enable-binary-logical-operations", true, "Enable support for binary logical operations in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableScalars, "querier.mimir-query-engine.enable-scalars", true, "Enable support for scalars in Mimir's query engine. Only applies if the Mimir query engine is in use.")
	f.BoolVar(&t.EnableSubqueries, "querier.mimir-query-engine.enable-subqueries", true, "Enable support for subqueries in Mimir's query engine. Only applies if the Mimir query engine is in use.")
}
