// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"

	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts     promql.EngineOpts
	FeatureToggles FeatureToggles
}

type FeatureToggles struct {
	EnableBinaryOperations bool `yaml:"enable_binary_operations" category:"experimental"`
}

// EnableAllFeatures enables all features supported by MQE, including experimental or incomplete features.
var EnableAllFeatures = FeatureToggles{
	// Note that we deliberately use a keyless literal here to force a compilation error if we don't keep this in sync with new fields added to FeatureToggles.
	true,
}

func (t *FeatureToggles) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&t.EnableBinaryOperations, "querier.mimir-query-engine.enable-binary-operations", true, "Enable support for binary operations in Mimir's query engine. Only applies if the Mimir query engine is in use.")
}
