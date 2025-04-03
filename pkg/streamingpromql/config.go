// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts
	Features   Features

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	Pedantic bool
}

type Features struct {
	DisabledFunctions flagext.StringSliceCSV `yaml:"disabled_functions" category:"experimental"`
}

// EnableAllFeatures enables all features supported by MQE, including experimental or incomplete features.
var EnableAllFeatures = Features{
	// Note that we deliberately use a keyless literal here to force a compilation error if we don't keep this in sync with new fields added to FeatureToggles.
	[]string{},
}

func (t *Features) RegisterFlags(f *flag.FlagSet) {
	f.Var(&t.DisabledFunctions, "querier.mimir-query-engine.disabled-functions", "Comma-separated list of function names to disable support for. Only applies if MQE is in use.")
}
