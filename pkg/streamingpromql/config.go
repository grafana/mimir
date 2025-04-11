// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"math"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts `yaml:"-"`
	Features   Features

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	// Should only be used in tests.
	Pedantic bool `yaml:"-"`

	UseQueryPlanning                     bool `yaml:"use_query_planning" category:"experimental"`
	EnableCommonSubexpressionElimination bool `yaml:"enable_common_subexpression_elimination" category:"experimental"`
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.UseQueryPlanning, "querier.mimir-query-engine.use-query-planning", false, "Use query planner when evaluating queries.")
	f.BoolVar(&o.EnableCommonSubexpressionElimination, "querier.mimir-query-engine.enable-common-subexpression-elimination", true, "Enable common subexpression elimination when evaluating queries. Only applies if query planner is enabled.")

	o.Features.RegisterFlags(f)
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

func NewTestEngineOpts() EngineOpts {
	return EngineOpts{
		CommonOpts: promql.EngineOpts{
			Logger:                   nil,
			Reg:                      nil,
			MaxSamples:               math.MaxInt,
			Timeout:                  100 * time.Second,
			EnableAtModifier:         true,
			EnableNegativeOffset:     true,
			NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		},

		Features: EnableAllFeatures,
		Pedantic: true,

		// Don't enable query planning by default, but do enable common subexpression elimination if query planning is enabled.
		EnableCommonSubexpressionElimination: true,
	}
}
