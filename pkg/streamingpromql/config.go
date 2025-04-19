// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"
	"math"
	"time"

	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts `yaml:"-"`

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

		Pedantic: true,

		// Don't enable query planning by default, but do enable common subexpression elimination if query planning is enabled.
		UseQueryPlanning:                     false,
		EnableCommonSubexpressionElimination: true,
	}
}
