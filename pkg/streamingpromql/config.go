// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"flag"

	"github.com/prometheus/prometheus/promql"
)

type EngineOpts struct {
	CommonOpts promql.EngineOpts `yaml:"-"`

	// When operating in pedantic mode, we panic if memory consumption is > 0 after Query.Close()
	// (indicating something was not returned to a pool).
	// Should only be used in tests.
	Pedantic bool `yaml:"-"`

	UseQueryPlanning bool `yaml:"use_query_planning" category:"experimental"`
}

func (o *EngineOpts) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&o.UseQueryPlanning, "querier.mimir-query-engine.use-query-planning", false, "Use query planner when evaluating queries.")
}
