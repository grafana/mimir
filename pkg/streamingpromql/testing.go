// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"math"
	"time"

	"github.com/prometheus/prometheus/promql"
)

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

		Features: MQEAllFeatures,
		Pedantic: true,
	}
}
