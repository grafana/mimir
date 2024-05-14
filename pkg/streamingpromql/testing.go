// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"math"
	"time"

	"github.com/prometheus/prometheus/promql"
)

func NewTestEngineOpts() promql.EngineOpts {
	return promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           math.MaxInt,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
}
