// SPDX-License-Identifier: AGPL-3.0-only

package streaming

import (
	"github.com/prometheus/prometheus/promql"
	"time"
)

func NewTestEngineOpts() promql.EngineOpts {
	return promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}
}
