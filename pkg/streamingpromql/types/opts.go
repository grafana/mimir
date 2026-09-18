// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"strings"
	"time"
)

// ExplainValue is an optional piece of information about a query that can be requested by a caller
// during the query execution (e.g. the cost on each selector, the optimized plan, etc.).
type ExplainValue string

// ExplainValueCost is a request for per-selector "cost" information during the execution of a query.
const ExplainValueCost = ExplainValue("cost")

// ParseExplainValues parses strings to known explain values and ignores unknown values.
func ParseExplainValues(vals []string) []ExplainValue {
	var out []ExplainValue

	for _, v := range vals {
		switch ExplainValue(strings.ToLower(v)) {
		case ExplainValueCost:
			out = append(out, ExplainValueCost)
		}
	}

	return out
}

type MimirQueryOpts struct {
	// Enables recording per-step statistics if the engine has it enabled as well. Disabled by default.
	enablePerStepStats bool
	// Lookback delta duration for this query.
	lookbackDelta time.Duration
	// Enables start timestamp usage in functions such as rate().
	useStartTimestamps *bool
	// Include extra diagnostic information in the query response.
	explain []ExplainValue
}

func NewMimirQueryOpts(enablePerStepStats bool, lookbackDelta time.Duration, useStartTimestamps *bool, explain []ExplainValue) *MimirQueryOpts {
	return &MimirQueryOpts{
		enablePerStepStats: enablePerStepStats,
		lookbackDelta:      lookbackDelta,
		useStartTimestamps: useStartTimestamps,
		explain:            explain,
	}
}

func (m *MimirQueryOpts) EnablePerStepStats() bool {
	return m.enablePerStepStats
}

func (m *MimirQueryOpts) LookbackDelta() time.Duration {
	return m.lookbackDelta
}

func (m *MimirQueryOpts) UseStartTimestamps() *bool {
	return m.useStartTimestamps
}

func (m *MimirQueryOpts) Explain() []ExplainValue {
	return m.explain
}
