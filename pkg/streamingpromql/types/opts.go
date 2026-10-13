// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"strings"
	"time"
)

// ParseExplainValues parses strings to explain values, using EXPLAIN_VALUE_UNKNOWN for
// any unknown string representations.
func ParseExplainValues(vals []string) []ExplainValue {
	var out []ExplainValue

	for _, v := range vals {
		switch strings.ToLower(v) {
		case "cost":
			out = append(out, EXPLAIN_VALUE_COST)
		default:
			out = append(out, EXPLAIN_VALUE_UNKNOWN)
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
