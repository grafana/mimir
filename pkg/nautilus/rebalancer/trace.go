// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// SlicerVersion identifies the algorithmic contract of runSlicer at
// the time a Trace is captured. Bump this whenever a change to the
// slicer would cause the same input Trace to produce a different
// Actions slice on replay (i.e., any change to phase ordering, move
// scoring, cooldown semantics, orphan accounting, etc.). Replay
// tools should refuse to verify a Trace whose SlicerVersion differs
// from the binary's.
const SlicerVersion = "1"

// RangeRate is the JSON-serializable view of a per-range rate signal.
// Mirrors the unexported rangeRate but with JSON tags suitable for
// trace persistence and external replay.
type RangeRate struct {
	Lo      uint32  `json:"lo"`
	Hi      uint32  `json:"hi"`
	Samples float64 `json:"samples"`
	Series  int64   `json:"series"`
}

// ConfigSnapshot freezes the slicer-relevant config knobs at the
// moment a rebalance round was captured, so replays use the same
// parameters even if production config changes between capture and
// replay.
type ConfigSnapshot struct {
	LoadWeightSeries  float64       `json:"load_weight_series"`
	LoadWeightSamples float64       `json:"load_weight_samples"`
	MovementBudget    float64       `json:"movement_budget"`
	MoveCooldown      time.Duration `json:"move_cooldown"`
}

// Trace is the full input/output of a single rebalance round, with
// enough information for an external tool (e.g., an AI verification
// agent) to deterministically replay runSlicer locally and check
// invariants.
//
// Determinism contract: for the same SlicerVersion,
//
//	ReplayTrace(t) == (endAssignment, t.Round.Actions)
//
// where endAssignment.Entries equals t.End.
//
// Trace contains only ingestion-flow metadata (hash ranges,
// partition IDs, instance IDs, counts). It contains no per-tenant or
// per-series content and is safe to persist or share.
type Trace struct {
	SlicerVersion string `json:"slicer_version"`

	// Round mirrors the lightweight summary kept for the admin
	// page's "Recent Rebalance Rounds" panel. Embedded by value so
	// the JSON shape is `{ ..., "round": {...}, ... }` rather than
	// hoisting all summary fields to the top level.
	Round RoundLog `json:"round"`

	// Inputs to runSlicer.
	Now              time.Time          `json:"now"`
	Start            []assignment.Entry `json:"start_assignment"`
	Rates            []RangeRate        `json:"rates"`
	Orphans          map[int32]int64    `json:"partition_orphans"`
	ActivePartitions []int32            `json:"active_partitions"`
	// Cooldowns is keyed by "lo:hi" (decimal) so the JSON map is
	// well-formed; use FormatHashRangeKey / ParseHashRangeKey.
	Cooldowns map[string]time.Time `json:"cooldowns"`
	Config    ConfigSnapshot       `json:"config"`

	// Output of runSlicer + its post-condition.
	End []assignment.Entry `json:"end_assignment"`
}

// FormatHashRangeKey encodes a HashRange into the "lo:hi" decimal
// string key used by Trace.Cooldowns. Decimal keeps the JSON
// human-readable; ranges fit in uint32 so length is bounded.
func FormatHashRangeKey(hr assignment.HashRange) string {
	return strconv.FormatUint(uint64(hr.Lo), 10) + ":" + strconv.FormatUint(uint64(hr.Hi), 10)
}

// ParseHashRangeKey is the inverse of FormatHashRangeKey.
func ParseHashRangeKey(s string) (assignment.HashRange, error) {
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return assignment.HashRange{}, fmt.Errorf("invalid hash range key %q: missing ':'", s)
	}
	lo, err := strconv.ParseUint(s[:colon], 10, 32)
	if err != nil {
		return assignment.HashRange{}, fmt.Errorf("invalid hash range key %q: bad lo: %w", s, err)
	}
	hi, err := strconv.ParseUint(s[colon+1:], 10, 32)
	if err != nil {
		return assignment.HashRange{}, fmt.Errorf("invalid hash range key %q: bad hi: %w", s, err)
	}
	return assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)}, nil
}

// ratesToWire converts the unexported rangeRate (used by the slicer)
// into the JSON-serializable RangeRate (used by Trace).
func ratesToWire(in []rangeRate) []RangeRate {
	out := make([]RangeRate, len(in))
	for i, r := range in {
		out[i] = RangeRate{Lo: r.hr.Lo, Hi: r.hr.Hi, Samples: r.samples, Series: r.series}
	}
	return out
}

// ratesFromWire is the inverse of ratesToWire, used by replay.
func ratesFromWire(in []RangeRate) []rangeRate {
	out := make([]rangeRate, len(in))
	for i, r := range in {
		out[i] = rangeRate{hr: assignment.HashRange{Lo: r.Lo, Hi: r.Hi}, samples: r.Samples, series: r.Series}
	}
	return out
}

// cooldownsToWire converts the slicer's internal cooldown map (keyed
// by HashRange) into the trace's string-keyed form. The whole map is
// captured even though only entries with deadline > now are
// load-bearing — extra entries are harmless on replay because
// isInMoveCooldown filters expired deadlines internally.
func cooldownsToWire(in map[assignment.HashRange]time.Time) map[string]time.Time {
	out := make(map[string]time.Time, len(in))
	for hr, t := range in {
		out[FormatHashRangeKey(hr)] = t
	}
	return out
}

// cooldownsFromWire is the inverse of cooldownsToWire. Malformed
// keys are silently dropped: a malformed key cannot match any real
// hash range anyway, so dropping it is the conservative choice.
func cooldownsFromWire(in map[string]time.Time) map[assignment.HashRange]time.Time {
	out := make(map[assignment.HashRange]time.Time, len(in))
	for k, t := range in {
		hr, err := ParseHashRangeKey(k)
		if err != nil {
			continue
		}
		out[hr] = t
	}
	return out
}

// ReplayTrace runs runSlicer with the inputs captured in t and
// returns the resulting assignment and actions. Used by external
// verification tools (and the package's own determinism tests) to
// confirm that the slicer is a pure function of its inputs.
//
// The returned assignment is a fresh allocation; the input Trace is
// not mutated.
func ReplayTrace(t Trace) (*assignment.Assignment, []Action) {
	r := &Rebalancer{
		cfg: Config{
			LoadWeightSeries:  t.Config.LoadWeightSeries,
			LoadWeightSamples: t.Config.LoadWeightSamples,
			MovementBudget:    t.Config.MovementBudget,
			MoveCooldown:      t.Config.MoveCooldown,
		},
		moveCooldowns: cooldownsFromWire(t.Cooldowns),
	}
	start := &assignment.Assignment{
		Entries: append([]assignment.Entry(nil), t.Start...),
	}
	return r.runSlicer(start, ratesFromWire(t.Rates), t.Orphans, t.ActivePartitions, t.Now)
}
