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
// scoring, cooldown semantics, movable-budget math, etc.). Replay
// tools should refuse to verify a Trace whose SlicerVersion differs
// from the binary's.
//
// Version "2" replaces the orphan-series model with a direct L_pid
// (TotalActiveSeries) + CompactionInterval movable-budget model and
// drops the samples signal from wire inputs. Traces captured under
// SlicerVersion "1" are not replayable against this binary.
//
// Version "3" extends RangeRate with partition_id so the slicer's
// loadMap is keyed by (partition, range) — required to keep residue
// on a previous owner separate from growth on the new owner. Traces
// captured under SlicerVersion "2" do not carry that field and
// cannot be replayed deterministically against the v3 slicer.
//
// Version "4" swaps Phase 3's load metric from head-series count to
// per-(partition, range) sample-rate EWMA. RangeRate gains
// SampleRate and MoveRecord gains Load (the moved range's sample
// rate at move time, used by the movable-budget math). Series stays
// on both shapes for observability but is no longer load-bearing.
// Traces captured under v3 zero-fill the new fields, so replaying
// them under v4 will produce a different — and arguably broken —
// Actions slice because Phase 3 will see no movable budget; the
// determinism contract requires matching versions.
const SlicerVersion = "4"

// RangeRate is the JSON-serializable view of a per-(partition, range)
// rate signal. Mirrors the unexported rangeRate but with JSON tags
// suitable for trace persistence and external replay.
//
// PartitionID is the Kafka partition whose TSDB head contained the
// Series count at capture time. The same (Lo, Hi) range can appear
// multiple times with different PartitionIDs to model residue (one
// entry on the previous owner) alongside growth (one entry on the
// current owner).
//
// SampleRate is the per-(partition, range) samples-per-second EWMA;
// it is the slicer's primary load signal from v4 onward. Series is
// retained as observability metadata and surfaces on the admin
// page, but does not feed Phase 3's hot/cold scoring.
type RangeRate struct {
	Lo          uint32  `json:"lo"`
	Hi          uint32  `json:"hi"`
	Series      int64   `json:"series"`
	SampleRate  float64 `json:"sample_rate"`
	PartitionID int32   `json:"partition_id"`
}

// MoveRecord is the JSON-serializable view of a moveRecord — a move
// off a source partition that still counts against the partition's
// movable budget while within the CompactionInterval window.
//
// Load is in the same units as RangeRate.SampleRate (samples per
// second) and is what Phase 3's sumRecentMoves discounts. Series
// is retained for admin-page continuity ("this many head series
// moved") but is not load-bearing.
type MoveRecord struct {
	Lo     uint32    `json:"lo"`
	Hi     uint32    `json:"hi"`
	Load   float64   `json:"load"`
	Series int64     `json:"series"`
	At     time.Time `json:"at"`
}

// ConfigSnapshot freezes the slicer-relevant config knobs at the
// moment a rebalance round was captured, so replays use the same
// parameters even if production config changes between capture and
// replay.
type ConfigSnapshot struct {
	MovementBudget     float64       `json:"movement_budget"`
	MoveCooldown       time.Duration `json:"move_cooldown"`
	CompactionInterval time.Duration `json:"compaction_interval"`
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
	Now        time.Time          `json:"now"`
	Start      []assignment.Entry `json:"start_assignment"`
	Rates      []RangeRate        `json:"rates"`
	PartitionL map[int32]int64    `json:"partition_l"`
	// PartitionQuerySamples is the per-partition EWMA of samples-per-
	// second scanned by named queries (queries the distributor
	// resolved to a single partition). Phase 1: observation-only.
	// Phase 2 actuator (David I et al) consumes this to drive
	// query-load partition→ingester rebalancing.
	PartitionQuerySamples map[int32]float64 `json:"partition_query_samples,omitempty"`
	// UnnamedQuerySamples is the per-ingester EWMA of samples-per-
	// second scanned by full-fanout queries (no resolvable __name__,
	// complex regexes). Reported per ingester because the work
	// scours all owned partitions; the rebalancer cannot move it to
	// any specific partition. Surfaced for observability so ops can
	// monitor the unnamed/named ratio and detect when locality-based
	// rebalancing is hitting its ceiling.
	UnnamedQuerySamples map[string]float64 `json:"unnamed_query_samples,omitempty"`
	// RecentMoves is keyed by partition ID (decimal string) so the
	// JSON map is well-formed.
	RecentMoves      map[string][]MoveRecord `json:"recent_moves"`
	ActivePartitions []int32                 `json:"active_partitions"`
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
		out[i] = RangeRate{
			Lo:          r.hr.Lo,
			Hi:          r.hr.Hi,
			Series:      r.series,
			SampleRate:  r.sampleRate,
			PartitionID: r.partitionID,
		}
	}
	return out
}

// ratesFromWire is the inverse of ratesToWire, used by replay.
func ratesFromWire(in []RangeRate) []rangeRate {
	out := make([]rangeRate, len(in))
	for i, r := range in {
		out[i] = rangeRate{
			hr:          assignment.HashRange{Lo: r.Lo, Hi: r.Hi},
			series:      r.Series,
			sampleRate:  r.SampleRate,
			partitionID: r.PartitionID,
		}
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

// recentMovesToWire converts the slicer's internal recentMoves map
// (keyed by partition ID) into the trace's string-keyed form. Like
// cooldowns, entries older than CompactionInterval are carried through
// because ReplayTrace invokes pruneRecentMoves before running the
// slicer.
func recentMovesToWire(in map[int32][]moveRecord) map[string][]MoveRecord {
	out := make(map[string][]MoveRecord, len(in))
	for pid, records := range in {
		if len(records) == 0 {
			continue
		}
		wire := make([]MoveRecord, len(records))
		for i, m := range records {
			wire[i] = MoveRecord{
				Lo:     m.hr.Lo,
				Hi:     m.hr.Hi,
				Load:   m.load,
				Series: m.series,
				At:     m.at,
			}
		}
		out[strconv.FormatInt(int64(pid), 10)] = wire
	}
	return out
}

// recentMovesFromWire is the inverse of recentMovesToWire. Malformed
// partition keys are silently dropped.
func recentMovesFromWire(in map[string][]MoveRecord) map[int32][]moveRecord {
	out := make(map[int32][]moveRecord, len(in))
	for k, records := range in {
		pid, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			continue
		}
		list := make([]moveRecord, len(records))
		for i, m := range records {
			list[i] = moveRecord{
				hr:     assignment.HashRange{Lo: m.Lo, Hi: m.Hi},
				load:   m.Load,
				series: m.Series,
				at:     m.At,
			}
		}
		out[int32(pid)] = list
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
			MovementBudget:     t.Config.MovementBudget,
			MoveCooldown:       t.Config.MoveCooldown,
			CompactionInterval: t.Config.CompactionInterval,
		},
		moveCooldowns: cooldownsFromWire(t.Cooldowns),
		recentMoves:   recentMovesFromWire(t.RecentMoves),
	}
	start := &assignment.Assignment{
		Entries: append([]assignment.Entry(nil), t.Start...),
	}
	return r.runSlicer(start, ratesFromWire(t.Rates), t.PartitionL, r.recentMoves, t.ActivePartitions, t.Now)
}
