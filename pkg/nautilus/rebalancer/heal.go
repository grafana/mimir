// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// healReport summarises what healAssignmentGaps had to do to make an
// invalid input pass Validate. All counters are exposed for logging
// and metrics; zero values mean "input was already valid".
type healReport struct {
	// gapsFilled is the number of synthetic filler entries the healer
	// inserted to plug holes in the keyspace coverage.
	gapsFilled int
	// gapHashesFilled is the total number of hash values that were
	// uncovered before healing.
	gapHashesFilled uint64
	// overlapsResolved is the number of input entries the healer had to
	// truncate or drop because their range overlapped an entry that came
	// earlier in the sorted order.
	overlapsResolved int
	// overlapHashesResolved is the total number of hash values that were
	// double-claimed before healing.
	overlapHashesResolved uint64
	// firstGap is the first uncovered range (Lo, Hi) discovered while
	// walking the input. Empty when no gaps were found. Useful for
	// pinpointing the failure window in logs without dumping every gap.
	firstGap assignment.HashRange
	// hasFirstGap distinguishes "no gaps were found" from "the first gap
	// happens to be {Lo:0, Hi:0}".
	hasFirstGap bool
}

// hashSpaceEnd is the inclusive last index of the 32-bit keyspace.
const hashSpaceEnd = uint64(math.MaxUint32)

// healAssignmentGaps takes an assignment that may have gaps, overlaps,
// missing prefix/suffix, or duplicate entries and produces an
// equivalent assignment that satisfies Assignment.Validate.
//
// Rationale: runSlicer requires its `current` input to tile the full
// 32-bit hash space. When LatestActiveAssignment returns a gappy
// snapshot (because a chain expired between rounds without a
// successor being persisted in time — e.g. several consecutive
// rounds failed validation and the in-flight lease horizons
// drifted past wall-clock for some (Range, PartitionID) pairs),
// runSlicer detects the problem mid-phase and reverts, the rebalance
// round bails at newAssignment.Validate(), no Apply happens, the
// lease horizon stays close to expiry, and distributors start
// returning "key_not_covered" for any write that hashes into the
// uncovered range. The system has no mechanism to escape this state
// on its own because the slicer only moves/merges/splits ranges it
// can see; it cannot synthesise the missing ones.
//
// Healing closes the loop: when the rebalance round detects an
// invalid `current`, it patches the gaps with filler entries (one
// per gap, partition assigned round-robin from activePartitions),
// drops or truncates entries that overlap, and proceeds with a
// well-formed input. The next round's slicer sees the fillers as
// regular tiles and can move them as needed under its normal
// budgets; the cooldown machinery keeps the patched ranges from
// being re-shuffled immediately.
//
// activePartitions must be non-empty. Returns the original input
// (and an unchanged report) when activePartitions is empty: callers
// have no good answer in that case and should fall back to whatever
// they already do for "no partitions" (currently: skip the round).
//
// The healed assignment is always a newly-allocated *Assignment; the
// input is never mutated.
func healAssignmentGaps(current *assignment.Assignment, activePartitions []int32) (*assignment.Assignment, healReport) {
	if current == nil || len(activePartitions) == 0 {
		return current, healReport{}
	}

	// Defensive copy and sort. Production callers pass an assignment
	// that's already sorted by Range.Lo, but Validate only checks the
	// adjacency relation, not strict sortedness — we re-sort so the
	// healer is correct on every shape of input.
	src := make([]assignment.Entry, len(current.Entries))
	copy(src, current.Entries)
	sort.Slice(src, func(i, j int) bool {
		if src[i].Range.Lo != src[j].Range.Lo {
			return src[i].Range.Lo < src[j].Range.Lo
		}
		return src[i].Range.Hi < src[j].Range.Hi
	})

	rep := healReport{}
	rrIdx := 0
	pickPartition := func() int32 {
		p := activePartitions[rrIdx%len(activePartitions)]
		rrIdx++
		return p
	}

	healed := make([]assignment.Entry, 0, len(src)+1)
	recordGap := func(from, to uint64) {
		rep.gapsFilled++
		rep.gapHashesFilled += to - from + 1
		if !rep.hasFirstGap {
			rep.firstGap = assignment.HashRange{Lo: uint32(from), Hi: uint32(to)}
			rep.hasFirstGap = true
		}
		healed = append(healed, assignment.Entry{
			Range:       assignment.HashRange{Lo: uint32(from), Hi: uint32(to)},
			PartitionID: pickPartition(),
		})
	}

	// next tracks the first hash position that has not yet been
	// covered by an emitted entry. Promoted to uint64 so we can
	// represent "past the end of the keyspace" (hashSpaceEnd+1)
	// without overflow, which lets the trailing-gap check below
	// be a single comparison.
	var next uint64
	for _, e := range src {
		lo := uint64(e.Range.Lo)
		hi := uint64(e.Range.Hi)
		switch {
		case lo > next:
			recordGap(next, lo-1)
			healed = append(healed, e)
			next = hi + 1
		case lo == next:
			healed = append(healed, e)
			next = hi + 1
		default: // lo < next: overlap
			rep.overlapsResolved++
			if hi < next {
				// Entry is fully shadowed by what we already
				// emitted.
				rep.overlapHashesResolved += hi - lo + 1
				continue
			}
			// Partial overlap: trim the entry's Lo to next.
			rep.overlapHashesResolved += next - lo
			trimmed := e
			trimmed.Range.Lo = uint32(next)
			healed = append(healed, trimmed)
			next = hi + 1
		}
	}
	if next <= hashSpaceEnd {
		recordGap(next, hashSpaceEnd)
	}
	return &assignment.Assignment{Entries: healed}, rep
}

// String renders the report as a compact, log-friendly summary.
func (r healReport) String() string {
	if r.gapsFilled == 0 && r.overlapsResolved == 0 {
		return "no-op"
	}
	if !r.hasFirstGap {
		return fmt.Sprintf("filled=%d/%d overlaps=%d/%d",
			r.gapsFilled, r.gapHashesFilled, r.overlapsResolved, r.overlapHashesResolved)
	}
	return fmt.Sprintf("filled=%d/%d overlaps=%d/%d first_gap=[%d,%d]",
		r.gapsFilled, r.gapHashesFilled, r.overlapsResolved, r.overlapHashesResolved,
		r.firstGap.Lo, r.firstGap.Hi)
}
