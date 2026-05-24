// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// LogEntry represents a (partition, hash range) ownership lease that
// is valid during the wall-clock window [From, To). Leases are
// time-boxed and, in the routine path, immutable: the rebalancer
// pre-issues each lease's successor before the current one expires,
// then leaves both entries in the log untouched. The only routine
// mutation is preemption — when a (Range, PartitionID) pair changes
// before its current lease expires, the active entry's To is
// truncated and a new entry is appended for the new owner.
//
// At any single wall-clock time, the set of currently-active entries
// (those with From <= at < To) tiles the full 32-bit hash space —
// provided the rebalancer has been alive recently enough to keep a
// successor lease in flight for every range. Once the latest
// successor for a range expires (e.g. the rebalancer crashes),
// consumers see no active entry for that range and fall back to
// whatever default routing they implement.
type LogEntry struct {
	Range       HashRange `json:"range"`
	PartitionID int32     `json:"partition_id"`
	From        time.Time `json:"from"`
	To          time.Time `json:"to"`
}

// ActiveAt reports whether the entry's lease covers wall-clock at,
// i.e. From <= at < To.
func (e LogEntry) ActiveAt(at time.Time) bool {
	return !at.Before(e.From) && at.Before(e.To)
}

// Log is a wall-clock-keyed sequence of (partition, hash range)
// ownership leases. The history of past leases (preempted or
// expired) is preserved so future consumers (e.g. queriers) can
// answer "who owned this hash range at sample-time T?" for T in
// the past.
//
// Operations:
//   - Apply ensures the desired tiling has a lease covering the
//     near future for every (Range, PartitionID), pre-issuing
//     successor leases when the current lease's expiry falls within
//     the lookahead window. Reassignments preempt the active lease
//     and append a new one for the new owner.
//   - Lookup picks the active-at-`at` entry whose range contains a
//     key.
//   - Prune drops entries whose lease ended (To) before a given
//     wall-clock.
//   - LeaseHorizon reports the minimum lease expiry across all
//     entries that are currently active or pre-issued for the
//     future, i.e. the earliest moment the next round must run by
//     to avoid a coverage gap.
//
// Log is not safe for concurrent use; callers must serialize access.
type Log struct {
	// entries holds all entries (active, pre-issued, and expired).
	// Kept sorted by (Range.Lo, From) for predictable iteration.
	entries []LogEntry
}

// NewLog returns an empty Log.
func NewLog() *Log {
	return &Log{}
}

// NewLogFromEntries returns a Log seeded with a defensive copy of
// entries. Used by callers that receive a flat snapshot over the
// wire and need to reconstruct a Log to query.
func NewLogFromEntries(entries []LogEntry) *Log {
	cp := make([]LogEntry, len(entries))
	copy(cp, entries)
	sortEntries(cp)
	return &Log{entries: cp}
}

// Apply ensures, for every (Range, PartitionID) in `next`, that the
// log holds a lease whose To is at least at + lookahead. Concretely:
//
//   - For pairs in `next` whose latest entry already extends past
//     at + lookahead, Apply is a no-op (the successor was queued in
//     a previous round). This is the steady-state case.
//
//   - For pairs in `next` whose latest entry is within the lookahead
//     window (or doesn't exist), Apply appends a successor lease
//     [latest.To, latest.To + leaseDuration). When the latest entry
//     has already expired or there is no prior entry at all (cold
//     start), the successor's From is at instead of latest.To, so
//     the lease starts immediately.
//
//   - For active entries whose (Range, PartitionID) is NOT in
//     `next`, To is truncated to at (preemption) and a new entry is
//     appended for the desired owner if applicable.
//
// Range splits and merges are handled as preemption-plus-creation:
// the old (Range, PID) entries don't appear in `next`, so they're
// preempted; the new (Range, PID) pairs aren't yet in the log, so
// they get fresh leases starting at at.
//
// Returns true if any entry was mutated or appended.
func (l *Log) Apply(at time.Time, next *Assignment, leaseDuration, lookahead time.Duration) (changed bool) {
	type key struct {
		r   HashRange
		pid int32
	}
	wanted := make(map[key]struct{}, len(next.Entries))
	for _, e := range next.Entries {
		wanted[key{r: e.Range, pid: e.PartitionID}] = struct{}{}
	}

	// Pre-compute the latest-To per (Range, PID) chain once. The
	// second pass below needs this for every entry in `next` (to
	// decide whether to extend an existing chain or start a fresh
	// lease); doing it with the naive O(M) latestTo() per call
	// makes Apply O(N*M) which, at dev-15 sizes (N=19200 tile entries,
	// M=100K log entries after 24h retention), pegs a single CPU
	// core for tens of seconds per round and is the dominant
	// rebalancer hotspot — confirmed by a pprof showing 98.7% of
	// CPU in latestTo. Building the index here makes the second
	// pass an O(1) map lookup, collapsing Apply to O(N+M).
	//
	// The first pass below mutates entry.To for preempted entries
	// (those NOT in `wanted`), but the second pass only consults
	// latestTo for keys that ARE in `wanted` — those To values are
	// never touched by the first pass, so building the index before
	// the first pass is safe.
	latestToIndex := make(map[key]time.Time, len(next.Entries))
	for _, e := range l.entries {
		k := key{r: e.Range, pid: e.PartitionID}
		if cur, ok := latestToIndex[k]; !ok || e.To.After(cur) {
			latestToIndex[k] = e.To
		}
	}

	// First pass: preempt every still-relevant entry (To > at) whose
	// (Range, PID) is not in `wanted`. Mark matched pairs so the
	// second pass can distinguish "chain continuing" from "absent
	// or just preempted".
	//
	// "Still-relevant" means the entry's lease has not already
	// ended at `at`. That set covers two cases the preemption must
	// handle:
	//
	//  1. Active entries (From <= at < To). These are in use by
	//     consumers right now; preemption clamps To to at so they
	//     stop being active immediately.
	//
	//  2. Pre-issued future entries (at < From < To). A previous
	//     round queued a successor for this (Range, PID); if the
	//     range has since been split, merged, or reassigned, that
	//     successor is no longer wanted and must not become active
	//     when wall-clock reaches its From. Clamping To = From
	//     produces a zero-duration window that ActiveAt will never
	//     match, killing the chain without leaving an overlap with
	//     the new lease that replaces it.
	//
	// Skipping pre-issued future entries here was the cause of a
	// production bug where successors of split/merged/reassigned
	// ranges activated alongside their replacements, creating
	// overlapping leases for the same partition once wall-clock
	// reached the successor's From.
	//
	// futureEntriesByKey records the indices of in-wanted future
	// entries (From > at, To > at). The second pass uses it to
	// preempt those entries when it has to seed a fresh lease at
	// `at` — see the !isActive branch below for the full bug
	// description. Building the index here keeps Apply at O(N+M);
	// the alternative (re-scanning l.entries from the second pass)
	// would re-introduce the O(N*M) bottleneck the latestToIndex
	// change fixed.
	matched := make(map[key]struct{}, len(next.Entries))
	futureEntriesByKey := make(map[key][]int, len(next.Entries))
	for i := range l.entries {
		e := &l.entries[i]
		if !e.To.After(at) {
			continue
		}
		k := key{r: e.Range, pid: e.PartitionID}
		if _, ok := wanted[k]; ok {
			// Only an active entry counts as "matched" for the
			// second pass — a pre-issued future entry is not yet
			// the live tile, so the second pass still needs to
			// observe its To via latestTo when extending the chain.
			if !e.From.After(at) {
				matched[k] = struct{}{}
			} else {
				futureEntriesByKey[k] = append(futureEntriesByKey[k], i)
			}
			continue
		}
		// Preempt: clamp To so this entry can never be returned by
		// ActiveAt at any t >= at. For active entries that means To
		// = at; for future entries, To = From (zero-duration).
		newTo := at
		if e.From.After(at) {
			newTo = e.From
		}
		if !e.To.Equal(newTo) {
			e.To = newTo
			changed = true
		}
	}

	// Second pass: for each desired pair, ensure a lease covers
	// [at, at+lookahead]. Append a fresh lease if there's no active
	// entry; append a successor if the latest entry will expire
	// within lookahead.
	deadline := at.Add(lookahead)
	for _, ne := range next.Entries {
		k := key{r: ne.Range, pid: ne.PartitionID}
		latestTo := latestToIndex[k]
		_, isActive := matched[k]
		switch {
		case !isActive:
			// No active lease for this pair (cold case, just
			// preempted, or fully expired). Seed a fresh lease
			// starting at `at`.
			//
			// Critical: always start at `at`, never at latestTo,
			// even when there's a still-alive pre-issued future
			// entry for this same (Range, PID). The "edge case"
			// branch this replaces (from = latestTo) was the cause
			// of the dev-15 "slicer received invalid input
			// assignment" pattern: when a chain was dropped at
			// round R and re-added at round R+1 within
			// leaseDuration of the previous round's pre-issued
			// successor, latestTo pointed at that surviving
			// successor's To (its From > at), so the fresh lease
			// got [latestTo, latestTo+leaseDuration]. That left
			// the window [at, future.From) uncovered for this
			// (Range, PID), and because the slicer's `next` tiles
			// the keyspace via exactly one (Range, PID) per hash,
			// no other chain covered the gap either. The next
			// round's LatestActiveAssignment then failed Validate
			// and runSlicer bailed, leases stopped being extended,
			// and distributors returned "key_not_covered" until
			// wall-clock crossed future.From minutes later.
			//
			// Starting at `at` always closes the [at, future.From)
			// gap. But it would *overlap* any still-alive future
			// entries for this key once wall-clock crosses their
			// From, recreating the same overlapping-leases bug
			// that the first pass's future-preemption was added to
			// fix. So preempt those futures here too: futureEntriesByKey[k]
			// was populated in the first pass with exactly the
			// entries we need to clamp.
			for _, idx := range futureEntriesByKey[k] {
				e := &l.entries[idx]
				if !e.To.Equal(e.From) {
					e.To = e.From
					changed = true
				}
			}
			l.entries = append(l.entries, LogEntry{
				Range:       k.r,
				PartitionID: k.pid,
				From:        at,
				To:          at.Add(leaseDuration),
			})
			changed = true
		case latestTo.After(deadline):
			// Steady-state no-op: a successor is already queued far
			// enough into the future.
		default:
			// Active lease will expire within lookahead. Append the
			// successor [latestTo, latestTo + leaseDuration).
			l.entries = append(l.entries, LogEntry{
				Range:       k.r,
				PartitionID: k.pid,
				From:        latestTo,
				To:          latestTo.Add(leaseDuration),
			})
			changed = true
		}
	}

	if changed {
		sortEntries(l.entries)
	}
	return changed
}

// Lookup returns the partition ID of the entry whose lease is
// active at `at` and whose range contains key. Returns (0, false)
// if no such entry exists — including when all relevant leases have
// expired (a sign the rebalancer hasn't published recently).
//
// At any single wall-clock time, the active entries tile the hash
// space without overlap, so at most one entry can match.
func (l *Log) Lookup(at time.Time, key uint32) (int32, bool) {
	for _, e := range l.entries {
		if !e.ActiveAt(at) {
			continue
		}
		if e.Range.Contains(key) {
			return e.PartitionID, true
		}
	}
	return 0, false
}

// ActiveAt returns a copy of all entries whose lease covers `at`.
// When `at` is the current wall-clock time and the log has been
// driven by recent Apply calls, the returned entries tile the full
// 32-bit hash space.
func (l *Log) ActiveAt(at time.Time) []LogEntry {
	var out []LogEntry
	for _, e := range l.entries {
		if e.ActiveAt(at) {
			out = append(out, e)
		}
	}
	return out
}

// Prune drops entries whose lease ended (To) strictly before
// closedBefore. Entries still active at closedBefore are never
// pruned, and pre-issued future leases (To > closedBefore) are
// always retained.
func (l *Log) Prune(closedBefore time.Time) {
	out := l.entries[:0]
	for _, e := range l.entries {
		if e.To.Before(closedBefore) {
			continue
		}
		out = append(out, e)
	}
	for i := len(out); i < len(l.entries); i++ {
		l.entries[i] = LogEntry{}
	}
	l.entries = out
}

// Entries returns a defensive copy of all entries in the log.
func (l *Log) Entries() []LogEntry {
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// LiveEntries returns a defensive copy of all entries whose lease
// has not yet ended at `at` — i.e. entries with To > at. This
// includes both currently-active leases (From <= at < To) and
// pre-issued future leases (From > at). Expired entries (To <= at)
// are excluded.
//
// This is the appropriate snapshot for streaming consumers that
// only need to route writes at or after `at`: by construction, an
// expired entry can never be returned by Lookup at any t >= at, so
// shipping it costs bytes but adds no information. The active
// entries plus pre-issued successors give continuous coverage
// across the next lease boundary, so a consumer that builds an
// ActiveTable for `at` and rebuilds when its current table
// expires sees no gap.
func (l *Log) LiveEntries(at time.Time) []LogEntry {
	out := make([]LogEntry, 0, len(l.entries))
	for _, e := range l.entries {
		if e.To.After(at) {
			out = append(out, e)
		}
	}
	return out
}

// Len returns the total number of entries (active, pre-issued, and
// expired) in the log.
func (l *Log) Len() int { return len(l.entries) }

// LeaseHorizon returns the soonest moment in the future at which
// some (Range, PartitionID) ownership chain runs out of coverage —
// i.e. the soonest at which the tiling will become incomplete
// unless the rebalancer queues a successor before then. Returns
// the zero time if no entry has To after at.
//
// Concretely: for each (Range, PartitionID) with at least one entry
// whose To is after at, the chain's end-of-coverage is the latest
// such To (the active lease's To when no successor is queued, or
// the successor's To when one is queued). LeaseHorizon returns the
// minimum chain-end across all chains.
//
// Callers (the rebalancer) use this to schedule the next round at
// horizon - lookahead. Once a successor is queued, the chain-end
// reflects the successor (not the active lease that's about to
// expire), so the next round is scheduled relative to when the
// successor itself needs its successor — i.e. one full lease
// duration later.
func (l *Log) LeaseHorizon(at time.Time) time.Time {
	type chainKey struct {
		r   HashRange
		pid int32
	}
	// For each (Range, PartitionID), the chain end is the latest
	// To across entries with To > at. Map cardinality is bounded by
	// the number of distinct active or pre-issued tiles (typically
	// a few hundred), so this allocation is cheap relative to a
	// rebalance round.
	chainEnd := make(map[chainKey]time.Time)
	for _, e := range l.entries {
		if !e.To.After(at) {
			continue
		}
		k := chainKey{e.Range, e.PartitionID}
		if existing, ok := chainEnd[k]; !ok || e.To.After(existing) {
			chainEnd[k] = e.To
		}
	}

	var horizon time.Time
	for _, t := range chainEnd {
		if horizon.IsZero() || t.Before(horizon) {
			horizon = t
		}
	}
	return horizon
}

// ActiveTilesFullSpace verifies that the entries active at `at`
// cover [0, math.MaxUint32] without gaps or overlaps. Returns nil
// on success or a descriptive error otherwise. A common cause of
// failure is "no active entries at `at`", indicating the latest
// successor lease has already expired.
func (l *Log) ActiveTilesFullSpace(at time.Time) error {
	active := l.ActiveAt(at)
	if len(active) == 0 {
		return fmt.Errorf("no active entries at %s (leases may have expired)", at)
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].Range.Lo < active[j].Range.Lo
	})
	if active[0].Range.Lo != 0 {
		return fmt.Errorf("first active entry starts at %d, want 0", active[0].Range.Lo)
	}
	if active[len(active)-1].Range.Hi != math.MaxUint32 {
		return fmt.Errorf("last active entry ends at %d, want %d", active[len(active)-1].Range.Hi, uint32(math.MaxUint32))
	}
	for i := 1; i < len(active); i++ {
		prev := active[i-1].Range.Hi
		curr := active[i].Range.Lo
		// Promote to uint64 before adding 1 so we don't wrap when
		// prev == MaxUint32 (which would silently match curr == 0
		// and hide a duplicate tile starting at 0).
		if uint64(curr) != uint64(prev)+1 {
			return fmt.Errorf("gap or overlap between active entries: prev.Hi=%d, curr.Lo=%d", prev, curr)
		}
	}
	return nil
}

// LatestActiveAssignment returns the entries whose leases are
// active at `at` collapsed into an *Assignment value. Useful when
// callers (e.g. the rebalancer's pushRangesToIngesters or admin UI)
// need the full-tiling representation rather than a flat list of
// entries.
//
// Returns nil if no entries are active at `at` (e.g. the log is
// empty or the latest successor for some range has already expired).
func (l *Log) LatestActiveAssignment(at time.Time) *Assignment {
	active := l.ActiveAt(at)
	if len(active) == 0 {
		return nil
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].Range.Lo < active[j].Range.Lo
	})
	entries := make([]Entry, len(active))
	for i, e := range active {
		entries[i] = Entry{Range: e.Range, PartitionID: e.PartitionID}
	}
	return &Assignment{Entries: entries}
}

// sortEntries sorts entries ascending by (Range.Lo, From). Used to
// keep the underlying slice in a predictable order after mutation.
func sortEntries(entries []LogEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Range.Lo != entries[j].Range.Lo {
			return entries[i].Range.Lo < entries[j].Range.Lo
		}
		return entries[i].From.Before(entries[j].From)
	})
}
