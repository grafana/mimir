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

	// First pass: preempt active entries whose (Range, PID) is not
	// in `wanted`. Mark matched pairs so the second pass can
	// distinguish "active and continuing" from "absent or just
	// preempted".
	matched := make(map[key]struct{}, len(next.Entries))
	for i := range l.entries {
		e := &l.entries[i]
		if !e.ActiveAt(at) {
			continue
		}
		k := key{r: e.Range, pid: e.PartitionID}
		if _, ok := wanted[k]; ok {
			matched[k] = struct{}{}
			continue
		}
		e.To = at
		changed = true
	}

	// Second pass: for each desired pair, ensure a lease covers
	// [at, at+lookahead]. Append a fresh lease if there's no active
	// entry; append a successor if the latest entry will expire
	// within lookahead.
	deadline := at.Add(lookahead)
	for _, ne := range next.Entries {
		k := key{r: ne.Range, pid: ne.PartitionID}
		latestTo, found := l.latestTo(k.r, k.pid)
		_, isActive := matched[k]
		switch {
		case !isActive:
			// No active lease for this pair (cold case, just
			// preempted, or fully expired). Start a fresh lease at
			// `at`. If there's a stale prior entry for the exact
			// same (Range, PID) whose To is in the past, we don't
			// reuse it — the gap (if any) is intentional and signals
			// to consumers the lease was reset.
			from := at
			if found && latestTo.After(at) {
				// Edge case: a previously-pre-issued future lease
				// exists but isn't active yet (its From > at). Start
				// the new lease where the old one ends instead of
				// at, so the chain stays continuous.
				from = latestTo
			}
			l.entries = append(l.entries, LogEntry{
				Range:       k.r,
				PartitionID: k.pid,
				From:        from,
				To:          from.Add(leaseDuration),
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

// latestTo returns the maximum To across all entries with the given
// (Range, PartitionID), and whether any such entry exists.
func (l *Log) latestTo(r HashRange, pid int32) (time.Time, bool) {
	var latest time.Time
	found := false
	for _, e := range l.entries {
		if e.Range != r || e.PartitionID != pid {
			continue
		}
		if !found || e.To.After(latest) {
			latest = e.To
			found = true
		}
	}
	return latest, found
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

// Len returns the total number of entries (active, pre-issued, and
// expired) in the log.
func (l *Log) Len() int { return len(l.entries) }

// LeaseHorizon returns the soonest moment in the future at which
// some currently-relevant lease expires, considering both active
// leases (From <= at < To) and pre-issued future leases
// (From > at). Returns the zero time if no entry's To is after at.
//
// Callers (the rebalancer) use this to schedule the next round at
// horizon - lookahead, ensuring a successor lease is appended
// before the current one ends.
func (l *Log) LeaseHorizon(at time.Time) time.Time {
	var horizon time.Time
	for _, e := range l.entries {
		if !e.To.After(at) {
			continue
		}
		if horizon.IsZero() || e.To.Before(horizon) {
			horizon = e.To
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
		if curr != prev+1 {
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
