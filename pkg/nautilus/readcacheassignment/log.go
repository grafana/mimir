// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"sort"
	"time"
)

// LogEntry represents a (PartitionID -> InstanceID) ownership lease
// valid during the wall-clock window [From, To). Two entries with the
// same PartitionID but different InstanceIDs and overlapping windows
// represent multi-ownership: the partition is being served by both
// readcache instances during the overlap.
type LogEntry struct {
	PartitionID int32     `json:"partition_id"`
	InstanceID  string    `json:"instance_id"`
	From        time.Time `json:"from"`
	To          time.Time `json:"to"`
}

// ActiveAt reports whether the entry's lease covers wall-clock at.
func (e LogEntry) ActiveAt(at time.Time) bool {
	return !at.Before(e.From) && at.Before(e.To)
}

// Assignment is a desired snapshot of partition->instance mapping. The
// rebalancer constructs one of these each round and hands it to
// Log.Apply.
type Assignment struct {
	Entries []AssignmentEntry `json:"entries"`
}

// AssignmentEntry pairs a partition with the instance that should own
// it in the next round.
type AssignmentEntry struct {
	PartitionID int32  `json:"partition_id"`
	InstanceID  string `json:"instance_id"`
}

// Log is a wall-clock-keyed sequence of (PartitionID, InstanceID)
// ownership leases. Like pkg/nautilus/assignment.Log it retains
// expired entries until Prune is called, so consumers can answer
// "who owned partition P at time T?" for past T.
//
// Log is not safe for concurrent use; callers must serialize access.
type Log struct {
	entries []LogEntry
}

// NewLog returns an empty Log.
func NewLog() *Log {
	return &Log{}
}

// NewLogFromEntries returns a Log seeded with a defensive copy of
// entries. Used by callers that receive a flat snapshot over the wire
// and need to reconstruct a Log to query.
func NewLogFromEntries(entries []LogEntry) *Log {
	cp := make([]LogEntry, len(entries))
	copy(cp, entries)
	sortEntries(cp)
	return &Log{entries: cp}
}

// Apply ensures, for every (PartitionID, InstanceID) in `next`, that
// the log holds a lease whose To is at least at + lookahead. Semantics
// mirror pkg/nautilus/assignment.Log.Apply.
//
// When the InstanceID for a partition changes between rounds, the old
// owner's active lease is preempted and a new lease is appended for
// the new owner starting at `at`.
//
// safetyWindow controls the preemption boundary for the old owner. It
// is the keystone of the "no full replay" move scheme: the new owner
// starts consuming at the Kafka live edge at `at`, while the old owner
// is kept alive (still consuming, still routable) until at+safetyWindow
// before its lease ends, then it freezes. The overlap guarantees there
// is no gap between the two owners' slices; it produces a small
// duplicate band that query-time dedup absorbs. A safetyWindow of 0
// reproduces the legacy immediate preemption (To = at), where the old
// owner stops the instant the move happens.
//
// In multi-owner mode `next` may contain multiple entries per
// PartitionID; each (PartitionID, InstanceID) pair gets its own lease
// chain. Removing one of the InstanceIDs preempts that pair while
// leaving the others alone.
//
// Returns true if any entry was mutated or appended.
func (l *Log) Apply(at time.Time, next *Assignment, leaseDuration, lookahead, safetyWindow time.Duration) (changed bool) {
	type key struct {
		pid      int32
		instance string
	}
	wanted := make(map[key]struct{}, len(next.Entries))
	for _, e := range next.Entries {
		wanted[key{pid: e.PartitionID, instance: e.InstanceID}] = struct{}{}
	}

	// Pre-compute the latest-To per (PartitionID, InstanceID) chain
	// once. See pkg/nautilus/assignment/log.go Apply for the
	// detailed rationale; in short, the second pass needs latest-To
	// for every entry in `next` and a per-call O(M) scan turns Apply
	// into O(N*M), which at dev-15 sizes (300 partitions, 60K log
	// entries) was the dominant rebalancer hotspot (98.7% of CPU
	// in a single function per pprof). Building the index here
	// makes the second pass O(1) per key, collapsing Apply to
	// O(N+M). The first pass mutates only To for keys NOT in
	// `wanted`; the second pass only consults latest-To for keys
	// IN `wanted`, so the pre-index is safe to build before the
	// first pass.
	latestToIndex := make(map[key]time.Time, len(next.Entries))
	for _, e := range l.entries {
		k := key{pid: e.PartitionID, instance: e.InstanceID}
		if cur, ok := latestToIndex[k]; !ok || e.To.After(cur) {
			latestToIndex[k] = e.To
		}
	}

	// futureEntriesByKey records the indices of in-wanted future
	// entries (From > at, To > at). The second pass uses it to
	// preempt those entries when it has to seed a fresh lease at
	// `at` — see pkg/nautilus/assignment/log.go Apply for the
	// full description of the drop-then-readd-within-leaseDuration
	// bug this prevents. Same mechanism applies here: the tier-2
	// (PartitionID, InstanceID) chains are managed identically to
	// tier-1's (Range, PartitionID) chains and have the same
	// failure mode.
	matched := make(map[key]struct{}, len(next.Entries))
	futureEntriesByKey := make(map[key][]int, len(next.Entries))
	for i := range l.entries {
		e := &l.entries[i]
		if !e.To.After(at) {
			continue
		}
		k := key{pid: e.PartitionID, instance: e.InstanceID}
		if _, ok := wanted[k]; ok {
			if !e.From.After(at) {
				matched[k] = struct{}{}
			} else {
				futureEntriesByKey[k] = append(futureEntriesByKey[k], i)
			}
			continue
		}
		// Preempt. An active lease being moved away is kept alive
		// until at+safetyWindow (capped at its natural end) so the
		// previous owner keeps consuming and stays routable across
		// the handoff; with safetyWindow=0 this collapses to the
		// legacy immediate To=at preemption. Pre-issued future leases
		// are clamped to To = From regardless (see
		// pkg/nautilus/assignment/log.go Apply for the rationale).
		newTo := at.Add(safetyWindow)
		if e.From.After(at) {
			newTo = e.From
		} else if newTo.After(e.To) {
			newTo = e.To
		}
		if !e.To.Equal(newTo) {
			e.To = newTo
			changed = true
		}
	}

	deadline := at.Add(lookahead)
	for _, ne := range next.Entries {
		k := key{pid: ne.PartitionID, instance: ne.InstanceID}
		latestTo := latestToIndex[k]
		_, isActive := matched[k]
		switch {
		case !isActive:
			// Always seed at `at`, preempting any alive in-wanted
			// future entries for this key. See
			// pkg/nautilus/assignment/log.go Apply !isActive case
			// for the bug this defends against.
			for _, idx := range futureEntriesByKey[k] {
				e := &l.entries[idx]
				if !e.To.Equal(e.From) {
					e.To = e.From
					changed = true
				}
			}
			l.entries = append(l.entries, LogEntry{
				PartitionID: k.pid,
				InstanceID:  k.instance,
				From:        at,
				To:          at.Add(leaseDuration),
			})
			changed = true
		case latestTo.After(deadline):
			// Successor already pre-issued.
		default:
			l.entries = append(l.entries, LogEntry{
				PartitionID: k.pid,
				InstanceID:  k.instance,
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

// Lookup returns the instance IDs currently owning partitionID at
// `at`. In single-owner mode the returned slice has length 0 or 1.
// In multi-owner mode all currently-active owners are returned.
func (l *Log) Lookup(at time.Time, partitionID int32) []string {
	var out []string
	for _, e := range l.entries {
		if e.PartitionID != partitionID {
			continue
		}
		if !e.ActiveAt(at) {
			continue
		}
		out = append(out, e.InstanceID)
	}
	return out
}

// OwnersDuring returns the distinct instance IDs that held partitionID
// at any point during the half-open wall-clock window [w0, w1). A lease
// [From, To) is included iff From < w1 && To > w0.
//
// This is the interval analogue of Lookup. A query whose interval spans
// a partition->instance move fans out to every readcache that owned the
// partition during the window: each readcache holds only the slice of
// the partition's data it ingested while it owned the partition (the
// new owner starts at the Kafka live edge and the previous owner keeps
// its frozen slice queryable until retention), so the distributor must
// query all of them and merge + dedup the results.
//
// Results are de-duplicated and returned sorted ascending for
// deterministic fan-out.
func (l *Log) OwnersDuring(partitionID int32, w0, w1 time.Time) []string {
	seen := make(map[string]struct{})
	for _, e := range l.entries {
		if e.PartitionID != partitionID {
			continue
		}
		if !e.From.Before(w1) || !e.To.After(w0) {
			continue
		}
		seen[e.InstanceID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ActiveAt returns a copy of all entries whose lease covers `at`.
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
// closedBefore.
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

// Entries returns a defensive copy of all entries.
func (l *Log) Entries() []LogEntry {
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// LiveEntries returns entries whose lease has not yet ended at `at`.
// Includes active and pre-issued future leases.
func (l *Log) LiveEntries(at time.Time) []LogEntry {
	out := make([]LogEntry, 0, len(l.entries))
	for _, e := range l.entries {
		if e.To.After(at) {
			out = append(out, e)
		}
	}
	return out
}

// Len returns the total number of entries.
func (l *Log) Len() int { return len(l.entries) }

// LeaseHorizon returns the soonest moment in the future at which some
// (PartitionID, InstanceID) chain runs out of coverage.
//
// Same semantics as pkg/nautilus/assignment.Log.LeaseHorizon.
func (l *Log) LeaseHorizon(at time.Time) time.Time {
	type chainKey struct {
		pid      int32
		instance string
	}
	chainEnd := make(map[chainKey]time.Time)
	for _, e := range l.entries {
		if !e.To.After(at) {
			continue
		}
		k := chainKey{e.PartitionID, e.InstanceID}
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

// sortEntries sorts entries ascending by (PartitionID, From,
// InstanceID).
func sortEntries(entries []LogEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].PartitionID != entries[j].PartitionID {
			return entries[i].PartitionID < entries[j].PartitionID
		}
		if !entries[i].From.Equal(entries[j].From) {
			return entries[i].From.Before(entries[j].From)
		}
		return entries[i].InstanceID < entries[j].InstanceID
	})
}
