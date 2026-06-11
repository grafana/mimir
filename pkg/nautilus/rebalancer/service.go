// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// assignmentUpdate is one message bound for a watch subscriber:
// either a full snapshot of the retention-bounded log (reset=true)
// or the entries created/mutated since the subscriber's previous
// message (reset=false, delta subscribers only).
type assignmentUpdate struct {
	entries []assignment.LogEntry
	reset   bool
	// pruneBefore is the server's retention horizon at broadcast
	// time. Delta subscribers prune their local log with it; zero
	// means retention is disabled.
	pruneBefore time.Time
}

// logStore provides thread-safe access to the assignment log plus a
// fan-out subscription mechanism for streaming RPC clients.
//
// Reads (snapshot, latestActiveAssignment) take a defensive copy so
// callers can iterate without holding the mutex. Apply mutates the
// log under the mutex and broadcasts to all subscribers via
// 1-buffered conflated channels. Conflation never loses state: for
// snapshot subscribers a newer snapshot replaces the pending one;
// for delta subscribers a new delta is merged (upsert by lease
// identity) into the pending update, so a slow subscriber receives
// the coalesced equivalent of every broadcast it missed.
type logStore struct {
	mu          sync.Mutex
	log         *assignment.Log
	subscribers map[*subscription]struct{}

	// lastBroadcast is the full entry set as of the previous
	// broadcast, used to compute the per-apply delta (entries whose
	// To changed plus appended entries; the rebalancer never deletes
	// or rewrites From in place, see Log.MergedWithEntries).
	lastBroadcast []assignment.LogEntry
	// lastPruneBefore is the retention horizon of the most recent
	// apply, stamped on initial snapshots handed to new subscribers.
	lastPruneBefore time.Time

	// ready flips to true the first time apply() runs. Until then,
	// subscribe() returns a nil initial snapshot so the gRPC handler
	// skips its initial Send: a rebalancer that has just restarted
	// may have only stale persisted entries in s.log whose leases
	// already expired, and broadcasting that view as "live" tells
	// readcaches/distributors to drop everything they own and refuse
	// new traffic until the next apply runs. Waiting for the first
	// apply guarantees that whatever we broadcast as "initial"
	// reflects an actual rebalancer decision, not residual state.
	//
	// See pkg/nautilus/rebalancer/rebalancer.go for the cold-start
	// path that triggers the first apply, and apply()'s !changed
	// fall-through below for the broadcast on the ready transition.
	ready bool

	// persistFn, if non-nil, is called inside apply() under the mutex
	// after the log is mutated but before the broadcast. A non-nil
	// error is logged at error level and otherwise ignored — the
	// broadcast proceeds with the in-memory state regardless so a
	// failed write to a degraded volume doesn't stall live routing.
	// The rebalancer's next changed apply() retries; durability is
	// best-effort during volume degradation.
	persistFn func([]assignment.LogEntry) error
	logger    log.Logger
}

// subscription holds a single watcher's conflated update channel.
type subscription struct {
	ch chan assignmentUpdate
	// wantsDeltas records whether the subscriber understands
	// incremental updates (WatchAssignmentsRequest.supports_deltas).
	// Legacy subscribers get a full snapshot on every broadcast.
	wantsDeltas bool
	// primed flips to true once the subscriber has been handed a
	// full snapshot (either as subscribe()'s initial or as a
	// reset broadcast); only primed delta subscribers may receive
	// deltas. Guarded by logStore.mu.
	primed bool
}

func newLogStore() *logStore {
	return &logStore{
		log:         assignment.NewLog(),
		subscribers: make(map[*subscription]struct{}),
	}
}

// apply installs next as the new desired tiling at wall-clock at,
// pre-issuing successor leases of duration leaseDuration whenever
// an existing lease's To falls within lookahead of at. Prunes
// entries whose leases ended before at-retention, and broadcasts
// the resulting snapshot to all subscribers if the log changed.
// Returns true on change. retention <= 0 disables pruning.
//
// On a stable cluster, most rounds are no-ops: the previous round
// pre-issued a successor whose To is comfortably past at+lookahead.
// Only when the lookahead window catches up does Apply append a new
// successor and trigger a broadcast.
//
// Broadcast contents cover the full retention-bounded log, history
// included, NOT just the live entries. The write path only needs
// leases covering `now`, but the distributor's read path resolves
// partitions over the query's whole wall-clock window
// (getReadcacheReplicationSetsForQuery): expired leases are exactly
// what tells it which partitions held a hash range earlier in the
// window, and which readcache holds a frozen slice from before a
// move. EntryRetention bounds the state size; it must exceed the
// querier's lookback (QueryIngestersWithin), which the flag
// documents.
//
// Delta subscribers receive only the entries this apply created or
// mutated; legacy subscribers receive the full snapshot. Sends
// happen while holding the mutex — conflateSendUpdate never blocks,
// and in-lock sending guarantees subscribers observe deltas in
// apply order and keeps the per-subscriber primed transition atomic
// with its first snapshot.
func (s *logStore) apply(at time.Time, next *assignment.Assignment, leaseDuration, lookahead, retention time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := s.log.Apply(at, next, leaseDuration, lookahead)
	if retention > 0 {
		s.log.Prune(at.Add(-retention))
	}
	// becameReady tracks the !ready -> ready transition so subscribers
	// that connected before the first apply can be primed even when
	// this apply makes no log change (e.g. a steady-state round that
	// finds existing successors already cover the lookahead window).
	// Without this, a subscriber attached at startup would never see
	// any snapshot until the next mutation-bearing round, which can
	// be up to LeaseDuration away.
	becameReady := !s.ready
	s.ready = true
	if !changed && !becameReady {
		return false
	}
	full := s.log.Entries()
	delta := diffAssignmentEntries(s.lastBroadcast, full)
	s.lastBroadcast = full
	if retention > 0 {
		s.lastPruneBefore = at.Add(-retention)
	}
	if changed && s.persistFn != nil {
		// Persist while still holding the mutex so a concurrent
		// subscribe()'s initial snapshot can't see a state that
		// hasn't been durably committed yet.
		if err := s.persistFn(full); err != nil && s.logger != nil {
			level.Error(s.logger).Log("msg", "failed to persist assignment log", "err", err)
		}
	}

	for sub := range s.subscribers {
		switch {
		case !sub.wantsDeltas || !sub.primed:
			sub.primed = true
			conflateSendUpdate(sub.ch, assignmentUpdate{entries: full, reset: true, pruneBefore: s.lastPruneBefore})
		case len(delta) == 0:
			// A primed delta subscriber has nothing to learn from a
			// no-op apply (e.g. the becameReady broadcast).
		default:
			conflateSendUpdate(sub.ch, assignmentUpdate{entries: delta, pruneBefore: s.lastPruneBefore})
		}
	}
	return changed
}

// diffAssignmentEntries returns the entries of cur that are absent
// from prev or whose To differs — exactly the mutations Log.Apply
// can make (appends and in-place To rewrites; From and the identity
// fields are immutable, and deletions only happen via Prune, which
// subscribers replicate locally via pruneBefore).
func diffAssignmentEntries(prev, cur []assignment.LogEntry) []assignment.LogEntry {
	type key struct {
		r      assignment.HashRange
		pid    int32
		fromMs int64
	}
	prevTo := make(map[key]time.Time, len(prev))
	for _, e := range prev {
		prevTo[key{r: e.Range, pid: e.PartitionID, fromMs: e.From.UnixMilli()}] = e.To
	}
	var out []assignment.LogEntry
	for _, e := range cur {
		if to, ok := prevTo[key{r: e.Range, pid: e.PartitionID, fromMs: e.From.UnixMilli()}]; !ok || !to.Equal(e.To) {
			out = append(out, e)
		}
	}
	return out
}

// setPersistFn installs a persist callback. Safe to call once at
// startup before the rebalancer's running loop has started.
func (s *logStore) setPersistFn(fn func([]assignment.LogEntry) error, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistFn = fn
	s.logger = logger
}

// seedFromEntries replaces the in-memory log with the provided
// entries. Used to load on-disk state during rebalancer startup
// before any apply() runs.
func (s *logStore) seedFromEntries(entries []assignment.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = assignment.NewLogFromEntries(entries)
}

// leaseHorizon returns the soonest moment in the future at which
// some currently-relevant lease (active or pre-issued) expires, or
// the zero time if no such lease exists.
func (s *logStore) leaseHorizon(at time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LeaseHorizon(at)
}

// snapshot returns a defensive copy of all log entries (active,
// pre-issued, and expired-but-not-yet-pruned).
func (s *logStore) snapshot() []assignment.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Entries()
}

// latestActiveAssignment returns the entries whose leases are
// active at `at` collapsed into an *Assignment value, or nil if no
// entries are active (e.g. all leases have expired).
func (s *logStore) latestActiveAssignment(at time.Time) *assignment.Assignment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LatestActiveAssignment(at)
}

// subscribe registers a new watcher. If apply() has run at least
// once (s.ready), it returns a full snapshot of the retained log
// (reset=true) so the caller can prime its consumer atomically with
// the subscription. If no apply has run yet, the
// returned initial is nil — the caller MUST skip its initial send
// in that case, and instead wait for the first broadcast on the
// updates channel. apply() guarantees a broadcast on the
// !ready -> ready transition so the subscriber is eventually
// primed.
//
// Returning nil initial before the first apply prevents a freshly
// restarted rebalancer from telling subscribers "the assignment is
// empty" based solely on stale persisted entries whose leases
// already expired during the restart window.
//
// Expired-but-retained entries are included in both the initial
// snapshot and subsequent broadcasts: the distributor's read path
// resolves ownership over the query's wall-clock window, so it needs
// the history, not just the leases covering `now`. See the apply()
// comment.
//
// wantsDeltas opts the subscriber into incremental broadcasts after
// its priming snapshot; see assignmentUpdate.
//
// The caller MUST invoke unsubscribe when finished.
func (s *logStore) subscribe(wantsDeltas bool) (initial *assignmentUpdate, updates <-chan assignmentUpdate, unsubscribe func()) {
	sub := &subscription{ch: make(chan assignmentUpdate, 1), wantsDeltas: wantsDeltas}
	s.mu.Lock()
	if s.ready {
		sub.primed = true
		initial = &assignmentUpdate{entries: s.log.Entries(), reset: true, pruneBefore: s.lastPruneBefore}
	}
	s.subscribers[sub] = struct{}{}
	s.mu.Unlock()
	return initial, sub.ch, func() {
		s.mu.Lock()
		delete(s.subscribers, sub)
		s.mu.Unlock()
		// Drain so a pending broadcast doesn't leak the goroutine if
		// it raced with unsubscribe.
		select {
		case <-sub.ch:
		default:
		}
	}
}

// numSubscribers is exported only for tests.
func (s *logStore) numSubscribers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subscribers)
}

// conflateSendUpdate performs a non-blocking send of u on ch. If the
// buffer is full, the pending update is drained and coalesced with u
// so slow subscribers never lose state:
//
//   - u is a snapshot (reset=true): it supersedes whatever was
//     pending, snapshot or delta.
//   - u is a delta on top of a pending snapshot: u is upserted into
//     the snapshot, which stays a snapshot.
//   - u is a delta on top of a pending delta: the deltas are
//     upserted together. Upsert is last-write-wins per lease, which
//     is correct because deltas arrive in apply order (sends happen
//     under the store mutex).
func conflateSendUpdate(ch chan assignmentUpdate, u assignmentUpdate) {
	for {
		select {
		case ch <- u:
			return
		default:
			// Coalesce with the pending value (if it's still there)
			// and retry. The pending value may have been consumed by
			// the time we get here; the default branch covers that.
			select {
			case pending := <-ch:
				u = coalesceUpdates(pending, u)
			default:
			}
		}
	}
}

func coalesceUpdates(pending, next assignmentUpdate) assignmentUpdate {
	if next.reset {
		return next
	}
	merged := assignment.NewLogFromEntries(pending.entries).MergedWithEntries(next.entries)
	pruneBefore := next.pruneBefore
	if pruneBefore.IsZero() {
		pruneBefore = pending.pruneBefore
	}
	return assignmentUpdate{
		entries:     merged.Entries(),
		reset:       pending.reset,
		pruneBefore: pruneBefore,
	}
}

// EntriesToProto converts domain LogEntry values into their wire
// representation. Both From and To are encoded as unix milliseconds.
func EntriesToProto(es []assignment.LogEntry) []LogEntry {
	out := make([]LogEntry, len(es))
	for i, e := range es {
		out[i] = LogEntry{
			Lo:          e.Range.Lo,
			Hi:          e.Range.Hi,
			PartitionId: e.PartitionID,
			FromUnixMs:  e.From.UnixMilli(),
			ToUnixMs:    e.To.UnixMilli(),
		}
	}
	return out
}

// EntriesFromProto converts wire LogEntry values back to the domain
// type.
func EntriesFromProto(es []LogEntry) []assignment.LogEntry {
	out := make([]assignment.LogEntry, len(es))
	for i, e := range es {
		out[i] = assignment.LogEntry{
			Range:       assignment.HashRange{Lo: e.Lo, Hi: e.Hi},
			PartitionID: e.PartitionId,
			From:        time.UnixMilli(e.FromUnixMs),
			To:          time.UnixMilli(e.ToUnixMs),
		}
	}
	return out
}
