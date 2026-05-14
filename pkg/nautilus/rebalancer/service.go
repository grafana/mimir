// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// logStore provides thread-safe access to the assignment log plus a
// fan-out subscription mechanism for streaming RPC clients.
//
// Reads (snapshot, latestActiveAssignment) take a defensive copy so
// callers can iterate without holding the mutex. Apply mutates the
// log under the mutex and broadcasts a fresh snapshot to all
// subscribers via 1-buffered conflated channels: a slow subscriber
// sees only the most recent snapshot, never every intermediate one.
type logStore struct {
	mu          sync.Mutex
	log         *assignment.Log
	subscribers map[*subscription]struct{}

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
	ch chan []assignment.LogEntry
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
// The broadcast snapshot only contains live entries (To > at):
// expired entries can never be returned by Lookup at any t >= at,
// so omitting them keeps the gRPC message size proportional to the
// active+pre-issued tile count rather than the full retention
// window. The unfiltered log is still available to admin views and
// (in future) queriers via snapshot().
func (s *logStore) apply(at time.Time, next *assignment.Assignment, leaseDuration, lookahead, retention time.Duration) bool {
	s.mu.Lock()
	changed := s.log.Apply(at, next, leaseDuration, lookahead)
	if retention > 0 {
		s.log.Prune(at.Add(-retention))
	}
	if !changed {
		s.mu.Unlock()
		return false
	}
	snap := s.log.LiveEntries(at)
	if s.persistFn != nil {
		// Persist while still holding the mutex so a concurrent
		// subscribe()'s initial snapshot can't see a state that
		// hasn't been durably committed yet.
		if err := s.persistFn(snap); err != nil && s.logger != nil {
			level.Error(s.logger).Log("msg", "failed to persist assignment log", "err", err)
		}
	}
	subs := make([]*subscription, 0, len(s.subscribers))
	for sub := range s.subscribers {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		conflateSend(sub.ch, snap)
	}
	return true
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

// subscribe registers a new watcher. It returns a snapshot of the
// log's live entries (those with To > at) so the caller can prime
// its consumer atomically with the subscription, a channel that
// receives subsequent snapshots, and an unsubscribe function the
// caller MUST invoke when finished. Expired entries are omitted
// from the initial snapshot and from subsequent broadcasts: a
// fresh subscriber can never use them and including them would
// inflate the wire message by the full retention window's worth of
// history.
func (s *logStore) subscribe(at time.Time) (initial []assignment.LogEntry, updates <-chan []assignment.LogEntry, unsubscribe func()) {
	sub := &subscription{ch: make(chan []assignment.LogEntry, 1)}
	s.mu.Lock()
	initial = s.log.LiveEntries(at)
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

// conflateSend performs a non-blocking send of snap on ch. If the
// buffer is already full, it drains the stale value and replaces it
// with snap so slow subscribers always see the most recent state.
func conflateSend(ch chan []assignment.LogEntry, snap []assignment.LogEntry) {
	for {
		select {
		case ch <- snap:
			return
		default:
			// Drain a stale value (if it's still there) and retry.
			// The drained value may or may not be present by the time
			// we get here; the default branch covers either case.
			select {
			case <-ch:
			default:
				return
			}
		}
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
