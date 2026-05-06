// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sync"
	"time"

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
	snap := s.log.Entries()
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

// subscribe registers a new watcher. It returns the current snapshot
// (so the caller can prime its consumer atomically with the
// subscription), a channel that receives subsequent snapshots, and an
// unsubscribe function the caller MUST invoke when finished.
func (s *logStore) subscribe() (initial []assignment.LogEntry, updates <-chan []assignment.LogEntry, unsubscribe func()) {
	sub := &subscription{ch: make(chan []assignment.LogEntry, 1)}
	s.mu.Lock()
	initial = s.log.Entries()
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
