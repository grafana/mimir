// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// readcacheLogStore is the (partition -> readcache instance) parallel
// to logStore. It uses the same conflated-broadcast pattern so a slow
// subscriber sees only the latest snapshot.
type readcacheLogStore struct {
	mu          sync.Mutex
	log         *readcacheassignment.Log
	subscribers map[*readcacheSubscription]struct{}

	// persistFn, if non-nil, is invoked inside apply() to persist
	// the post-mutation snapshot. See logStore.persistFn for
	// semantics.
	persistFn func([]readcacheassignment.LogEntry) error
	logger    log.Logger
}

type readcacheSubscription struct {
	ch chan []readcacheassignment.LogEntry
}

func newReadcacheLogStore() *readcacheLogStore {
	return &readcacheLogStore{
		log:         readcacheassignment.NewLog(),
		subscribers: make(map[*readcacheSubscription]struct{}),
	}
}

// apply installs next as the new desired (partition -> instance)
// assignment, pre-issuing successors and pruning expired entries. See
// logStore.apply for semantics.
func (s *readcacheLogStore) apply(at time.Time, next *readcacheassignment.Assignment, leaseDuration, lookahead, retention time.Duration) bool {
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
		if err := s.persistFn(snap); err != nil && s.logger != nil {
			level.Error(s.logger).Log("msg", "failed to persist readcache assignment log", "err", err)
		}
	}
	subs := make([]*readcacheSubscription, 0, len(s.subscribers))
	for sub := range s.subscribers {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	for _, sub := range subs {
		conflateSendReadcache(sub.ch, snap)
	}
	return true
}

// setPersistFn installs a persist callback.
func (s *readcacheLogStore) setPersistFn(fn func([]readcacheassignment.LogEntry) error, logger log.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistFn = fn
	s.logger = logger
}

// seedFromEntries replaces the in-memory log with the provided
// entries.
func (s *readcacheLogStore) seedFromEntries(entries []readcacheassignment.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = readcacheassignment.NewLogFromEntries(entries)
}

// leaseHorizon returns the soonest moment in the future at which some
// currently-relevant readcache lease (active or pre-issued) expires.
func (s *readcacheLogStore) leaseHorizon(at time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LeaseHorizon(at)
}

// snapshot returns a defensive copy of all readcache log entries.
func (s *readcacheLogStore) snapshot() []readcacheassignment.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Entries()
}

// activeEntries returns the readcache ownership leases valid at at.
func (s *readcacheLogStore) activeEntries(at time.Time) []readcacheassignment.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LiveEntries(at)
}

// subscribe registers a new watcher. Same semantics as
// logStore.subscribe.
func (s *readcacheLogStore) subscribe(at time.Time) (initial []readcacheassignment.LogEntry, updates <-chan []readcacheassignment.LogEntry, unsubscribe func()) {
	sub := &readcacheSubscription{ch: make(chan []readcacheassignment.LogEntry, 1)}
	s.mu.Lock()
	initial = s.log.LiveEntries(at)
	s.subscribers[sub] = struct{}{}
	s.mu.Unlock()
	return initial, sub.ch, func() {
		s.mu.Lock()
		delete(s.subscribers, sub)
		s.mu.Unlock()
		select {
		case <-sub.ch:
		default:
		}
	}
}

// numSubscribers is exported only for tests.
func (s *readcacheLogStore) numSubscribers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subscribers)
}

func conflateSendReadcache(ch chan []readcacheassignment.LogEntry, snap []readcacheassignment.LogEntry) {
	for {
		select {
		case ch <- snap:
			return
		default:
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

// ReadcacheEntriesToProto converts domain LogEntry values into their
// wire representation.
func ReadcacheEntriesToProto(es []readcacheassignment.LogEntry) []ReadcacheLogEntry {
	out := make([]ReadcacheLogEntry, len(es))
	for i, e := range es {
		out[i] = ReadcacheLogEntry{
			PartitionId: e.PartitionID,
			InstanceId:  e.InstanceID,
			FromUnixMs:  e.From.UnixMilli(),
			ToUnixMs:    e.To.UnixMilli(),
		}
	}
	return out
}

// ReadcacheEntriesFromProto converts wire LogEntry values back to
// the domain type.
func ReadcacheEntriesFromProto(es []ReadcacheLogEntry) []readcacheassignment.LogEntry {
	out := make([]readcacheassignment.LogEntry, len(es))
	for i, e := range es {
		out[i] = readcacheassignment.LogEntry{
			PartitionID: e.PartitionId,
			InstanceID:  e.InstanceId,
			From:        time.UnixMilli(e.FromUnixMs),
			To:          time.UnixMilli(e.ToUnixMs),
		}
	}
	return out
}
