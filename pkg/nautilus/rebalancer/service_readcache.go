// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// readcacheUpdate is the readcache-log analogue of assignmentUpdate:
// a full snapshot (reset=true) or an upsert delta bound for one
// subscriber.
type readcacheUpdate struct {
	entries     []readcacheassignment.LogEntry
	reset       bool
	pruneBefore time.Time
	// replicaMap expands the logical instance IDs named in entries to
	// their concrete zone pods. Nil/empty means identity (RF=1): the
	// lease instance ID is also the dial target. Sent in full on
	// every update because it is small (O(slots × zones)) and
	// subscribers treat it as a wholesale replacement.
	replicaMap readcacheassignment.ReplicaMap
}

// readcacheLogStore is the (partition -> readcache instance) parallel
// to logStore. It uses the same coalescing-broadcast pattern: slow
// snapshot subscribers see only the latest snapshot, slow delta
// subscribers see their missed deltas merged into one.
type readcacheLogStore struct {
	mu          sync.Mutex
	log         *readcacheassignment.Log
	subscribers map[*readcacheSubscription]struct{}

	// lastBroadcast / lastPruneBefore mirror logStore; see there.
	lastBroadcast   []readcacheassignment.LogEntry
	lastPruneBefore time.Time

	// replicaMap is the current logical->concrete expansion published
	// alongside every broadcast. Owned by the rebalancer's rebalance
	// loop via setReplicaMap.
	replicaMap readcacheassignment.ReplicaMap

	// ready flips to true on the first apply() call. Until then,
	// subscribe() returns nil initial and the gRPC handler skips its
	// initial send: a freshly-restarted rebalancer whose persisted
	// readcache log loaded only stale entries (To <= now) would
	// otherwise tell every readcache "you own nothing", causing the
	// fleet to drop all partitions. See logStore.ready for the full
	// rationale.
	ready bool

	// persistFn, if non-nil, is invoked inside apply() to persist
	// the post-mutation snapshot. See logStore.persistFn for
	// semantics.
	persistFn func([]readcacheassignment.LogEntry) error
	logger    log.Logger
}

type readcacheSubscription struct {
	ch chan readcacheUpdate
	// wantsDeltas / primed mirror subscription; see there.
	wantsDeltas bool
	primed      bool
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
func (s *readcacheLogStore) apply(at time.Time, next *readcacheassignment.Assignment, leaseDuration, lookahead, retention, safetyWindow time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := s.log.Apply(at, next, leaseDuration, lookahead, safetyWindow)
	if retention > 0 {
		s.log.Prune(at.Add(-retention))
	}
	// becameReady primes subscribers attached before the first apply
	// even if this apply makes no log change. See logStore.apply for
	// the full rationale.
	becameReady := !s.ready
	s.ready = true
	if !changed && !becameReady {
		return false
	}
	// Full retention-bounded state, history included: the
	// distributor's read path needs expired leases to resolve which
	// readcache holds a frozen slice from before a partition move.
	// Delta subscribers get only this apply's mutations; see
	// logStore.apply for the broadcast-under-mutex rationale.
	full := s.log.Entries()
	delta := diffReadcacheEntries(s.lastBroadcast, full)
	s.lastBroadcast = full
	if retention > 0 {
		s.lastPruneBefore = at.Add(-retention)
	}
	if changed && s.persistFn != nil {
		if err := s.persistFn(full); err != nil && s.logger != nil {
			level.Error(s.logger).Log("msg", "failed to persist readcache assignment log", "err", err)
		}
	}

	for sub := range s.subscribers {
		switch {
		case !sub.wantsDeltas || !sub.primed:
			sub.primed = true
			conflateSendReadcache(sub.ch, readcacheUpdate{entries: full, reset: true, pruneBefore: s.lastPruneBefore, replicaMap: s.replicaMap})
		case len(delta) == 0:
		default:
			conflateSendReadcache(sub.ch, readcacheUpdate{entries: delta, pruneBefore: s.lastPruneBefore, replicaMap: s.replicaMap})
		}
	}
	return changed
}

// setReplicaMap installs m as the current logical->concrete expansion.
// When the map changes, every subscriber is re-primed with a full
// snapshot carrying the new map: a replica-map change (a zone pod
// joining or leaving) does not necessarily change any lease, so
// piggy-backing on apply() alone would leave clients dialing stale
// concrete pods until the next lease rotation.
//
// The rebroadcast is skipped until the store is ready (first apply),
// for the same reason subscribe() withholds its initial snapshot: a
// freshly-restarted rebalancer must not publish a pre-apply view.
func (s *readcacheLogStore) setReplicaMap(m readcacheassignment.ReplicaMap) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.replicaMap.Equal(m) {
		return
	}
	s.replicaMap = m.Clone()
	if !s.ready {
		return
	}
	full := s.log.Entries()
	s.lastBroadcast = full
	for sub := range s.subscribers {
		sub.primed = true
		conflateSendReadcache(sub.ch, readcacheUpdate{entries: full, reset: true, pruneBefore: s.lastPruneBefore, replicaMap: s.replicaMap})
	}
}

// getReplicaMap returns the current logical->concrete expansion. The
// returned map is the store's own instance and must not be mutated;
// setReplicaMap always replaces it wholesale.
func (s *readcacheLogStore) getReplicaMap() readcacheassignment.ReplicaMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicaMap
}

// diffReadcacheEntries mirrors diffAssignmentEntries for the
// readcache log's (PartitionID, InstanceID, From) lease identity.
func diffReadcacheEntries(prev, cur []readcacheassignment.LogEntry) []readcacheassignment.LogEntry {
	type key struct {
		pid      int32
		instance string
		fromMs   int64
	}
	prevTo := make(map[key]time.Time, len(prev))
	for _, e := range prev {
		prevTo[key{pid: e.PartitionID, instance: e.InstanceID, fromMs: e.From.UnixMilli()}] = e.To
	}
	var out []readcacheassignment.LogEntry
	for _, e := range cur {
		if to, ok := prevTo[key{pid: e.PartitionID, instance: e.InstanceID, fromMs: e.From.UnixMilli()}]; !ok || !to.Equal(e.To) {
			out = append(out, e)
		}
	}
	return out
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

// subscribe registers a new watcher. Same semantics as
// logStore.subscribe, including the !s.ready -> nil initial gate
// that prevents a freshly-restarted rebalancer from broadcasting a
// stale-but-expired persisted log as the authoritative "you own
// nothing" snapshot. See logStore.subscribe for the full rationale.
func (s *readcacheLogStore) subscribe(wantsDeltas bool) (initial *readcacheUpdate, updates <-chan readcacheUpdate, unsubscribe func()) {
	sub := &readcacheSubscription{ch: make(chan readcacheUpdate, 1), wantsDeltas: wantsDeltas}
	s.mu.Lock()
	if s.ready {
		sub.primed = true
		initial = &readcacheUpdate{entries: s.log.Entries(), reset: true, pruneBefore: s.lastPruneBefore, replicaMap: s.replicaMap}
	}
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

// conflateSendReadcache mirrors conflateSendUpdate; see there for the
// coalescing rules.
func conflateSendReadcache(ch chan readcacheUpdate, u readcacheUpdate) {
	for {
		select {
		case ch <- u:
			return
		default:
			select {
			case pending := <-ch:
				u = coalesceReadcacheUpdates(pending, u)
			default:
			}
		}
	}
}

func coalesceReadcacheUpdates(pending, next readcacheUpdate) readcacheUpdate {
	if next.reset {
		return next
	}
	merged := readcacheassignment.NewLogFromEntries(pending.entries).MergedWithEntries(next.entries)
	pruneBefore := next.pruneBefore
	if pruneBefore.IsZero() {
		pruneBefore = pending.pruneBefore
	}
	return readcacheUpdate{
		entries:     merged.Entries(),
		reset:       pending.reset,
		pruneBefore: pruneBefore,
		// Every update carries the store's map as of when it was
		// built, so the later one is always at least as fresh —
		// including the transition back to nil (RF=1).
		replicaMap: next.replicaMap,
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

// ReplicaMapToProto converts a ReplicaMap into its wire form. Logical
// IDs are emitted in sorted order so the encoded message is stable
// across broadcasts (the map itself has no ordering). Returns nil for
// an empty map, which the wire contract reads as identity (RF=1).
func ReplicaMapToProto(m readcacheassignment.ReplicaMap) []ReadcacheReplicaSet {
	if len(m) == 0 {
		return nil
	}
	out := make([]ReadcacheReplicaSet, 0, len(m))
	for _, logical := range m.SortedLogicalIDs() {
		reps := m[logical]
		set := ReadcacheReplicaSet{LogicalId: logical}
		if len(reps) > 0 {
			set.Replicas = make([]ReadcacheReplica, len(reps))
			for i, rep := range reps {
				set.Replicas[i] = ReadcacheReplica{InstanceId: rep.InstanceID, Zone: rep.Zone}
			}
		}
		out = append(out, set)
	}
	return out
}

// ReplicaMapFromProto converts wire replica sets back into a
// ReplicaMap. Returns nil for an empty slice so consumers keep the
// identity behaviour.
func ReplicaMapFromProto(sets []ReadcacheReplicaSet) readcacheassignment.ReplicaMap {
	if len(sets) == 0 {
		return nil
	}
	out := make(readcacheassignment.ReplicaMap, len(sets))
	for _, set := range sets {
		if set.LogicalId == "" {
			continue
		}
		var reps []readcacheassignment.Replica
		if len(set.Replicas) > 0 {
			reps = make([]readcacheassignment.Replica, len(set.Replicas))
			for i, rep := range set.Replicas {
				reps[i] = readcacheassignment.Replica{InstanceID: rep.InstanceId, Zone: rep.Zone}
			}
		}
		out[set.LogicalId] = reps
	}
	if len(out) == 0 {
		return nil
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
