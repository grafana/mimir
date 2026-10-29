// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// LockObserver is told how long the shards' locks are waited for and held, by what holds them: pushes that wait on a
// shard stall the partition's ingestion, and the reads with strong consistency behind it.
type LockObserver interface {
	// ObserveWrite is a write lock's wait and hold, by the phase that held it, for a sample of the holds: LockTotals has
	// them all.
	ObserveWrite(phase string, waited, held time.Duration)
	// ObserveRead is a read lock's hold, by the phase that held it, for a sample of the reads.
	ObserveRead(phase string, held time.Duration)
}

var lockObserver atomic.Pointer[LockObserver]

// SetLockObserver has the shards' lock holds observed, or not when observer is nil.
func SetLockObserver(observer LockObserver) {
	if observer == nil {
		lockObserver.Store(nil)
		return
	}
	lockObserver.Store(&observer)
}

// lockPhase is what holds a shard's lock.
type lockPhase uint8

const (
	phaseOther lockPhase = iota
	phasePush
	phaseAppenderCommit
	phaseSeriesCreate
	phaseActiveSeries
	phaseCost
	phaseHeadTick
	phaseFreeze
	phaseCompactionScan
	phaseCompactSelected
	phasePrune
	phaseColdMerge
	phaseOOOCompaction
	phaseShrinkEntries
	phaseHeadGC
	phasePostingsInstall
	phaseSelect
	phaseLabels
	phaseCardinality
	phaseSnapshot
)

var lockPhaseNames = [...]string{
	phaseOther:           "other",
	phasePush:            "push",
	phaseAppenderCommit:  "appender_commit",
	phaseSeriesCreate:    "series_create",
	phaseActiveSeries:    "active_series",
	phaseCost:            "cost_attribution",
	phaseHeadTick:        "head_tick",
	phaseFreeze:          "freeze",
	phaseCompactionScan:  "compaction_scan",
	phaseCompactSelected: "compact_selected",
	phasePrune:           "retention",
	phaseColdMerge:       "cold_merge",
	phaseOOOCompaction:   "ooo_compaction",
	phaseShrinkEntries:   "shrink_entries",
	phaseHeadGC:          "head_gc",
	phasePostingsInstall: "postings_install",
	phaseSelect:          "select",
	phaseLabels:          "labels",
	phaseCardinality:     "cardinality",
	phaseSnapshot:        "snapshot",
}

// timedRWMutex is a shard's lock, whose write holds are observed when an observer is set. Its holder names its phase
// with as right after locking: a write lock is held by one goroutine, so the fields need no synchronization of their
// own. A site that doesn't name its phase is observed as other.
type timedRWMutex struct {
	sync.RWMutex
	// Since when the write lock is held, zero when it isn't observed, how long it was waited for, and by what.
	writeAt     time.Time
	writeWaited time.Duration
	writePhase  lockPhase
	// How many write holds were timed, which picks the sample the observer observes.
	writes uint32
}

func (m *timedRWMutex) Lock() {
	if lockObserver.Load() == nil {
		m.RWMutex.Lock()
		return
	}
	start := time.Now()
	m.RWMutex.Lock()
	now := time.Now()
	m.writeAt, m.writeWaited, m.writePhase = now, now.Sub(start), phaseOther
}

func (m *timedRWMutex) TryLock() bool {
	if !m.RWMutex.TryLock() {
		return false
	}
	if lockObserver.Load() != nil {
		m.writeAt, m.writeWaited, m.writePhase = time.Now(), 0, phaseOther
	}
	return true
}

func (m *timedRWMutex) Unlock() {
	if !m.writeAt.IsZero() {
		held, waited, phase := time.Since(m.writeAt), m.writeWaited, m.writePhase
		m.writeAt = time.Time{}
		totals := &lockTotals[(uintptr(unsafe.Pointer(m))>>6)%lockTotalStripes][phase]
		totals.held.Add(int64(held))
		totals.waited.Add(int64(waited))
		totals.holds.Add(1)
		// Under the write lock, so the count needs no synchronization.
		if m.writes++; m.writes%writeSampleEvery == 0 {
			if observer := lockObserver.Load(); observer != nil {
				(*observer).ObserveWrite(lockPhaseNames[phase], waited, held)
			}
		}
	}
	m.RWMutex.Unlock()
}

// writeSampleEvery is how many write holds one is observed of by the observer's histograms: observing every hold of every
// push was about 2% of a busy ingester's CPU, mostly the histograms' atomic adds that all the shards' holders share.
const writeSampleEvery = 64

// lockTotalStripes is how many sets of totals write holds add to, picked by the lock: a shard's holders add to their own,
// which other shards' rarely share.
const lockTotalStripes = 64

type phaseTotals struct {
	held, waited, holds atomic.Int64
}

var lockTotals [lockTotalStripes][len(lockPhaseNames)]phaseTotals

// LockTotals calls visit with each phase's write holds of the shards' locks since the process started: how many there
// were, and how long they held and waited for the locks.
func LockTotals(visit func(phase string, holds int64, held, waited time.Duration)) {
	for phase, name := range lockPhaseNames {
		var holds, held, waited int64
		for stripe := range lockTotals {
			totals := &lockTotals[stripe][phase]
			holds, held, waited = holds+totals.holds.Load(), held+totals.held.Load(), waited+totals.waited.Load()
		}
		if holds > 0 {
			visit(name, holds, time.Duration(held), time.Duration(waited))
		}
	}
}

// as names what holds the write lock. Callers hold it.
func (m *timedRWMutex) as(phase lockPhase) {
	m.writePhase = phase
}

// readSampleEvery is how many reads one is observed of: a read's lock is taken once per shard, which timing each would
// add a few percent to the smallest ones.
const readSampleEvery = 64

var readSamples atomic.Uint64

// readTimer times a read's holds of the shards' read locks, when it is one of the sample.
type readTimer struct {
	phase lockPhase
	on    bool
}

func startReadTimer(phase lockPhase) readTimer {
	if lockObserver.Load() == nil {
		return readTimer{}
	}
	return readTimer{phase: phase, on: readSamples.Add(1)%readSampleEvery == 0}
}

// held returns when the read lock was taken, which release takes, for a sampled read.
func (r readTimer) held() time.Time {
	if !r.on {
		return time.Time{}
	}
	return time.Now()
}

// release observes the hold since at, before the caller unlocks.
func (r readTimer) release(at time.Time) {
	if !r.on || at.IsZero() {
		return
	}
	if observer := lockObserver.Load(); observer != nil {
		(*observer).ObserveRead(lockPhaseNames[r.phase], time.Since(at))
	}
}
