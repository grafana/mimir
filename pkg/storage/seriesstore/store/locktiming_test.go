// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingLockObserver struct {
	mu     sync.Mutex
	writes map[string]int
	waited map[string]time.Duration
	reads  map[string]int
}

func (o *recordingLockObserver) ObserveWrite(phase string, waited, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.writes[phase]++
	o.waited[phase] += waited
}

func (o *recordingLockObserver) ObserveRead(phase string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reads[phase]++
}

func TestShardLocksAreObservedByWhatHoldsThem(t *testing.T) {
	observer := &recordingLockObserver{writes: map[string]int{}, waited: map[string]time.Duration{}, reads: map[string]int{}}
	SetLockObserver(observer)
	t.Cleanup(func() { SetLockObserver(nil) })
	totals := func() (holds map[string]int64, waited map[string]time.Duration) {
		holds, waited = map[string]int64{}, map[string]time.Duration{}
		LockTotals(func(phase string, count int64, _, wait time.Duration) { holds[phase], waited[phase] = count, wait })
		return holds, waited
	}
	holdsBefore, waitedBefore := totals()

	var shard shardState
	shard.Lock()
	shard.as(phaseFreeze)
	shard.Unlock()
	// A site that doesn't name its phase.
	shard.Lock()
	shard.Unlock()
	require.True(t, shard.TryLock())
	shard.as(phasePush)
	shard.Unlock()

	// A writer waiting on a reader is told how long it waited.
	shard.RLock()
	locked := make(chan struct{})
	go func() {
		shard.Lock()
		shard.as(phasePush)
		shard.Unlock()
		close(locked)
	}()
	time.Sleep(20 * time.Millisecond)
	shard.RUnlock()
	<-locked

	// The totals have every hold, by phase.
	holds, waited := totals()
	for phase, want := range map[string]int64{"freeze": 1, "other": 1, "push": 2} {
		require.Equal(t, want, holds[phase]-holdsBefore[phase], phase)
	}
	require.GreaterOrEqual(t, waited["push"]-waitedBefore["push"], 10*time.Millisecond)

	// The observer has one write hold in writeSampleEvery, counting the ones before.
	for range 4*writeSampleEvery - 4 {
		shard.Lock()
		shard.as(phasePush)
		shard.Unlock()
	}
	require.Equal(t, map[string]int{"push": 4}, observer.writes)

	// One read in readSampleEvery.
	for range 4 * readSampleEvery {
		timer := startReadTimer(phaseSelect)
		shard.RLock()
		at := timer.held()
		timer.release(at)
		shard.RUnlock()
	}
	require.Equal(t, map[string]int{"select": 4}, observer.reads)

	// Without an observer, nothing is recorded.
	SetLockObserver(nil)
	holdsBefore, _ = totals()
	for range writeSampleEvery {
		shard.Lock()
		shard.as(phaseFreeze)
		shard.Unlock()
	}
	holds, _ = totals()
	require.Equal(t, holdsBefore["freeze"], holds["freeze"])
	require.Equal(t, map[string]int{"push": 4}, observer.writes)
}
