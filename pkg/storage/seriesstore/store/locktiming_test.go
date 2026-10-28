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

	require.Equal(t, map[string]int{"freeze": 1, "other": 1, "push": 2}, observer.writes)
	require.GreaterOrEqual(t, observer.waited["push"], 10*time.Millisecond)

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
	shard.Lock()
	shard.as(phaseFreeze)
	shard.Unlock()
	require.Equal(t, 1, observer.writes["freeze"])
}
