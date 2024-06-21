// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"sync"

	"github.com/grafana/dskit/services"
	"go.uber.org/atomic"
)

var (
	errPartitionOffsetWatcherStopped = errors.New("partition offset watcher is stopped")
)

type partitionOffsetWatcher struct {
	services.Service

	// mx protects the following fields.
	mx sync.Mutex

	// lastConsumedOffset holds the last consumed offset or -1 if nothing has been consumed yet.
	// We don't worry about exhausting the int64 space because it seems implausible.
	lastConsumedOffset int64

	// watchGroups is a map where the key is the offset a group of goroutines is waiting for.
	//
	// Why a map? Because what goroutines will wait for is the "last produced offset". Since we poll it
	// periodically, we expect at any given time we have many goroutines waiting on few different offsets.
	watchGroups map[int64]*partitionOffsetWatchGroup

	// stopped holds whether the watcher has been stopped.
	stopped bool
}

func newPartitionOffsetWatcher() *partitionOffsetWatcher {
	w := &partitionOffsetWatcher{
		lastConsumedOffset: -1, // -1 means nothing has been consumed yet.
		watchGroups:        map[int64]*partitionOffsetWatchGroup{},
	}

	w.Service = services.NewIdleService(nil, w.stopping)

	return w
}

func (w *partitionOffsetWatcher) stopping(_ error) error {
	w.mx.Lock()
	defer w.mx.Unlock()

	// Ensure no other goroutine will Notify() or Wait() once stopped.
	w.stopped = true

	// Release all waiting goroutines.
	for waitForOffset, watchGroup := range w.watchGroups {
		delete(w.watchGroups, waitForOffset)
		watchGroup.releaseWaiting(errPartitionOffsetWatcherStopped)
	}

	return nil
}

// Notify that a given offset has been consumed. This function is expected to run very quickly
// and never block in practice.
func (w *partitionOffsetWatcher) Notify(lastConsumedOffset int64) {
	w.mx.Lock()
	defer w.mx.Unlock()

	// Ensure the service has not been stopped.
	if w.stopped {
		return
	}

	// Ensure the input offset is greater than the previous one.
	if lastConsumedOffset < w.lastConsumedOffset {
		return
	}

	w.lastConsumedOffset = lastConsumedOffset

	// Wake up all goroutines waiting for an offset which has been consumed.
	for waitForOffset, watchGroup := range w.watchGroups {
		if waitForOffset <= w.lastConsumedOffset {
			delete(w.watchGroups, waitForOffset)
			watchGroup.releaseWaiting(nil)
		}
	}
}

// Wait until the given offset has been consumed or the context is canceled.
func (w *partitionOffsetWatcher) Wait(ctx context.Context, waitForOffset int64) error {
	// A negative offset is used to signal the partition is empty,
	// so we can immediately return.
	if waitForOffset < 0 {
		return nil
	}

	var watchGroup *partitionOffsetWatchGroup

	{
		w.mx.Lock()

		// Ensure the service has not been stopped.
		if w.stopped {
			w.mx.Unlock()
			return errPartitionOffsetWatcherStopped
		}

		// Check if the condition is already met.
		if waitForOffset <= w.lastConsumedOffset {
			w.mx.Unlock()
			return nil
		}

		// Add to or create the watch group.
		var ok bool
		watchGroup, ok = w.watchGroups[waitForOffset]
		if !ok {
			watchGroup = &partitionOffsetWatchGroup{
				waitDone: make(chan struct{}),
				count:    atomic.NewInt64(1),
			}

			w.watchGroups[waitForOffset] = watchGroup
		} else {
			// To avoid race conditions with the garbage collection done when this counter drops to 0,
			// it's important to increase this counter while holding the lock (even if this counter
			// is an atomic).
			watchGroup.count.Inc()
		}

		w.mx.Unlock()
	}

	defer func() {
		if watchGroup.count.Dec() != 0 {
			return
		}

		// Garbage collection:
		//
		// If it was the last goroutine to wait in the group, then ensure the group is removed
		// from the map. This garbage collection is important to avoid accumulating entries
		// in the watchGroups map if consumption is stuck and Wait() goroutines stop
		// waiting because context has been canceled or their deadline expired.
		w.mx.Lock()

		// We need to check the count again while holding the lock to avoid race conditions.
		if actualWatchGroup, ok := w.watchGroups[waitForOffset]; ok && watchGroup == actualWatchGroup && actualWatchGroup.count.Load() == 0 {
			delete(w.watchGroups, waitForOffset)
		}

		w.mx.Unlock()
	}()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-watchGroup.waitDone:
		return watchGroup.waitErr
	}
}

// LastConsumedOffset returns the last consumed offset.
func (w *partitionOffsetWatcher) LastConsumedOffset() int64 {
	w.mx.Lock()
	defer w.mx.Unlock()

	return w.lastConsumedOffset
}

// waitingGoroutinesCount returns the number of active watch groups (an active group has at least
// 1 goroutine waiting). This function is useful for testing.
func (w *partitionOffsetWatcher) watchGroupsCount() int {
	w.mx.Lock()
	defer w.mx.Unlock()

	return len(w.watchGroups)
}

type partitionOffsetWatchGroup struct {
	// waitDone channel gets closed once the waiting goroutines should be released.
	waitDone chan struct{}

	// waitErr is the error to return to waiting goroutines once waitDone channel is closed.
	// It's safe to read this value without holding any lock after waitDone is closed.
	waitErr error

	// count is the number of waiting goroutines.
	count *atomic.Int64
}

func (g *partitionOffsetWatchGroup) releaseWaiting(err error) {
	g.waitErr = err
	close(g.waitDone)
}
