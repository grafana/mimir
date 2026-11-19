// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
)

// errOffsetMarkerWriterStopped is the cancellation cause of a write abandoned by stop.
var errOffsetMarkerWriterStopped = errors.New("offset marker writer stopped")

const (
	// offsetMarkerFinalWriteTimeout bounds the last marker write at shutdown.
	offsetMarkerFinalWriteTimeout = 10 * time.Second

	offsetMarkerMinBackoff = time.Second
	offsetMarkerMaxBackoff = time.Minute
)

// markerSnapshot is the committed offsets of every cluster, taken under one lock so that a job spanning
// clusters is never recorded half-applied, and the clusters' Kafka identities.
type markerSnapshot struct {
	offsets markerOffsets
	ids     []clusterIdentity
}

// offsetMarkerWriter writes the offset marker to object storage from a background goroutine, so that the
// scheduling loop and the Kafka commits never wait for object storage.
//
// Callers hand it the latest snapshot with submit and carry on. A snapshot that hasn't been written yet is
// replaced by a newer one, so snapshots never queue. After a failed write it backs off, and then tries again
// with the latest snapshot.
type offsetMarkerWriter struct {
	store   *offsetMarkerStore
	topic   string
	group   string
	refresh time.Duration
	logger  log.Logger
	metrics *schedulerMetrics

	// onBase, if set, is called from the writer's goroutine with the marker found in object storage
	// when the writer has read it, which is nil if there is none.
	onBase func(*offsetMarker)

	ctx    context.Context
	cancel context.CancelCauseFunc
	ready  chan struct{}
	wake   chan struct{}
	stopCh chan struct{}
	done   chan struct{}

	startOnce, readyOnce, stopOnce sync.Once

	mu           sync.Mutex
	pending      *markerSnapshot
	pendingForce bool
	busy         bool
	baseLoaded   bool
	base         *offsetMarker // The last marker read or written, nil if there is none.
	finalTimeout time.Duration

	// minBackoff and maxBackoff pace retries after failures, and reads of the base marker.
	minBackoff, maxBackoff time.Duration

	// Only used by the writer's goroutine.
	gate        markerWriteGate
	failBackoff time.Duration
}

func newOffsetMarkerWriter(store *offsetMarkerStore, topic, group string, refresh time.Duration, metrics *schedulerMetrics, logger log.Logger) *offsetMarkerWriter {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &offsetMarkerWriter{
		store:   store,
		topic:   topic,
		group:   group,
		refresh: refresh,
		logger:  logger,
		metrics: metrics,
		ctx:     ctx,
		cancel:  cancel,
		ready:   make(chan struct{}),
		wake:    make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
		gate:    markerWriteGate{refreshInterval: refresh},

		minBackoff: offsetMarkerMinBackoff,
		maxBackoff: offsetMarkerMaxBackoff,

		finalTimeout: offsetMarkerFinalWriteTimeout,
	}
}

// start starts the goroutine. It does nothing until release is called.
func (w *offsetMarkerWriter) start() {
	w.startOnce.Do(func() { go w.run() })
}

// release lets the writer start reading the base marker and writing. The scheduler calls it once
// its startup offsets are loaded.
func (w *offsetMarkerWriter) release() {
	w.readyOnce.Do(func() { close(w.ready) })
}

// submit hands the writer the latest snapshot. It never blocks. A snapshot not written yet is replaced.
// force makes the writer write it even if nothing changed.
func (w *offsetMarkerWriter) submit(snap markerSnapshot, force bool) {
	w.mu.Lock()
	w.pending = &snap
	w.pendingForce = w.pendingForce || force
	w.mu.Unlock()

	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// idle returns true if there is nothing waiting to be written and no write in flight.
func (w *offsetMarkerWriter) idle() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pending == nil && !w.busy && w.baseLoaded
}

// stop makes one last write of the latest snapshot, if any, within timeout, and then stops the goroutine.
// A write in flight is abandoned. It is safe to call more than once.
func (w *offsetMarkerWriter) stop(timeout time.Duration) {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.finalTimeout = timeout
		w.mu.Unlock()
		close(w.stopCh)
		w.cancel(errOffsetMarkerWriterStopped)
	})
	// If start was never called, there is no goroutine to wait for.
	started := true
	w.startOnce.Do(func() { started = false })
	if started {
		<-w.done
	}
}

func (w *offsetMarkerWriter) run() {
	defer close(w.done)

	select {
	case <-w.ready:
	case <-w.stopCh:
		return
	}

	if !w.loadBase() {
		return
	}

	for {
		select {
		case <-w.wake:
		case <-w.stopCh:
			w.finalWrite()
			return
		}
		if w.stopped() {
			w.finalWrite()
			return
		}
		w.drain()
	}
}

func (w *offsetMarkerWriter) stopped() bool {
	select {
	case <-w.stopCh:
		return true
	default:
		return false
	}
}

// loadBase reads the marker in object storage, retrying with backoff. It returns false if the writer
// was stopped first.
func (w *offsetMarkerWriter) loadBase() bool {
	boff := backoff.New(w.ctx, backoff.Config{MinBackoff: w.minBackoff, MaxBackoff: w.maxBackoff})
	for boff.Ongoing() {
		m, err := w.store.read(w.ctx)
		if err == nil && m != nil {
			err = validateMarkerIdentity(m, w.topic, w.group)
		}
		if err == nil {
			w.mu.Lock()
			w.base = m
			w.baseLoaded = true
			w.mu.Unlock()
			if w.onBase != nil {
				w.onBase(m)
			}
			return true
		}
		if w.ctx.Err() != nil {
			return false
		}
		w.metrics.offsetMarkerReadFailures.Inc()
		level.Warn(w.logger).Log("msg", "failed to read offset marker; offset marker writes are paused until it can be read", "err", err)
		boff.Wait()
	}
	return false
}

// take removes and returns the pending snapshot.
func (w *offsetMarkerWriter) take() (*markerSnapshot, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	snap, force := w.pending, w.pendingForce
	w.pending, w.pendingForce = nil, false
	w.busy = snap != nil
	return snap, force
}

// drain writes pending snapshots until there are none, or the writer is stopped.
func (w *offsetMarkerWriter) drain() {
	for {
		if w.stopped() {
			return // The final write takes the pending snapshot.
		}
		snap, force := w.take()
		if snap == nil {
			return
		}
		err := w.write(w.ctx, *snap, force)

		w.mu.Lock()
		w.busy = false
		if err != nil && w.pending == nil {
			// Try again with the same snapshot, after backing off, unless a newer one arrives. If the writer
			// is stopping, it is the final write that does.
			w.pending, w.pendingForce = snap, force
		}
		w.mu.Unlock()

		if err == nil {
			w.failBackoff = 0
			continue
		}
		if w.ctx.Err() != nil {
			return
		}
		w.failBackoff = min(max(2*w.failBackoff, w.minBackoff), w.maxBackoff)
		select {
		case <-time.After(w.failBackoff):
		case <-w.stopCh:
			return
		}
	}
}

// finalWrite makes the shutdown write, bounded by the final timeout.
func (w *offsetMarkerWriter) finalWrite() {
	w.mu.Lock()
	timeout := w.finalTimeout
	snap, force := w.pending, w.pendingForce
	w.pending, w.pendingForce = nil, false
	w.mu.Unlock()
	if snap == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := w.write(ctx, *snap, force); err != nil {
		level.Warn(w.logger).Log("msg", "failed to write the offset marker at shutdown", "err", err)
	}
}

// write writes the snapshot unless there is nothing to write, or nothing has changed. Failures are
// counted and logged.
func (w *offsetMarkerWriter) write(ctx context.Context, snap markerSnapshot, force bool) error {
	// Never overwrite the marker with an empty one: that's the state before startup has loaded anything
	// (e.g. a shutdown during startup), and the marker is the safety net.
	if snap.offsets.empty() {
		return nil
	}

	w.mu.Lock()
	loaded, base := w.baseLoaded, w.base
	w.mu.Unlock()
	if !loaded {
		// Not an error: writes are paused until the base marker is read. This only happens at shutdown.
		return nil
	}

	merged := mergeMarkerOffsets(snap.offsets, base, snap.ids)
	now := time.Now()
	if !w.gate.shouldWrite(merged, now, force) {
		return nil
	}

	m := newOffsetMarker(w.topic, w.group, merged, snap.ids, now)
	if err := w.store.write(ctx, m); err != nil {
		if w.ctx.Err() == nil || ctx != w.ctx {
			w.metrics.offsetMarkerWriteFailures.Inc()
			level.Warn(w.logger).Log("msg", "failed to write offset marker; it is retried and doesn't affect scheduling", "err", err)
		}
		return fmt.Errorf("write offset marker: %w", err)
	}

	w.gate.recordWrite(merged, now)
	w.mu.Lock()
	w.base = m
	w.mu.Unlock()
	w.metrics.offsetMarkerWrites.Inc()
	w.metrics.offsetMarkerLastWrite.SetToCurrentTime()
	return nil
}
