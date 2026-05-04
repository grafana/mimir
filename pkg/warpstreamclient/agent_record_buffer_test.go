// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordingFlush captures every flush invocation made by a buffer. onFlush,
// if set, is called from inside the FlushFunc and may block; its returned
// error is passed to done (used to inject failures or hold the flush to
// coordinate timing).
type recordingFlush struct {
	mu    sync.Mutex
	calls []recordingFlushCall

	onFlush func(nodeID int32, records []*kgo.Record) error
}

type recordingFlushCall struct {
	nodeID  int32
	records []*kgo.Record
}

func newRecordingFlush() *recordingFlush {
	return &recordingFlush{}
}

func (r *recordingFlush) Func() FlushFunc {
	return func(_ context.Context, nodeID int32, records []*kgo.Record, done func(error)) {
		r.mu.Lock()
		r.calls = append(r.calls, recordingFlushCall{nodeID: nodeID, records: records})
		r.mu.Unlock()
		var err error
		if r.onFlush != nil {
			err = r.onFlush(nodeID, records)
		}
		done(err)
	}
}

func (r *recordingFlush) snapshot() []recordingFlushCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordingFlushCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recordingFlush) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func makeRecord(topic string, partition int32, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
}

func TestAgentRecordBuffer_Add(t *testing.T) {
	t.Run("linger timer triggers flush", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, 20*time.Millisecond, 1<<20, flush.Func(), m)
		t.Cleanup(a.Close)

		done := make(chan error, 1)
		a.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done callback did not fire after linger expired")
		}
		require.Equal(t, 1, flush.callCount())
		assert.Equal(t, int32(1), flush.snapshot()[0].nodeID)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.lingerFlushesTotal))
	})

	t.Run("batch full triggers immediate flush before linger", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		// Small cap so the second Add's bytes overflow.
		a := NewAgentRecordBuffer(1, time.Hour, 100, flush.Func(), m)
		t.Cleanup(a.Close)

		first := makeRecord("t", 0, string(make([]byte, 50)))
		second := makeRecord("t", 0, string(make([]byte, 50)))

		done1 := make(chan error, 1)
		a.Add([]*kgo.Record{first}, func(err error) { done1 <- err })

		done2 := make(chan error, 1)
		a.Add([]*kgo.Record{second}, func(err error) { done2 <- err })

		require.Eventually(t, func() bool { return flush.callCount() >= 1 }, time.Second, 10*time.Millisecond,
			"overflow should flush immediately, before linger")

		select {
		case err := <-done1:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("first done did not fire after overflow flush")
		}

		a.Close()
		select {
		case err := <-done2:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("second done did not fire after Close")
		}
		assert.Equal(t, float64(2), testutil.ToFloat64(m.lingerFlushesTotal),
			"one overflow flush + one Close flush")
	})

	t.Run("close flushes pending batch", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(7, time.Hour, 1<<20, flush.Func(), m)

		done := make(chan error, 1)
		a.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		require.Equal(t, 0, flush.callCount(), "no flush expected before Close")
		a.Close()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Close")
		}
		require.Equal(t, 1, flush.callCount())
		assert.Equal(t, float64(1), testutil.ToFloat64(m.lingerFlushesTotal))
	})

	t.Run("done propagates flush error", func(t *testing.T) {
		flush := newRecordingFlush()
		boom := errors.New("boom")
		flush.onFlush = func(int32, []*kgo.Record) error { return boom }
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, 10*time.Millisecond, 1<<20, flush.Func(), m)
		t.Cleanup(a.Close)

		done := make(chan error, 1)
		a.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, boom)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		assert.Equal(t, float64(1), testutil.ToFloat64(m.lingerFlushesTotal),
			"flush metric counts every flush attempt regardless of outcome")
	})

	t.Run("empty records: cb fires synchronously with nil", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, time.Hour, 1<<20, flush.Func(), m)
		t.Cleanup(a.Close)

		done := make(chan error, 1)
		a.Add(nil, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("cb did not fire for empty Add")
		}
		assert.Equal(t, 0, flush.callCount(), "empty Add must not invoke flush")
		assert.Equal(t, float64(0), testutil.ToFloat64(m.lingerFlushesTotal))
	})

	t.Run("add after close fails fast", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, time.Hour, 1<<20, flush.Func(), m)
		a.Close()

		done := make(chan error, 1)
		a.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, errBufferClosed)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Add on closed buffer")
		}
		assert.Equal(t, float64(0), testutil.ToFloat64(m.lingerFlushesTotal),
			"rejected Add must not increment flush metric")
	})
}

func TestAgentRecordBuffer_Close(t *testing.T) {
	t.Run("idempotent", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, time.Hour, 1<<20, flush.Func(), m)

		a.Close()
		a.Close() // second call must not panic or hang
	})

	t.Run("waits for in-flight flush to complete", func(t *testing.T) {
		flush := newRecordingFlush()
		release := make(chan struct{})
		flush.onFlush = func(int32, []*kgo.Record) error {
			<-release
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		a := NewAgentRecordBuffer(1, 10*time.Millisecond, 1<<20, flush.Func(), m)

		done := make(chan error, 1)
		a.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		require.Eventually(t, func() bool { return flush.callCount() == 1 },
			time.Second, 10*time.Millisecond)

		closed := make(chan struct{})
		go func() {
			a.Close()
			close(closed)
		}()

		select {
		case <-closed:
			t.Fatal("Close returned before in-flight flush completed")
		case <-time.After(100 * time.Millisecond):
		}

		close(release)
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close did not return after flush completed")
		}
		<-done
	})
}
