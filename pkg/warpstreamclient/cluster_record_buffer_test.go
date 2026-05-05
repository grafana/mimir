// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fixedAgentResolver routes every record to the same agent NodeID.
func fixedAgentResolver(nodeID int32) AgentResolver {
	return func(string, int32) (int32, bool) { return nodeID, true }
}

func TestClusterRecordBuffer_Add(t *testing.T) {
	t.Run("single agent: routes through and flushes via linger", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(20*time.Millisecond, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		require.Equal(t, 1, flush.callCount())
		assert.Equal(t, int32(1), flush.snapshot()[0].nodeID)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("two agents flush independently into separate batches", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 2,
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(20*time.Millisecond, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}

		calls := flush.snapshot()
		require.Len(t, calls, 2)
		nodeIDs := []int32{calls[0].nodeID, calls[1].nodeID}
		assert.ElementsMatch(t, []int32{1, 2}, nodeIDs)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("multi-partition records to same agent batch into one flush", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 1, // both partitions go to the same agent
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(20*time.Millisecond, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}

		calls := flush.snapshot()
		require.Len(t, calls, 1, "records to the same agent should batch into one flush")
		assert.Equal(t, int32(1), calls[0].nodeID)
		assert.Len(t, calls[0].records, 2)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("done called once even when records span two agents", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 2,
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(10*time.Millisecond, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		var fires atomic.Int32
		ch := make(chan struct{}, 1)
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(error) {
			fires.Add(1)
			select {
			case ch <- struct{}{}:
			default:
			}
		})

		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		// done must not fire a second time.
		require.Never(t, func() bool { return fires.Load() > 1 },
			100*time.Millisecond, 10*time.Millisecond)
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("done returns first error when one of two agents fails", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 2,
			},
		}
		flush := newRecordingFlush()
		boom := errors.New("agent 2 failed")
		flush.onFlush = func(nodeID int32, _ []*kgo.Record) error {
			if nodeID == 2 {
				return boom
			}
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(10*time.Millisecond, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, boom)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("unresolved record fails the whole add synchronously", func(t *testing.T) {
		flush := newRecordingFlush()
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{{"t", 0}: 1},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.Error(t, err)
			assert.ErrorContains(t, err, "no agent assigned")
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("empty records: done fires synchronously with nil", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(context.Background(), nil, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("done did not fire for empty Add")
		}
		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("add after close fails fast", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		c.Close()

		done := make(chan error, 1)
		c.Add(context.Background(), []*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, errBufferClosed)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Add on closed cluster")
		}
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("context already canceled: done fires synchronously with ctx error", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan error, 1)
		c.Add(ctx, []*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("done did not fire for pre-canceled ctx")
		}
		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})

	t.Run("context canceled mid-flight: done fires with ctx error but records still flush", func(t *testing.T) {
		flush := newRecordingFlush()
		release := make(chan struct{})
		flush.onFlush = func(int32, []*kgo.Record) error {
			<-release
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(10*time.Millisecond, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		ctx, cancel := context.WithCancel(context.Background())
		const value = "v"
		done := make(chan error, 1)
		c.Add(ctx, []*kgo.Record{makeRecord("t", 0, value)}, func(err error) { done <- err })

		// Wait for the linger to fire and the flush goroutine to enter onFlush.
		require.Eventually(t, func() bool { return flush.callCount() == 1 },
			time.Second, 10*time.Millisecond)

		// While the flush is held, the bytes are accounted as in-flight.
		assert.Equal(t, int64(len(value)), c.BufferedBytes(),
			"records held by the flush must be reflected as in-flight bytes")
		assert.Equal(t, int64(1), c.BufferedRecords())

		// Cancel while the flush is still blocked.
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after ctx cancel")
		}

		// ctx-cancel detaches the caller, but the records are still being
		// produced — bufferedBytes / bufferedRecords stay non-zero until the
		// actual flush completes. This matches franz-go's
		// BufferedProduceBytes / BufferedProduceRecords semantics.
		assert.Equal(t, int64(len(value)), c.BufferedBytes())
		assert.Equal(t, int64(1), c.BufferedRecords())

		// Release the flush; the records still get produced even though the
		// caller already gave up. Bookkeeping drains once the flush reports.
		close(release)
		require.Eventually(t, func() bool { return c.BufferedBytes() == 0 },
			time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool { return c.BufferedRecords() == 0 },
			time.Second, 10*time.Millisecond)
	})

	t.Run("multi-agent: ctx canceled mid-flight fires done exactly once with ctx error", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 2,
			},
		}
		flush := newRecordingFlush()
		// Hold both agents' flushes so we can cancel before either completes.
		release := make(chan struct{})
		flush.onFlush = func(int32, []*kgo.Record) error {
			<-release
			return nil
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(10*time.Millisecond, 1<<20, strat.Primary, flush.Func(), m)
		t.Cleanup(c.Close)

		ctx, cancel := context.WithCancel(context.Background())
		var fires atomic.Int32
		done := make(chan error, 1)
		const valueA, valueB = "aa", "bb"
		c.Add(ctx, []*kgo.Record{
			makeRecord("t", 0, valueA),
			makeRecord("t", 1, valueB),
		}, func(err error) {
			fires.Add(1)
			done <- err
		})

		// Wait for both agent flushes to enter onFlush (i.e., both linger
		// timers fired and dispatched).
		require.Eventually(t, func() bool { return flush.callCount() == 2 },
			time.Second, 10*time.Millisecond)

		// Both agents' bytes are accounted as in-flight.
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes(),
			"both agents' records counted as in-flight bytes")
		assert.Equal(t, int64(2), c.BufferedRecords())

		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after ctx cancel")
		}

		// ctx-cancel detaches the caller; both agents' records remain in the
		// producer until their flushes actually complete.
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes())
		assert.Equal(t, int64(2), c.BufferedRecords())

		// Releasing both flushes invokes reportResult on both agents; neither
		// must double-fire done. Also exercises the firstErr fan-in path:
		// once.Do guarantees the late reports are no-ops.
		close(release)
		require.Never(t, func() bool { return fires.Load() > 1 },
			100*time.Millisecond, 10*time.Millisecond)
		require.Eventually(t, func() bool { return c.BufferedBytes() == 0 },
			time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool { return c.BufferedRecords() == 0 },
			time.Second, 10*time.Millisecond)
	})

	t.Run("context canceled after success is a no-op", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(10*time.Millisecond, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		ctx, cancel := context.WithCancel(context.Background())
		var fires atomic.Int32
		var observedErr atomic.Pointer[error]
		c.Add(ctx, []*kgo.Record{makeRecord("t", 0, "v")}, func(err error) {
			fires.Add(1)
			observedErr.Store(&err)
		})

		require.Eventually(t, func() bool { return fires.Load() == 1 },
			time.Second, 10*time.Millisecond, "done did not fire on flush success")
		// Success drained bufferedBytes; the late ctx-cancel must not touch it.
		assert.Equal(t, int64(0), c.BufferedBytes())

		// Cancel after the success has already fired done; the watcher should
		// have been detached, so no second firing occurs.
		cancel()
		require.Never(t, func() bool { return fires.Load() > 1 },
			100*time.Millisecond, 10*time.Millisecond)
		require.NotNil(t, observedErr.Load())
		assert.NoError(t, *observedErr.Load())
		assert.Equal(t, int64(0), c.BufferedBytes(),
			"late ctx-cancel must not double-decrement bufferedBytes")
		assert.Equal(t, int64(0), c.BufferedRecords(),
			"late ctx-cancel must not double-decrement bufferedRecords")
	})
}

func TestClusterRecordBuffer_Close(t *testing.T) {
	t.Run("idempotent", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)

		c.Close()
		c.Close() // must not panic or hang
	})

	t.Run("flushes pending in every per-agent buffer", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{"t", 0}: 1,
				{"t", 1}: 2,
			},
		}
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, strat.Primary, flush.Func(), m)

		done := make(chan error, 1)
		const valueA, valueB = "a", "b"
		c.Add(context.Background(), []*kgo.Record{
			makeRecord("t", 0, valueA),
			makeRecord("t", 1, valueB),
		}, func(err error) { done <- err })

		assert.Equal(t, 0, flush.callCount())
		assert.Equal(t, int64(len(valueA)+len(valueB)), c.BufferedBytes(),
			"records buffered before Close are accounted")
		assert.Equal(t, int64(2), c.BufferedRecords())

		c.Close()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Close")
		}
		assert.Equal(t, 2, flush.callCount())
		assert.Equal(t, int64(0), c.BufferedBytes())
		assert.Equal(t, int64(0), c.BufferedRecords())
	})
}
