// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
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
		c.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
		require.Equal(t, 1, flush.callCount())
		assert.Equal(t, int32(1), flush.snapshot()[0].nodeID)
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
		c.Add([]*kgo.Record{
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
		c.Add([]*kgo.Record{
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
		c.Add([]*kgo.Record{
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
		// Allow time for any spurious second firing.
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, int32(1), fires.Load())
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
		c.Add([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, boom)
		case <-time.After(time.Second):
			t.Fatal("done did not fire")
		}
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
		c.Add([]*kgo.Record{
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
		assert.Equal(t, 0, flush.callCount(), "no flush should run when any record is unresolved")
	})

	t.Run("empty records: done fires synchronously with nil", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		t.Cleanup(c.Close)

		done := make(chan error, 1)
		c.Add(nil, func(err error) { done <- err })

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("done did not fire for empty Add")
		}
		assert.Equal(t, 0, flush.callCount(), "empty Add must not invoke flush")
	})

	t.Run("add after close fails fast", func(t *testing.T) {
		flush := newRecordingFlush()
		m := newMetrics(prometheus.NewPedanticRegistry())
		c := NewClusterRecordBuffer(time.Hour, 1<<20, fixedAgentResolver(1), flush.Func(), m)
		c.Close()

		done := make(chan error, 1)
		c.Add([]*kgo.Record{makeRecord("t", 0, "v")}, func(err error) { done <- err })

		select {
		case err := <-done:
			require.ErrorIs(t, err, errBufferClosed)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Add on closed cluster")
		}
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
		c.Add([]*kgo.Record{
			makeRecord("t", 0, "a"),
			makeRecord("t", 1, "b"),
		}, func(err error) { done <- err })

		assert.Equal(t, 0, flush.callCount(), "no flush expected before Close")
		c.Close()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("done did not fire after Close")
		}
		assert.Equal(t, 2, flush.callCount(), "every agent's pending batch should flush on Close")
	})
}
