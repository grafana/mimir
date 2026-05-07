// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"bytes"
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

		require.Equal(t, 0, flush.callCount())
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
		assert.Equal(t, 0, flush.callCount())
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

// TestAgentRecordBuffer_BufferedWireBytes verifies the running wire-byte
// counter matches the bytes kmsg.RecordBatch.AppendTo produces for the
// equivalent batch — across varying record counts, varying timestamps (so
// tsDelta varint widths matter), and varying offsets (so offsetDelta varint
// widths matter). Drift between the counter and the encoder would manifest
// here as an inequality.
func TestAgentRecordBuffer_BufferedWireBytes(t *testing.T) {
	cases := []struct {
		name    string
		records []*kgo.Record
	}{
		{
			name: "single small record",
			records: []*kgo.Record{
				{Value: []byte("hello"), Timestamp: time.UnixMilli(1_000_000)},
			},
		},
		{
			name: "two records, same timestamp",
			records: []*kgo.Record{
				{Value: []byte("a"), Timestamp: time.UnixMilli(1_000_000)},
				{Value: []byte("b"), Timestamp: time.UnixMilli(1_000_000)},
			},
		},
		{
			name: "many records crossing offsetDelta varint boundary (127→128)",
			records: func() []*kgo.Record {
				out := make([]*kgo.Record, 200)
				for i := range out {
					out[i] = &kgo.Record{
						Value:     []byte("v"),
						Timestamp: time.UnixMilli(1_000_000),
					}
				}
				return out
			}(),
		},
		{
			name: "records crossing tsDelta varlong boundary",
			records: []*kgo.Record{
				{Value: []byte("a"), Timestamp: time.UnixMilli(1_000_000)},
				{Value: []byte("b"), Timestamp: time.UnixMilli(1_000_000 + 64)},
				{Value: []byte("c"), Timestamp: time.UnixMilli(1_000_000 + 8192)},
				{Value: []byte("d"), Timestamp: time.UnixMilli(1_000_000 + 1<<22)},
			},
		},
		{
			name: "records with key, value and headers",
			records: []*kgo.Record{
				{
					Key:   []byte("k1"),
					Value: []byte("v1"),
					Headers: []kgo.RecordHeader{
						{Key: "h1", Value: []byte("v1")},
						{Key: "h2", Value: []byte("v2")},
					},
					Timestamp: time.UnixMilli(1_000_000),
				},
				{
					Key:       []byte("k2-longer"),
					Value:     []byte("v2-longer-value"),
					Timestamp: time.UnixMilli(1_000_050),
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Use a never-firing linger and a large cap so Add never flushes,
			// letting us read the running counter for the full batch.
			a := NewAgentRecordBuffer(
				1,
				time.Hour,
				1<<30,
				func(_ context.Context, _ int32, _ []*kgo.Record, done func(error)) { done(nil) },
				newMetrics(prometheus.NewPedanticRegistry()),
			)
			t.Cleanup(a.Close)

			a.Add(tc.records, func(error) {})

			a.mu.Lock()
			running := a.bufferedWireBytes
			a.mu.Unlock()

			actual := actualUncompressedMultiRecordBatchWireSize(tc.records)
			assert.Equal(t, actual, running,
				"running bufferedWireBytes must equal the actual on-wire batch size produced by kmsg.RecordBatch.AppendTo")
		})
	}
}

// TestAgentRecordBuffer_BufferedWireBytes_AfterEarlyFlush guards the early-
// flush + re-cost branch in Add. A regression that drops the second
// computeAddCostLocked call would silently mis-account every flush boundary
// (e.g. miss the recordBatchHeaderBytes for the new batch, or use stale
// offsetDelta values), breaking convergence with the encoder.
func TestAgentRecordBuffer_BufferedWireBytes_AfterEarlyFlush(t *testing.T) {
	flushed := make(chan []*kgo.Record, 1)
	a := NewAgentRecordBuffer(
		1,
		// Linger long enough that overflow is the only flush trigger we exercise.
		time.Hour,
		// Tight cap: anything beyond ~512 bytes pushes a small batch over.
		512,
		func(_ context.Context, _ int32, recs []*kgo.Record, done func(error)) {
			flushed <- recs
			done(nil)
		},
		newMetrics(prometheus.NewPedanticRegistry()),
	)
	t.Cleanup(a.Close)

	// First Add fills most of the cap.
	first := []*kgo.Record{
		{Value: bytes.Repeat([]byte("x"), 200), Timestamp: time.UnixMilli(1_000_000)},
		{Value: bytes.Repeat([]byte("y"), 200), Timestamp: time.UnixMilli(1_000_010)},
	}
	a.Add(first, func(error) {})

	// Second Add doesn't fit on top of the first → must trigger an early
	// flush of `first` and re-cost as a fresh batch anchored on second[0].
	second := []*kgo.Record{
		{Value: bytes.Repeat([]byte("z"), 200), Timestamp: time.UnixMilli(2_000_000)},
	}
	a.Add(second, func(error) {})

	// First batch must have been flushed with exactly `first`.
	select {
	case got := <-flushed:
		require.Equal(t, len(first), len(got))
	case <-time.After(time.Second):
		t.Fatal("early flush did not fire")
	}

	// Running counter must equal the actual on-wire bytes for the *new* batch
	// (which only carries `second`, anchored at second[0].Timestamp, offsetDelta=0).
	a.mu.Lock()
	running := a.bufferedWireBytes
	anchor := a.bufferedFirstTimestamp
	a.mu.Unlock()

	assert.Equal(t, second[0].Timestamp.UnixMilli(), anchor,
		"firstTimestamp anchor must reset to the first record of the post-flush batch")
	assert.Equal(t, actualUncompressedMultiRecordBatchWireSize(second), running,
		"post-flush bufferedWireBytes must be costed from scratch (header + offsetDelta=0 + tsDelta=0)")
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
