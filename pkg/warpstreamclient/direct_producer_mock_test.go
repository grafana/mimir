// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type mockDirectProducerCall struct {
	nodeID     int32
	partitions []topicPartitionRecords
	err        error     // nil on success; context.Err() if ctx was cancelled
	sentAt     time.Time // when the call was recorded (after any configured delay)
}

// mockDirectProducer is a test double for DirectProducer. All maps are keyed by nodeID.
// Absent keys return a success response immediately.
//
// To block a Produce until the test releases it, close the channel in blockCh[nodeID].
type mockDirectProducer struct {
	mu      sync.Mutex
	calls   []mockDirectProducerCall
	delays  map[int32]time.Duration
	errs    map[int32]error
	blockCh map[int32]chan struct{}

	// respFn, when set, drives the success path and may also script per-call
	// errors. errs[nodeID] (if set) still takes precedence. Tests that need
	// scripted per-attempt outcomes (e.g. retry-loop tests) set respFn with
	// their own atomic counter to vary the response/error per call.
	respFn func(nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error)
}

func newMockDirectProducer() *mockDirectProducer {
	return &mockDirectProducer{
		delays:  make(map[int32]time.Duration),
		errs:    make(map[int32]error),
		blockCh: make(map[int32]chan struct{}),
	}
}

func (m *mockDirectProducer) ProduceSync(ctx context.Context, nodeID int32, partitions []topicPartitionRecords) ProduceResult {
	m.mu.Lock()
	delay := m.delays[nodeID]
	err := m.errs[nodeID]
	block := m.blockCh[nodeID]
	respFn := m.respFn
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			m.record(nodeID, partitions, ctx.Err())
			return ProduceResult{err: ctx.Err()}
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			m.record(nodeID, partitions, ctx.Err())
			return ProduceResult{err: ctx.Err()}
		}
	}
	if err != nil {
		m.record(nodeID, partitions, err)
		return ProduceResult{err: err}
	}
	if respFn != nil {
		resp, fnErr := respFn(nodeID, partitions)
		m.record(nodeID, partitions, fnErr)
		return ProduceResult{resp: resp, err: fnErr}
	}
	m.record(nodeID, partitions, nil)
	return ProduceResult{resp: kmsg.NewPtrProduceResponse()}
}

func (m *mockDirectProducer) record(nodeID int32, partitions []topicPartitionRecords, err error) {
	m.mu.Lock()
	m.calls = append(m.calls, mockDirectProducerCall{nodeID: nodeID, partitions: partitions, err: err, sentAt: time.Now()})
	m.mu.Unlock()
}

func (m *mockDirectProducer) recordedCalls() []mockDirectProducerCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]mockDirectProducerCall, len(m.calls))
	copy(out, m.calls)
	return out
}

// recordedCallNodeIDs returns the nodeID of every recorded call, in order.
func (m *mockDirectProducer) recordedCallNodeIDs() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int32, len(m.calls))
	for i, c := range m.calls {
		out[i] = c.nodeID
	}
	return out
}
func TestMockDirectProducer_MultipleCalls(t *testing.T) {
	t.Run("calls are recorded in order across multiple produces", func(t *testing.T) {
		m := newMockDirectProducer()
		ctx := context.Background()

		_ = m.ProduceSync(ctx, 1, nil)
		_ = m.ProduceSync(ctx, 2, nil)
		_ = m.ProduceSync(ctx, 1, nil)

		calls := m.recordedCalls()
		require.Len(t, calls, 3)
		assert.Equal(t, int32(1), calls[0].nodeID)
		assert.Equal(t, int32(2), calls[1].nodeID)
		assert.Equal(t, int32(1), calls[2].nodeID)
	})

	t.Run("concurrent produces are race-safe", func(t *testing.T) {
		m := newMockDirectProducer()
		var wg sync.WaitGroup
		for i := range 10 {
			wg.Add(1)
			go func(nodeID int32) {
				defer wg.Done()
				_ = m.ProduceSync(context.Background(), nodeID, nil)
			}(int32(i))
		}
		wg.Wait()
		assert.Len(t, m.recordedCalls(), 10)
	})

	t.Run("block/release controls Produce completion order", func(t *testing.T) {
		m := newMockDirectProducer()
		gate := make(chan struct{})
		m.blockCh[1] = gate

		done := make(chan struct{})
		go func() {
			_ = m.ProduceSync(context.Background(), 1, nil)
			close(done)
		}()

		// Goroutine is blocked; no calls recorded yet.
		time.Sleep(10 * time.Millisecond)
		assert.Empty(t, m.recordedCalls())

		close(gate)
		<-done
		assert.Len(t, m.recordedCalls(), 1)
	})
}

func TestMockDirectProducer(t *testing.T) {
	tests := map[string]struct {
		nodeID   int32
		setup    func(*mockDirectProducer)
		wantErr  bool
		minDelay time.Duration
	}{
		"default returns success": {
			nodeID: 1,
			setup:  func(_ *mockDirectProducer) {},
		},
		"configured error is returned": {
			nodeID:  2,
			setup:   func(m *mockDirectProducer) { m.errs[2] = errors.New("x") },
			wantErr: true,
		},
		"configured delay is honoured": {
			nodeID:   3,
			setup:    func(m *mockDirectProducer) { m.delays[3] = 20 * time.Millisecond },
			minDelay: 20 * time.Millisecond,
		},
		"call is recorded": {
			nodeID: 4,
			setup:  func(_ *mockDirectProducer) {},
		},
		"context cancellation returns error": {
			nodeID:  5,
			setup:   func(m *mockDirectProducer) { m.delays[5] = time.Hour },
			wantErr: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := newMockDirectProducer()
			tc.setup(m)

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			start := time.Now()
			err := m.ProduceSync(ctx, tc.nodeID, nil).error()

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.GreaterOrEqual(t, time.Since(start), tc.minDelay)
			}
			assert.Len(t, m.recordedCalls(), 1)
			assert.Equal(t, tc.nodeID, m.recordedCalls()[0].nodeID)
		})
	}
}
