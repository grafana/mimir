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
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

type mockDirectProducerCall struct {
	nodeID int32
	req    *kmsg.ProduceRequest
	err    error     // nil on success; context.Err() if ctx was cancelled
	sentAt time.Time // when the call was recorded (after any configured delay)
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
}

func newMockDirectProducer() *mockDirectProducer {
	return &mockDirectProducer{
		delays:  make(map[int32]time.Duration),
		errs:    make(map[int32]error),
		blockCh: make(map[int32]chan struct{}),
	}
}

func (m *mockDirectProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
	m.mu.Lock()
	delay := m.delays[nodeID]
	err := m.errs[nodeID]
	block := m.blockCh[nodeID]
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			m.record(nodeID, req, ctx.Err())
			return nil, ctx.Err()
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			m.record(nodeID, req, ctx.Err())
			return nil, ctx.Err()
		}
	}
	m.record(nodeID, req, err)
	if err != nil {
		return nil, err
	}
	return kmsg.NewPtrProduceResponse(), nil
}

func (m *mockDirectProducer) record(nodeID int32, req *kmsg.ProduceRequest, err error) {
	m.mu.Lock()
	m.calls = append(m.calls, mockDirectProducerCall{nodeID: nodeID, req: req, err: err, sentAt: time.Now()})
	m.mu.Unlock()
}

func (m *mockDirectProducer) recordedCalls() []mockDirectProducerCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]mockDirectProducerCall, len(m.calls))
	copy(out, m.calls)
	return out
}

// KafkaDirectProducer requires a live broker; this compile-time check is the only
// test that can run without one.
func TestKafkaDirectProducerImplementsDirectProducer(t *testing.T) {
	var _ DirectProducer = (*KafkaDirectProducer)(nil)
}

func TestMockDirectProducerMultipleCalls(t *testing.T) {
	t.Run("calls are recorded in order across multiple produces", func(t *testing.T) {
		m := newMockDirectProducer()
		ctx := context.Background()
		req := &kmsg.ProduceRequest{}

		_, _ = m.Produce(ctx, 1, req)
		_, _ = m.Produce(ctx, 2, req)
		_, _ = m.Produce(ctx, 1, req)

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
				_, _ = m.Produce(context.Background(), nodeID, &kmsg.ProduceRequest{})
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
			_, _ = m.Produce(context.Background(), 1, &kmsg.ProduceRequest{})
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
			_, err := m.Produce(ctx, tc.nodeID, &kmsg.ProduceRequest{})

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

func TestKafkaDirectProducerProduce(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(1)
		partition     = int32(0)
		// kfake assigns NodeID 0 to the single broker in a one-broker cluster.
		brokerNodeID = int32(0)
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Fetch topic metadata to obtain the topic UUID. Both Topic (name) and TopicID
	// (UUID) are set in the ProduceRequest so the request is valid regardless of
	// which API version the connection negotiates: v0-v12 use the name, v13+ the UUID.
	metaResp, err := client.Request(context.Background(), &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topicName)}},
	})
	require.NoError(t, err)
	meta := metaResp.(*kmsg.MetadataResponse)
	require.Len(t, meta.Topics, 1)
	topicID := meta.Topics[0].TopicID

	producer := NewKafkaDirectProducer(client)

	records := []*kgo.Record{{
		Topic:     topicName,
		Partition: partition,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Timestamp: time.Now(),
	}}
	req := buildProduceRequest(topicName, topicID, 9, records)

	resp, err := producer.Produce(context.Background(), brokerNodeID, req)
	require.NoError(t, err)
	require.NoError(t, parseProduceResponse(resp))

	// Verify the record was actually stored by consuming it back.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(clusterAddr),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topicName: {partition: kgo.NewOffset().AtStart()},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fetches := consumer.PollFetches(fetchCtx)
	require.NoError(t, fetches.Err())
	require.Len(t, fetches.Records(), 1)
	assert.Equal(t, []byte("test-key"), fetches.Records()[0].Key)
	assert.Equal(t, []byte("test-value"), fetches.Records()[0].Value)
}
