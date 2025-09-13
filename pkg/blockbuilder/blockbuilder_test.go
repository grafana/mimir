// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestBlockBuilder(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	tenants := []string{"1", "2", "3"}
	samplesPerTenant := 10

	// We have 10 records per tenant. And there are 3 tenants.
	cases := []struct {
		name                   string
		startOffset, endOffset int64
		// For per tenant slice.
		expSampleRangeStart int
		expSampleRangeEnd   int
	}{
		{
			name:                "first offset till somewhere in between",
			startOffset:         0,
			endOffset:           3 * 6,
			expSampleRangeStart: 0,
			expSampleRangeEnd:   6,
		},
		{
			name:                "somewhere in between till last offset",
			startOffset:         int64(3 * 6),
			endOffset:           3 * 10,
			expSampleRangeStart: 6,
			expSampleRangeEnd:   10,
		},
		{
			name:                "somewhere in between to somewhere in between",
			startOffset:         int64(3 * 3),
			endOffset:           3 * 6,
			expSampleRangeStart: 3,
			expSampleRangeEnd:   6,
		},
		{
			name:                "entire partition",
			startOffset:         0,
			endOffset:           3 * 10,
			expSampleRangeStart: 0,
			expSampleRangeEnd:   10,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
			kafkaClient := mustKafkaClient(t, kafkaAddr)
			kafkaClient.AddConsumeTopics(testTopic)

			cfg, overrides := blockBuilderConfig(t, kafkaAddr)

			producedSamples := make(map[string][]mimirpb.Sample, 0)
			recsPerTenant := 0
			kafkaRecTime := time.Now().Add(-time.Hour)
			for range samplesPerTenant {
				for _, tenant := range tenants {
					samples := produceSamples(ctx, t, kafkaClient, 1, kafkaRecTime, tenant, kafkaRecTime.Add(-time.Minute))
					producedSamples[tenant] = append(producedSamples[tenant], samples...)
				}
				recsPerTenant++

				kafkaRecTime = kafkaRecTime.Add(10 * time.Minute)
			}
			require.NotEmpty(t, producedSamples)

			scheduler := &mockSchedulerClient{}
			scheduler.addJob(
				schedulerpb.JobKey{
					Id:    "test-job-4898",
					Epoch: 90000,
				},
				schedulerpb.JobSpec{
					Topic:       testTopic,
					Partition:   1,
					StartOffset: c.startOffset,
					EndOffset:   c.endOffset,
				},
			)

			bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, scheduler)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
			})

			require.Eventually(t, func() bool {
				return scheduler.completeJobCallCount() > 0
			}, 5*time.Second, 100*time.Millisecond, "expected job completion")

			require.EqualValues(t,
				[]schedulerpb.JobKey{{Id: "test-job-4898", Epoch: 90000}},
				scheduler.completeJobCalls,
			)

			for _, tenant := range tenants {
				expSamples := producedSamples[tenant][c.expSampleRangeStart:c.expSampleRangeEnd]

				compareQueryWithDir(t,
					path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant),
					expSamples, nil,
					labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
				)
			}
		})
	}
}

func TestBlockBuilder_WipeOutDataDirOnStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	f, err := os.CreateTemp(cfg.DataDir, "block")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	bb, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Verify that the data_dir was wiped out on the block-builder's start.
	list, err := os.ReadDir(cfg.DataDir)
	require.NoError(t, err, "expected data_dir to exist")
	require.Empty(t, list, "expected data_dir to be empty")
}

func produceSamples(ctx context.Context, t *testing.T, kafkaClient *kgo.Client, partition int32, ts time.Time, tenantID string, sampleTs ...time.Time) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, st := range sampleTs {
		samples = append(samples, floatSample(st.UnixMilli(), 1)...)
	}

	req := createWriteRequest(tenantID, samples, nil)
	val, err := req.Marshal()
	require.NoError(t, err)

	produceRecords(ctx, t, kafkaClient, ts, tenantID, testTopic, partition, val)
	return samples
}

func mustKafkaClient(t *testing.T, addrs ...string) *kgo.Client {
	writeClient, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		// We will choose the partition of each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}

func produceRecords(
	ctx context.Context,
	t *testing.T,
	kafkaClient *kgo.Client,
	ts time.Time,
	userID string,
	topic string,
	part int32,
	val []byte,
) kgo.ProduceResults {
	rec := &kgo.Record{
		Timestamp: ts,
		Key:       []byte(userID),
		Value:     val,
		Topic:     topic,
		Partition: part, // samples in this batch are split between N partitions
	}
	produceResult := kafkaClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())
	return produceResult
}

type mockSchedulerClient struct {
	mu   sync.Mutex
	jobs []struct {
		key  schedulerpb.JobKey
		spec schedulerpb.JobSpec
	}
	runCalls         int
	getJobCalls      int
	completeJobCalls []schedulerpb.JobKey
	closeCalls       int
}

func (m *mockSchedulerClient) Run(_ context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runCalls++
}

func (m *mockSchedulerClient) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
}

func (m *mockSchedulerClient) GetJob(ctx context.Context) (schedulerpb.JobKey, schedulerpb.JobSpec, error) {
	m.mu.Lock()
	m.getJobCalls++

	if len(m.jobs) > 0 {
		job := m.jobs[0]
		m.jobs = m.jobs[1:]
		m.mu.Unlock()
		return job.key, job.spec, nil
	}

	m.mu.Unlock()

	// Otherwise there isn't a job available. Block until context is done.
	<-ctx.Done()
	return schedulerpb.JobKey{}, schedulerpb.JobSpec{}, ctx.Err()
}

func (m *mockSchedulerClient) CompleteJob(key schedulerpb.JobKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.completeJobCalls = append(m.completeJobCalls, key)

	// Do nothing.
	return nil
}

func (m *mockSchedulerClient) FailJob(key schedulerpb.JobKey) error {
	return nil
}

// addJob adds a job to the fake back-end for this mock scheduler client.
func (m *mockSchedulerClient) addJob(key schedulerpb.JobKey, spec schedulerpb.JobSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = append(m.jobs, struct {
		key  schedulerpb.JobKey
		spec schedulerpb.JobSpec
	}{key: key, spec: spec})
}

func (m *mockSchedulerClient) completeJobCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.completeJobCalls)
}
