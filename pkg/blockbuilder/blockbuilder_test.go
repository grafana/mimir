// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"os"
	"path"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestBlockBuilder(t *testing.T) {
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

	fetchModes := []struct {
		name                string
		fetchConcurrencyMax int
	}{
		{name: "without concurrent fetchers", fetchConcurrencyMax: 0},
		{name: "with concurrent fetchers", fetchConcurrencyMax: 12},
	}

	for _, fm := range fetchModes {
		t.Run(fm.name, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					t.Parallel()

					synctest.Test(t, func(t *testing.T) {
						ctx := t.Context()

						var vnet kfake.VirtualNetwork

						_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic, testkafka.WithVirtualNetwork(&vnet))

						kafkaClient, err := kgo.NewClient(
							kgo.SeedBrokers(kafkaAddr),
							kgo.Dialer(vnet.DialContext),
							kgo.RecordPartitioner(kgo.ManualPartitioner()), // we will choose the partition of each record
						)
						require.NoError(t, err)
						t.Cleanup(kafkaClient.Close)

						kafkaClient.AddConsumeTopics(testTopic)

						cfg, overrides := blockBuilderConfig(t, kafkaAddr, nil)
						cfg.GenerateSparseIndexHeaders = true
						cfg.Kafka.Dialer = vnet.DialContext
						cfg.Kafka.FetchConcurrencyMax = fm.fetchConcurrencyMax
						// Lower FetchMaxWait so concurrent fetchers don't block on speculative reads
						// past the job's end offset in tests where no further records arrive.
						cfg.Kafka.FetchMaxWait = 500 * time.Millisecond

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
							require.NoError(t, services.StopAndAwaitTerminated(context.Background(), bb))
						})

						require.Eventually(t, func() bool {
							return scheduler.completeJobCallCount() > 0
						}, 5*time.Second, 100*time.Millisecond, "expected job completion")

						require.EqualValues(t,
							[]schedulerpb.JobKey{{Id: "test-job-4898", Epoch: 90000}},
							scheduler.completeJobCalls,
						)

						for _, tenant := range tenants {
							tenantBucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant)
							if bb.cfg.GenerateSparseIndexHeaders {
								validateSparseIndexHeadersInDir(t, ctx, tenantBucketDir, cfg)
							}

							expSamples := producedSamples[tenant][c.expSampleRangeStart:c.expSampleRangeEnd]
							compareQueryWithDir(t,
								tenantBucketDir,
								expSamples, nil,
								labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
							)
						}
					})
				})
			}
		})
	}
}

func TestBlockBuilder_WipeOutDataDirOnStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	cfg, overrides := blockBuilderConfig(t, kafkaAddr, nil)

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

// Asserts that a job spanning multiple write compartments consumes each compartment's range from
// that compartment's own Kafka client and merges the results into a single block. The two
// compartments point at two distinct Kafka clusters whose samples interleave in time, so the
// uploaded block contains the timestamp-ordered union only if each compartment is read through its
// own client and the two are merged: a bug reading every compartment from one client would miss the
// other cluster's data. It runs with and without concurrent fetchers, since those take different
// per-cluster read paths.
func TestConsumeJob_MultipleWriteCompartments(t *testing.T) {
	for _, fetchConcurrencyMax := range []int{0, 8} {
		name := "without concurrent fetchers"
		if fetchConcurrencyMax > 0 {
			name = "with concurrent fetchers"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			_, kafkaAddrA := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
			_, kafkaAddrB := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
			producerA := mustKafkaClient(t, kafkaAddrA)
			producerB := mustKafkaClient(t, kafkaAddrB)

			tenants := []string{"1", "2", "3"}
			const roundsPerCluster = 5
			const recordsPerCluster = roundsPerCluster * 3 // One record per tenant per round.

			produceToCluster := func(producer *kgo.Client, start time.Time) map[string][]mimirpb.Sample {
				out := make(map[string][]mimirpb.Sample)
				recTime := start
				for range roundsPerCluster {
					for _, tenant := range tenants {
						samples := produceSamples(ctx, t, producer, 1, recTime, tenant, recTime.Add(-time.Minute))
						out[tenant] = append(out[tenant], samples...)
					}
					recTime = recTime.Add(20 * time.Minute)
				}
				return out
			}

			// Cluster A and B samples interleave in time (B offset 10 minutes from A), so the merge must
			// interleave the two sources in timestamp order rather than drain one then the other.
			now := time.Now()
			producedA := produceToCluster(producerA, now.Add(-100*time.Minute))
			producedB := produceToCluster(producerB, now.Add(-90*time.Minute))

			bb, cfg, _ := newMultiClusterBlockBuilder(t, fetchConcurrencyMax, kafkaAddrA, kafkaAddrB)

			spec := schedulerpb.JobSpec{
				Topic:     testTopic,
				Partition: 1,
				OffsetRanges: map[int32]schedulerpb.OffsetRange{
					0: {StartOffset: 0, EndOffset: recordsPerCluster},
					1: {StartOffset: 0, EndOffset: recordsPerCluster},
				},
			}
			require.NoError(t, bb.consumeJob(ctx, schedulerpb.JobKey{Id: "test-job-multi-cluster", Epoch: 1}, spec))

			for _, tenant := range tenants {
				tenantBucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant)
				// The block holds the union of both clusters' samples, queryable in timestamp order.
				expSamples := append(append([]mimirpb.Sample{}, producedA[tenant]...), producedB[tenant]...)
				slices.SortFunc(expSamples, func(a, b mimirpb.Sample) int { return cmp.Compare(a.TimestampMs, b.TimestampMs) })
				compareQueryWithDir(t,
					tenantBucketDir,
					expSamples, nil,
					labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
				)
			}
		})
	}
}

// failingConsumer is a RecordConsumer whose Consume always fails.
type failingConsumer struct{ err error }

func (c failingConsumer) Consume(context.Context, iter.Seq[*kgo.Record]) error { return c.err }

// Asserts the all-or-nothing contract of the multi-cluster merge: if forwarding the merged records
// downstream fails, consumeMultiCluster returns the error (so consumeJob won't commit the job)
// instead of swallowing it. This exercises the merge-side failure arm; the producer-side arm (a
// source erroring mid-consume) is exercised by the merge unit tests.
func TestConsumeMultiCluster_FailsJobWhenConsumerFails(t *testing.T) {
	ctx := context.Background()

	_, kafkaAddrA := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	_, kafkaAddrB := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	producerA := mustKafkaClient(t, kafkaAddrA)
	producerB := mustKafkaClient(t, kafkaAddrB)

	// Both clusters must have data so the job takes the merge path (two active ranges) rather than
	// the single-range shortcut.
	now := time.Now()
	for _, tenant := range []string{"1", "2"} {
		produceSamples(ctx, t, producerA, 1, now, tenant, now.Add(-time.Minute))
		produceSamples(ctx, t, producerB, 1, now, tenant, now.Add(-time.Minute))
	}

	bb, cfg, overrides := newMultiClusterBlockBuilder(t, 0, kafkaAddrA, kafkaAddrB)
	builder := NewTSDBBuilder(1, cfg, overrides, test.NewTestingLogger(t), bb.tsdbBuilderMetrics, bb.tsdbMetrics)
	t.Cleanup(func() { _ = builder.Close() })

	wantErr := errors.New("downstream boom")
	spec := schedulerpb.JobSpec{
		Topic:     testTopic,
		Partition: 1,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 2},
			1: {StartOffset: 0, EndOffset: 2},
		},
	}
	err := bb.consumeMultiCluster(ctx, test.NewTestingLogger(t), failingConsumer{err: wantErr}, builder, spec)
	require.ErrorIs(t, err, wantErr)
}

// newMultiClusterBlockBuilder builds a compartments-enabled block-builder wired to consume one
// cluster per addr, each from its own Kafka cluster, ready for consumeJob/consumeMultiCluster to be
// driven directly. The full service path can't set this up because several distinct broker
// addresses can't be produced from one templated address.
func newMultiClusterBlockBuilder(t *testing.T, fetchConcurrencyMax int, addrs ...string) (*BlockBuilder, Config, *validation.Overrides) {
	t.Helper()

	cfg, overrides := blockBuilderConfig(t, addrs[0], nil)
	cfg.Kafka.FetchConcurrencyMax = fetchConcurrencyMax
	// Lower FetchMaxWait so concurrent fetchers don't block on speculative reads past the job's
	// end offset once all records have been consumed.
	cfg.Kafka.FetchMaxWait = 500 * time.Millisecond
	cfg.Compartments = compartments.Config{
		Enabled: true,
		Read:    compartments.ReadConfig{NumCompartments: 1},
		Write:   compartments.WriteConfig{NumCompartments: len(addrs)},
	}

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, &mockSchedulerClient{})
	require.NoError(t, err)

	for clusterID, addr := range addrs {
		kcfg := cfg.Kafka
		kcfg.Address = flagext.StringSliceCSV{addr}
		c, err := ingest.NewKafkaReaderClient(kcfg, bb.clusters[clusterID].kprom, test.NewTestingLogger(t))
		require.NoError(t, err)
		t.Cleanup(c.Close)
		bb.clusters[clusterID].client = c

		// With concurrent fetching enabled, start the per-cluster reader-metrics service so the
		// fetcher path is exercised as it is in production (starting() does this on the full path).
		if metrics := bb.clusters[clusterID].metrics; metrics != nil {
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), metrics))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), metrics))
			})
		}
	}
	return bb, cfg, overrides
}

// Asserts that a compartment-mode spec (carrying OffsetRanges) reaching a block-builder with
// compartments disabled fails loudly rather than being silently consumed from the wrong cluster.
func TestConsumeJob_RejectsCompartmentSpecWhenCompartmentsDisabled(t *testing.T) {
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	cfg, overrides := blockBuilderConfig(t, kafkaAddr, nil)
	require.False(t, cfg.Compartments.Enabled)

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, &mockSchedulerClient{})
	require.NoError(t, err)

	spec := schedulerpb.JobSpec{
		Topic:        testTopic,
		Partition:    0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{0: {StartOffset: 0, EndOffset: 1}},
	}
	err = bb.consumeJob(context.Background(), schedulerpb.JobKey{Id: "mismatch", Epoch: 1}, spec)
	require.ErrorContains(t, err, "offset_ranges should not be set")
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
