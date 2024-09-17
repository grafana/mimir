// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

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

func TestBlockBuilder_consumerLagRecords(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)

	produceRecords(ctx, t, kafkaClient, time.Now().Add(-time.Hour), "1", testTopic, 0, []byte(`test value`))

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)
	cfg.PartitionAssignment = map[string][]int32{
		"block-builder-0": {0}, // Explicitly use one partition only to simplify the test.
	}

	reg := prometheus.NewPedanticRegistry()
	bb, err := New(cfg, test.NewTestingLogger(t), reg, overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	require.Eventually(t, func() bool {
		return assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_blockbuilder_consumer_lag_records The per-topic-partition number of records, instance needs to work through each cycle.
			# TYPE cortex_blockbuilder_consumer_lag_records gauge
			cortex_blockbuilder_consumer_lag_records{partition="0"} 1
		`), "cortex_blockbuilder_consumer_lag_records"))
	}, 5*time.Second, 100*time.Millisecond)
}

// Testing block builder starting up with an existing kafka commit.
func TestBlockBuilder_StartWithExistingCommit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	kafkaCluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	// Producing some records
	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	var producedSamples []mimirpb.Sample
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
	for kafkaRecTime.Before(cycleEndStartup) {
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
		producedSamples = append(producedSamples, samples...)

		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
	}
	require.NotEmpty(t, producedSamples)

	// Fetch all the records that were produced to choose one to commit.
	var recs []*kgo.Record
	for len(recs) < len(producedSamples) {
		fetches := kafkaClient.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		recs = append(recs, fetches.Records()...)
	}
	require.Len(t, recs, len(producedSamples))

	// Choosing the midpoint record to commit and as the last seen record as well.
	commitRec := recs[len(recs)/2]
	require.NotNil(t, commitRec)

	lastRec := commitRec
	blockEnd := commitRec.Timestamp.Truncate(cfg.ConsumeInterval).Add(cfg.ConsumeInterval)
	offset := kadm.Offset{
		Topic:       commitRec.Topic,
		Partition:   commitRec.Partition,
		At:          commitRec.Offset + 1,
		LeaderEpoch: -1, // not a group consumer
		Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastRec.Timestamp.UnixMilli(), blockEnd.UnixMilli()),
	}
	commitOffset(ctx, t, kafkaClient, testGroup, offset)

	// Set up a hook to track commits from block-builder to kafka. Those indicate the end of a cycle.
	kafkaCommits := atomic.NewInt32(0)
	kafkaCluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCommits.Add(1)
		return nil, nil, false
	})

	bb, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// We expect at least several cycles because of how the pushed records were structured.
	require.Eventually(t, func() bool { return kafkaCommits.Load() >= 3 }, 5*time.Second, 100*time.Millisecond, "expected kafka commits")

	// Because there is a commit, on startup, block-builder must consume samples only after the commit.
	expSamples := producedSamples[1+(len(producedSamples)/2):]

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		expSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

// When there is no commit on startup, and the first cycleEnd is at T, and all the samples are from before T-lookback, then
// the first consumption cycle skips all the records.
func TestBlockBuilder_StartWithLookbackOnNoCommit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	kafkaCluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)
	cfg.LookbackOnNoCommit = 3 * time.Hour

	// Producing some records, all before LookbackOnNoCommit
	kafkaRecTime := time.Now().Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
	for range 3 {
		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
		produceSamples(ctx, t, kafkaClient, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
	}

	// Set up a hook to track commits from block-builder to kafka. Those indicate the end of a cycle.
	kafkaCommits := atomic.NewInt32(0)
	kafkaCluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCommits.Add(1)
		return nil, nil, false
	})

	reg := prometheus.NewPedanticRegistry()

	bb, err := New(cfg, test.NewTestingLogger(t), reg, overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Nothing is consumed due to zero lag.
	require.Eventually(t, func() bool { return kafkaCommits.Load() == 0 }, 5*time.Second, 100*time.Millisecond, "expected skipping all records before lookback period")

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blockbuilder_consumer_lag_records The per-topic-partition number of records, instance needs to work through each cycle.
		# TYPE cortex_blockbuilder_consumer_lag_records gauge
		cortex_blockbuilder_consumer_lag_records{partition="0"} 0
		cortex_blockbuilder_consumer_lag_records{partition="1"} 0
	`), "cortex_blockbuilder_consumer_lag_records"))
}

func TestBlockBuilder_WithMultipleTenants(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	kafkaCluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeInterval)

	producedPerTenantSamples := make(map[string][]mimirpb.Sample, 0)
	tenants := []string{"1", "2", "3"}

	// Producing some records for multiple tenants
	for range 10 {
		for _, tenant := range tenants {
			samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenant, kafkaRecTime)
			producedPerTenantSamples[tenant] = append(producedPerTenantSamples[tenant], samples...)
		}

		kafkaRecTime = kafkaRecTime.Add(15 * time.Second)
	}

	// Set up a hook to track commits from block-builder to kafka. Those indicate the end of a cycle.
	kafkaCommits := atomic.NewInt32(0)
	kafkaCluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCommits.Add(1)
		return nil, nil, false
	})

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Wait for end of the cycles. We expect at least several cycles because of how the pushed records were structured.
	require.Eventually(t, func() bool { return kafkaCommits.Load() > 1 }, 5*time.Second, 100*time.Millisecond, "expected kafka commits")

	for _, tenant := range tenants {
		bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant)
		db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })

		compareQuery(t,
			db,
			producedPerTenantSamples[tenant],
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	}
}

func TestBlockBuilder_WithOutOfOrderRecordsAndSamples(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	// We don't want to run the service here. Instead, the test cases below trigger and assert the consumption cycles explicitly.
	require.NoError(t, bb.starting(ctx))
	t.Cleanup(func() {
		require.NoError(t, bb.stopping(nil))
	})

	const tenantID = "1"

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenantID)

	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)
	cycleEnd := cycleEndStartup

	var allSamples []mimirpb.Sample

	// Out of order sample w.r.t. samples in last cycle. But for this cycle,
	// the TSDB starts fresh. So in terms of the actual block building, it will be
	// taken as in-order. Depending on if the out-of-order and in-order sample below
	// crosses the 2h block boundary, we expect either 1 or 2 blocks.
	{
		kafkaRecTime := cycleEnd
		outOfOrderSampleTime := kafkaRecTime.Add(-time.Hour)
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, outOfOrderSampleTime)
		allSamples = append(allSamples, samples...)
	}

	// In-order sample w.r.t. last consume cycle.
	{
		kafkaRecTime := cycleEnd.Add(cfg.ConsumeInterval / 10)
		inOrderSampleTime := kafkaRecTime.Add(-time.Minute)
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, inOrderSampleTime)
		allSamples = append(allSamples, samples...)
	}

	// Advance the tracking time to the next cycle.
	cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)

	// Sample is not a part of the next cycle (a future sample) but its kafka record falls in this cycle.
	// This sample should not go into the first cycle.
	{
		kafkaRecTime := cycleEnd.Add(cfg.ConsumeInterval / 10)
		sampleTime := cycleEnd.Add(cfg.ConsumeInterval)
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, sampleTime)
		allSamples = append(allSamples, samples...)
	}

	// In-order sample falls within the next cycle, but the kafka record time does not fall within the next consume cycle.
	// This sample should not go into the first cycle.
	{
		kafkaRecTime := cycleEnd.Add(time.Minute)
		sampleTime := cycleEndStartup.Add(cfg.ConsumeInterval / 10).Add(time.Minute)
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, sampleTime)
		allSamples = append(allSamples, samples...)
	}

	t.Run("consume only out of order and in-order", func(t *testing.T) {
		require.NoError(t, bb.nextConsumeCycle(ctx, cycleEnd))

		compareQuery(t,
			mustTSDBOpen(t, bucketDir),
			allSamples[:len(allSamples)-2], // Don't expect the last two sample.
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("future record", func(t *testing.T) {
		// The sample from above which was in-order but the kafka record was in future
		// should get consumed in this cycle. The other sample that is still in the future should not be consumed.
		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
		require.NoError(t, bb.nextConsumeCycle(ctx, cycleEnd))

		// The second to last sample in allSamples is the one that is still in the future. We don't expect it here.
		expSamples := append([]mimirpb.Sample(nil), allSamples[:len(allSamples)-2]...)
		expSamples = append(expSamples, allSamples[len(allSamples)-1])
		compareQuery(t,
			mustTSDBOpen(t, bucketDir),
			expSamples,
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("future sample", func(t *testing.T) {
		// The future sample gets consumed here. This also tests the case
		// where even though the kafka record after this was processed completely,
		// we still go back to the last kafka commit and consume the sample that happened to be in the future.
		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
		require.NoError(t, bb.nextConsumeCycle(ctx, cycleEnd))

		compareQuery(t,
			mustTSDBOpen(t, bucketDir),
			allSamples,
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})
}

func TestBlockBuilder_WithNonMonotonicRecordTimestamps(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	const tenantID = "1"

	var expSamplesPhase1, expSamplesPhase2 []mimirpb.Sample
	{
		// Simple first record with all samples in the block.
		kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-2 * cfg.ConsumeIntervalBuffer)

		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, kafkaRecTime)
		require.Len(t, samples, 1)
		expSamplesPhase1 = append(expSamplesPhase1, samples...)
	}

	var lastSeenRecTime time.Time
	{
		// Record in the buffer zone with a sample in the block and a sample outside the block.
		// This is the last record in this cycle. So this will be the "last seen" record for the next cycle.
		kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(cfg.ConsumeIntervalBuffer / 2)
		inBlockTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeIntervalBuffer)
		lastSeenRecTime = kafkaRecTime

		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, inBlockTime, kafkaRecTime)
		require.Len(t, samples, 2)
		expSamplesPhase1 = append(expSamplesPhase1, samples[0])
		expSamplesPhase2 = append(expSamplesPhase2, samples[1])
	}

	{
		// Record outside this cycle but with sample valid for this block.
		// This sample will be consumed in the next cycle because of the record timestamp.
		// The block builder will treat this as the stopping point for the cycle since the record timestamp is outside the cycle.
		kafkaRecTime := cycleEndStartup.Add(cfg.ConsumeInterval - time.Minute)
		inBlockTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeIntervalBuffer + time.Minute)

		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, inBlockTime)
		require.Len(t, samples, 1)
		expSamplesPhase2 = append(expSamplesPhase2, samples[0])
	}

	{
		// Record inside this cycle with sample in this block, but it is not consumed in this cycle
		// because block builder stops at the previous record.
		// This sample will be consumed in the next cycle.
		// To test correct working of non-monotonic timestamps, we should have this record's timestamp to be
		// before the last seen timestamp of the cycle.
		// If we were working with record timestamp, this sample will go missing. If we use offset of record
		// instead, then this sample will not go missing.
		kafkaRecTime := lastSeenRecTime.Add(-2 * time.Minute)
		inBlockTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeIntervalBuffer + 2*time.Minute)

		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, tenantID, inBlockTime)
		require.Len(t, samples, 1)
		expSamplesPhase2 = append(expSamplesPhase2, samples[0])
	}

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	// We don't want to run the service here. Instead, the test cases below trigger and assert the consumption cycles explicitly.
	require.NoError(t, bb.starting(ctx))
	t.Cleanup(func() {
		require.NoError(t, bb.stopping(nil))
	})

	runTest := func(name string, end time.Time, expSamples []mimirpb.Sample) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, bb.nextConsumeCycle(ctx, end))

			bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenantID)
			db := mustTSDBOpen(t, bucketDir)

			compareQuery(t,
				db,
				expSamples,
				nil,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})
	}

	runTest("phase 1", cycleEndStartup, expSamplesPhase1)
	runTest("phase 2", cycleEndStartup.Add(cfg.ConsumeInterval), append(expSamplesPhase1, expSamplesPhase2...))
}

func TestBlockBuilder_RetryOnTransientErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	kafkaCluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)

	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	// Producing some records
	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	var producedSamples []mimirpb.Sample
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-1 * time.Hour).Add(29 * time.Minute)
	for kafkaRecTime.Before(cycleEndStartup) {
		samples := produceSamples(ctx, t, kafkaClient, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
		producedSamples = append(producedSamples, samples...)

		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
	}
	require.NotEmpty(t, producedSamples)

	// Set up a hook to track commits from block-builder to kafka. Those indicate the end of a cycle.
	kafkaCommits := atomic.NewInt32(0)
	kafkaCommitAttempts := atomic.NewInt32(0)
	kafkaCluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		kafkaCluster.KeepControl()

		// Fail every first attempt to commit an offset to check the block-builder can handle it.
		if kafkaCommitAttempts.Add(1) == 1 {
			return nil, fmt.Errorf("test failure on commit"), true
		}

		kafkaCommits.Add(1)
		return nil, nil, false
	})

	reg := prometheus.NewPedanticRegistry()

	bb, err := New(cfg, test.NewTestingLogger(t), reg, overrides)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// We expect at least several cycles because of how the pushed records were structured.
	require.Eventually(t, func() bool { return kafkaCommits.Load() >= 1 }, 50*time.Second, 100*time.Millisecond, "expected kafka commits")

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		producedSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

func produceSamples(ctx context.Context, t *testing.T, kafkaClient *kgo.Client, ts time.Time, tenantID string, sampleTs ...time.Time) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, ts := range sampleTs {
		samples = append(samples, floatSample(ts.UnixMilli(), 1)...)
	}
	val := createWriteRequest(t, tenantID, samples, nil)
	produceRecords(ctx, t, kafkaClient, ts, tenantID, testTopic, 0, val)
	return samples
}

func TestCycleEndAtStartup(t *testing.T) {
	testCases := []struct {
		nowTimeStr string
		interval   time.Duration
		buffer     time.Duration
		testFunc   func(t *testing.T, cycleEnd time.Time)
	}{
		{
			nowTimeStr: "14:12:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "13:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
			},
		},
		{
			nowTimeStr: "14:16:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
			},
		},
		{
			nowTimeStr: "14:50:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
			},
		},
		{
			nowTimeStr: "14:12:00",
			interval:   30 * time.Minute,
			buffer:     time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:01:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
			},
		},
		{
			nowTimeStr: "14:32:00",
			interval:   30 * time.Minute,
			buffer:     time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:31:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("now=%s/%s+%s", tc.nowTimeStr, tc.interval, tc.buffer), func(t *testing.T) {
			now := mustTimeParse(t, time.TimeOnly, tc.nowTimeStr)
			cycleEnd := cycleEndAtStartup(now, tc.interval, tc.buffer)
			tc.testFunc(t, cycleEnd)
		})
	}
}

func TestNextCycleEnd(t *testing.T) {
	testCases := []struct {
		nowTimeStr string
		interval   time.Duration
		buffer     time.Duration
		testFunc   func(t *testing.T, cycleEnd time.Time, wait time.Duration)
	}{
		{
			nowTimeStr: "14:12:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time, wait time.Duration) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
				require.Equal(t, 3*time.Minute, wait)
			},
		},
		{
			nowTimeStr: "14:17:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time, wait time.Duration) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "15:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
				require.Equal(t, 58*time.Minute, wait)
			},
		},
		{
			nowTimeStr: "14:47:00",
			interval:   time.Hour,
			buffer:     15 * time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time, wait time.Duration) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "15:15:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
				require.Equal(t, 28*time.Minute, wait)
			},
		},
		{
			nowTimeStr: "14:12:00",
			interval:   30 * time.Minute,
			buffer:     time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time, wait time.Duration) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "14:31:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
				require.Equal(t, 19*time.Minute, wait)
			},
		},
		{
			nowTimeStr: "14:32:00",
			interval:   30 * time.Minute,
			buffer:     time.Minute,
			testFunc: func(t *testing.T, cycleEnd time.Time, wait time.Duration) {
				wantCycleEnd := mustTimeParse(t, time.TimeOnly, "15:01:00")
				require.Equal(t, wantCycleEnd, cycleEnd)
				require.Equal(t, 29*time.Minute, wait)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("now=%s/%s+%s", tc.nowTimeStr, tc.interval, tc.buffer), func(t *testing.T) {
			now := mustTimeParse(t, time.TimeOnly, tc.nowTimeStr)
			cycleEnd, wait := nextCycleEnd(now, tc.interval, tc.buffer)
			tc.testFunc(t, cycleEnd, wait)
		})
	}
}

func TestPartitionStateFromLag(t *testing.T) {
	testTime := time.UnixMilli(time.Now().UnixMilli())

	commitRecTs := testTime.Add(-time.Hour)
	lastRecOffset := int64(30)
	blockEndTs := testTime.Add(-10 * time.Minute)

	testKafkaOffset := kadm.Offset{
		Topic:       testTopic,
		Partition:   0,
		At:          30,
		LeaderEpoch: -1,
		Metadata:    marshallCommitMeta(commitRecTs.UnixMilli(), lastRecOffset, blockEndTs.UnixMilli()),
	}

	testCases := []struct {
		name           string
		lag            kadm.GroupMemberLag
		fallbackMillis int64
		wantState      partitionState
	}{
		{
			name: "no commit",
			lag: kadm.GroupMemberLag{
				Topic:     testTopic,
				Partition: 0,
				Commit:    kadm.Offset{},
			},
			fallbackMillis: testTime.UnixMilli(),
			wantState: partitionState{
				Commit:                kadm.Offset{},
				CommitRecordTimestamp: testTime,
				LastSeenOffset:        0,
				LastBlockEnd:          time.UnixMilli(0),
			},
		},
		{
			name: "with commit",
			lag: kadm.GroupMemberLag{
				Topic:     testTopic,
				Partition: 0,
				Commit:    testKafkaOffset,
			},
			fallbackMillis: testTime.UnixMilli(),
			wantState: partitionState{
				Commit:                testKafkaOffset,
				CommitRecordTimestamp: commitRecTs,
				LastSeenOffset:        lastRecOffset,
				LastBlockEnd:          blockEndTs,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := partitionStateFromLag(log.NewNopLogger(), tc.lag, tc.fallbackMillis)
			require.Equal(t, tc.wantState, state)
		})
	}
}

func mustTimeParse(t *testing.T, layout, v string) time.Time {
	ts, err := time.Parse(layout, v)
	require.NoError(t, err)
	require.False(t, ts.IsZero())
	return ts
}

func mustTSDBOpen(t *testing.T, bucketDir string) *tsdb.DB {
	db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}
