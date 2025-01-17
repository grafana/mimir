// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
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
		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
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
	require.Eventually(t, func() bool { return kafkaCommits.Load() >= 3 }, 15*time.Second, 100*time.Millisecond, "expected kafka commits")

	// Because there is a commit, on startup, block-builder must consume samples only after the commit.
	expSamples := producedSamples[1+(len(producedSamples)/2):]

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		expSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

func TestBlockBuilder_StartWithExistingCommit_PullMode(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderPullModeConfig(t, kafkaAddr)

	// Producing some records
	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	var producedSamples []mimirpb.Sample
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
	for kafkaRecTime.Before(cycleEndStartup) {
		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
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

	scheduler := &mockSchedulerClient{}
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-4898",
			Epoch: 90000,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      0,
			StartOffset:    commitRec.Offset + 1,
			EndOffset:      3000000,
			CommitRecTs:    commitRec.Timestamp,
			LastSeenOffset: lastRec.Offset,
			LastBlockEndTs: blockEnd,
			CycleEndTs:     cycleEndStartup,
			CycleEndOffset: int64(len(producedSamples)),
		},
	)

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, scheduler)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Wait for end of the cycles. We expect at least several cycles because of how the pushed records were structured.
	require.Eventually(t, func() bool {
		_, _, completeJobCalls, _ := scheduler.callCounts()
		return completeJobCalls > 0
	}, 5*time.Second, 100*time.Millisecond, "expected job completion")

	require.EqualValues(t,
		[]schedulerpb.JobKey{{Id: "test-job-4898", Epoch: 90000}},
		scheduler.completeJobCalls,
	)

	// Because there is a commit, on startup, block-builder must consume samples only after the commit.
	expSamples := producedSamples[1+(len(producedSamples)/2):]

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
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
		produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
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

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// There should be no samples in the tsdb.
	compareQuery(t,
		db,
		nil,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

// When there is no commit on startup, and the first cycleEnd is at T, and all the samples are from before T-lookback, then
// the first consumption cycle skips all the records.
func TestBlockBuilder_StartWithLookbackOnNoCommit_PullMode(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)

	cfg, overrides := blockBuilderPullModeConfig(t, kafkaAddr)
	cfg.LookbackOnNoCommit = 3 * time.Hour

	// Producing some records, all before LookbackOnNoCommit
	var firstRecordTime time.Time
	kafkaRecTime := time.Now().Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
	for range 3 {
		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
		if firstRecordTime.IsZero() {
			firstRecordTime = kafkaRecTime
		}
		produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
	}

	fallback := time.Now().Add(-cfg.LookbackOnNoCommit)

	scheduler := &mockSchedulerClient{}
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-4898",
			Epoch: 90001,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      0,
			StartOffset:    1,
			EndOffset:      5,
			CommitRecTs:    fallback,
			LastSeenOffset: 0,
			LastBlockEndTs: time.UnixMilli(0),
			CycleEndTs:     kafkaRecTime,
			CycleEndOffset: 3,
		},
	)

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, scheduler)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	require.Eventually(t, func() bool {
		return bb.jobIteration.Load() > 0
	}, 5*time.Second, 100*time.Millisecond, "expected job completion")

	require.EqualValues(t,
		[]schedulerpb.JobKey{{Id: "test-job-4898", Epoch: 90001}},
		scheduler.completeJobCalls,
	)

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// There should be no samples in the tsdb.
	compareQuery(t,
		db,
		nil,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

// When there are no records to consume before the last cycle section, the whole cycle should bail.
func TestBlockBuilder_ReachHighWatermarkBeforeLastCycleSection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	kafkaCluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)
	cfg, overrides := blockBuilderConfig(t, kafkaAddr)

	// Producing backlog of records in partition 0.
	// For this test, the last-produced record must belong to the second to last cycle's section; i.e. the last section has nothing to consume.
	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	var producedSamples []mimirpb.Sample
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-5 * time.Hour).Add(29 * time.Minute)
	lastKafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeInterval)
	for kafkaRecTime.Before(lastKafkaRecTime) {
		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
		producedSamples = append(producedSamples, samples...)

		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
	}
	require.NotEmpty(t, producedSamples)

	// Produce extra record to partition 1 to verify that block-builder finished the whole cycle and didn't get stuck consuming partition 0.
	{
		const partition = 1
		kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-time.Minute)
		samples := floatSample(kafkaRecTime.UnixMilli(), 100)
		val := createWriteRequest(t, "1", samples, nil)
		produceRecords(ctx, t, kafkaClient, kafkaRecTime, "1", testTopic, partition, val)
		producedSamples = append(producedSamples, samples...)
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

	// Wait for the end of all cycles. We expect at least several cycle sections because of how the pushed records were structured.
	require.Eventually(t, func() bool { return kafkaCommits.Load() == 5 }, 20*time.Second, 100*time.Millisecond, "expected kafka commits")

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_blockbuilder_consumer_lag_records The per-topic-partition number of records, instance needs to work through each cycle.
		# TYPE cortex_blockbuilder_consumer_lag_records gauge
		cortex_blockbuilder_consumer_lag_records{partition="0"} 8
		cortex_blockbuilder_consumer_lag_records{partition="1"} 1
	`), "cortex_blockbuilder_consumer_lag_records"))

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		producedSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

// When there are no records to consume before the last cycle section, the whole cycle should bail.
func TestBlockBuilder_ReachHighWatermarkBeforeLastCycleSection_PullMode(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)
	cfg, overrides := blockBuilderPullModeConfig(t, kafkaAddr)

	// Producing backlog of records in partition 0.
	// For this test, the last-produced record must belong to the second to last cycle's section; i.e. the last section has nothing to consume.
	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

	var producedSamples []mimirpb.Sample
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-5 * time.Hour).Add(29 * time.Minute)
	firstKafkaRecTime := kafkaRecTime.Add(-time.Minute)
	lastKafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeInterval)
	for kafkaRecTime.Before(lastKafkaRecTime) {
		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
		producedSamples = append(producedSamples, samples...)

		kafkaRecTime = kafkaRecTime.Add(cfg.ConsumeInterval / 2)
	}
	require.NotEmpty(t, producedSamples)

	var p1RecTime time.Time

	// Produce extra record to partition 1 to verify that block-builder finished the whole cycle and didn't get stuck consuming partition 0.
	{
		const partition = 1
		kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-time.Minute)
		p1RecTime = kafkaRecTime
		samples := floatSample(kafkaRecTime.UnixMilli(), 100)
		val := createWriteRequest(t, "1", samples, nil)
		produceRecords(ctx, t, kafkaClient, kafkaRecTime, "1", testTopic, partition, val)
		producedSamples = append(producedSamples, samples...)
	}

	scheduler := &mockSchedulerClient{}
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-p0-4898",
			Epoch: 90002,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      0,
			StartOffset:    0,
			EndOffset:      int64(len(producedSamples)),
			CommitRecTs:    firstKafkaRecTime,
			LastSeenOffset: 0,
			LastBlockEndTs: time.UnixMilli(0),
			CycleEndTs:     kafkaRecTime,
			CycleEndOffset: int64(len(producedSamples) - 2),
		},
	)
	// A second job to include the extra record in partition 1.
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-p1-4899",
			Epoch: 90070,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      1,
			StartOffset:    0,
			EndOffset:      1,
			CommitRecTs:    p1RecTime,
			LastSeenOffset: 0,
			LastBlockEndTs: time.UnixMilli(0),
			CycleEndTs:     p1RecTime.Add(1 * time.Minute),
			CycleEndOffset: 1,
		},
	)

	reg := prometheus.NewPedanticRegistry()
	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), reg, overrides, scheduler)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Wait for both jobs to complete.
	require.Eventually(t, func() bool {
		return bb.jobIteration.Load() >= 2
	}, 20*time.Second, 100*time.Millisecond, "expected job completion")

	runCalls, getJobCalls, completeCalls, flushCalls := scheduler.callCounts()
	assert.Equal(t, runCalls, 1)
	assert.Equal(t, getJobCalls, 3, "expect 2 completed getJob calls and one in-flight")
	assert.Equal(t, completeCalls, 2)
	assert.Equal(t, flushCalls, 0)

	require.EqualValues(t,
		[]schedulerpb.JobKey{{Id: "test-job-p0-4898", Epoch: 90002}, {Id: "test-job-p1-4899", Epoch: 90070}},
		scheduler.completeJobCalls,
	)

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		producedSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
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
			samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenant, kafkaRecTime)
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
		db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
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

func TestBlockBuilder_WithMultipleTenants_PullMode(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)

	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderPullModeConfig(t, kafkaAddr)

	cycleEndStartup := cycleEndAtStartup(time.Now(), cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)
	kafkaRecTime := cycleEndStartup.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeInterval)
	firstTime := kafkaRecTime

	scheduler := &mockSchedulerClient{}
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-4898",
			Epoch: 90003,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      0,
			StartOffset:    0,
			EndOffset:      30,
			CommitRecTs:    firstTime,
			LastSeenOffset: 0,
			LastBlockEndTs: firstTime,
			CycleEndTs:     cycleEndStartup,
			CycleEndOffset: 30,
		},
	)

	producedPerTenantSamples := make(map[string][]mimirpb.Sample, 0)
	tenants := []string{"1", "2", "3"}

	// Producing some records for multiple tenants
	for range 10 {
		for _, tenant := range tenants {
			samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenant, kafkaRecTime)
			producedPerTenantSamples[tenant] = append(producedPerTenantSamples[tenant], samples...)
		}

		kafkaRecTime = kafkaRecTime.Add(15 * time.Second)
	}

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, scheduler)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Wait for end of the cycles. We expect at least several cycles because of how the pushed records were structured.
	require.Eventually(t, func() bool {
		_, _, completeJobCalls, _ := scheduler.callCounts()
		return completeJobCalls > 0
	}, 5*time.Second, 100*time.Millisecond, "expected job completion")

	require.EqualValues(t,
		[]schedulerpb.JobKey{{Id: "test-job-4898", Epoch: 90003}},
		scheduler.completeJobCalls,
	)

	for _, tenant := range tenants {
		bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant)
		db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
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

		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenantID, kafkaRecTime)
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

		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenantID, inBlockTime, kafkaRecTime)
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

		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenantID, inBlockTime)
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

		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, tenantID, inBlockTime)
		require.Len(t, samples, 1)
		expSamplesPhase2 = append(expSamplesPhase2, samples[0])
	}

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	// We don't want to run the service here. Instead, the test cases below trigger and assert the consumption cycles explicitly.
	require.NoError(t, bb.starting(ctx))
	t.Cleanup(func() {
		require.NoError(t, bb.stoppingStandaloneMode(nil))
	})

	runTest := func(name string, end time.Time, expSamples []mimirpb.Sample) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, bb.nextConsumeCycle(ctx, end))

			bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenantID)
			db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

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
		samples := produceSamples(ctx, t, kafkaClient, 0, kafkaRecTime, "1", kafkaRecTime.Add(-time.Minute))
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
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		producedSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

func produceSamples(ctx context.Context, t *testing.T, kafkaClient *kgo.Client, partition int32, ts time.Time, tenantID string, sampleTs ...time.Time) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, st := range sampleTs {
		samples = append(samples, floatSample(st.UnixMilli(), 1)...)
	}
	val := createWriteRequest(t, tenantID, samples, nil)
	produceRecords(ctx, t, kafkaClient, ts, tenantID, testTopic, partition, val)
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
		wantState      PartitionState
	}{
		{
			name: "no commit",
			lag: kadm.GroupMemberLag{
				Topic:     testTopic,
				Partition: 0,
				Commit:    kadm.Offset{},
			},
			fallbackMillis: testTime.UnixMilli(),
			wantState: PartitionState{
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
			wantState: PartitionState{
				Commit:                testKafkaOffset,
				CommitRecordTimestamp: commitRecTs,
				LastSeenOffset:        lastRecOffset,
				LastBlockEnd:          blockEndTs,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := PartitionStateFromLag(log.NewNopLogger(), tc.lag, tc.fallbackMillis)
			require.Equal(t, tc.wantState, state)
		})
	}
}

func TestPullMode(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	kafkaClient := mustKafkaClient(t, kafkaAddr)
	kafkaClient.AddConsumeTopics(testTopic)

	cfg, overrides := blockBuilderPullModeConfig(t, kafkaAddr)

	startTime := time.Date(2024, 12, 20, 6, 10, 0, 0, time.UTC)
	var expSamples []mimirpb.Sample

	for i := range 5 {
		expSamples = append(expSamples, produceSamples(ctx, t, kafkaClient, 0, startTime, "1",
			startTime.Add(time.Duration(i)*time.Hour),
		)...)
		expSamples = append(expSamples, produceSamples(ctx, t, kafkaClient, 1, startTime, "1",
			// Shift slightly to avoid duplicate timestamps.
			startTime.Add(time.Duration(i)*time.Hour).Add(1*time.Minute),
		)...)
	}

	scheduler := &mockSchedulerClient{}
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-p0-0",
			Epoch: 220,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      0,
			StartOffset:    0,
			EndOffset:      6,
			CommitRecTs:    startTime.Add(-1 * time.Minute),
			LastSeenOffset: 0,
			LastBlockEndTs: startTime.Add(-1 * time.Minute),
			CycleEndTs:     startTime.Add(6 * time.Hour),
			CycleEndOffset: 5,
		},
	)
	scheduler.addJob(
		schedulerpb.JobKey{
			Id:    "test-job-p1-0",
			Epoch: 233,
		},
		schedulerpb.JobSpec{
			Topic:          testTopic,
			Partition:      1,
			StartOffset:    0,
			EndOffset:      6,
			CommitRecTs:    startTime.Add(-1 * time.Minute),
			LastSeenOffset: 0,
			LastBlockEndTs: startTime.Add(-1 * time.Minute),
			CycleEndTs:     startTime.Add(6 * time.Hour),
			CycleEndOffset: 5,
		},
	)

	bb, err := newWithSchedulerClient(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides, scheduler)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	require.Eventually(t, func() bool {
		_, _, completeJobCalls, _ := scheduler.callCounts()
		return completeJobCalls == 2
	}, 5*time.Second, 100*time.Millisecond, "expected to complete two jobs")

	require.EqualValues(t,
		[]schedulerpb.JobKey{{Id: "test-job-p0-0", Epoch: 220}, {Id: "test-job-p1-0", Epoch: 233}},
		scheduler.completeJobCalls,
	)

	bucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, "1")
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	compareQuery(t,
		db,
		expSamples,
		nil,
		labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
	)
}

func blockBuilderPullModeConfig(t *testing.T, addr string) (Config, *validation.Overrides) {
	cfg, overrides := blockBuilderConfig(t, addr)
	cfg.SchedulerConfig = SchedulerConfig{
		Address:        "localhost:099", // Trigger pull mode initialization.
		UpdateInterval: 20 * time.Millisecond,
		MaxUpdateAge:   1 * time.Second,
	}
	return cfg, overrides
}

func mustTimeParse(t *testing.T, layout, v string) time.Time {
	ts, err := time.Parse(layout, v)
	require.NoError(t, err)
	require.False(t, ts.IsZero())
	return ts
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

// addJob adds a job to the fake back-end for this mock scheduler client.
func (m *mockSchedulerClient) addJob(key schedulerpb.JobKey, spec schedulerpb.JobSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs = append(m.jobs, struct {
		key  schedulerpb.JobKey
		spec schedulerpb.JobSpec
	}{key: key, spec: spec})
}

func (m *mockSchedulerClient) callCounts() (int, int, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runCalls, m.getJobCalls, len(m.completeJobCalls), m.closeCalls
}
