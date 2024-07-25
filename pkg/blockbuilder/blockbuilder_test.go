// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	testTopic = "test"
	testGroup = "testgroup"
)

func blockBuilderConfig(t *testing.T, addr string) (Config, *validation.Overrides) {
	cfg := Config{
		ConsumeInterval:       time.Hour,
		ConsumeIntervalBuffer: 15 * time.Minute,
		Kafka: KafkaConfig{
			Address:       addr,
			Topic:         testTopic,
			ClientID:      "1",
			DialTimeout:   10 * time.Second,
			PollTimeout:   500 * time.Millisecond,
			ConsumerGroup: testGroup,
		},
		LookbackOnNoCommit: 12 * time.Hour,
	}

	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	cfg.BlocksStorageConfig.Bucket.StorageBackendConfig.Backend = bucket.Filesystem
	cfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	return cfg, overrides
}

// TestBlockBuilder tests a lot of different cases of BlockBuilder lifecycle.
// It tests for both float and histogram samples while also testing multiple partitions.
// The test is simplified by always producing float samples to partition 0 and histogram samples
// to partition 1. That way, managing and checking timestamps for each partition remains the same.
func TestBlockBuilder(t *testing.T) {
	const (
		userID = "1"
	)

	require.NoError(t, os.Setenv("POD_NAME", "block-builder-0"))

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 2, testTopic)

	type kafkaRecInfo struct {
		sampleTs, recTs time.Time
	}
	var (
		kafkaSamples   []mimirpb.Sample
		kafkaHSamples  []mimirpb.Histogram
		kafkaRecords   []kafkaRecInfo
		cfg, overrides = blockBuilderConfig(t, addr)
		compactCalled  = make(chan struct{}, 10)
		writeClient    = newKafkaProduceClient(t, addr)
		bucketDir      = path.Join(cfg.BlocksStorageConfig.Bucket.Filesystem.Directory, userID)
		kafkaTime      = time.Now().Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
		parentT        = t
	)
	cfg.TotalBlockBuilders = 1
	cfg.TotalPartitions = 2

	bb, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	bb.tsdbBuilder = func() builder {
		testBuilder := newTestTSDBBuilder(cfg, overrides)
		testBuilder.compactFunc = func() {
			compactCalled <- struct{}{}
		}
		return testBuilder
	}

	// Helper functions.
	createAndProduceSample := func(t *testing.T, sampleTs, kafkaRecTs time.Time) {
		samples := floatSample(sampleTs.UnixMilli())
		val := createWriteRequest(t, "", samples, nil)
		produceRecords(ctx, t, writeClient, kafkaRecTs, userID, testTopic, 0, val)

		hSamples := histogramSample(sampleTs.UnixMilli())
		val = createWriteRequest(t, "", nil, hSamples)
		produceRecords(ctx, t, writeClient, kafkaRecTs, userID, testTopic, 1, val)

		kafkaSamples = append(kafkaSamples, samples...)
		kafkaHSamples = append(kafkaHSamples, hSamples...)
		kafkaRecords = append(kafkaRecords, kafkaRecInfo{sampleTs, kafkaRecTs})
	}

	filterSamples := func(s []mimirpb.Sample, maxTime time.Time) []mimirpb.Sample {
		maxT := maxTime.UnixMilli()
		var res []mimirpb.Sample
		for _, sample := range s {
			if sample.TimestampMs < maxT {
				res = append(res, sample)
			}
		}
		return res
	}

	filterHistogramSamples := func(s []mimirpb.Histogram, maxTime time.Time) []mimirpb.Histogram {
		maxT := maxTime.UnixMilli()
		var res []mimirpb.Histogram
		for _, sample := range s {
			if sample.Timestamp < maxT {
				res = append(res, sample)
			}
		}
		return res
	}

	collectCompacts := func() int {
		counts := 0
		done := false
		for !done {
			select {
			case <-compactCalled:
				counts++
			case <-time.After(3 * time.Second):
				done = true
			}
		}
		return counts
	}

	dbOnBucketDir := func(t *testing.T) *tsdb.DB {
		db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}

	numBlocksInBucket := func(t *testing.T) int {
		files, err := os.ReadDir(bucketDir)
		require.NoError(t, err)
		count := 0
		for _, f := range files {
			if !f.IsDir() {
				continue
			}
			_, err := ulid.ParseStrict(f.Name())
			if err == nil {
				count++
			}
		}
		return count
	}

	getCommitMeta := func(t *testing.T, part int32) (int64, int64, int64) {
		offsets, err := kadm.NewClient(bb.kafkaClients[part]).FetchOffsetsForTopics(ctx, testGroup, testTopic)
		require.NoError(t, err)
		offset, ok := offsets.Lookup(testTopic, part)
		require.True(t, ok)
		commitRecTs, lastRecTs, blockEnd, err := unmarshallCommitMeta(offset.Metadata)
		require.NoError(t, err)
		return commitRecTs, lastRecTs, blockEnd
	}

	checkCommit := func(t *testing.T, cycleEnd time.Time) {
		// Check if the kafka commit is as expected.
		var expCommitTs, expLastTs int64
		expBlockMax := cycleEnd.Truncate(cfg.ConsumeInterval).UnixMilli()
		commitTimeFinalised := false
		for _, r := range kafkaRecords {
			// The last record before the cycleEnd is the last seen timestamp.
			if r.recTs.Before(cycleEnd) {
				expLastTs = r.recTs.UnixMilli()
			}

			// The commit timestamp is timestamp until which all samples are in a block.
			// If there is a sample after the expected blockMax, then the commit timestamp
			// is of the record one before it.
			if r.sampleTs.UnixMilli() >= expBlockMax {
				commitTimeFinalised = true
			}
			if !commitTimeFinalised && r.sampleTs.UnixMilli() < expBlockMax {
				expCommitTs = r.recTs.UnixMilli()
			}
		}

		for _, part := range []int32{0, 1} {
			commitRecTs, lastRecTs, blockEnd := getCommitMeta(t, part)
			require.Equal(t, expCommitTs, commitRecTs)
			require.Equal(t, expLastTs, lastRecTs)
			require.Equal(t, expBlockMax, blockEnd)
		}
	}

	nextConsumeCycleWithChecks := func(t *testing.T, cycleEnd time.Time, expBlocksCreated, expCompacts int) {
		blocksBefore := numBlocksInBucket(t)

		require.NoError(t, bb.NextConsumeCycle(ctx, cycleEnd))
		require.Equal(t, expCompacts, collectCompacts(), "mismatch in compact calls")

		blocksAfter := numBlocksInBucket(t)
		require.Equal(t, expBlocksCreated, blocksAfter-blocksBefore, "mismatch in blocks created")

		checkCommit(t, cycleEnd)
	}

	t.Run("starting fresh with existing data but no kafka commit", func(t *testing.T) {
		// LookbackOnNoCommit is 12h, so this sample should be skipped.
		oldTime := time.Now().Truncate(cfg.ConsumeInterval).Add(-13 * time.Hour)
		createAndProduceSample(t, oldTime.Add(-time.Minute), oldTime)
		kafkaSamples = kafkaSamples[:len(kafkaSamples)-1]
		kafkaHSamples = kafkaHSamples[:len(kafkaHSamples)-1]
		cycleEOnStart := cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)

		for i := int64(0); i < 12; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 2)
			createAndProduceSample(t, kafkaTime.Add(-time.Minute), kafkaTime)
		}

		require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
		parentT.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
		})

		// Since there was no commit record, with LookbackOnNoCommit as 12h, we will have
		// 12 compact calls per partition. One per 1h.
		for want := 22; want > 0; want-- {
			select {
			case <-compactCalled:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		compacts := collectCompacts()
		require.True(t, compacts <= 2)

		checkCommit(t, cycleEOnStart)

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)),
			filterHistogramSamples(kafkaHSamples, cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)),
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	var cycleEnd time.Time
	t.Run("when there is some lag", func(t *testing.T) {
		//t.Skip()
		// Add samples worth at least 3 cycles.
		for i := int64(0); i < 6; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 2)
			createAndProduceSample(t, kafkaTime.Add(-time.Minute), kafkaTime)
		}

		cycleEnd = kafkaTime.Add(-cfg.ConsumeInterval).Truncate(cfg.ConsumeInterval).Add(cfg.ConsumeInterval + cfg.ConsumeIntervalBuffer)
		require.NoError(t, bb.NextConsumeCycle(ctx, cycleEnd))

		// We want number of cycles to be at least 3 per partition. But not more than 4 because of any
		// time calculation differences because of when the test was called.
		compacts := collectCompacts()
		require.GreaterOrEqual(t, compacts, 6)
		require.LessOrEqual(t, compacts, 8)

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			filterHistogramSamples(kafkaHSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("normal case of no lag", func(t *testing.T) {
		//t.Skip()
		for i := int64(0); i < 3; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
			createAndProduceSample(t, kafkaTime.Add(-time.Minute), kafkaTime)
		}

		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
		// 1 per partition.
		nextConsumeCycleWithChecks(t, cycleEnd, 2, 2)

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			filterHistogramSamples(kafkaHSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("out of order w.r.t. old cycle and future record with valid sample", func(t *testing.T) {
		//t.Skip()
		kafkaTime = cycleEnd

		// Out of order sample w.r.t. samples in last cycle. But for this cycle,
		// the TSDB starts fresh. So in terms of the actual block building, it will be
		// taken as in-order. Depending on if the out-of-order and in-order sample below
		// crosses the 2h block boundary, we expect either 1 or 2 blocks.
		oooSampleTime := kafkaTime.Add(-time.Hour)
		createAndProduceSample(t, oooSampleTime, kafkaTime)

		// In-order sample w.r.t. last consume cycle.
		kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
		inOrderSampleTime := kafkaTime.Add(-time.Minute)
		createAndProduceSample(t, inOrderSampleTime, kafkaTime)

		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)

		// Sample that is not a part of next consume cycle (a future sample) but the kafka
		// record falls in this cycle. So this sample should not go in this cycle.
		kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
		createAndProduceSample(t, cycleEnd.Add(cfg.ConsumeInterval), kafkaTime)
		// In-order sample falls within the next consume cycle but the kafka record time does not
		// fall within the next consume cycle. So this sample should not go in the next cycle.
		kafkaTime = cycleEnd.Add(time.Minute)
		createAndProduceSample(t, inOrderSampleTime.Add(time.Minute), kafkaTime)

		t.Run("consume only out of order and in-order", func(t *testing.T) {
			// 1 per partition.
			expBlockCreation := 2
			if inOrderSampleTime.Truncate(2*time.Hour).Compare(oooSampleTime.Truncate(2*time.Hour)) != 0 {
				// The samples cross the 2h boundary. 2 per partition.
				expBlockCreation = 4
			}
			nextConsumeCycleWithChecks(t, cycleEnd, expBlockCreation, 2)

			compareQuery(t,
				dbOnBucketDir(t),
				kafkaSamples[:len(kafkaSamples)-2], // Don't expect the last two sample.
				kafkaHSamples[:len(kafkaHSamples)-2],
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})

		t.Run("future record", func(t *testing.T) {
			// The sample from above which was in-order but the kafka record was in future
			// should get consumed in this cycle. The other sample that is still in the future
			// should not be consumed.
			cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
			// 2 compact calls per partition because the cycleEnd lags with the committed record because of future sample.
			nextConsumeCycleWithChecks(t, cycleEnd, 2, 4)

			// The second to last sample in kafkaSamples is the one that is still in the future.
			// So we exclude that.
			var expSamples []mimirpb.Sample
			expSamples = append(expSamples, kafkaSamples[:len(kafkaSamples)-2]...)
			expSamples = append(expSamples, kafkaSamples[len(kafkaSamples)-1])

			var expHSamples []mimirpb.Histogram
			expHSamples = append(expHSamples, kafkaHSamples[:len(kafkaHSamples)-2]...)
			expHSamples = append(expHSamples, kafkaHSamples[len(kafkaHSamples)-1])
			compareQuery(t,
				dbOnBucketDir(t),
				expSamples,
				expHSamples,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})

		t.Run("future sample", func(t *testing.T) {
			// The future sample gets consumed here. This also tests the case
			// where even though the kafka record after this was processed completely,
			// we still go back to the last kafka commit and consume the sample
			// that happened to be in the future.
			cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
			// 3 compact calls per partition because the cycleEnd lags with the committed record because of future sample.
			nextConsumeCycleWithChecks(t, cycleEnd, 2, 6)

			compareQuery(t,
				dbOnBucketDir(t),
				kafkaSamples,
				kafkaHSamples,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})
	})
}

// Testing block builder starting up with an existing kafka commit.
func TestBlockBuilder_StartupWithExistingCommit(t *testing.T) {
	require.NoError(t, os.Setenv("POD_NAME", "block-builder-0"))

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	logger := test.NewTestingLogger(t)

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, testTopic)
	writeClient := newKafkaProduceClient(t, addr)

	cfg, overrides := blockBuilderConfig(t, addr)
	cfg.TotalPartitions = 1
	cfg.TotalBlockBuilders = 1
	// Producing some records
	kafkaTime := time.Now().Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
	ceStartup := cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)
	var expSamples []mimirpb.Sample
	for i := int64(0); i < 12; i++ {
		kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 2)
		if kafkaTime.After(ceStartup) {
			break
		}
		sampleTs := kafkaTime.Add(-time.Minute)
		samples := floatSample(sampleTs.UnixMilli())
		val := createWriteRequest(t, "", samples, nil)
		produceRecords(ctx, t, writeClient, kafkaTime, "1", testTopic, 0, val)
		expSamples = append(expSamples, samples...)
	}

	// Fetching all the records that were produced in order to choose
	// a record and then commit it.
	opts := []kgo.Opt{
		kgo.ClientID("1"), kgo.SeedBrokers(addr), kgo.ConsumeTopics(testTopic),
		kgo.ConsumerGroup(testGroup),
		kgo.Balancers(kgo.RoundRobinBalancer()),
		kgo.DisableAutoCommit(),
	}
	kc, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	fetches := kc.PollFetches(ctx)
	require.NoError(t, fetches.Err())
	var recs []*kgo.Record
	for it := fetches.RecordIter(); !it.Done(); {
		recs = append(recs, it.Next())
	}
	require.Len(t, recs, len(expSamples))
	// Choosing the midpoint record to commit and as the last seen record as well.
	commitRec := recs[len(recs)/2]
	lastRec := commitRec
	blockEnd := commitRec.Timestamp.Truncate(cfg.ConsumeInterval).Add(cfg.ConsumeInterval)

	meta := marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastRec.Timestamp.UnixMilli(), blockEnd.UnixMilli())
	err = commitRecord(ctx, log.NewNopLogger(), kc, testGroup, commitRec, meta)
	require.NoError(t, err)
	kc.CloseAllowingRebalance()
	// Because there is a commit, on startup, the block builder should consume samples only after the commit.
	expSamples = expSamples[1+(len(expSamples)/2):]

	bb, err := New(cfg, logger, prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)
	compactCalled := make(chan struct{}, 10)
	bb.tsdbBuilder = func() builder {
		testBuilder := newTestTSDBBuilder(cfg, overrides)
		testBuilder.compactFunc = func() {
			compactCalled <- struct{}{}
		}
		return testBuilder
	}
	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// We expect at least N cycles for a lagging block-builder to catch up from the last commit to the partition's high watermark.
	compacts := 4
	if len(recs) != 12 {
		compacts = 3
	}
	for i := 0; i < compacts; i++ {
		<-compactCalled
	}

	bucketDir := path.Join(cfg.BlocksStorageConfig.Bucket.Filesystem.Directory, "1")
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

func produceRecords(
	ctx context.Context,
	t *testing.T,
	writeClient *kgo.Client,
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
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())
	return produceResult
}

func newKafkaProduceClient(t *testing.T, addrs ...string) *kgo.Client {
	writeClient, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		// We will choose the partition of each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}

type testTSDBBuilder struct {
	tsdbBuilder *tsdbBuilder
	procFunc    func(rec *kgo.Record, blockMin, blockMax int64, recordProcessedBefore bool)
	compactFunc func()
}

func newTestTSDBBuilder(cfg Config, limits *validation.Overrides) testTSDBBuilder {
	return testTSDBBuilder{
		tsdbBuilder: newTSDBBuilder(log.NewNopLogger(), limits, cfg.BlocksStorageConfig),
	}
}

func (t testTSDBBuilder) process(ctx context.Context, rec *kgo.Record, blockMin, blockMax int64, recordProcessedBefore bool) (_ bool, err error) {
	ok, err := t.tsdbBuilder.process(ctx, rec, blockMin, blockMax, recordProcessedBefore)
	if t.procFunc != nil {
		t.procFunc(rec, blockMin, blockMax, recordProcessedBefore)
	}
	return ok, err
}

func (t testTSDBBuilder) compactAndUpload(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) (int, error) {
	numBlocks, err := t.tsdbBuilder.compactAndUpload(ctx, blockUploaderForUser)
	if t.compactFunc != nil {
		t.compactFunc()
	}
	return numBlocks, err
}

func (t testTSDBBuilder) close() error {
	return nil
}

func TestKafkaCommitMetaMarshalling(t *testing.T) {
	v1 := int64(892734)
	v2 := int64(598237948)
	v3 := int64(340237948)

	o1, o2, o3, err := unmarshallCommitMeta(marshallCommitMeta(v1, v2, v3))
	require.NoError(t, err)
	require.Equal(t, v1, o1)
	require.Equal(t, v2, o2)
	require.Equal(t, v3, o3)

	// Unsupported version
	_, _, _, err = unmarshallCommitMeta("2,2,3,4")
	require.Error(t, err)
	require.Equal(t, "unsupported commit meta version 2", err.Error())

	// Error parsing
	_, _, _, err = unmarshallCommitMeta("1,3,4")
	require.Error(t, err)
}

func TestBlockBuilderMultiTenancy(t *testing.T) {
	require.NoError(t, os.Setenv("POD_NAME", "block-builder-0"))

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, testTopic)
	writeClient := newKafkaProduceClient(t, addr)

	var (
		cfg, overrides = blockBuilderConfig(t, addr)
		cycleEnd       = cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)
		kafkaTime      = cycleEnd.Truncate(cfg.ConsumeInterval).Add(-cfg.ConsumeInterval)
		expSamples     = map[string][]mimirpb.Sample{}
		uids           = []string{"1", "2", "3"}
	)
	cfg.TotalPartitions = 1
	cfg.TotalBlockBuilders = 1
	// Producing some records for multiple tenants
	for i := 0; i < 10; i++ {
		sampleTs := kafkaTime
		samples := floatSample(sampleTs.UnixMilli())
		for _, uid := range uids {
			val := createWriteRequest(t, uid, samples, nil)
			produceRecords(ctx, t, writeClient, kafkaTime, uid, testTopic, 0, val)
			expSamples[uid] = append(expSamples[uid], samples...)
		}
		kafkaTime = kafkaTime.Add(15 * time.Second)
	}

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)
	compactCalled := make(chan struct{}, 1)
	bb.tsdbBuilder = func() builder {
		testBuilder := newTestTSDBBuilder(cfg, overrides)
		testBuilder.compactFunc = func() {
			compactCalled <- struct{}{}
		}
		return testBuilder
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Wait for all compacts.
	done := false
	for !done {
		select {
		case <-compactCalled:
		case <-time.After(3 * time.Second):
			done = true
		}
	}

	for _, uid := range uids {
		bucketDir := path.Join(cfg.BlocksStorageConfig.Bucket.Filesystem.Directory, uid)
		db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })

		compareQuery(t,
			db,
			expSamples[uid],
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	}
}
