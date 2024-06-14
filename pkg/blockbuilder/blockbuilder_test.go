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
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestBlockBuilder_SinglePartition(t *testing.T) {
	const (
		testTopic = "test"
		userID    = "1"
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, testTopic)

	var (
		kafkaSamples  []mimirpb.Sample
		cfg           = blockBuilderConfig(t, addr, testTopic)
		compactCalled = make(chan struct{}, 10)
		writeClient   = newKafkaProduceClient(t, addr)
		bucketDir     = path.Join(cfg.BlocksStorageConfig.Bucket.Filesystem.Directory, userID)
		kafkaTime     = time.Now().Truncate(cfg.ConsumeInterval).Add(-7 * time.Hour).Add(29 * time.Minute)
		parentT       = t
	)

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	bb.tsdbBuilder = func() builder {
		testBuilder := newTestTSDBBuilder(cfg, overrides)
		testBuilder.compactFunc = func() {
			compactCalled <- struct{}{}
		}
		return testBuilder
	}

	// Helper functions.
	var (
		//createBlockBuilder = func() *BlockBuilder {
		//	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
		//	require.NoError(t, err)
		//
		//	bb.tsdbBuilder = func() builder {
		//		testBuilder := newTestTSDBBuilder(cfg, overrides)
		//		testBuilder.compactFunc = func() {
		//			compactCalled <- struct{}{}
		//		}
		//		return testBuilder
		//	}
		//
		//	return bb
		//}

		createAndProduceSample = func(sampleTs, kafkaRecTs time.Time) {
			samples := floatSample(sampleTs.UnixMilli())
			val := createWriteRequest(t, samples, nil)
			produceRecords(t, ctx, writeClient, kafkaRecTs, userID, testTopic, 0, val)
			kafkaSamples = append(kafkaSamples, samples...)
		}

		filterSamples = func(s []mimirpb.Sample, maxTime time.Time) []mimirpb.Sample {
			maxT := maxTime.UnixMilli()
			var res []mimirpb.Sample
			for _, sample := range s {
				if sample.TimestampMs < maxT {
					res = append(res, sample)
				}
			}
			return res
		}

		collectCompacts = func() int {
			counts := 0
			done := false
			for !done {
				select {
				case <-compactCalled:
					counts++
				default:
					done = true
				}
			}
			return counts
		}

		dbOnBucketDir = func(givenT *testing.T) *tsdb.DB {
			db, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
			require.NoError(givenT, err)
			givenT.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}

		numBlocksInBucket = func(givenT *testing.T) int {
			files, err := os.ReadDir(bucketDir)
			require.NoError(givenT, err)
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

		//getCommitMeta = func() (int64, int64, int64) {
		//	offsets, err := kadm.NewClient(bb.kafkaClient).FetchOffsetsForTopics(ctx, cfg.Kafka.ConsumerGroup, testTopic)
		//	require.NoError(t, err)
		//	offset, ok := offsets.Lookup(testTopic, 0)
		//	require.True(t, ok)
		//	commitRecTs, lastRecTs, blockEnd, err := unmarshallCommitMeta(offset.Metadata)
		//	require.NoError(t, err)
		//	return commitRecTs, lastRecTs, blockEnd
		//}

		//bb = createBlockBuilder()

		nextConsumeCycleWithChecks = func(cycleEnd time.Time, expBlocksCreated, expCompacts int) {
			blocksBefore := numBlocksInBucket(t)
			require.NoError(t, bb.nextConsumeCycle(ctx, cycleEnd))
			require.Equal(t, expCompacts, collectCompacts(), "mismatch in compact calls")
			blocksAfter := numBlocksInBucket(t)
			require.Equal(t, expBlocksCreated, blocksAfter-blocksBefore, "mismatch in blocks created")
		}
	)

	t.Run("starting fresh with existing data but no kafka commit", func(t *testing.T) {
		for i := int64(0); i < 12; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 2)
			createAndProduceSample(kafkaTime.Add(-time.Minute), kafkaTime)
		}

		require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
		parentT.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
		})

		// Since there was no commit record, the first consume cycle will consume everything
		// in one go. So each partition will have one compact call (although they will produce
		// more than one block).
		select {
		case <-compactCalled:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEndAtStartup(cfg.ConsumeInterval, cfg.ConsumeIntervalBuffer)),
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)

		// TODO(codesome): check that commit has the right information.
	})

	var cycleEnd time.Time
	t.Run("when there is some lag after startup", func(t *testing.T) {
		// Add samples worth at least 3 cycles.
		for i := int64(0); i < 6; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 2)
			createAndProduceSample(kafkaTime.Add(-time.Minute), kafkaTime)
		}

		cycleEnd = kafkaTime.Add(-cfg.ConsumeInterval).Truncate(cfg.ConsumeInterval).Add(cfg.ConsumeInterval + cfg.ConsumeIntervalBuffer)

		require.NoError(t, bb.nextConsumeCycle(ctx, cycleEnd))

		// We want number of cycles to be at least 3 per partition. But not more than 4 because of any
		// time calculation differences because of when the test was called.
		compacts := collectCompacts()
		require.GreaterOrEqual(t, compacts, 3)
		require.LessOrEqual(t, compacts, 4)

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("restart scenario when there is a kafka commit", func(t *testing.T) {
		// TODO(codesome): likely requires a new test because startup uses wall clock
	})

	t.Run("normal case of no lag", func(t *testing.T) {
		for i := int64(0); i < 3; i++ {
			kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
			createAndProduceSample(kafkaTime.Add(-time.Minute), kafkaTime)
		}

		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
		nextConsumeCycleWithChecks(cycleEnd, 1, 1)

		compareQuery(t,
			dbOnBucketDir(t),
			filterSamples(kafkaSamples, cycleEnd.Add(-cfg.ConsumeIntervalBuffer)),
			nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	})

	t.Run("out of order w.r.t. old cycle and future record with valid sample", func(t *testing.T) {
		kafkaTime = cycleEnd

		// Out of order sample w.r.t. samples in last cycle. But for this cycle,
		// the TSDB starts fresh. So in terms of the actual block building, it will be
		// taken as in-order. Depending on if the out-of-order and in-order sample below
		// crosses the 2h block boundary, we expect either 1 or 2 blocks.
		oooSampleTime := kafkaTime.Add(-time.Hour)
		createAndProduceSample(oooSampleTime, kafkaTime)

		// In-order sample w.r.t. last consume cycle.
		kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
		inOrderSampleTime := kafkaTime.Add(-time.Minute)
		createAndProduceSample(inOrderSampleTime, kafkaTime)

		cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)

		// Sample that is not a part of next consume cycle (a future sample) but the kafka
		// record falls in this cycle. So this sample should not go in this cycle.
		kafkaTime = kafkaTime.Add(cfg.ConsumeInterval / 10)
		createAndProduceSample(cycleEnd.Add(cfg.ConsumeInterval), kafkaTime)
		// In-order sample falls within the next consume cycle but the kafka record time does not
		// fall within the next consume cycle. So this sample should not go in the next cycle.
		kafkaTime = cycleEnd.Add(time.Minute)
		createAndProduceSample(inOrderSampleTime.Add(time.Minute), kafkaTime)

		t.Run("consume only out of order nad in-order", func(t *testing.T) {
			expBlockCreation := 1
			if inOrderSampleTime.Truncate(2*time.Hour).Compare(oooSampleTime.Truncate(2*time.Hour)) != 0 {
				// The samples cross the 2h boundary.
				expBlockCreation = 2
			}
			nextConsumeCycleWithChecks(cycleEnd, expBlockCreation, 1)

			compareQuery(t,
				dbOnBucketDir(t),
				kafkaSamples[:len(kafkaSamples)-2], // Don't expect the last two sample.
				nil,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})

		t.Run("future record", func(t *testing.T) {
			// The sample from above which was in-order but the kafka record was in future
			// should get consumed in this cycle. The other sample that is still in the future
			// should not be consumed.
			cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
			// 2 compact calls because the cycleEnd lags with the committed record because of future sample.
			nextConsumeCycleWithChecks(cycleEnd, 1, 2)

			// The second to last sample in kafkaSamples is the one that is still in the future.
			// So we exclude that.
			var expSamples []mimirpb.Sample
			expSamples = append(expSamples, kafkaSamples[:len(kafkaSamples)-2]...)
			expSamples = append(expSamples, kafkaSamples[len(kafkaSamples)-1])
			compareQuery(t,
				dbOnBucketDir(t),
				expSamples,
				nil,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})

		t.Run("future sample", func(t *testing.T) {
			// The future sample gets consumed here. This also tests the case
			// where even though the kafka record after this was processed completely,
			// we still go back to the last kafka commit and consume the sample
			// that happened to be in the future.
			cycleEnd = cycleEnd.Add(cfg.ConsumeInterval)
			// 3 compact calls because the cycleEnd lags with the committed record because of future sample.
			nextConsumeCycleWithChecks(cycleEnd, 1, 3)

			compareQuery(t,
				dbOnBucketDir(t),
				kafkaSamples,
				nil,
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			)
		})

	})
}

func blockBuilderConfig(t *testing.T, addr, topic string) Config {
	cfg := Config{
		ConsumeInterval:       time.Hour,
		ConsumeIntervalBuffer: 15 * time.Minute,
		Kafka: KafkaConfig{
			Address:       addr,
			Topic:         topic,
			ClientID:      "1",
			DialTimeout:   10 * time.Second,
			ConsumerGroup: "testgroup",
		},
	}

	cfg.BlocksStorageConfig.TSDB.Dir = t.TempDir()
	cfg.BlocksStorageConfig.Bucket.StorageBackendConfig.Backend = bucket.Filesystem
	cfg.BlocksStorageConfig.Bucket.Filesystem.Directory = t.TempDir()

	return cfg
}

func produceRecords(t *testing.T, ctx context.Context, writeClient *kgo.Client, ts time.Time, userID, topic string, part int32, val []byte) kgo.ProduceResults {
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

func (t testTSDBBuilder) compactAndUpload(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error {
	err := t.tsdbBuilder.compactAndUpload(ctx, blockUploaderForUser)
	if t.compactFunc != nil {
		t.compactFunc()
	}
	return err
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

func TestKafkaCommitMetadata(t *testing.T) {
	const (
		testTopic = "test"
		testGroup = "testgroup"
		numRecs   = 10
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, testTopic)
	writeClient := newKafkaProduceClient(t, addr)

	for i := int64(0); i < numRecs; i++ {
		val := createWriteRequest(t, floatSample(i), nil)
		produceRecords(t, ctx, writeClient, time.Now(), "1", testTopic, 0, val)
	}

	opts := []kgo.Opt{
		kgo.ClientID("1"),
		kgo.SeedBrokers(addr),
		kgo.DialTimeout(10 * time.Second),
		kgo.ConsumeTopics(testTopic),
		kgo.ConsumerGroup(testGroup),
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
	require.Len(t, recs, numRecs)

	commitRec := recs[numRecs/2]
	lastRec := recs[numRecs-1]
	blockEnd := time.Now().Truncate(1 * time.Hour).UnixMilli()

	err = commitRecord(ctx, log.NewNopLogger(), kc, testTopic, commitRec, lastRec.Timestamp.UnixMilli(), blockEnd)
	require.NoError(t, err)

	// Checking the commit
	offsets, err := kadm.NewClient(kc).FetchOffsetsForTopics(ctx, testGroup, testTopic)
	require.NoError(t, err)

	offset, ok := offsets.Lookup(testTopic, 0)
	require.True(t, ok)
	require.Equal(t, kadm.Offset{
		Topic:       testTopic,
		Partition:   0,
		At:          commitRec.Offset + 1,
		LeaderEpoch: commitRec.LeaderEpoch,
		Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastRec.Timestamp.UnixMilli(), blockEnd),
	}, offset.Offset)
}
