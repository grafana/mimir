package blockbuilder

import (
	"context"
	"errors"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
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

func TestBlockBuilder_BuildBlocks(t *testing.T) {
	const (
		testTopic     = "test"
		numPartitions = 2
		userID        = "1"
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, numPartitions, testTopic)
	writeClient := newKafkaProduceClient(t, addr)

	cfg := blockBuilderConfig(t, addr, testTopic)

	var expSamples []mimirpb.Sample

	testEpoch := time.Now().Truncate(cfg.ConsumeInterval).Add(-6 * time.Hour).Add(30 * time.Minute)
	for i := int64(0); i < 6; i++ {
		ts := testEpoch.Add(-time.Minute)
		samples := floatSample(ts.UnixMilli())
		val := createWriteRequest(t, samples, nil)
		produceRecords(t, ctx, writeClient, testEpoch, userID, testTopic, int32(i%numPartitions), val)
		testEpoch = testEpoch.Add(time.Hour)
		expSamples = append(expSamples, samples...)
	}

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)

	bb, err := New(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry(), overrides)
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

	// Since there was no commit record, the first consume cycle will consume everything
	// in one go. So each partition will have one compact call (although they will produce
	// more than one block).
	for want := numPartitions; want > 0; want-- {
		select {
		case <-compactCalled:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	bucketDir := path.Join(cfg.BlocksStorageConfig.Bucket.Filesystem.Directory, userID)
	newDB, err := tsdb.Open(bucketDir, log.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	// Check correctness of samples in the blocks.
	// TODO(codesome): adjust expSamples because not all samples might be processed
	// in first consumption based on the timestamp.
	compareQuery(t, newDB, expSamples, nil, labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"))

	//// Add samples for T+3h and trigger the cycle.
	//var lastProducedOffest int64
	//for i := 0; i < 10; i++ {
	//	ts := testEpoch.Add(3 * time.Hour)
	//	val := createWriteRequest(t, floatSample(ts.UnixMilli()), nil)
	//	// these samples are only for first partition
	//	produceResult := produceRecords(t, ctx, writeClient, ts, userID, testTopic, 1, val)
	//	lastProducedOffest = produceResult[0].Record.Offset
	//}
	//
	//cycleEnd := testEpoch.Add(4 * time.Hour)
	//err = bb.nextConsumeCycle(ctx, cycleEnd)
	//require.NoError(t, err)
	//
	//// Assert that one block was compacted (a block per partition per cycle's bounds).
	//select {
	//case <-compactCalled:
	//case <-ctx.Done():
	//	t.Fatal(ctx.Err())
	//}
	//
	//offsets, err := kadm.NewClient(writeClient).ListCommittedOffsets(ctx, testTopic)
	//require.NoError(t, err)
	//offset, ok := offsets.Lookup(testTopic, 1)
	//require.True(t, ok)
	//require.Equal(t, lastProducedOffest+1, offset.Offset) // +1 because lastProducedOffset points at already consumed record
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

	err = commitRecord(ctx, log.NewNopLogger(), kc, testTopic, commitRec, lastRec, blockEnd)
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
