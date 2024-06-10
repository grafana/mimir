package blockbuilder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestBlockBuilder_BuildBlocks(t *testing.T) {
	const (
		testTopic     = "test"
		numPartitions = 2

		userID = "1"
	)

	testEpoch := time.Now().Truncate(time.Hour).Add(-12 * time.Hour)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(numPartitions, testTopic),
	)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	writeClient := newKafkaProduceClient(t, addrs...)

	// Prepopulate 2 groups of samples for T+1h and T+2h.
	for i := int64(0); i < 10; i++ {
		ts := testEpoch.Add(time.Duration(i/5) * time.Hour)
		val := createWriteRequest(t, floatSample(ts.UnixMilli()), nil)
		rec := &kgo.Record{
			Timestamp: ts,
			Key:       []byte(userID),
			Value:     val,
			Topic:     testTopic,
			Partition: int32(i % numPartitions), // samples in this batch are split between N partitions
		}
		produceResult := writeClient.ProduceSync(ctx, rec)
		require.NoError(t, produceResult.FirstErr())
	}

	cfg := Config{
		ConsumeInterval:       time.Hour,
		ConsumeIntervalBuffer: time.Minute,
		Kafka: KafkaConfig{
			Address:       addrs[0],
			Topic:         testTopic,
			ClientID:      "1",
			DialTimeout:   10 * time.Second,
			ConsumerGroup: "testgroup",
		},
		BlocksStorageConfig: mimir_tsdb.BlocksStorageConfig{
			TSDB: mimir_tsdb.TSDBConfig{
				Dir: t.TempDir(),
			},
		},
	}
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)

	bb, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	compactCalled := make(chan struct{}, 10)
	testBuilder := testTSDBBuilder{
		procFunc: func(ctx context.Context, rec *kgo.Record, blockMin, blockMax int64, _ bool) (bool, error) {
			return true, nil
		},
		compactFunc: func(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error {
			compactCalled <- struct{}{}
			return nil
		},
	}

	bb.tsdbBuilder = func() builder {
		return testBuilder
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, bb))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, bb))
	})

	// Assert that N total blocks were compacted (a block per partition per cycle's bounds).
	for want := 4; want > 0; want-- {
		select {
		case <-compactCalled:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	// Add samples for T+3h and trigger the cycle.
	var lastProducedOffest int64
	for i := 0; i < 10; i++ {
		ts := testEpoch.Add(3 * time.Hour)
		val := createWriteRequest(t, floatSample(ts.UnixMilli()), nil)
		rec := &kgo.Record{
			Timestamp: ts,
			Key:       []byte(userID),
			Value:     val,
			Topic:     testTopic,
			Partition: 1, // these samples are only for first partition
		}
		produceResult := writeClient.ProduceSync(ctx, rec)
		require.NoError(t, produceResult.FirstErr())

		lastProducedOffest = produceResult[0].Record.Offset
	}

	cycleEnd := testEpoch.Add(4 * time.Hour)
	err = bb.nextConsumeCycle(ctx, cycleEnd)
	require.NoError(t, err)

	// Assert that one block was compacted (a block per partition per cycle's bounds).
	select {
	case <-compactCalled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	offsets, err := kadm.NewClient(writeClient).ListCommittedOffsets(ctx, testTopic)
	require.NoError(t, err)
	offset, ok := offsets.Lookup(testTopic, 1)
	require.True(t, ok)
	require.Equal(t, lastProducedOffest+1, offset.Offset) // +1 because lastProducedOffset points at already consumed record
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
	procFunc    func(ctx context.Context, rec *kgo.Record, blockMin, blockMax int64, recordProcessedBefore bool) (bool, error)
	compactFunc func(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error
}

func (t testTSDBBuilder) process(ctx context.Context, rec *kgo.Record, blockMin, blockMax int64, recordProcessedBefore bool) (_ bool, err error) {
	return t.procFunc(ctx, rec, blockMin, blockMax, recordProcessedBefore)
}

func (t testTSDBBuilder) compactAndUpload(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error {
	return t.compactFunc(ctx, blockUploaderForUser)
}

func (t testTSDBBuilder) close() error {
	return nil
}
