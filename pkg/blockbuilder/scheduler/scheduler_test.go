package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func mustKafkaClient(t *testing.T, addrs ...string) *kgo.Client {
	writeClient, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.AllowAutoTopicCreation(),
		// We will choose the partition of each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}

func TestMonitor(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	cli := mustKafkaClient(t, kafkaAddr)

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Topic: "ingest",
		},
		BuilderConsumerGroup:   "test-builder",
		SchedulerConsumerGroup: "test-scheduler",
		SchedulingInterval:     1000000 * time.Hour,
	}
	reg := prometheus.NewPedanticRegistry()
	sched, err := New(cfg, test.NewTestingLogger(t), reg)
	sched.kafkaClient = cli
	sched.adminClient = kadm.NewClient(cli)
	require.NoError(t, err)

	// Partition i gets i records.
	for i := int32(0); i < 4; i++ {
		for n := int32(0); n < i; n++ {
			produceResult := cli.ProduceSync(ctx, &kgo.Record{
				Timestamp: time.Unix(int64(i*n), 1),
				Value:     []byte(fmt.Sprintf("value-%d-%d", i, n)),
				Topic:     "ingest",
				Partition: i,
			})
			require.NoError(t, produceResult.FirstErr())
		}
	}

	sched.updateSchedule(ctx)

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_partition_start_offset The observed start offset of each partition.
		# TYPE cortex_blockbuilder_scheduler_partition_start_offset gauge
		cortex_blockbuilder_scheduler_partition_start_offset{partition="0"} 0
		cortex_blockbuilder_scheduler_partition_start_offset{partition="1"} 0
		cortex_blockbuilder_scheduler_partition_start_offset{partition="2"} 0
		cortex_blockbuilder_scheduler_partition_start_offset{partition="3"} 0
	`), "cortex_blockbuilder_scheduler_partition_start_offset"))
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_partition_end_offset The observed end offset of each partition.
		# TYPE cortex_blockbuilder_scheduler_partition_end_offset gauge
		cortex_blockbuilder_scheduler_partition_end_offset{partition="0"} 0
		cortex_blockbuilder_scheduler_partition_end_offset{partition="1"} 1
		cortex_blockbuilder_scheduler_partition_end_offset{partition="2"} 2
		cortex_blockbuilder_scheduler_partition_end_offset{partition="3"} 3
	`), "cortex_blockbuilder_scheduler_partition_end_offset"))
}

func TestFetchSingle(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 16, "ingest")
	cli := mustKafkaClient(t, kafkaAddr)

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Topic: "ingest",
		},
		BuilderConsumerGroup:   "test-builder",
		SchedulerConsumerGroup: "test-scheduler",
		SchedulingInterval:     1000000 * time.Hour,
	}
	reg := prometheus.NewPedanticRegistry()
	sched, err := New(cfg, test.NewTestingLogger(t), reg)
	sched.kafkaClient = cli
	sched.adminClient = kadm.NewClient(cli)
	require.NoError(t, err)

	// Partition i gets i+1 records.
	for i := int32(0); i < 4; i++ {
		for n := int32(0); n < i+1; n++ {
			produceResult := cli.ProduceSync(ctx, &kgo.Record{
				Timestamp: time.Unix(int64(i*n), 1),
				Value:     []byte(fmt.Sprintf("value-%d-%d", i, n)),
				Topic:     "ingest",
				Partition: i,
			})
			require.NoError(t, produceResult.FirstErr())
		}
	}

	t.Run("fetch first record of each partition", func(t *testing.T) {
		r, err := sched.fetchSingleRecords(ctx, map[string]map[int32]kgo.Offset{
			"ingest": {
				0: kgo.NewOffset().At(0),
				1: kgo.NewOffset().At(0),
				2: kgo.NewOffset().At(0),
				3: kgo.NewOffset().At(0),
			},
		})
		require.NoError(t, err)
		require.Len(t, r, 4)
		require.Equal(t, "value-0-0", string(r[0].Value))
		require.Equal(t, "value-1-0", string(r[1].Value))
		require.Equal(t, "value-2-0", string(r[2].Value))
		require.Equal(t, "value-3-0", string(r[3].Value))
	})

	t.Run("fetch later record of each partition", func(t *testing.T) {
		r, err := sched.fetchSingleRecords(ctx, map[string]map[int32]kgo.Offset{
			"ingest": {
				0: kgo.NewOffset().At(0),
				1: kgo.NewOffset().At(1),
				2: kgo.NewOffset().At(2),
				3: kgo.NewOffset().At(3),
			},
		})
		require.NoError(t, err)
		require.Len(t, r, 4)
		require.Equal(t, "value-0-0", string(r[0].Value))
		require.Equal(t, "value-1-1", string(r[1].Value))
		require.Equal(t, "value-2-2", string(r[2].Value))
		require.Equal(t, "value-3-3", string(r[3].Value))
	})

	t.Run("fetch subset of partitions", func(t *testing.T) {
		r, err := sched.fetchSingleRecords(ctx, map[string]map[int32]kgo.Offset{
			"ingest": {
				0: kgo.NewOffset().At(0),
				2: kgo.NewOffset().At(0),
			},
		})
		require.NoError(t, err)
		require.Len(t, r, 2)
		require.Equal(t, "value-0-0", string(r[0].Value))
		require.Equal(t, "value-2-0", string(r[2].Value))
	})
}
