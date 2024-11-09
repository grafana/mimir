// SPDX-License-Identifier: AGPL-3.0-only

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

func mustSchedulerWithKafkaAddr(t *testing.T, addr string) (*BlockBuilderScheduler, *kgo.Client) {
	cli := mustKafkaClient(t, addr)
	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Topic: "ingest",
		},
		ConsumerGroup:      "test-builder",
		SchedulingInterval: 1000000 * time.Hour,
	}
	reg := prometheus.NewPedanticRegistry()
	sched, err := New(cfg, test.NewTestingLogger(t), reg)
	sched.adminClient = kadm.NewClient(cli)
	require.NoError(t, err)
	return sched, cli
}

func mustScheduler(t *testing.T) (*BlockBuilderScheduler, *kgo.Client) {
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	return mustSchedulerWithKafkaAddr(t, kafkaAddr)
}

func TestClientInterface(t *testing.T) {
	sched, _ := mustScheduler(t)
	sched.completeObservationMode()
	now := time.Now()

	// Set up some jobs in the queue.
	sched.jobs.addOrUpdate("ingest/64/1000", jobSpec{
		topic:       "ingest",
		partition:   64,
		startOffset: 1000,
		endOffset:   2000,
		commitRecTs: now.Add(-2 * time.Hour),
	})
	sched.jobs.addOrUpdate("ingest/65/256", jobSpec{
		topic:       "ingest",
		partition:   65,
		startOffset: 256,
		endOffset:   9111,
		commitRecTs: now.Add(-1 * time.Hour),
	})

	// And now do some things a client might do using the client interface.

	a1key, a1spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a1spec)
	require.Equal(t, "ingest/64/1000", a1key.id)

	// Heartbeat a bunch of times.
	require.NoError(t, sched.updateJob(a1key, "w0", false, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", false, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", false, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", false, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", false, a1spec))

	// Complete a bunch of times.
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))

	// Take the next job.
	a2key, a2spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a2spec)
	require.Equal(t, "ingest/65/256", a2key.id)

	// Heartbeat a bunch of times.
	require.NoError(t, sched.updateJob(a2key, "w0", false, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", false, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", false, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", false, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", false, a2spec))

	// Complete a bunch of times.
	require.NoError(t, sched.updateJob(a2key, "w0", true, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", true, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", true, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", true, a2spec))
	require.NoError(t, sched.updateJob(a2key, "w0", true, a2spec))

	// And repeat completion with the first job. Like clients will do.
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))
	require.NoError(t, sched.updateJob(a1key, "w0", true, a1spec))

	{
		k, jobSpec, err := sched.assignJob("w0")
		require.ErrorIs(t, err, errNoJobAvailable)
		require.Zero(t, jobSpec)
		require.Zero(t, k)
	}
}

func TestStartup(t *testing.T) {
	sched, _ := mustScheduler(t)
	// (a new scheduler starts in observation mode.)

	{
		_, _, err := sched.assignJob("w0")
		require.ErrorContains(t, err, "observation period not complete")
	}

	now := time.Now()

	// Some jobs that ostensibly exist, but scheduler doesn't know about.
	j1 := job{
		key: jobKey{
			id:    "ingest/64/1000",
			epoch: 10,
		},
		spec: jobSpec{
			topic:       "ingest",
			partition:   64,
			startOffset: 1000,
			commitRecTs: now.Add(-1 * time.Hour),
		},
	}
	j2 := job{
		key: jobKey{
			id:    "ingest/65/256",
			epoch: 11,
		},
		spec: jobSpec{
			topic:       "ingest",
			partition:   65,
			startOffset: 256,
			commitRecTs: now.Add(-2 * time.Hour),
		},
	}
	j3 := job{
		key: jobKey{
			id:    "ingest/66/57",
			epoch: 12,
		},
		spec: jobSpec{
			topic:       "ingest",
			partition:   66,
			startOffset: 57,
			commitRecTs: now.Add(-3 * time.Hour),
		},
	}

	// Clients will be pinging with their updates for some time.

	require.NoError(t, sched.updateJob(j1.key, "w0", false, j1.spec))

	require.NoError(t, sched.updateJob(j2.key, "w0", true, j2.spec))

	require.NoError(t, sched.updateJob(j3.key, "w0", false, j3.spec))
	require.NoError(t, sched.updateJob(j3.key, "w0", false, j3.spec))
	require.NoError(t, sched.updateJob(j3.key, "w0", false, j3.spec))
	require.NoError(t, sched.updateJob(j3.key, "w0", true, j3.spec))

	// Convert the observations to actual jobs.
	sched.completeObservationMode()

	// Now that we're out of observation mode, we should know about all the jobs.

	require.NoError(t, sched.updateJob(j1.key, "w0", false, j1.spec))
	require.NoError(t, sched.updateJob(j1.key, "w0", false, j1.spec))

	require.NoError(t, sched.updateJob(j2.key, "w0", true, j2.spec))

	require.NoError(t, sched.updateJob(j3.key, "w0", true, j3.spec))

	_, ok := sched.jobs.jobs[j1.key.id]
	require.True(t, ok)

	// And eventually they'll all complete.
	require.NoError(t, sched.updateJob(j1.key, "w0", true, j1.spec))
	require.NoError(t, sched.updateJob(j2.key, "w0", true, j2.spec))
	require.NoError(t, sched.updateJob(j3.key, "w0", true, j3.spec))

	{
		_, _, err := sched.assignJob("w0")
		require.ErrorIs(t, err, errNoJobAvailable)
	}

	// And we can resume normal operation:
	sched.jobs.addOrUpdate("ingest/65/256", jobSpec{
		topic:       "ingest",
		partition:   65,
		startOffset: 256,
		endOffset:   9111,
		commitRecTs: now.Add(-1 * time.Hour),
	})

	a1key, a1spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a1spec)
	require.Equal(t, "ingest/65/256", a1key.id)
}

func TestObserve(t *testing.T) {
	m := make(obsMap)

	require.NoError(t,
		m.update(
			jobKey{id: "ingest/64/1000", epoch: 10},
			"w0",
			true,
			jobSpec{topic: "ingest", commitRecTs: time.Unix(5, 0)},
		),
	)
	require.NoError(t,
		m.update(
			jobKey{id: "ingest/64/1000", epoch: 10},
			"w0",
			true,
			jobSpec{topic: "ingest", commitRecTs: time.Unix(5, 0)},
		),
	)
	require.NoError(t,
		m.update(
			jobKey{id: "ingest/64/99999999", epoch: 84},
			"w0",
			true,
			jobSpec{topic: "ingest", commitRecTs: time.Unix(99, 0)},
		),
	)
	require.Len(t, m, 2)

	require.ErrorIs(t,
		m.update(
			jobKey{id: "ingest/64/1000", epoch: 9},
			"w9",
			false,
			jobSpec{topic: "ingest", commitRecTs: time.Unix(2, 0)},
		),
		errBadEpoch,
	)
}

func TestStateFromObservations(t *testing.T) {
	sched, _ := mustScheduler(t)
	m := make(obsMap)
	require.NoError(t,
		m.update(
			jobKey{id: "ingest/63/5524", epoch: 10},
			"w0",
			false,
			jobSpec{topic: "ingest", partition: 63, commitRecTs: time.Unix(2, 0), endOffset: 6000},
		),
	)
	require.NoError(t,
		m.update(
			jobKey{id: "ingest/64/1000", epoch: 11},
			"w0",
			true,
			jobSpec{topic: "ingest", partition: 64, commitRecTs: time.Unix(5, 0), endOffset: 2000},
		),
	)

	offs, q := sched.computeStateFromObservations(
		m, kadm.Offsets{
			"ingest": {
				63: kadm.Offset{
					Partition: 63,
					At:        5000,
				},
				64: kadm.Offset{
					Partition: 64,
					At:        800,
				},
				65: kadm.Offset{
					Partition: 65,
					At:        974,
				},
			},
		},
	)

	requireOffset(t, offs, "ingest", 63, 5000, "ingest/65 is in progress, so we should not move the offset")
	requireOffset(t, offs, "ingest", 64, 2000)
	requireOffset(t, offs, "ingest", 65, 974, "ingest/65 should be unchanged - not among observations")

	require.Len(t, q.jobs, 1)
	require.Equal(t, 11, int(q.epoch))
}

func requireOffset(t *testing.T, offs kadm.Offsets, topic string, partition int32, expected int64, msgAndArgs ...interface{}) {
	t.Helper()
	o, ok := offs.Lookup(topic, partition)
	require.True(t, ok, msgAndArgs...)
	require.Equal(t, expected, o.At, msgAndArgs...)
}

func TestMonitor(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	sched, cli := mustSchedulerWithKafkaAddr(t, kafkaAddr)
	reg := sched.register.(*prometheus.Registry)

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
