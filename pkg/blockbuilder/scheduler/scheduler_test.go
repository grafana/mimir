// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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

func TestObservations(t *testing.T) {
	sched, _ := mustScheduler(t)
	// Initially we're in observation mode. We have Kafka's start offsets, but no client jobs.

	sched.committed = kadm.Offsets{
		"ingest": {
			1: kadm.Offset{
				Topic:     "ingest",
				Partition: 1,
				At:        5000,
			},
			2: kadm.Offset{
				Topic:     "ingest",
				Partition: 2,
				At:        800,
			},
			3: kadm.Offset{
				Topic:     "ingest",
				Partition: 3,
				At:        974,
			},
			4: kadm.Offset{
				Topic:     "ingest",
				Partition: 4,
				At:        500,
			},
			5: kadm.Offset{
				Topic:     "ingest",
				Partition: 5,
				At:        12000,
			},
		},
	}

	{
		nq := newJobQueue(988*time.Hour, test.NewTestingLogger(t))
		sched.jobs = nq
		sched.finalizeObservations()
		require.Len(t, nq.jobs, 0, "No observations, no jobs")
	}

	type observation struct {
		key       jobKey
		spec      jobSpec
		workerID  string
		complete  bool
		expectErr error
	}
	var clientData []observation
	const (
		complete   = true
		inProgress = false
	)
	maybeBadEpoch := errors.New("maybe bad epoch")
	mkJob := func(isComplete bool, worker string, partition int32, id string, epoch int64, commitRecTs time.Time, endOffset int64, expectErr error) {
		clientData = append(clientData, observation{
			key: jobKey{id: id, epoch: epoch},
			spec: jobSpec{
				topic:       "ingest",
				partition:   partition,
				commitRecTs: commitRecTs,
				endOffset:   endOffset,
			},
			workerID:  worker,
			complete:  isComplete,
			expectErr: expectErr,
		})
	}

	// Rig up a bunch of data that clients are collectively sending.

	// Partition 1: one job in progress.
	mkJob(inProgress, "w0", 1, "ingest/1/5524", 10, time.Unix(200, 0), 6000, nil)

	// Partition 2: Many complete jobs, followed by an in-progress job.
	mkJob(complete, "w0", 2, "ingest/2/1", 3, time.Unix(1, 0), 15, nil)
	mkJob(complete, "w0", 2, "ingest/2/16", 4, time.Unix(2, 0), 31, nil)
	mkJob(complete, "w0", 2, "ingest/2/32", 4, time.Unix(3, 0), 45, nil)
	mkJob(complete, "w0", 2, "ingest/2/1000", 11, time.Unix(500, 0), 2000, nil)
	mkJob(inProgress, "w0", 2, "ingest/2/2001", 12, time.Unix(600, 0), 2199, nil)

	// (Partition 3 has no updates.)

	// Partition 4 has a series of completed jobs that are entirely after what was found in Kafka.
	mkJob(complete, "w0", 4, "ingest/4/500", 15, time.Unix(500, 0), 599, nil)
	mkJob(complete, "w1", 4, "ingest/4/600", 16, time.Unix(600, 0), 699, nil)
	mkJob(complete, "w2", 4, "ingest/4/700", 17, time.Unix(700, 0), 799, nil)
	mkJob(complete, "w3", 4, "ingest/4/800", 18, time.Unix(800, 0), 899, nil)
	// Here's a conflicting completion report from a worker whose lease was revoked at one point. It should be effectively dropped.
	mkJob(complete, "w99", 4, "ingest/4/600", 6, time.Unix(600, 0), 699, maybeBadEpoch)

	// Partition 5 has a number of conflicting in-progress reports.
	mkJob(inProgress, "w100", 5, "ingest/5/12000", 30, time.Unix(200, 0), 6000, maybeBadEpoch)
	mkJob(inProgress, "w101", 5, "ingest/5/12000", 31, time.Unix(200, 0), 6000, maybeBadEpoch)
	mkJob(inProgress, "w102", 5, "ingest/5/12000", 32, time.Unix(200, 0), 6000, maybeBadEpoch)
	mkJob(inProgress, "w103", 5, "ingest/5/12000", 33, time.Unix(200, 0), 6000, maybeBadEpoch)
	mkJob(inProgress, "w104", 5, "ingest/5/12000", 34, time.Unix(200, 0), 6000, nil)

	// Partition 6 has a complete job, but wasn't among the offsets we learned from Kafka.
	mkJob(complete, "w0", 6, "ingest/6/500", 48, time.Unix(500, 0), 599, nil)
	// Partition 7 has an in-progress job, but wasn't among the offsets we learned from Kafka.
	mkJob(complete, "w1", 7, "ingest/7/92874", 52, time.Unix(1500, 0), 93874, nil)

	rnd := rand.New(rand.NewSource(64_000))

	sendUpdates := func() {
		for range 3 {
			// Simulate the arbitrary order of client updates.
			rnd.Shuffle(len(clientData), func(i, j int) { clientData[i], clientData[j] = clientData[j], clientData[i] })
			for _, c := range clientData {
				t.Log("sending update", c.key, c.workerID)
				err := sched.updateJob(c.key, c.workerID, c.complete, c.spec)
				if errors.Is(c.expectErr, maybeBadEpoch) {
					require.True(t, errors.Is(err, errBadEpoch) || err == nil, "expected either bad epoch or no error, got %v", err)
				} else {
					require.NoError(t, err)
				}
			}
		}
	}

	sendUpdates()

	sched.completeObservationMode()
	requireOffset(t, sched.committed, "ingest", 1, 5000, "ingest/1 is in progress, so we should not move the offset")
	requireOffset(t, sched.committed, "ingest", 2, 2000, "ingest/2 job was complete, so it should move the offset forward")
	requireOffset(t, sched.committed, "ingest", 3, 974, "ingest/3 should be unchanged - no updates")
	requireOffset(t, sched.committed, "ingest", 4, 899, "ingest/4 should be moved forward to account for the completed jobs")
	requireOffset(t, sched.committed, "ingest", 5, 12000, "ingest/5 has nothing new completed")
	requireOffset(t, sched.committed, "ingest", 6, 599, "ingest/6 should have been added to the offsets")

	require.Len(t, sched.jobs.jobs, 3)
	require.Equal(t, 35, int(sched.jobs.epoch))

	// Now verify that the same set of updates can be sent now that we're out of observation mode.

	sendUpdates()
}

func requireOffset(t *testing.T, offs kadm.Offsets, topic string, partition int32, expected int64, msgAndArgs ...interface{}) {
	t.Helper()
	o, ok := offs.Lookup(topic, partition)
	require.True(t, ok, msgAndArgs...)
	require.Equal(t, expected, o.At, msgAndArgs...)
}

func TestOffsetMovement(t *testing.T) {
	sched, _ := mustScheduler(t)
	sched.completeObservationMode()

	// We have some client updates.
	require.NoError(t, sched.updateJob(jobKey{id: "ingest/1/5524", epoch: 10}, "w0", false, jobSpec{
		topic:       "ingest",
		partition:   1,
		commitRecTs: time.Unix(200, 0),
		endOffset:   6000,
	}))

	requireOffset(t, sched.committed, "ingest", 1, 0, "ingest/1 is in progress, so we should not move the offset")

	require.NoError(t, sched.updateJob(jobKey{id: "ingest/1/5524", epoch: 10}, "w0", true, jobSpec{
		topic:       "ingest",
		partition:   1,
		commitRecTs: time.Unix(200, 0),
		endOffset:   6000,
	}))

}

func TestMonitor(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	sched, cli := mustSchedulerWithKafkaAddr(t, kafkaAddr)
	reg := sched.register.(*prometheus.Registry)

	sched.completeObservationMode()

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
