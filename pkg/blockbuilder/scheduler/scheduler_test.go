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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

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

func mustSchedulerWithKafkaAddr(t *testing.T, addr string) (*BlockBuilderScheduler, *kgo.Client) {
	cli := mustKafkaClient(t, addr)
	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Topic: "ingest",
		},
		ConsumerGroup:       "test-builder",
		SchedulingInterval:  1000000 * time.Hour,
		JobSize:             1 * time.Hour,
		MaxJobsPerPartition: 1,
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

	// Some jobs that ostensibly exist, but scheduler doesn't know about.
	j1 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/64/1000",
			epoch: 10,
		},
		spec: schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   64,
			StartOffset: 1000,
		},
	}
	j2 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/65/256",
			epoch: 11,
		},
		spec: schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   65,
			StartOffset: 256,
		},
	}
	j3 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/66/57",
			epoch: 12,
		},
		spec: schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   66,
			StartOffset: 57,
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
	sched.completeObservationMode(context.Background())

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
	e := sched.jobs.add("ingest/65/256", schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   65,
		StartOffset: 256,
		EndOffset:   9111,
	})
	require.NoError(t, e)
	a1key, a1spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a1spec)
	require.Equal(t, "ingest/65/256", a1key.id)
}

// Verify that we skip jobs that are before the committed offset due to extraneous bug situations.
func TestAssignJobSkipsObsoleteOffsets(t *testing.T) {
	sched, _ := mustScheduler(t)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())
	// Add some jobs, then move the committed offsets past some of them.
	s1 := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 256,
		EndOffset:   9111,
	}
	s2 := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   2,
		StartOffset: 50,
		EndOffset:   128,
	}
	s3 := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   2,
		StartOffset: 700,
		EndOffset:   900,
	}

	require.NoError(t, sched.jobs.add("ingest/1/256", s1))
	require.NoError(t, sched.jobs.add("ingest/2/50", s2))
	require.NoError(t, sched.jobs.add("ingest/2/700", s3))

	require.Equal(t, 3, sched.jobs.count())

	sched.advanceCommittedOffset("ingest", 1, 256)
	sched.advanceCommittedOffset("ingest", 2, 500)

	// Advancing offsets doesn't actually remove any jobs.
	require.Equal(t, 3, sched.jobs.count())

	var assignedJobs []*schedulerpb.JobSpec

	for {
		_, s, err := sched.assignJob("big-time-worker-64")
		if errors.Is(err, errNoJobAvailable) {
			break
		}
		require.NoError(t, err)
		assignedJobs = append(assignedJobs, &s)
	}

	require.ElementsMatch(t,
		[]*schedulerpb.JobSpec{&s1, &s3}, assignedJobs,
		"s2 should be skipped because its start offset is behind p2's committed offset",
	)
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
		nq := newJobQueue(988*time.Hour, test.NewTestingLogger(t), noOpJobCreationPolicy[schedulerpb.JobSpec]{})
		sched.jobs = nq
		sched.finalizeObservations()
		require.Len(t, nq.jobs, 0, "No observations, no jobs")
	}

	type observation struct {
		key       jobKey
		spec      schedulerpb.JobSpec
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
			spec: schedulerpb.JobSpec{
				Topic:     "ingest",
				Partition: partition,
				EndOffset: endOffset,
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

	sched.completeObservationMode(context.Background())
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

	sched.committed = kadm.Offsets{
		"ingest": {
			1: kadm.Offset{
				Topic:     "ingest",
				Partition: 1,
				At:        5000,
			},
		},
	}
	sched.completeObservationMode(context.Background())

	spec := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 5000,
		EndOffset:   6000,
	}

	e := sched.jobs.add("ingest/1/5524", spec)
	require.NoError(t, e)
	key, _, err := sched.jobs.assign("w0")
	require.NoError(t, err)

	require.NoError(t, sched.updateJob(key, "w0", false, spec))
	requireOffset(t, sched.committed, "ingest", 1, 5000, "ingest/1 is in progress, so we should not move the offset")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	requireOffset(t, sched.committed, "ingest", 1, 6000, "ingest/1 is complete, so offset should be advanced")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	requireOffset(t, sched.committed, "ingest", 1, 6000, "ingest/1 is complete, so offset should be advanced")
	sched.advanceCommittedOffset("ingest", 1, 2000)
	requireOffset(t, sched.committed, "ingest", 1, 6000, "committed offsets cannot rewind")

	sched.advanceCommittedOffset("ingest", 2, 6222)
	requireOffset(t, sched.committed, "ingest", 2, 6222, "should create knowledge of partition 2")
}

func TestKafkaFlush(t *testing.T) {
	sched, _ := mustScheduler(t)
	ctx := context.Background()
	var err error
	sched.committed, err = sched.fetchCommittedOffsets(ctx)
	require.NoError(t, err)

	sched.completeObservationMode(ctx)

	flushAndRequireOffsets := func(topic string, offsets map[int32]int64, args ...interface{}) {
		require.NoError(t, sched.flushOffsetsToKafka(ctx))
		offs, err := sched.fetchCommittedOffsets(ctx)
		require.NoError(t, err)
		for partition, expected := range offsets {
			requireOffset(t, offs, topic, partition, expected, args...)
		}
	}

	flushAndRequireOffsets("ingest", map[int32]int64{}, "no group found -> no offsets")

	sched.advanceCommittedOffset("ingest", 1, 2000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
	})

	sched.advanceCommittedOffset("ingest", 4, 65535)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
		4: 65535,
	})

	sched.advanceCommittedOffset("ingest", 1, 4000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 4000,
		4: 65535,
	}, "should be able to advance an existing offset")
}

func TestUpdateSchedule(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	sched, cli := mustSchedulerWithKafkaAddr(t, kafkaAddr)
	reg := sched.register.(*prometheus.Registry)

	sched.completeObservationMode(ctx)

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

func TestConsumptionRanges(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		offsets        []*offsetTime
		start          int64
		resume         int64
		end            int64
		jobSize        time.Duration
		endTime        time.Time
		expectedRanges []*offsetTime
		minScanTime    time.Time
		msg            string
	}{
		"no new data": {
			// End offset is the one that was consumed last time.
			offsets: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 199*1000000, time.UTC)},
			},
			resume:         2000,
			end:            2000,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"old data with single unconsumed record": {
			offsets: []*offsetTime{
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
			},
			resume:      2000,
			end:         2001,
			jobSize:     200 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 2001, time: time.Time{}},
			},
		},
		"one record: no new data": {
			offsets: []*offsetTime{
				{offset: 1999, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
			},
			resume:         2000,
			end:            2000,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"empty partition: no data": {
			offsets:        []*offsetTime{},
			resume:         0,
			end:            0,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"data gaps wider than job size": {
			offsets: []*offsetTime{
				{offset: 999, time: time.Date(2025, 3, 1, 10, 0, 0, 99*1000000, time.UTC)},
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 103*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 104*1000000, time.UTC)},
				{offset: 1005, time: time.Date(2025, 3, 1, 10, 0, 0, 105*1000000, time.UTC)},
				{offset: 1006, time: time.Date(2025, 3, 1, 10, 0, 0, 106*1000000, time.UTC)},
				{offset: 1007, time: time.Date(2025, 3, 1, 10, 0, 0, 107*1000000, time.UTC)},
				{offset: 1008, time: time.Date(2025, 3, 1, 10, 0, 0, 108*1000000, time.UTC)},
				{offset: 1009, time: time.Date(2025, 3, 1, 10, 0, 0, 109*1000000, time.UTC)},
				{offset: 1010, time: time.Date(2025, 3, 1, 10, 0, 0, 110*1000000, time.UTC)},
				{offset: 1011, time: time.Date(2025, 3, 1, 10, 0, 0, 111*1000000, time.UTC)},
				{offset: 1012, time: time.Date(2025, 3, 1, 10, 0, 0, 112*1000000, time.UTC)},
				// (large gap that would produce duplicates in a naive implementation)
				{offset: 1013, time: time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC)},
				{offset: 1014, time: time.Date(2025, 3, 1, 10, 0, 0, 501*1000000, time.UTC)},
				{offset: 1015, time: time.Date(2025, 3, 1, 10, 0, 0, 502*1000000, time.UTC)},
				{offset: 1016, time: time.Date(2025, 3, 1, 10, 0, 0, 503*1000000, time.UTC)},
				{offset: 1017, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
			},
			resume:      1000,
			end:         1020,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 99*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1013, time: time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC)},
				{offset: 1014, time: time.Date(2025, 3, 1, 10, 0, 0, 501*1000000, time.UTC)},
				{offset: 1017, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)},
				{offset: 1020, time: time.Time{}},
			},
		},
		"records with duplicate timestamps": {
			offsets: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1005, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1006, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1007, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
				{offset: 1008, time: time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC)},
			},
			resume:      1000,
			end:         1009,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1000, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC)},
				{offset: 1009, time: time.Time{}},
			},
		},
		"resumption offset is before min scan time": {
			offsets: []*offsetTime{
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC)},
			},
			resume:      1001,
			end:         1004,
			jobSize:     100 * time.Millisecond,
			endTime:     time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 0, 0, 150*1000000, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Time{}},
			},
		},
		"min scan time later than any data": {
			offsets: []*offsetTime{
				{offset: 1001, time: time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC)},
				{offset: 1002, time: time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC)},
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC)},
				{offset: 1004, time: time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC)},
			},
			resume:         1001,
			end:            1004,
			jobSize:        100 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 900*1000000, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"resume < start and start == end": {
			offsets:        []*offsetTime{},
			start:          1004,
			resume:         1000,
			end:            1004,
			jobSize:        1 * time.Minute,
			endTime:        time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"resume < start < end": {
			offsets: []*offsetTime{
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 0, 0, time.UTC)},
			},
			start:       1003,
			resume:      1000,
			end:         1004,
			jobSize:     1 * time.Minute,
			endTime:     time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 0, 0, time.UTC)},
				{offset: 1004, time: time.Time{}},
			},
		},
		"resume == start == end": {
			offsets:        []*offsetTime{},
			start:          1003,
			resume:         1003,
			end:            1003,
			jobSize:        1 * time.Minute,
			endTime:        time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{},
		},
		"hour-based ranges when resume < start": {
			offsets: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 3000, time: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			start:       2000,
			resume:      100,
			end:         10001,
			jobSize:     1 * time.Hour,
			endTime:     time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC),
			minScanTime: time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC),
			expectedRanges: []*offsetTime{
				{offset: 2000, time: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 3000, time: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
				{offset: 10001, time: time.Time{}},
			},
			msg: "if resumption offset has fallen off the retention window, we should produce jobs beginning at start",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			f := &mockOffsetFinder{offsets: tt.offsets, end: tt.end}
			j, err := probeInitialJobOffsets(ctx, f, "topic", 0, tt.start, tt.resume, tt.end, tt.endTime, tt.jobSize, tt.minScanTime, test.NewTestingLogger(t))
			assert.NoError(t, err)
			assert.EqualValues(t, tt.expectedRanges, j, tt.msg)
		})
	}
}

// Create an offset finder that we can prepopulate with offset scenarios.
type mockOffsetFinder struct {
	offsets []*offsetTime
	end     int64
}

func (o *mockOffsetFinder) offsetAfterTime(_ context.Context, _ string, _ int32, t time.Time) (int64, time.Time, error) {
	// scan the offsets slice and return the lowest offset whose time is after t.
	mint := time.Time{}
	maxt := time.Time{}
	off := int64(-1)
	for _, pair := range o.offsets {
		if pair.time.After(t) {
			if mint.IsZero() || mint.After(pair.time) {
				mint = pair.time
				off = pair.offset
			}
			if maxt.Before(pair.time) {
				maxt = pair.time
			}
		}
	}
	if off == -1 {
		// Like ListOffsetsAfterMilli, we return the end offset if we don't find any new data.
		return o.end, time.Time{}, nil
	}
	return off, mint, nil
}

var _ offsetStore = (*mockOffsetFinder)(nil)

func TestLimitNPolicy(t *testing.T) {
	allow1 := limitPerPartitionJobCreationPolicy{partitionLimit: 1}

	ok := allow1.canCreateJob(jobKey{id: "job1"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 0}, []*schedulerpb.JobSpec{})
	require.True(t, ok)

	ok = allow1.canCreateJob(jobKey{id: "job4"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 0}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 1},
	})
	require.True(t, ok)

	ok = allow1.canCreateJob(jobKey{id: "job5"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 1},
	})
	require.False(t, ok)

	ok = allow1.canCreateJob(jobKey{id: "job5"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 2},
		{Topic: "topic", Partition: 3},
		{Topic: "topic", Partition: 3},
	})
	require.True(t, ok)

	allow2 := limitPerPartitionJobCreationPolicy{partitionLimit: 2}
	ok = allow2.canCreateJob(jobKey{id: "job6"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 2},
		{Topic: "topic", Partition: 3},
	})
	require.True(t, ok)
	ok = allow2.canCreateJob(jobKey{id: "job6"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 1},
		{Topic: "topic", Partition: 2},
	})
	require.True(t, ok)
	ok = allow2.canCreateJob(jobKey{id: "job6"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 1},
		{Topic: "topic", Partition: 1},
	})
	require.False(t, ok)
	ok = allow2.canCreateJob(jobKey{id: "job6"}, &schedulerpb.JobSpec{Topic: "topic", Partition: 1}, []*schedulerpb.JobSpec{
		{Topic: "topic", Partition: 1},
		{Topic: "topic", Partition: 1},
		{Topic: "topic", Partition: 1},
	})
	require.False(t, ok)
}

func TestPartitionState(t *testing.T) {
	pt := &partitionState{}
	sz := 1 * time.Hour

	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	var job *offsetRange
	var err error

	job, err = pt.updateEndOffset(100, time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)
	job, err = pt.updateEndOffset(200, time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Equal(t, &offsetRange{start: 100, end: 200}, job)
	require.Nil(t, err)

	job, err = pt.updateEndOffset(201, time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.NoError(t, err)
	job, err = pt.updateEndOffset(202, time.Date(2025, 3, 1, 11, 2, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	job, err = pt.updateEndOffset(203, time.Date(2025, 3, 1, 11, 3, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)

	job, err = pt.updateEndOffset(300, z.Add(2*time.Hour), sz)
	require.Equal(t, &offsetRange{start: 200, end: 300}, job)
	require.NoError(t, err)

	// And, if the time goes backwards, we return an error.
	job, err = pt.updateEndOffset(300, z.Add(-2*time.Hour), sz)
	require.Nil(t, job)
	require.ErrorContains(t, err, "time went backwards")
}

func TestPartitionState_TerminallyDormantPartition(t *testing.T) {
	pt := &partitionState{}
	sz := 1 * time.Hour
	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		z = z.Add(7 * time.Minute)
		j, err := pt.updateEndOffset(0, z, sz)
		assert.Nil(t, j)
		assert.NoError(t, err)
	}
}

func TestPartitionState_PartitionBecomesInactive(t *testing.T) {
	pt := &partitionState{}
	sz := 1 * time.Hour

	// A bunch of data observed:
	var j *offsetRange
	var err error
	j, err = pt.updateEndOffset(10, time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(11, time.Date(2025, 3, 1, 10, 1, 11, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 10, 1, 12, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	// data ceases. continue to get observations in the same bucket.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 10, 1, 13, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// as we cross into the next bucket, there's still no new data.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 1, 0, 0, time.UTC), sz)
	assert.Equal(t, &offsetRange{start: 10, end: 12}, j)
	assert.NoError(t, err)
	// and we keep getting the same offset.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 2, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 3, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// And in the next job bucket, still no new data.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 12, 1, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
}

func TestBlockBuilderScheduler_EnqueuePendingJobs(t *testing.T) {
	// Test that job detection and enqueueiing work as expected w/r/t the
	// job creation policy.

	sched, _ := mustScheduler(t)
	sched.cfg.MaxJobsPerPartition = 1
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState(part)

	pt.addPendingJob(&offsetRange{start: 10, end: 20})
	pt.addPendingJob(&offsetRange{start: 20, end: 30})
	pt.addPendingJob(&offsetRange{start: 30, end: 40})

	assert.Equal(t, 3, pt.pendingJobs.Len())

	sched.enqueuePendingJobs()
	assert.Equal(t, 2, pt.pendingJobs.Len())
	sched.enqueuePendingJobs()
	assert.Equal(t, 2, pt.pendingJobs.Len())

	j, spec, err := sched.jobs.assign("worker1")

	assert.NoError(t, err)
	assert.Equal(t, "ingest/1/10", j.id)
	assert.Equal(t, schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   20,
	}, spec)

	sched.enqueuePendingJobs()
	assert.Equal(t, 2, pt.pendingJobs.Len(), "enqueue should be a no-op until the assigned job is completed")

	e := sched.jobs.completeJob(j, "worker1")
	require.NoError(t, e)

	sched.enqueuePendingJobs()
	assert.Equal(t, 1, pt.pendingJobs.Len(), "enqueue should have succeeded after completing the job")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs_Unlimited(t *testing.T) {
	sched, _ := mustScheduler(t)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState(part)

	pt.addPendingJob(&offsetRange{start: 10, end: 20})
	pt.addPendingJob(&offsetRange{start: 20, end: 30})
	pt.addPendingJob(&offsetRange{start: 30, end: 40})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len(), "a single enqueue call should have drained all pending jobs")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs_CommitRace(t *testing.T) {
	sched, _ := mustScheduler(t)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState(part)
	pt.addPendingJob(&offsetRange{start: 10, end: 20})

	sched.advanceCommittedOffset("ingest", part, 20)

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count(), "the job should have been ignored because it's behind the committed offset")
}
