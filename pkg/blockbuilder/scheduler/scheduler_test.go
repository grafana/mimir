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
	sched.jobs.addOrUpdate("ingest/65/256", schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   65,
		StartOffset: 256,
		EndOffset:   9111,
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
		nq := newJobQueue(988*time.Hour, test.NewTestingLogger(t), specLessThan, noOpJobCreationPolicy[schedulerpb.JobSpec]{})
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

	sched.committed = kadm.Offsets{
		"ingest": {
			1: kadm.Offset{
				Topic:     "ingest",
				Partition: 1,
				At:        5000,
			},
		},
	}
	sched.completeObservationMode()

	spec := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 5000,
		EndOffset:   6000,
	}

	sched.jobs.addOrUpdate("ingest/1/5524", spec)
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

	sched.completeObservationMode()

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

type timeOffset struct {
	time   time.Time
	offset int64
}

func TestConsumptionRanges(t *testing.T) {
	ctx := context.Background()

	type offsetRange struct {
		start, end int64
	}

	tests := map[string]struct {
		offsets        []timeOffset
		commit         int64
		partEnd        int64
		jobSize        time.Duration
		boundary       time.Time
		expectedRanges []offsetRange
		minScanTime    time.Time
		msg            string
	}{
		"basic hour-based consumption ranges": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC), 1000},
				{time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC), 2000},
				{time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC), 3000},
				{time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC), 4000},
				{time.Date(2025, 3, 1, 14, 0, 0, 0, time.UTC), 5000},
				{time.Date(2025, 3, 1, 14, 30, 0, 0, time.UTC), 5500},
				{time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC), 6000},
				{time.Date(2025, 3, 1, 16, 0, 0, 0, time.UTC), 7000},
			},
			commit:      2000,
			partEnd:     10001,
			jobSize:     1 * time.Hour,
			boundary:    time.Date(2025, 3, 1, 15, 0, 0, 0, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 0, time.UTC),
			expectedRanges: []offsetRange{
				{start: 2000, end: 3000},
				{start: 3000, end: 4000},
				{start: 4000, end: 5000},
				{start: 5000, end: 6000},
			},
			msg: "consumption should exclude the offset on the boundary, but otherwise cover (commit, boundary]",
		},
		"no new data": {
			// End offset is the one that was consumed last time.
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1000},
				{time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 199*1000000, time.UTC), 1999},
			},
			commit:         2000,
			partEnd:        2001,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
		"old data with single unconsumed record": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 199*1000000, time.UTC), 1999},
				{time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC), 2000},
			},
			commit:      2000,
			partEnd:     2001,
			jobSize:     200 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 2000, end: 2001},
			},
		},
		"one record: no new data": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 199*1000000, time.UTC), 1999},
			},
			commit:         2000,
			partEnd:        2000,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
		"boundary before data -> no ranges": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 1000*1000000, time.UTC), 1000},
				{time.Date(2025, 3, 1, 10, 0, 0, 1001*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 1002*1000000, time.UTC), 1002},
			},
			commit:         1000,
			partEnd:        1003,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
		"boundary at start of data": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 3000*1000000, time.UTC), 3000},
				{time.Date(2025, 3, 1, 10, 0, 0, 4000*1000000, time.UTC), 4000},
			},
			commit:         3000,
			partEnd:        4001,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 3000*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 3000*1000000, time.UTC),
			expectedRanges: []offsetRange{},
			msg:            "all data is >= boundary -> no eligible jobs",
		},
		"boundary at end of data": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 3000*1000000, time.UTC), 3000},
			},
			commit:         3000,
			partEnd:        3001,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 3000*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 3000*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
		"empty partition: no data": {
			offsets:        []timeOffset{},
			commit:         0,
			partEnd:        0,
			jobSize:        200 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
		"data gaps wider than range width": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 99*1000000, time.UTC), 999},
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1000},
				{time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1002},
				{time.Date(2025, 3, 1, 10, 0, 0, 103*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 104*1000000, time.UTC), 1004},
				{time.Date(2025, 3, 1, 10, 0, 0, 105*1000000, time.UTC), 1005},
				{time.Date(2025, 3, 1, 10, 0, 0, 106*1000000, time.UTC), 1006},
				{time.Date(2025, 3, 1, 10, 0, 0, 107*1000000, time.UTC), 1007},
				{time.Date(2025, 3, 1, 10, 0, 0, 108*1000000, time.UTC), 1008},
				{time.Date(2025, 3, 1, 10, 0, 0, 109*1000000, time.UTC), 1009},
				{time.Date(2025, 3, 1, 10, 0, 0, 110*1000000, time.UTC), 1010},
				{time.Date(2025, 3, 1, 10, 0, 0, 111*1000000, time.UTC), 1011},
				{time.Date(2025, 3, 1, 10, 0, 0, 112*1000000, time.UTC), 1012},
				// (large gap that would produce duplicate jobs in a naive implementation)
				{time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC), 1013},
				{time.Date(2025, 3, 1, 10, 0, 0, 501*1000000, time.UTC), 1014},
				{time.Date(2025, 3, 1, 10, 0, 0, 502*1000000, time.UTC), 1015},
				{time.Date(2025, 3, 1, 10, 0, 0, 503*1000000, time.UTC), 1016},
				{time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC), 1017},
			},
			commit:      1000,
			partEnd:     10001,
			jobSize:     100 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 1000, end: 1013},
				{start: 1013, end: 1017},
			},
		},
		"records with the duplicate timestamps": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1000},
				{time.Date(2025, 3, 1, 10, 0, 0, 101*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1002},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1004},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1005},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1006},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1007},
				{time.Date(2025, 3, 1, 10, 0, 0, 102*1000000, time.UTC), 1008},
			},
			commit:      1000,
			partEnd:     1009,
			jobSize:     100 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 1000, end: 1009},
			},
		},
		"no data between commit and time boundary": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1000},
				{time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC), 1002},
				{time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC), 1004},
			},
			commit:      1000,
			partEnd:     1005,
			jobSize:     100 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 200*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 1000, end: 1001},
			},
		},
		"commit before retention window": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC), 1004},
				{time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC), 1005},
			},
			commit:      1000,
			partEnd:     1005,
			jobSize:     100 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 500*1000000, time.UTC),
			minScanTime: time.Date(2025, 2, 20, 10, 0, 0, 500*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 1003, end: 1004},
				{start: 1004, end: 1005},
			},
		},
		"committed before min scan time": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC), 1002},
				{time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC), 1004},
			},
			commit:      1001,
			partEnd:     1004,
			jobSize:     100 * time.Millisecond,
			boundary:    time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime: time.Date(2025, 3, 1, 10, 0, 0, 150*1000000, time.UTC),
			expectedRanges: []offsetRange{
				{start: 1002, end: 1003},
				{start: 1003, end: 1004},
			},
		},
		"min scan time later than any data": {
			offsets: []timeOffset{
				{time.Date(2025, 3, 1, 10, 0, 0, 100*1000000, time.UTC), 1001},
				{time.Date(2025, 3, 1, 10, 0, 0, 200*1000000, time.UTC), 1002},
				{time.Date(2025, 3, 1, 10, 0, 0, 300*1000000, time.UTC), 1003},
				{time.Date(2025, 3, 1, 10, 0, 0, 400*1000000, time.UTC), 1004},
			},
			commit:         1001,
			partEnd:        1004,
			jobSize:        100 * time.Millisecond,
			boundary:       time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 3, 1, 10, 0, 0, 900*1000000, time.UTC),
			expectedRanges: []offsetRange{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			println(name)

			f := &mockOffsetFinder{offsets: tt.offsets, end: tt.partEnd}
			j, err := computePartitionJobs(ctx, f, "topic", 0, tt.commit, tt.partEnd, tt.boundary, tt.jobSize, tt.minScanTime)
			assert.NoError(t, err)

			// Convert offsetRange to JobSpec.
			expectedJobs := make([]*schedulerpb.JobSpec, len(tt.expectedRanges))
			for i, r := range tt.expectedRanges {
				expectedJobs[i] = &schedulerpb.JobSpec{
					Topic:       "topic",
					Partition:   0,
					StartOffset: r.start,
					EndOffset:   r.end,
				}
			}
			assert.Equal(t, expectedJobs, j, tt.msg)
		})
	}
}

// Create an offset finder that we can prepopulate with offset scenarios.
type mockOffsetFinder struct {
	offsets []timeOffset
	end     int64
}

func (o *mockOffsetFinder) offsetAfterTime(_ context.Context, _ string, _ int32, t time.Time) (int64, error) {
	// scan the offsets slice and return the lowest offset whose time is after t.
	mint := time.Time{}
	off := int64(-1)
	for _, pair := range o.offsets {
		if pair.time.After(t) {
			if mint.IsZero() || mint.After(pair.time) {
				mint = pair.time
				off = pair.offset
			}
		}
	}
	if off == -1 {
		// Like ListOffsetsAfterMilli, we return the end offset if we don't find any new data.
		return o.end, nil
	}
	return off, nil
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
