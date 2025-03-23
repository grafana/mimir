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
	j1 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/64/1000",
			epoch: 10,
		},
		spec: schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   64,
			StartOffset: 1000,
			CommitRecTs: now.Add(-1 * time.Hour),
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
			CommitRecTs: now.Add(-2 * time.Hour),
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
			CommitRecTs: now.Add(-3 * time.Hour),
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
		CommitRecTs: now.Add(-1 * time.Hour),
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
		nq := newJobQueue(988*time.Hour, test.NewTestingLogger(t), specLessThan)
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
				Topic:       "ingest",
				Partition:   partition,
				CommitRecTs: commitRecTs,
				EndOffset:   endOffset,
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
		CommitRecTs: time.Unix(200, 0),
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
	l, err := sched.fetchLag(ctx)
	require.NoError(t, err)
	sched.committed = commitOffsetsFromLag(l)

	sched.completeObservationMode()

	flushAndRequireOffsets := func(topic string, offsets map[int32]int64) {
		require.NoError(t, sched.flushOffsetsToKafka(ctx))
		l, err = sched.fetchLag(ctx)
		require.NoError(t, err)
		offs := commitOffsetsFromLag(l)
		for partition, expected := range offsets {
			requireOffset(t, offs, topic, partition, expected)
		}
	}

	flushAndRequireOffsets("ingest", map[int32]int64{
		0: 0,
		1: 0,
		2: 0,
		3: 0,
	})

	sched.advanceCommittedOffset("ingest", 1, 2000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		0: 0,
		1: 2000,
		2: 0,
		3: 0,
	})

	// Introducing a partition that wasn't initially present should work.
	sched.advanceCommittedOffset("ingest", 4, 65535)
	flushAndRequireOffsets("ingest", map[int32]int64{
		0: 0,
		1: 2000,
		2: 0,
		3: 0,
		4: 65535,
	})
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

func TestLessThan(t *testing.T) {
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	assert.True(t, specLessThan(schedulerpb.JobSpec{CommitRecTs: oneHourAgo}, schedulerpb.JobSpec{CommitRecTs: now}))
	assert.False(t, specLessThan(schedulerpb.JobSpec{CommitRecTs: now}, schedulerpb.JobSpec{CommitRecTs: oneHourAgo}))
	assert.False(t, specLessThan(schedulerpb.JobSpec{CommitRecTs: now}, schedulerpb.JobSpec{CommitRecTs: now}))
}

type timeOffset struct {
	time   int64
	offset int64
}

func TestConsumptionRanges(t *testing.T) {
	ctx := context.Background()

	type offsetRange struct {
		start int64
		end   int64
	}

	tests := map[string]struct {
		offsets        []timeOffset
		commit         int64
		partEnd        int64
		width          time.Duration
		boundary       time.Time
		expectedRanges []offsetRange
		msg            string
	}{
		"basic consumption ranges": {
			offsets: []timeOffset{
				{100, 1000},
				{101, 1001},
				{200, 2000},
				{300, 3000},
				{400, 4000},
				{500, 5000},
				{599, 5999},
				{600, 6000},
				{700, 7000},
			},
			commit:   2000,
			partEnd:  10001,
			width:    200 * time.Millisecond,
			boundary: time.UnixMilli(600),
			expectedRanges: []offsetRange{
				{start: 2000, end: 4000},
				{start: 4000, end: 6000},
			},
			msg: "consumption should exclude the offset on the boundary, but otherwise cover (commit, boundary]",
		},
		"no new data": {
			// End offset is the one that was consumed last time.
			offsets: []timeOffset{
				{100, 1000},
				{101, 1001},
				{199, 1999},
			},
			commit:         2000,
			partEnd:        2001,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(600),
			expectedRanges: []offsetRange{},
		},
		"old data with single unconsumed record": {
			offsets: []timeOffset{
				{199, 1999},
				{200, 2000},
			},
			commit:   2000,
			partEnd:  2001,
			width:    200 * time.Millisecond,
			boundary: time.UnixMilli(600),
			expectedRanges: []offsetRange{
				{start: 2000, end: 2001},
			},
		},
		"one record: no new data": {
			offsets: []timeOffset{
				{199, 1999},
			},
			commit:         2000,
			partEnd:        2000,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(600),
			expectedRanges: []offsetRange{},
		},
		"boundary before data -> no ranges": {
			offsets: []timeOffset{
				{1000, 1000},
				{1001, 1001},
				{1002, 1002},
			},
			commit:         1000,
			partEnd:        1003,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(600),
			expectedRanges: []offsetRange{},
		},
		"boundary at start of data": {
			offsets: []timeOffset{
				{3000, 3000},
				{4000, 4000},
			},
			commit:         3000,
			partEnd:        4001,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(3000),
			expectedRanges: []offsetRange{},
			msg:            "all data is >= boundary -> no eligible jobs",
		},
		"boundary at end of data": {
			offsets: []timeOffset{
				{3000, 3000},
			},
			commit:         3000,
			partEnd:        3001,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(3000),
			expectedRanges: []offsetRange{},
		},
		"empty partition: no data": {
			offsets:        []timeOffset{},
			commit:         0,
			partEnd:        0,
			width:          200 * time.Millisecond,
			boundary:       time.UnixMilli(600),
			expectedRanges: []offsetRange{},
		},
		"data gaps wider than range width": {
			offsets: []timeOffset{
				{99, 999},
				{100, 1000},
				{101, 1001},
				{102, 1002},
				{103, 1003},
				{104, 1004},
				{105, 1005},
				{106, 1006},
				{107, 1007},
				{108, 1008},
				{109, 1009},
				{110, 1010},
				{111, 1011},
				{112, 1012},
				// (large gap that would produce duplicate jobs in a naive implementation)
				{500, 1013},
				{501, 1014},
				{502, 1015},
				{503, 1016},
				{600, 1017},
			},
			commit:   1000,
			partEnd:  10001,
			width:    100 * time.Millisecond,
			boundary: time.UnixMilli(600),
			expectedRanges: []offsetRange{
				{start: 1000, end: 1013},
				{start: 1013, end: 1017},
			},
		},
		"records with the same timestamp": {
			offsets: []timeOffset{
				{100, 1000},
				{101, 1001},
				{102, 1002},
				{102, 1003},
				{102, 1004},
				{102, 1005},
				{102, 1006},
				{102, 1007},
				{102, 1008},
			},
			commit:   1000,
			partEnd:  1009,
			width:    100 * time.Millisecond,
			boundary: time.UnixMilli(600),
			expectedRanges: []offsetRange{
				{start: 1000, end: 1009},
			},
		},
	}

	/*
		Testing todos:
		- test that the boundary works appropriately when $now is right next to it. (Like right at the beginning of the hour.)

	*/

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			println(name)

			f := &mockOffsetFinder{offsets: tt.offsets, end: tt.partEnd}
			j, err := computePartitionJobs(ctx, f, "topic", 0, tt.commit, tt.partEnd, tt.boundary, tt.width)
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

// Create an offsetStore that we can just prepopulate with offset scenarios.
type mockOffsetFinder struct {
	offsets []timeOffset
	end     int64
}

func (o *mockOffsetFinder) offsetAfterTime(_ context.Context, topic string, partition int32, t time.Time) (int64, error) {
	// scan the offsets slice and return the lowest offset whose time is after t.
	mint := time.Time{}
	off := int64(-1)
	for _, pair := range o.offsets {
		pairTime := time.UnixMilli(pair.time)
		if pairTime.After(t) {
			if mint.IsZero() || mint.After(pairTime) {
				mint = pairTime
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
