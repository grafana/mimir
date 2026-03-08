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

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
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
			Address:      flagext.StringSliceCSV{addr},
			Topic:        "ingest",
			FetchMaxWait: 10 * time.Millisecond,
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

func mustScheduler(t *testing.T, partitions int32) (*BlockBuilderScheduler, *kgo.Client) {
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitions, "ingest")
	return mustSchedulerWithKafkaAddr(t, kafkaAddr)
}

// observationCompleteLocked: a getter for tests.
func (s *BlockBuilderScheduler) observationCompleteLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observationComplete
}

// TestService tests the scheduler in a very basic way through its Service interface.
func TestService(t *testing.T) {
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	sched, cli := mustSchedulerWithKafkaAddr(t, kafkaAddr)

	// Signal our channel any time the schedule is updated.
	scheduleUpdated := make(chan struct{})
	sched.onScheduleUpdated = func() {
		select {
		case scheduleUpdated <- struct{}{}:
		default:
		}
	}

	// Configure all timers and intervals to be muy rapido.
	sched.cfg.SchedulingInterval = 5 * time.Millisecond
	sched.cfg.EnqueueInterval = 5 * time.Millisecond
	sched.cfg.StartupObserveTime = 10 * time.Millisecond
	sched.cfg.JobLeaseExpiry = 10 * time.Millisecond
	sched.cfg.LookbackOnNoCommit = 1 * time.Minute
	sched.cfg.MaxScanAge = 1 * time.Hour
	sched.cfg.JobSize = 3 * time.Millisecond

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, sched))

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.WithoutCancel(ctx), sched))
	})

	require.Eventually(t, sched.observationCompleteLocked, 5*time.Second, 10*time.Millisecond)

	// Partition i gets 10*i records.
	for i := range int32(4) {
		for n := range 10 * i {
			<-scheduleUpdated

			produceResult := cli.ProduceSync(ctx, &kgo.Record{
				Timestamp: time.Now(),
				Value:     fmt.Appendf(nil, "value-%d-%d", i, n),
				Topic:     "ingest",
				Partition: i,
			})
			require.NoError(t, produceResult.FirstErr())
		}
	}

	require.Eventually(t, func() bool {
		return sched.jobs.count() > 0
	}, 30*time.Second, 10*time.Millisecond)

	var spec schedulerpb.JobSpec
	clientDone := make(chan struct{})

	// Simulate a client doing some client stuff.
	go func() {
		var key jobKey
		var err error
		key, spec, err = sched.assignJob("w0")
		require.NoError(t, err)
		require.NoError(t, sched.updateJob(key, "w0", true, spec))
		close(clientDone)
	}()
	<-clientDone

	require.NoError(t, sched.flushOffsetsToKafka(ctx))

	// And our offsets should have advanced.
	offs, err := sched.fetchCommittedOffsets(ctx)
	require.NoError(t, err)
	o, ok := offs.Lookup(spec.Topic, spec.Partition)
	require.True(t, ok)
	require.Equal(t, spec.EndOffset, o.At)
}

func TestStartup(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	// (a new scheduler starts in observation mode.)
	sched.getPartitionState("ingest", 64).initCommit(1000)
	sched.getPartitionState("ingest", 65).initCommit(256)
	sched.getPartitionState("ingest", 66).initCommit(57)

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
			EndOffset:   1100,
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
			EndOffset:   300,
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
			EndOffset:   100,
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
	e := sched.jobs.add("ingest/65/300", schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   65,
		StartOffset: 300,
		EndOffset:   400,
	})
	require.NoError(t, e)
	a1key, a1spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a1spec)
	require.Equal(t, "ingest/65/300", a1key.id)

	requireGaps(t, sched.register.(*prometheus.Registry), 0, 0)
}

func requireGaps(t *testing.T, reg *prometheus.Registry, planned, committed int, msgAndArgs ...any) {
	t.Helper()

	var b strings.Builder

	b.WriteString(`# HELP cortex_blockbuilder_scheduler_job_gap_detected The number of times an unexpected gap was detected between jobs.
		# TYPE cortex_blockbuilder_scheduler_job_gap_detected counter
		`)

	b.WriteString(fmt.Sprintf(
		"cortex_blockbuilder_scheduler_job_gap_detected{offset_type=\"planned\"} %d\n", planned))
	b.WriteString(fmt.Sprintf(
		"cortex_blockbuilder_scheduler_job_gap_detected{offset_type=\"committed\"} %d\n", committed))

	require.NoError(t,
		promtest.GatherAndCompare(reg, strings.NewReader(b.String()),
			"cortex_blockbuilder_scheduler_job_gap_detected"),
		msgAndArgs...,
	)
}

// Verify that we skip jobs that are before the committed offset due to either extraneous bug situations
// or ongoing job completions.
func TestAssignJobSkipsObsoleteOffsets(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
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

	p1 := sched.getPartitionState("ingest", 1)
	p1.initCommit(256)
	p2 := sched.getPartitionState("ingest", 2)
	p2.initCommit(500)

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

func TestAssignJobSkipsObsoleteOffsets_PriorScheduler(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())
	// Add some jobs, then move the committed offsets past some of them.
	s1 := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 256,
		EndOffset:   9111,
	}

	require.NoError(t, sched.jobs.add("ingest/1/256", s1))
	require.Equal(t, 1, sched.jobs.count())

	// Simulate a completion of a job that was created by a prior scheduler.
	p1 := sched.getPartitionState("ingest", 1)
	p1.initCommit(5000)
	// Advancing offsets doesn't actually remove any jobs.
	require.Equal(t, 1, sched.jobs.count())

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
		[]*schedulerpb.JobSpec{&s1}, assignedJobs,
		"s1 should not have been skipped",
	)
}

func TestObservations(t *testing.T) {
	sched, _ := mustScheduler(t, 10)
	// Initially we're in observation mode. We have Kafka's commit offsets, but no client jobs.

	sched.getPartitionState("ingest", 1).initCommit(5000)
	sched.getPartitionState("ingest", 2).initCommit(800)
	sched.getPartitionState("ingest", 3).initCommit(974)
	sched.getPartitionState("ingest", 4).initCommit(500)
	sched.getPartitionState("ingest", 5).initCommit(12000)
	sched.getPartitionState("ingest", 6) // no commit for 6
	sched.getPartitionState("ingest", 7) // no commit for 7
	sched.getPartitionState("ingest", 8).initCommit(1000)
	sched.getPartitionState("ingest", 9).initCommit(1000)

	{
		nq := newJobQueue(988*time.Hour, noOpJobCreationPolicy[schedulerpb.JobSpec]{}, 2, sched.metrics, test.NewTestingLogger(t))
		sched.jobs = nq
		sched.finalizeObservations()
		require.Empty(t, nq.jobs, "No observations, no jobs")
	}

	type observation struct {
		key              jobKey
		spec             schedulerpb.JobSpec
		workerID         string
		complete         bool
		expectStartupErr error
		expectNormalErr  error
	}
	var clientData []observation
	const (
		complete   = true
		inProgress = false
	)
	maybeBadEpoch := errors.New("maybe bad epoch")
	mkJob := func(isComplete bool, worker string, partition int32, id string, epoch int64, startOffset, endOffset int64, expectStartupErr, expectNormalErr error) {
		clientData = append(clientData, observation{
			key: jobKey{id: id, epoch: epoch},
			spec: schedulerpb.JobSpec{
				Topic:       "ingest",
				Partition:   partition,
				StartOffset: startOffset,
				EndOffset:   endOffset,
			},
			workerID:         worker,
			complete:         isComplete,
			expectStartupErr: expectStartupErr,
			expectNormalErr:  expectNormalErr,
		})
	}

	// Rig up a bunch of data that clients are collectively sending.

	// Partition 1: one job in progress.
	mkJob(inProgress, "w0", 1, "ingest/1/5000", 10, 5000, 6000, nil, nil)

	// Partition 2: Many complete jobs, followed by an in-progress job.
	mkJob(complete, "w0", 2, "ingest/2/400", 3, 400, 800, nil, nil)
	mkJob(complete, "w0", 2, "ingest/2/800", 4, 800, 1000, nil, nil)
	mkJob(complete, "w0", 2, "ingest/2/1000", 5, 1000, 1200, nil, nil)
	mkJob(complete, "w0", 2, "ingest/2/1200", 11, 1200, 1400, nil, nil)
	mkJob(inProgress, "w0", 2, "ingest/2/1400", 12, 1400, 1600, nil, nil)

	// (Partition 3 has no updates.)

	// Partition 4 has a series of completed jobs that are entirely after what was found in Kafka.
	mkJob(complete, "w0", 4, "ingest/4/500", 15, 500, 600, nil, nil)
	mkJob(complete, "w1", 4, "ingest/4/600", 16, 600, 700, nil, nil)
	mkJob(complete, "w2", 4, "ingest/4/700", 17, 700, 800, nil, nil)
	mkJob(complete, "w3", 4, "ingest/4/800", 18, 800, 900, nil, nil)
	// Here's a conflicting completion report from a worker whose lease was revoked at one point. It should be effectively dropped.
	mkJob(complete, "w99", 4, "ingest/4/600", 6, 600, 700, maybeBadEpoch, maybeBadEpoch)

	// Partition 5 has a number of conflicting in-progress reports.
	mkJob(inProgress, "w100", 5, "ingest/5/12000", 30, 12000, 13000, maybeBadEpoch, errBadEpoch)
	mkJob(inProgress, "w101", 5, "ingest/5/12000", 31, 12000, 13000, maybeBadEpoch, errBadEpoch)
	mkJob(inProgress, "w102", 5, "ingest/5/12000", 32, 12000, 13000, maybeBadEpoch, errBadEpoch)
	mkJob(inProgress, "w103", 5, "ingest/5/12000", 33, 12000, 13000, maybeBadEpoch, errBadEpoch)
	mkJob(inProgress, "w104", 5, "ingest/5/12000", 34, 12000, 13000, nil, nil)

	// Partition 6 has a complete job but had no commit at startup. We allow
	// transitioning from empty to commit to any offset.
	mkJob(complete, "w0", 6, "ingest/6/500", 48, 500, 600, nil, nil)
	// Partition 7 has an in-progress job, but had no commit at startup. We
	// honor this job and allow it to influence the planned/resumption offset.
	mkJob(inProgress, "w1", 7, "ingest/7/92874", 52, 92874, 93874, nil, nil)

	// Partition 8 has a number of reports and has a hole that should should not be passed.
	mkJob(complete, "w0", 8, "ingest/8/1000", 53, 1000, 1100, nil, nil)
	mkJob(complete, "w1", 8, "ingest/8/1100", 54, 1100, 1200, nil, nil)
	mkJob(complete, "w2", 8, "ingest/8/1200", 55, 1200, 1300, nil, nil)
	// this one is absent mkJob(complete, "w3", 8, "ingest/8/1300", 56, 1300, 1400, nil)
	mkJob(complete, "w4", 8, "ingest/8/1400", 57, 1400, 1500, nil, nil)
	mkJob(complete, "w5", 8, "ingest/8/1500", 58, 1500, 1600, nil, nil)
	mkJob(complete, "w6", 8, "ingest/8/1600", 59, 1600, 1700, nil, nil)

	// Partition 9 is similar to 8 but the gap is followed by an in-progress job.
	mkJob(complete, "w0", 9, "ingest/9/1000", 60, 1000, 1100, nil, nil)
	mkJob(complete, "w1", 9, "ingest/9/1100", 61, 1100, 1200, nil, nil)
	mkJob(complete, "w2", 9, "ingest/9/1200", 62, 1200, 1300, nil, nil)
	// this one is absent mkJob(complete, "w3", 9, "ingest/9/1300", 63, 1300, 1400, nil)
	mkJob(inProgress, "w4", 9, "ingest/9/1400", 64, 1400, 1500, nil, errJobNotFound)

	sendUpdates := func() {
		// Send all updates multiple times in random order.
		for i := range 10 {
			rnd := rand.New(rand.NewSource(int64(i)))
			t.Run(fmt.Sprintf("send_updates_seed_%d", i), func(t *testing.T) {
				rnd.Shuffle(len(clientData), func(i, j int) { clientData[i], clientData[j] = clientData[j], clientData[i] })
				for _, c := range clientData {
					t.Log("sending update", c.key, c.workerID)
					err := sched.updateJob(c.key, c.workerID, c.complete, c.spec)
					expectedErr := c.expectStartupErr
					if sched.observationComplete {
						expectedErr = c.expectNormalErr
					}

					if errors.Is(expectedErr, maybeBadEpoch) {
						require.True(t, errors.Is(err, errBadEpoch) || err == nil, "job %V: expected either bad epoch or no error, got %v", c.key, err)
					} else if expectedErr != nil {
						require.ErrorIs(t, err, expectedErr, "job %V: expected %v, got %v", c.key, expectedErr, err)
					} else {
						require.NoError(t, err, "job %V: expected no error", c.key)
					}
				}
			})
		}
	}

	verifyCommits := func() {
		sched.requireOffset(t, "ingest", 1, 5000, "ingest/1 is in progress, so we should not move the offset")
		sched.requireOffset(t, "ingest", 2, 1400, "ingest/2 job was complete up to 1400, so it should move the offset forward")
		sched.requireOffset(t, "ingest", 3, 974, "ingest/3 should be unchanged - no updates")
		sched.requireOffset(t, "ingest", 4, 900, "ingest/4 should be moved forward to account for the completed jobs")
		sched.requireOffset(t, "ingest", 5, 12000, "ingest/5 has nothing new completed")
		sched.requireOffset(t, "ingest", 6, 600, "ingest/6 allowed to move the commit")
		sched.requireOffset(t, "ingest", 7, offsetEmpty, "ingest/7 has an in-progress job, but had no commit at startup")
		sched.requireOffset(t, "ingest", 8, 1300, "ingest/8 should be committed only until the gap")
		sched.requireOffset(t, "ingest", 9, 1300, "ingest/9 should be committed only until the gap")
	}

	sendUpdates()
	sched.completeObservationMode(context.Background())

	verifyCommits()

	// Make sure the resumption offsets account for the gaps.
	offs, err := sched.consumptionOffsets(context.Background(), "ingest", time.Now())
	require.NoError(t, err)
	require.ElementsMatch(t, []partitionOffsets{
		{topic: "ingest", partition: 0, resume: 0},
		{topic: "ingest", partition: 1, resume: 6000},
		{topic: "ingest", partition: 2, resume: 1600},
		{topic: "ingest", partition: 3, resume: 974},
		{topic: "ingest", partition: 4, resume: 900},
		{topic: "ingest", partition: 5, resume: 13000},
		{topic: "ingest", partition: 6, resume: 600},
		{topic: "ingest", partition: 7, resume: 93874},
		{topic: "ingest", partition: 8, resume: 1300},
		{topic: "ingest", partition: 9, resume: 1300},
	}, offs)

	require.Len(t, sched.jobs.jobs, 4, "should be 4 in-progress jobs")
	require.Equal(t, 65, int(sched.jobs.epoch))

	// Verify that the same set of updates can be sent now that we're out of
	// observation mode, and that offsets are not changed.
	sendUpdates()
	verifyCommits()
}

func (s *BlockBuilderScheduler) requireOffset(t *testing.T, topic string, partition int32, expected int64, msgAndArgs ...any) {
	t.Helper()
	ps := s.getPartitionState(topic, partition)
	require.Equal(t, expected, ps.committed.offset(), msgAndArgs...)
}

func TestOffsetMovement(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ps := sched.getPartitionState("ingest", 1)
	ps.initCommit(5000)
	sched.completeObservationMode(context.Background())

	spec := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 5000,
		EndOffset:   6000,
	}

	e := sched.jobs.add("ingest/1/5524", spec)
	require.NoError(t, e)
	ps.addPlannedJob("ingest/1/5524", spec)
	key, _, err := sched.jobs.assign("w0")
	require.NoError(t, err)

	require.NoError(t, sched.updateJob(key, "w0", false, spec))
	sched.requireOffset(t, "ingest", 1, 5000, "ingest/1 is in progress, so we should not move the offset")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	sched.requireOffset(t, "ingest", 1, 6000, "ingest/1 is complete, so we should advance")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	sched.requireOffset(t, "ingest", 1, 6000, "re-completing the same job shouldn't change the commit")

	p1 := sched.getPartitionState("ingest", 1)
	p1.committed.advance("ancient_job", schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   1,
		StartOffset: 1000,
		EndOffset:   2000,
	})
	sched.requireOffset(t, "ingest", 1, 6000, "committed offsets cannot rewind")

	p2 := sched.getPartitionState("ingest", 2)
	p2.committed.advance("ancient_job2", schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   2,
		StartOffset: 6000,
		EndOffset:   6222,
	})
	sched.requireOffset(t, "ingest", 2, 6222, "should create knowledge of partition 2")
}

func TestKafkaFlush(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ctx := context.Background()
	sched.completeObservationMode(ctx)

	flushAndRequireOffsets := func(topic string, offsets map[int32]int64, args ...any) {
		require.NoError(t, sched.flushOffsetsToKafka(ctx))

		offs, err := sched.fetchCommittedOffsets(ctx)
		require.NoError(t, err)
		offcount := 0
		offs.Each(func(o kadm.Offset) {
			offcount++
		})
		require.Equal(t, len(offsets), offcount)

		for partition, expected := range offsets {
			o, ok := offs.Lookup(topic, partition)
			require.True(t, ok, args...)
			require.Equal(t, expected, o.At, args...)
		}
	}

	flushAndRequireOffsets("ingest", map[int32]int64{}, "no group found -> no offsets")

	_ = sched.getPartitionState("ingest", 0)
	// (No commit yet for p0.)

	p1 := sched.getPartitionState("ingest", 1)
	p1.initCommit(2000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
	})

	p4 := sched.getPartitionState("ingest", 4)
	p4.initCommit(65535)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
		4: 65535,
	})

	p1.initCommit(4000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 4000,
		4: 65535,
	}, "should be able to advance an existing offset")

	reg := sched.register.(*prometheus.Registry)
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_partition_committed_offset The observed committed offset of each partition.
		# TYPE cortex_blockbuilder_scheduler_partition_committed_offset gauge
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="1"} 4000
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="4"} 65535
	`), "cortex_blockbuilder_scheduler_partition_committed_offset"), "should only modify commit gauge for non-empty commit offsets")
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_partition_planned_offset The planned offset of each partition.
		# TYPE cortex_blockbuilder_scheduler_partition_planned_offset gauge
		cortex_blockbuilder_scheduler_partition_planned_offset{partition="1"} 4000
		cortex_blockbuilder_scheduler_partition_planned_offset{partition="4"} 65535
	`), "cortex_blockbuilder_scheduler_partition_planned_offset"), "should only modify planned gauge for non-empty planned offsets")
}

func TestUpdateSchedule(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest")
	sched, cli := mustSchedulerWithKafkaAddr(t, kafkaAddr)
	reg := sched.register.(*prometheus.Registry)

	sched.completeObservationMode(ctx)

	// Partition i gets i records.
	for i := range int32(4) {
		for n := range i {
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

func TestInitialOffsetProbing(t *testing.T) {
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
			expectedRanges: []*offsetTime{{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
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
			expectedRanges: []*offsetTime{{offset: 2000, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
		},
		"empty partition: no data": {
			offsets:        []*offsetTime{},
			resume:         0,
			end:            0,
			jobSize:        200 * time.Millisecond,
			endTime:        time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC),
			minScanTime:    time.Date(2025, 2, 20, 10, 0, 0, 600*1000000, time.UTC),
			expectedRanges: []*offsetTime{{offset: 0, time: time.Date(2025, 3, 1, 10, 0, 0, 600*1000000, time.UTC)}},
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
			expectedRanges: []*offsetTime{{offset: 1004, time: time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC)}},
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
			expectedRanges: []*offsetTime{{offset: 1003, time: time.Date(2025, 3, 1, 10, 3, 40, 0, time.UTC)}},
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
			f := &mockOffsetFinder{offsets: tt.offsets, end: tt.end, distinctTimes: make(map[time.Time]struct{})}
			j, err := probeInitialOffsets(ctx, f, "topic", 0, tt.start, tt.resume, tt.end, tt.endTime, tt.jobSize, tt.minScanTime, test.NewTestingLogger(t))
			assert.NoError(t, err)
			assert.EqualValues(t, tt.expectedRanges, j, tt.msg)
		})
	}
}

// Create an offset finder that we can prepopulate with offset scenarios.
type mockOffsetFinder struct {
	offsets       []*offsetTime
	end           int64
	distinctTimes map[time.Time]struct{}
}

func (o *mockOffsetFinder) offsetAfterTime(_ context.Context, _ string, _ int32, t time.Time) (int64, time.Time, error) {
	o.distinctTimes[t] = struct{}{}
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

func TestPopulateInitialJobs_ProbeTimesReused(t *testing.T) {
	// Ensure that the probe times are reused across partitions, which is a
	// necessary condition for cached offset reuse in the offsetFinder.
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxScanAge = 1 * time.Hour

	finder := &mockOffsetFinder{
		offsets: []*offsetTime{
			{offset: 100, time: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)},
			{offset: 200, time: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)},
			{offset: 300, time: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)},
		},
		end:           1000,
		distinctTimes: make(map[time.Time]struct{}),
	}

	consumeOffs := []partitionOffsets{
		{topic: "topic", partition: 0, start: 100, resume: 100, end: 1000},
		{topic: "topic", partition: 1, start: 100, resume: 100, end: 1000},
		{topic: "topic", partition: 2, start: 100, resume: 100, end: 1000},
		{topic: "topic", partition: 3, start: 100, resume: 100, end: 1000},
	}

	sched.populateInitialJobs(context.Background(), consumeOffs, finder, time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC))
	require.Len(t, finder.distinctTimes, 4, "four partitions will each be probed four times, but the probe times should be the same across each partition")
}

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
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
	sz := 1 * time.Hour

	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	var job *schedulerpb.JobSpec
	var err error

	job, err = pt.updateEndOffset(100, time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)
	job, err = pt.updateEndOffset(200, time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 100,
		EndOffset:   200,
	}, job)
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
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 200,
		EndOffset:   300,
	}, job)
	require.NoError(t, err)

	// And, if the time goes backwards, we return an error.
	job, err = pt.updateEndOffset(300, z.Add(-2*time.Hour), sz)
	require.Nil(t, job)
	require.ErrorContains(t, err, "time went backwards")
}

func TestPartitionState_TerminallyDormantPartition(t *testing.T) {
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
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
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
	sz := 1 * time.Hour

	// A bunch of data observed:
	var j *schedulerpb.JobSpec
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
	assert.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 10,
		EndOffset:   12,
	}, j)
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

func TestPartitionState_ParallelJobs(t *testing.T) {

	t.Run("planned job order required", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		// It is a logical error to add a job to the planned list out of order. The safe completion logic requires it.

		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})
		require.Panics(t, func() {
			ps.addPlannedJob("job2", schedulerpb.JobSpec{
				Topic:       "ingest",
				Partition:   1,
				StartOffset: 200,
				EndOffset:   300,
			})
		})
	})

	// Test 1: Complete a single job at the front of the queue
	t.Run("complete_single_job_at_front", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		jobSpec := schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		}
		ps.addPlannedJob("job1", jobSpec)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(100), ps.committed.offset())

		err := ps.completeJob("job1")
		require.NoError(t, err)

		// Verify job was completed and garbage collected
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(200), ps.committed.offset())
	})

	t.Run("complete_nonexistent_job", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)
		// Try to complete a job that doesn't exist
		err := ps.completeJob("nonexistent_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)
	})

	// Test 3: Complete multiple jobs in order (garbage collection)
	t.Run("complete_multiple_jobs_in_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		// Add multiple planned jobs
		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job2", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 200,
			EndOffset:   300,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})

		// Verify initial state
		require.Equal(t, 3, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 3)
		require.Equal(t, int64(100), ps.committed.offset())

		// Complete first job - should be garbage collected
		err := ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 2, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 2)
		require.Equal(t, int64(200), ps.committed.offset())

		// Complete second job - should be garbage collected
		err = ps.completeJob("job2")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(300), ps.committed.offset())

		// Complete third job - should be garbage collected
		err = ps.completeJob("job3")
		require.NoError(t, err)
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(400), ps.committed.offset())
	})

	// Test 4: Complete jobs out of order (only front jobs get garbage collected)
	t.Run("complete_jobs_out_of_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		// Add multiple planned jobs
		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job2", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 200,
			EndOffset:   300,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})

		// Complete job2 first (out of order)
		err := ps.completeJob("job2")
		require.NoError(t, err)

		// Job2 should be marked complete but not garbage collected yet
		require.Equal(t, 3, ps.plannedJobs.Len(), "expecting no garbage collection yet")
		require.Len(t, ps.plannedJobsMap, 3, "expecting no garbage collection yet")
		require.Equal(t, int64(100), ps.committed.offset(), "should be no advancement yet")

		// Complete job1 - should garbage collect both job1 and job2
		err = ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len(), "expecting garbage collection of job1 and job2")
		require.Len(t, ps.plannedJobsMap, 1, "expecting garbage collection of job1 and job2")
		require.Equal(t, int64(300), ps.committed.offset(), "should be advanced commit to the end of job3")
		require.Equal(t, "job3", ps.plannedJobs.Front().Value.(*jobState).jobID, "expecting job3 to be the remaining planned job")
	})

	// Test 5: Complete job with empty partition state
	t.Run("complete_job_empty_partition", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		// Try to complete a job when no jobs exist
		err := ps.completeJob("any_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)

		// Verify state remains unchanged
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(100), ps.committed.offset())
	})
}

func TestBlockBuilderScheduler_EnqueuePendingJobs(t *testing.T) {
	// Test that job detection and enqueueing work as expected w/r/t the
	// job creation policy.

	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 1
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)

	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   20,
	})
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 20,
		EndOffset:   30,
	})
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 30,
		EndOffset:   40,
	})

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
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)

	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   20,
	})
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 20,
		EndOffset:   30,
	})
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 30,
		EndOffset:   40,
	})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len(), "a single enqueue call should have drained all pending jobs")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs_CommitRace(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)
	pt.initCommit(20)
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   20,
	})

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count(), "the job should have been ignored because it's behind the committed offset")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs_StartupRace(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)
	// Assume at startup we compute this job offset range:
	pt.addPendingJob(&schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   30,
	})

	// But the job we imported from the existing workers now being completed may be (10, 20):
	pt.initCommit(20)

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 1, sched.jobs.count(), "the job should NOT have been ignored because it isn't fully behind the commit")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs_GapDetection(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	reg := sched.register.(*prometheus.Registry)

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)
	// Assume at startup we compute this set of job specs:
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 0, EndOffset: 30})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 30, EndOffset: 40})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 40, EndOffset: 50})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	assert.True(t, pt.planned.empty())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.planned.offset())

	requireGaps(t, reg, 0, 0)

	// this one introduces a gap:
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 60, EndOffset: 70})

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.planned.offset())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(70), pt.planned.offset())

	requireGaps(t, reg, 1, 0)

	// the gap may not be the first job:
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 70, EndOffset: 80})
	// (gap)
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 100, EndOffset: 110})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 110, EndOffset: 120})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count())
	assert.Equal(t, int64(70), pt.planned.offset())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 7, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(120), pt.planned.offset())

	requireGaps(t, reg, 2, 0)

	// Now simulate completing these jobs and expect commit gaps where appropriate.
	expectedStart := int64(0)
	commitGaps := 0

	for j := 0; ; j++ {
		k, spec, err := sched.assignJob("w0")
		if errors.Is(err, errNoJobAvailable) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, sched.updateJob(k, "w0", true, spec))

		if spec.StartOffset != expectedStart {
			commitGaps++
		}

		expectedStart = spec.EndOffset
		requireGaps(t, reg, 2, commitGaps, "expected %d commit gaps at job %d", commitGaps, j)
	}
}

func TestBlockBuilderScheduler_NoCommit_NoGap(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	reg := sched.register.(*prometheus.Registry)

	const part int32 = 1
	requireGaps(t, reg, 0, 0)

	pp := sched.getPartitionState("ingest", part)
	require.True(t, pp.planned.empty())
	require.True(t, pp.committed.empty())

	k := jobKey{"myjob5", 5}
	spec := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 10,
		EndOffset:   20,
	}

	pp.planned.advance(k.id, spec)
	requireGaps(t, reg, 0, 0, "advancing an empty planned offset should not register a gap")

	pp.committed.advance(k.id, spec)
	requireGaps(t, reg, 0, 0, "advancing an empty committed offset should not register a gap")

	// Now create a gap:
	k2 := jobKey{"myjob7", 23}
	spec2 := schedulerpb.JobSpec{
		Topic:       "ingest",
		Partition:   part,
		StartOffset: 40,
		EndOffset:   50,
	}

	pp.planned.advance(k2.id, spec2)
	requireGaps(t, reg, 1, 0, "a gap after a non-empty planned offset should register a gap")

	pp.committed.advance(k2.id, spec2)
	requireGaps(t, reg, 1, 1, "a gap after a non-empty committed offset should register a gap")
}

// TestStartupToRegularModeJobProduction tests that the scheduler correctly
// produces jobs when transitioning from startup mode to regular mode with a
// variety of cases.
func TestStartupToRegularModeJobProduction(t *testing.T) {
	type endOffsetObservation struct {
		offset    int64
		timestamp time.Time
	}

	type testCase struct {
		name                     string
		partitionAbsentInitially bool
		initialStart             int64
		initialResume            int64
		initialEnd               int64
		initialTime              time.Time
		offsets                  []*offsetTime
		futureObservations       []endOffsetObservation
		expectedFinalEnd         int64
	}

	tests := [...]testCase{
		{
			name: "dormant partition that receives data after startup",
			// The scenario is this:
			// * we learn the resume and end offsets at startup of a partition that is inactive. (resume == end)
			// * this partition's end offset grows before we produce its first job.
			// * the next time we produce a job it *should* start at the end offset we learned at startup.
			initialStart:  50,
			initialResume: 100,
			initialEnd:    100,
			initialTime:   time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			futureObservations: []endOffsetObservation{
				{offset: 200, timestamp: time.Date(2025, 3, 1, 10, 45, 0, 0, time.UTC)},
				{offset: 300, timestamp: time.Date(2025, 3, 1, 11, 15, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 300,
		},
		{
			name:          "multiple observations, same bucket",
			initialStart:  100,
			initialResume: 100,
			initialEnd:    100,
			initialTime:   time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			futureObservations: []endOffsetObservation{
				{offset: 150, timestamp: time.Date(2025, 3, 1, 10, 30, 0, 0, time.UTC)},
				{offset: 200, timestamp: time.Date(2025, 3, 1, 10, 45, 0, 0, time.UTC)},
				{offset: 250, timestamp: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 250,
		},
		{
			name:          "multiple observations, different buckets",
			initialStart:  100,
			initialResume: 100,
			initialEnd:    100,
			initialTime:   time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			futureObservations: []endOffsetObservation{
				{offset: 200, timestamp: time.Date(2025, 3, 1, 10, 30, 0, 0, time.UTC)},
				{offset: 300, timestamp: time.Date(2025, 3, 1, 11, 15, 0, 0, time.UTC)},
				{offset: 400, timestamp: time.Date(2025, 3, 1, 12, 30, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 400,
		},
		{
			name:          "no growth after initial",
			initialStart:  100,
			initialResume: 200,
			initialEnd:    200,
			initialTime:   time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC),
			futureObservations: []endOffsetObservation{
				{offset: 200, timestamp: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 200, timestamp: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 200,
		},
		{
			name:          "end later than resume",
			initialStart:  100,
			initialResume: 200,
			initialEnd:    500,
			initialTime:   time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC),
			offsets: []*offsetTime{
				{offset: 200, time: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)},
				{offset: 500, time: time.Date(2025, 3, 1, 10, 15, 0, 0, time.UTC)},
			},
			futureObservations: []endOffsetObservation{
				{offset: 600, timestamp: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 700, timestamp: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 700,
		},
		{
			name:                     "partition absent at startup",
			partitionAbsentInitially: true,
			initialTime:              time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC),
			futureObservations: []endOffsetObservation{
				{offset: 600, timestamp: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: 700, timestamp: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			expectedFinalEnd: 700,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched, _ := mustScheduler(t, 1)
			sched.cfg.MaxScanAge = 1 * time.Hour
			sched.cfg.JobSize = 1 * time.Hour

			// Create a mock offset finder that returns the initial end offset
			finder := &mockOffsetFinder{
				offsets:       tt.offsets,
				end:           tt.initialEnd,
				distinctTimes: make(map[time.Time]struct{}),
			}

			var consumeOffs []partitionOffsets

			if !tt.partitionAbsentInitially {
				consumeOffs = []partitionOffsets{
					{
						topic:     "topic",
						partition: 0,
						start:     tt.initialStart,
						resume:    tt.initialResume,
						end:       tt.initialEnd,
					},
				}
			}

			// Call populateInitialJobs to set up initial state
			sched.populateInitialJobs(context.Background(), consumeOffs, finder, tt.initialTime)

			collectedJobs := []*schedulerpb.JobSpec{}

			// Apply future end offset observations and collect jobs returned from updateEndOffset
			ps := sched.getPartitionState("topic", 0)

			for _, obs := range tt.futureObservations {
				job, err := ps.updateEndOffset(obs.offset, obs.timestamp, sched.cfg.JobSize)
				require.NoError(t, err)
				if job != nil {
					collectedJobs = append(collectedJobs, job)
				}
			}

			// Verify that jobs cover [resume, final_end_offset) without gaps

			if len(collectedJobs) == 0 {
				require.GreaterOrEqual(t, tt.initialResume, tt.expectedFinalEnd,
					"only data-less partitions should produce no jobs")
				return
			}

			// Verify first job starts at resume offset
			require.Equal(t, tt.initialResume, collectedJobs[0].StartOffset,
				"first job should start at resume offset")

			// Verify last job ends at expected final end
			require.Equal(t, tt.expectedFinalEnd, collectedJobs[len(collectedJobs)-1].EndOffset,
				"last job should end at expected final end offset")

			// Verify no gaps and no overlaps between consecutive jobs
			for i := 0; i < len(collectedJobs)-1; i++ {
				current := collectedJobs[i]
				next := collectedJobs[i+1]

				require.Equal(t, current.EndOffset, next.StartOffset,
					"jobs should have no gaps: job %d ends at %d but next job starts at %d",
					i, current.EndOffset, next.StartOffset)
			}
		})
	}
}
