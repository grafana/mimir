// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

type dialerFunc func(ctx context.Context, network string, address string) (net.Conn, error)

func mustSchedulerWithKafkaAddrAndDialer(t *testing.T, addr string, dialer dialerFunc) (*BlockBuilderScheduler, *kgo.Client) {
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.Dialer(dialer),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // we will choose the partition of each record
	)
	require.NoError(t, err)
	t.Cleanup(cli.Close)

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Address:      flagext.StringSliceCSV{addr},
			Dialer:       dialer,
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
	sched.adminClients = []*kadm.Client{kadm.NewClient(cli)}
	require.NoError(t, err)
	return sched, cli
}

func mustScheduler(t *testing.T, partitions int32) (*BlockBuilderScheduler, *kgo.Client) {
	var vnet kfake.VirtualNetwork
	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitions, "ingest", testkafka.WithVirtualNetwork(&vnet))
	return mustSchedulerWithKafkaAddrAndDialer(t, kafkaAddr, vnet.DialContext)
}

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

func testMetrics(t testing.TB) *schedulerMetrics {
	t.Helper()
	m := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	return &m
}

// groupByPartition groups a flat list of per-(partition, WC) offsets by
// partition, matching what completeObservationMode passes to populateInitialJobs.
func groupByPartition(offs ...partitionOffsets) map[int32][]partitionOffsets {
	m := make(map[int32][]partitionOffsets)
	for _, off := range offs {
		m[off.partition] = append(m[off.partition], off)
	}
	return m
}

func TestPartitionStateBundlesWCsOnRollover(t *testing.T) {
	ps := newPartitionState("mimir-read-comp-1", 0, 2, true, testMetrics(t), log.NewNopLogger())
	jobSize := time.Hour
	t0 := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)

	// First observation in bucket t0 seeds the per-WC start offsets.
	ps.updateEndOffset(0, 100)
	ps.updateEndOffset(1, 200)
	job, err := ps.updateTime(t0, jobSize)
	require.NoError(t, err)
	require.Nil(t, job, "expected no job on first update")
	// More data arrives in the same bucket.
	ps.updateEndOffset(0, 150)
	ps.updateEndOffset(1, 260)
	_, err = ps.updateTime(t0.Add(10*time.Minute), jobSize)
	require.NoError(t, err)
	// Next bucket triggers a job covering both WCs.
	ps.updateEndOffset(0, 150)
	ps.updateEndOffset(1, 280)
	job, err = ps.updateTime(t0.Add(time.Hour), jobSize)
	require.NoError(t, err)
	require.NotNil(t, job, "expected a job on bucket rollover")
	if job.Topic != "mimir-read-comp-1" || job.Partition != 0 {
		t.Fatalf("unexpected job identity: topic=%q partition=%d", job.Topic, job.Partition)
	}
	if len(job.Ranges()) != 2 {
		t.Fatalf("unexpected job: %+v", job)
	}
	wantWC0 := schedulerpb.OffsetRange{StartOffset: 100, EndOffset: 150}
	wantWC1 := schedulerpb.OffsetRange{StartOffset: 200, EndOffset: 280}
	if job.Ranges()[0] != wantWC0 || job.Ranges()[1] != wantWC1 {
		t.Fatalf("clusterID ranges = %+v", job.Ranges())
	}
}

// TestPartitionState_AbsentWCKeepsPriorOffset asserts that when a WC is absent
// from an observation (it didn't report the partition that cycle), it keeps its
// previously observed end offset rather than being reset: it's omitted from the
// job while it has no new data, and once it does, its range resumes from the
// retained offset.
func TestPartitionState_AbsentWCKeepsPriorOffset(t *testing.T) {
	ps := newPartitionState("topic", 0, 2, true, testMetrics(t), log.NewNopLogger())
	jobSize := time.Hour
	t0 := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)

	// Seed both WCs in the first bucket.
	ps.updateEndOffset(0, 100)
	ps.updateEndOffset(1, 200)
	_, err := ps.updateTime(t0, jobSize)
	require.NoError(t, err)

	// Bucket 2: only WC 0 reports. WC 1 is absent, so it's omitted from the job
	// (no new data) and keeps its prior offset.
	ps.updateEndOffset(0, 150)
	job, err := ps.updateTime(t0.Add(time.Hour), jobSize)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Len(t, job.Ranges(), 1)
	require.Equal(t, schedulerpb.OffsetRange{StartOffset: 100, EndOffset: 150}, job.Ranges()[0])

	// Bucket 3: WC 1 finally reports. Its range must resume from the retained
	// offset (200), not reset to 0.
	ps.updateEndOffset(1, 260)
	job, err = ps.updateTime(t0.Add(2*time.Hour), jobSize)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Len(t, job.Ranges(), 1)
	require.Equal(t, schedulerpb.OffsetRange{StartOffset: 200, EndOffset: 260}, job.Ranges()[1])
}

func TestWCKafkaConfigs(t *testing.T) {
	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Topic:   "mimir-read-comp-1",
			Address: flagext.StringSliceCSV{"kafka-write-<write-compartment-id>:9092"},
		},
	}
	cfg.Compartments = compartments.Config{
		Enabled: true,
		Read:    compartments.ReadConfig{NumCompartments: 3},
		Write:   compartments.WriteConfig{NumCompartments: 3},
	}
	sched, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	got := sched.wcKafkaConfigs()
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for wc := 0; wc < 3; wc++ {
		if got[wc].Topic != "mimir-read-comp-1" {
			t.Fatalf("clusterID %d topic = %q", wc, got[wc].Topic)
		}
		want := fmt.Sprintf("kafka-write-%d:9092", wc)
		if got[wc].Address.String() != want {
			t.Fatalf("clusterID %d address = %q, want %q", wc, got[wc].Address.String(), want)
		}
	}
}

// observationCompleteLocked: a getter for tests.
func (s *BlockBuilderScheduler) observationCompleteLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observationComplete
}

// TestService tests the scheduler in a very basic way through its Service interface.
func TestService(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var vnet kfake.VirtualNetwork

		_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest", testkafka.WithVirtualNetwork(&vnet))

		cli, err := kgo.NewClient(
			kgo.SeedBrokers(kafkaAddr),
			kgo.Dialer(vnet.DialContext),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		require.NoError(t, err)
		t.Cleanup(cli.Close)

		cfg := Config{
			Kafka: ingest.KafkaConfig{
				Address:      flagext.StringSliceCSV{kafkaAddr},
				Topic:        "ingest",
				FetchMaxWait: 10 * time.Millisecond,
				Dialer:       vnet.DialContext,
			},
			ConsumerGroup:       "test-builder",
			SchedulingInterval:  1000000 * time.Hour,
			JobSize:             1 * time.Hour,
			MaxJobsPerPartition: 1,
		}

		reg := prometheus.NewPedanticRegistry()
		sched, err := New(cfg, test.NewTestingLogger(t), reg)
		require.NoError(t, err)
		sched.adminClients = []*kadm.Client{kadm.NewClient(cli)}

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
		offs, err := sched.fetchCommittedOffsets(ctx, 0)
		require.NoError(t, err)
		o, ok := offs.Lookup(spec.Topic, spec.Partition)
		require.True(t, ok)
		require.Equal(t, spec.Ranges()[0].EndOffset, o.At)
	})
}

func TestServiceStopsCleanlyDuringStartupObservation(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	sched.cfg.StartupObserveTime = time.Hour
	sched.cfg.LookbackOnNoCommit = time.Minute
	sched.cfg.MaxScanAge = time.Hour

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, sched))
	require.NoError(t, services.StopAndAwaitTerminated(ctx, sched))
	require.NoError(t, sched.FailureCase())
}

func TestStartup(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	// (a new scheduler starts in observation mode.)
	sched.getPartitionState("ingest", 64).initCommit(0, 1000)
	sched.getPartitionState("ingest", 65).initCommit(0, 256)
	sched.getPartitionState("ingest", 66).initCommit(0, 57)

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
		spec: schedulerpb.NewNonCompartmentJobSpec("ingest", 64, 1000, 1100),
	}
	j2 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/65/256",
			epoch: 11,
		},
		spec: schedulerpb.NewNonCompartmentJobSpec("ingest", 65, 256, 300),
	}
	j3 := job[schedulerpb.JobSpec]{
		key: jobKey{
			id:    "ingest/66/57",
			epoch: 12,
		},
		spec: schedulerpb.NewNonCompartmentJobSpec("ingest", 66, 57, 100),
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
	e := sched.jobs.add("ingest/65/300", schedulerpb.NewNonCompartmentJobSpec("ingest", 65, 300, 400))
	require.NoError(t, e)
	a1key, a1spec, err := sched.assignJob("w0")
	require.NoError(t, err)
	require.NotZero(t, a1spec)
	require.Equal(t, "ingest/65/300", a1key.id)

	requireGaps(t, sched.register.(*prometheus.Registry), 0, 0)
}

func ptrSpec(topic string, partition int32, startOffset, endOffset int64) *schedulerpb.JobSpec {
	s := schedulerpb.NewNonCompartmentJobSpec(topic, partition, startOffset, endOffset)
	return &s
}

func requireGaps(t *testing.T, reg *prometheus.Registry, planned, committed int, msgAndArgs ...any) {
	t.Helper()

	var b strings.Builder

	b.WriteString(`# HELP cortex_blockbuilder_scheduler_job_gap_detected The number of times an unexpected gap was detected between jobs.
		# TYPE cortex_blockbuilder_scheduler_job_gap_detected counter
		`)

	fmt.Fprintf(&b,
		"cortex_blockbuilder_scheduler_job_gap_detected{offset_type=\"planned\"} %d\n", planned)
	fmt.Fprintf(&b,
		"cortex_blockbuilder_scheduler_job_gap_detected{offset_type=\"committed\"} %d\n", committed)

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
	s1 := schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 256, 9111)
	s2 := schedulerpb.NewNonCompartmentJobSpec("ingest", 2, 50, 128)
	s3 := schedulerpb.NewNonCompartmentJobSpec("ingest", 2, 700, 900)

	require.NoError(t, sched.jobs.add("ingest/1/256", s1))
	require.NoError(t, sched.jobs.add("ingest/2/50", s2))
	require.NoError(t, sched.jobs.add("ingest/2/700", s3))

	require.Equal(t, 3, sched.jobs.count())

	p1 := sched.getPartitionState("ingest", 1)
	p1.initCommit(0, 256)
	p2 := sched.getPartitionState("ingest", 2)
	p2.initCommit(0, 500)

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
	s1 := schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 256, 9111)

	require.NoError(t, sched.jobs.add("ingest/1/256", s1))
	require.Equal(t, 1, sched.jobs.count())

	// Simulate a completion of a job that was created by a prior scheduler.
	p1 := sched.getPartitionState("ingest", 1)
	p1.initCommit(0, 5000)
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

	sched.getPartitionState("ingest", 1).initCommit(0, 5000)
	sched.getPartitionState("ingest", 2).initCommit(0, 800)
	sched.getPartitionState("ingest", 3).initCommit(0, 974)
	sched.getPartitionState("ingest", 4).initCommit(0, 500)
	sched.getPartitionState("ingest", 5).initCommit(0, 12000)
	sched.getPartitionState("ingest", 6) // no commit for 6
	sched.getPartitionState("ingest", 7) // no commit for 7
	sched.getPartitionState("ingest", 8).initCommit(0, 1000)
	sched.getPartitionState("ingest", 9).initCommit(0, 1000)

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
			key:              jobKey{id: id, epoch: epoch},
			spec:             schedulerpb.NewNonCompartmentJobSpec("ingest", partition, startOffset, endOffset),
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
	offs, err := sched.consumptionOffsets(context.Background(), "ingest", time.Now(), 0)
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
	require.Equal(t, expected, ps.committedOffset(0), msgAndArgs...)
}

func TestOffsetMovement(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ps := sched.getPartitionState("ingest", 1)
	ps.initCommit(0, 5000)
	sched.completeObservationMode(context.Background())

	spec := schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 5000, 6000)

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
	p1.offsets[0].committed.advance("ancient_job", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 1000, 2000).Ranges()[0])
	sched.requireOffset(t, "ingest", 1, 6000, "committed offsets cannot rewind")

	p2 := sched.getPartitionState("ingest", 2)
	p2.offsets[0].committed.advance("ancient_job2", schedulerpb.NewNonCompartmentJobSpec("ingest", 2, 6000, 6222).Ranges()[0])
	sched.requireOffset(t, "ingest", 2, 6222, "should create knowledge of partition 2")
}

func TestKafkaFlush(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ctx := context.Background()
	sched.completeObservationMode(ctx)

	flushAndRequireOffsets := func(topic string, offsets map[int32]int64, args ...any) {
		require.NoError(t, sched.flushOffsetsToKafka(ctx))

		offs, err := sched.fetchCommittedOffsets(ctx, 0)
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
	p1.initCommit(0, 2000)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
	})

	p4 := sched.getPartitionState("ingest", 4)
	p4.initCommit(0, 65535)
	flushAndRequireOffsets("ingest", map[int32]int64{
		1: 2000,
		4: 65535,
	})

	p1.initCommit(0, 4000)
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

	var vnet kfake.VirtualNetwork

	_, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 4, "ingest", testkafka.WithVirtualNetwork(&vnet))
	sched, cli := mustSchedulerWithKafkaAddrAndDialer(t, kafkaAddr, vnet.DialContext)
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

func TestUpdateScheduleMultiWC(t *testing.T) {
	ctx := context.Background()
	const topic = "comp-0"

	// Two write WC clusters, each holding partition 0 of the read topic.
	_, addr0 := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, topic)
	_, addr1 := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, topic)
	cli0 := mustKafkaClient(t, addr0)
	cli1 := mustKafkaClient(t, addr1)

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Address:      flagext.StringSliceCSV{addr0},
			Topic:        topic,
			FetchMaxWait: 10 * time.Millisecond,
		},
		Compartments: compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: 2},
		},
		ConsumerGroup:       "test-builder",
		SchedulingInterval:  1000000 * time.Hour,
		JobSize:             time.Hour,
		MaxJobsPerPartition: 1,
	}
	reg := prometheus.NewPedanticRegistry()
	sched, err := New(cfg, test.NewTestingLogger(t), reg)
	require.NoError(t, err)
	sched.adminClients = []*kadm.Client{kadm.NewClient(cli0), kadm.NewClient(cli1)}
	sched.completeObservationMode(ctx)

	// WC 0 holds 3 records, WC 1 holds 5, on partition 0.
	produce := func(cli *kgo.Client, n int) {
		for i := 0; i < n; i++ {
			r := cli.ProduceSync(ctx, &kgo.Record{Timestamp: time.Now(), Value: []byte("v"), Topic: topic, Partition: 0})
			require.NoError(t, r.FirstErr())
		}
	}
	produce(cli0, 3)
	produce(cli1, 5)

	// Force a bucket rollover on the next updateSchedule.
	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	ps.jobBucket = time.Now().Add(-2 * time.Hour).Truncate(time.Hour)

	sched.updateSchedule(ctx)

	require.Equal(t, 1, ps.pendingJobs.Len())
	front := ps.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
	require.Len(t, front.Ranges(), 2)
	require.Equal(t, int64(3), front.Ranges()[0].EndOffset)
	require.Equal(t, int64(5), front.Ranges()[1].EndOffset)
}

func TestCommitPerWC(t *testing.T) {
	ctx := context.Background()
	const topic = "comp-0"

	_, addr0 := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, topic)
	_, addr1 := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, topic)
	cli0 := mustKafkaClient(t, addr0)
	cli1 := mustKafkaClient(t, addr1)

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Address:      flagext.StringSliceCSV{addr0},
			Topic:        topic,
			FetchMaxWait: 10 * time.Millisecond,
		},
		Compartments: compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: 2},
		},
		ConsumerGroup:       "test-builder",
		SchedulingInterval:  1000000 * time.Hour,
		JobSize:             time.Hour,
		MaxJobsPerPartition: 0,
	}
	sched, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	sched.adminClients = []*kadm.Client{kadm.NewClient(cli0), kadm.NewClient(cli1)}
	sched.completeObservationMode(ctx)

	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	spec := schedulerpb.JobSpec{
		Topic:     topic,
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 100},
			1: {StartOffset: 0, EndOffset: 200},
		},
	}
	ps.addPlannedJob("comp-0/0/0", spec)
	require.NoError(t, ps.completeJob("comp-0/0/0"))

	// Completing the job advances each WC's committed offset independently.
	require.Equal(t, int64(100), ps.committedOffset(0))
	require.Equal(t, int64(200), ps.committedOffset(1))

	committed0, _ := sched.snapOffsets(0)
	committed1, _ := sched.snapOffsets(1)
	require.NotEmpty(t, committed0)
	require.NotEmpty(t, committed1)

	require.NoError(t, sched.flushOffsetsToKafka(ctx))

	// Each WC's range was committed to its own cluster's consumer group.
	offs0, err := sched.adminClients[0].FetchOffsets(ctx, sched.cfg.ConsumerGroup)
	require.NoError(t, err)
	o0, ok := offs0.Lookup(topic, 0)
	require.True(t, ok)
	require.Equal(t, int64(100), o0.At)

	offs1, err := sched.adminClients[1].FetchOffsets(ctx, sched.cfg.ConsumerGroup)
	require.NoError(t, err)
	o1, ok := offs1.Lookup(topic, 0)
	require.True(t, ok)
	require.Equal(t, int64(200), o1.At)
}

// mustMultiWCScheduler builds a compartments-enabled scheduler backed by one
// real Kafka cluster per write WC, all serving the same read topic.
func mustMultiWCScheduler(t *testing.T, partitions int32, topic string, numWriteWCs int) (*BlockBuilderScheduler, []*kgo.Client) {
	clients := make([]*kgo.Client, numWriteWCs)
	admins := make([]*kadm.Client, numWriteWCs)
	var addr0 string
	for wc := 0; wc < numWriteWCs; wc++ {
		_, addr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitions, topic)
		if wc == 0 {
			addr0 = addr
		}
		clients[wc] = mustKafkaClient(t, addr)
		admins[wc] = kadm.NewClient(clients[wc])
	}

	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Address:      flagext.StringSliceCSV{addr0},
			Topic:        topic,
			FetchMaxWait: 10 * time.Millisecond,
		},
		Compartments: compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: numWriteWCs},
		},
		ConsumerGroup:       "test-builder",
		SchedulingInterval:  1000000 * time.Hour,
		JobSize:             time.Hour,
		MaxScanAge:          10 * time.Hour,
		MaxJobsPerPartition: 0,
	}
	sched, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	sched.adminClients = admins
	return sched, clients
}

// mustMultiWCSchedulerNoKafka builds a compartments-enabled scheduler with the
// right number of write WCs but no real Kafka clients, for tests that drive the
// partition state directly (e.g. through mock offset finders or observations).
func mustMultiWCSchedulerNoKafka(t *testing.T, topic string, numWriteWCs int) *BlockBuilderScheduler {
	cfg := Config{
		Kafka: ingest.KafkaConfig{Topic: topic},
		Compartments: compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: numWriteWCs},
		},
		ConsumerGroup:       "test-builder",
		JobSize:             time.Hour,
		MaxScanAge:          10 * time.Hour,
		MaxJobsPerPartition: 0,
	}
	sched, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	sched.adminClients = make([]*kadm.Client, numWriteWCs)
	return sched
}

// TestStartupInitCommitPerWC asserts that committed offsets are recovered from
// every write WC's own cluster and applied to that WC's offset tracker.
func TestStartupInitCommitPerWC(t *testing.T) {
	ctx := context.Background()
	const topic = "comp-0"
	sched, _ := mustMultiWCScheduler(t, 1, topic, 2)

	commit := func(wc int, at int64) {
		offs := kadm.Offsets{}
		offs.AddOffset(topic, 0, at, 0)
		require.NoError(t, sched.adminClients[wc].CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, offs))
	}
	commit(0, 100)
	commit(1, 250)

	for wc := range sched.adminClients {
		c, err := sched.fetchCommittedOffsets(ctx, wc)
		require.NoError(t, err)
		c.Each(func(o kadm.Offset) {
			sched.getPartitionState(o.Topic, o.Partition).initCommit(wc, o.At)
		})
	}

	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	require.Equal(t, int64(100), ps.committedOffset(0))
	require.Equal(t, int64(250), ps.committedOffset(1))
	require.Equal(t, int64(100), ps.plannedOffset(0), "planned initially tracks committed")
	require.Equal(t, int64(250), ps.plannedOffset(1))
}

// TestFetchCommittedOffsetsSkipsForeignTopics asserts that fetchCommittedOffsets
// ignores committed offsets for any topic other than this scheduler's read
// topic, so a consumer group shared on a cluster can't bleed another read
// compartment's offsets into this scheduler's (partition-keyed) state.
func TestFetchCommittedOffsetsSkipsForeignTopics(t *testing.T) {
	ctx := context.Background()
	const topic = "comp-0"
	sched, _ := mustMultiWCScheduler(t, 1, topic, 1)

	offs := kadm.Offsets{}
	offs.AddOffset(topic, 0, 100, 0)
	require.NoError(t, sched.adminClients[0].CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, offs))

	// The scheduler's own read topic is recovered.
	c, err := sched.fetchCommittedOffsets(ctx, 0)
	require.NoError(t, err)
	require.Len(t, c, 1)
	require.Equal(t, int64(100), c[topic][0].At)

	// The same committed offset is ignored once it's no longer this scheduler's
	// read topic, i.e. it belongs to another read compartment sharing the group.
	sched.cfg.Kafka.Topic = "comp-1"
	c, err = sched.fetchCommittedOffsets(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, c)
}

// TestStartupSeedsBundledBacklogJobs asserts that the startup backlog probe
// produces bundled cross-WC jobs whose per-WC ranges line up with each WC's
// probed offsets across job-size buckets.
func TestStartupSeedsBundledBacklogJobs(t *testing.T) {
	const topic = "comp-0"
	sched := mustMultiWCSchedulerNoKafka(t, topic, 2)
	sched.cfg.JobSize = time.Hour
	sched.cfg.MaxScanAge = 10 * time.Hour

	mkFinder := func(o0, o1, o2 int64, end int64) *mockOffsetFinder {
		return &mockOffsetFinder{
			offsets: []*offsetTime{
				{offset: o0, time: time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)},
				{offset: o1, time: time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)},
				{offset: o2, time: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)},
			},
			end: end,
		}
	}
	finder0 := mkFinder(100, 200, 300, 400)
	finder1 := mkFinder(1000, 1100, 1200, 1300)

	consumeOffs := []partitionOffsets{
		{topic: topic, partition: 0, clusterID: 0, start: 0, resume: 100, end: 400},
		{topic: topic, partition: 0, clusterID: 1, start: 0, resume: 1000, end: 1300},
	}

	endTime := time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC)
	sched.populateInitialJobs(context.Background(), groupByPartition(consumeOffs...), []offsetStore{finder0, finder1}, endTime)

	ps := sched.getPartitionState(topic, 0)
	// The bucket ending at each WC's high watermark (offsets 300->400 / 1200->1300)
	// has no boundary record, so - like the non-compartment path - it is left to
	// normal operation rather than seeded at startup. Only the two buckets below the
	// high watermark are seeded.
	require.Equal(t, 2, ps.pendingJobs.Len(), "buckets below the high watermark each yield one bundled job")

	first := ps.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
	require.Equal(t, topic, first.Topic)
	require.Equal(t, int32(0), first.Partition)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 100, EndOffset: 200},
		1: {StartOffset: 1000, EndOffset: 1100},
	}, first.Ranges())

	second := ps.pendingJobs.Front().Next().Value.(*schedulerpb.JobSpec)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 200, EndOffset: 300},
		1: {StartOffset: 1100, EndOffset: 1200},
	}, second.Ranges())
}

// TestStartupBundlesMisalignedWCs asserts that two write WCs whose records carry
// skewed timestamps — wc1's third batch lands one wall-clock bucket later than
// wc0's — produce correctly bundled per-bucket jobs for the buckets below their
// high watermark. The skewed final batch sits in the bucket ending at each WC's
// high watermark, which has no boundary record, so (like the non-compartment
// path) it is deferred to normal operation rather than seeded at startup.
func TestStartupBundlesMisalignedWCs(t *testing.T) {
	const topic = "comp-0"
	sched := mustMultiWCSchedulerNoKafka(t, topic, 2)
	sched.cfg.JobSize = time.Hour
	sched.cfg.MaxScanAge = 10 * time.Hour

	// Record times sit between the 15m probe grid points (jobSize/4), so the
	// strict-after lookup in mockOffsetFinder is unambiguous.
	finder0 := &mockOffsetFinder{
		offsets: []*offsetTime{
			{offset: 100, time: time.Date(2025, 3, 1, 9, 7, 0, 0, time.UTC)},
			{offset: 200, time: time.Date(2025, 3, 1, 10, 7, 0, 0, time.UTC)},
			{offset: 300, time: time.Date(2025, 3, 1, 11, 7, 0, 0, time.UTC)},
		},
		end: 300,
	}
	finder1 := &mockOffsetFinder{
		offsets: []*offsetTime{
			{offset: 1000, time: time.Date(2025, 3, 1, 9, 7, 0, 0, time.UTC)},
			{offset: 1100, time: time.Date(2025, 3, 1, 10, 7, 0, 0, time.UTC)},
			// wc1's third batch is one wall-clock bucket later than wc0's.
			{offset: 1200, time: time.Date(2025, 3, 1, 12, 7, 0, 0, time.UTC)},
		},
		end: 1200,
	}

	consumeOffs := []partitionOffsets{
		{topic: topic, partition: 0, clusterID: 0, start: 0, resume: 100, end: 300},
		{topic: topic, partition: 0, clusterID: 1, start: 0, resume: 1000, end: 1200},
	}

	endTime := time.Date(2025, 3, 1, 13, 0, 0, 0, time.UTC)
	sched.populateInitialJobs(context.Background(), groupByPartition(consumeOffs...), []offsetStore{finder0, finder1}, endTime)

	ps := sched.getPartitionState(topic, 0)
	// The complete bucket below the high watermark is seeded and bundles both WCs;
	// the skewed final batch (offsets 200->300 / 1100->1200, at each WC's high
	// watermark) is deferred to normal operation.
	require.Equal(t, 1, ps.pendingJobs.Len(), "only the complete sub-watermark bucket is seeded at startup")

	first := ps.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 100, EndOffset: 200},
		1: {StartOffset: 1000, EndOffset: 1100},
	}, first.Ranges())
}

// TestFinalizeObservationsMultiWC asserts that observation recovery imports the
// contiguous prefix of bundled jobs and stops at the first per-WC offset gap.
func TestFinalizeObservationsMultiWC(t *testing.T) {
	const topic = "comp-0"
	sched := mustMultiWCSchedulerNoKafka(t, topic, 2)
	sched.jobs = newJobQueue(time.Hour, noOpJobCreationPolicy[schedulerpb.JobSpec]{}, 2, sched.metrics, test.NewTestingLogger(t))

	spec := func(wc0s, wc0e, wc1s, wc1e int64) schedulerpb.JobSpec {
		return schedulerpb.JobSpec{Topic: topic, Partition: 0, OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: wc0s, EndOffset: wc0e},
			1: {StartOffset: wc1s, EndOffset: wc1e},
		}}
	}
	mkObs := func(id string, epoch int64, complete bool, s schedulerpb.JobSpec) {
		sched.observations[id] = &observation{key: jobKey{id: id, epoch: epoch}, spec: s, workerID: "w0", complete: complete}
	}

	// A and B are contiguous on both WCs. C has a gap on WC 1 (starts at 500 but
	// WC 1's planned offset is only 400), so it and anything after must be skipped.
	mkObs("A", 1, true, spec(0, 100, 0, 200))
	mkObs("B", 2, true, spec(100, 200, 200, 400))
	mkObs("C", 3, true, spec(200, 300, 500, 700))

	sched.finalizeObservations()

	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	require.Equal(t, int64(200), ps.plannedOffset(0), "WC 0 planned advances through A and B")
	require.Equal(t, int64(400), ps.plannedOffset(1), "WC 1 planned stops before the gap at C")
	require.Equal(t, int64(200), ps.committedOffset(0))
	require.Equal(t, int64(400), ps.committedOffset(1))
	require.NotContains(t, ps.plannedJobsMap, "C", "the gapped job must not be imported")
}

// wcRangeArg pairs a WC ID with its offset range for building test JobSpecs.
type wcRangeArg struct {
	wc  int32
	rng schedulerpb.OffsetRange
}

// wcRangesMap collects (WC ID, range) pairs into a JobSpec OffsetRanges map.
func wcRangesMap(ranges ...wcRangeArg) map[int32]schedulerpb.OffsetRange {
	m := make(map[int32]schedulerpb.OffsetRange, len(ranges))
	for _, r := range ranges {
		m[r.wc] = r.rng
	}
	return m
}

// TestFinalizeObservationsSparseWC covers sparse specs, where a job lists only the
// WCs that had new data. A sort keyed on a single representative range mis-orders
// these and falsely detects a gap; the per-WC topological ordering must import
// them in contiguous order regardless of insertion order.
func TestFinalizeObservationsSparseWC(t *testing.T) {
	const topic = "comp-0"
	sched := mustMultiWCSchedulerNoKafka(t, topic, 2)
	sched.jobs = newJobQueue(time.Hour, noOpJobCreationPolicy[schedulerpb.JobSpec]{}, 2, sched.metrics, test.NewTestingLogger(t))

	// Seed committed offsets so planned is non-empty: WC 0 resumes at 1000, WC 1 at 0.
	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	ps.initCommit(0, 1000)
	ps.initCommit(1, 0)

	r := func(wc int32, s, e int64) wcRangeArg {
		return wcRangeArg{wc: wc, rng: schedulerpb.OffsetRange{StartOffset: s, EndOffset: e}}
	}
	mkObs := func(id string, epoch int64, ranges ...wcRangeArg) {
		sched.observations[id] = &observation{
			key:      jobKey{id: id, epoch: epoch},
			spec:     schedulerpb.JobSpec{Topic: topic, Partition: 0, OffsetRanges: wcRangesMap(ranges...)},
			workerID: "w0",
			complete: true,
		}
	}

	// J1 spans both WCs; its WC 0 start (1000) is far above J2's WC 1 start (50), so
	// ordering on a single range would place J2 before J1 and flag a false gap on
	// WC 1. J2 continues only WC 1 and must be imported after J1.
	mkObs("J1", 1, r(0, 1000, 1100), r(1, 0, 50))
	mkObs("J2", 2, r(1, 50, 120))

	sched.finalizeObservations()

	require.Equal(t, int64(1100), ps.plannedOffset(0), "WC 0 advances through J1")
	require.Equal(t, int64(120), ps.plannedOffset(1), "WC 1 advances through J1 then J2")
	require.Equal(t, int64(1100), ps.committedOffset(0))
	require.Equal(t, int64(120), ps.committedOffset(1))
}

// TestFinalizeObservationsOffsetCycle checks that an impossible cyclic ordering
// (WC 0 orders J1 before J2 while WC 1 orders J2 before J1) is detected and the
// whole partition's recovery is skipped rather than imported in a wrong order.
func TestFinalizeObservationsOffsetCycle(t *testing.T) {
	const topic = "comp-0"
	sched := mustMultiWCSchedulerNoKafka(t, topic, 2)
	sched.jobs = newJobQueue(time.Hour, noOpJobCreationPolicy[schedulerpb.JobSpec]{}, 2, sched.metrics, test.NewTestingLogger(t))

	r := func(wc int32, s, e int64) wcRangeArg {
		return wcRangeArg{wc: wc, rng: schedulerpb.OffsetRange{StartOffset: s, EndOffset: e}}
	}
	mkObs := func(id string, epoch int64, ranges ...wcRangeArg) {
		sched.observations[id] = &observation{
			key:      jobKey{id: id, epoch: epoch},
			spec:     schedulerpb.JobSpec{Topic: topic, Partition: 0, OffsetRanges: wcRangesMap(ranges...)},
			workerID: "w0",
			complete: true,
		}
	}

	mkObs("J1", 1, r(0, 0, 10), r(1, 20, 30))
	mkObs("J2", 2, r(0, 20, 30), r(1, 0, 10))

	sched.finalizeObservations()

	ps := sched.getPartitionState(sched.cfg.Kafka.Topic, 0)
	require.True(t, ps.plannedEmpty(0), "no job imported for WC 0")
	require.True(t, ps.plannedEmpty(1), "no job imported for WC 1")
	require.Empty(t, ps.plannedJobsMap)
}

func TestOrderObservationsForImport(t *testing.T) {
	rng := func(wc int32, start, end int64) wcRangeArg {
		return wcRangeArg{wc: wc, rng: schedulerpb.OffsetRange{StartOffset: start, EndOffset: end}}
	}
	obs := func(id string, ranges ...wcRangeArg) *observation {
		return &observation{key: jobKey{id: id}, spec: schedulerpb.JobSpec{Topic: "t", Partition: 0, OffsetRanges: wcRangesMap(ranges...)}}
	}
	ids := func(observations []*observation) []string {
		out := make([]string, len(observations))
		for i, o := range observations {
			out[i] = o.key.id
		}
		return out
	}

	t.Run("single WC sorts by start offset", func(t *testing.T) {
		got, err := orderObservationsForImport([]*observation{
			obs("c", rng(0, 20, 30)),
			obs("a", rng(0, 0, 10)),
			obs("b", rng(0, 10, 20)),
		}, 1)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, ids(got))
	})

	t.Run("multi-WC sparse, predecessor before successor", func(t *testing.T) {
		// J2 only continues WC 1; J1 spans both with a high WC 0 start, so a single
		// representative offset would mis-order them. J1 must come first.
		got, err := orderObservationsForImport([]*observation{
			obs("J2", rng(1, 50, 120)),
			obs("J1", rng(0, 1000, 1100), rng(1, 0, 50)),
		}, 2)
		require.NoError(t, err)
		require.Equal(t, []string{"J1", "J2"}, ids(got))
	})

	t.Run("disjoint WC sets all emitted", func(t *testing.T) {
		got, err := orderObservationsForImport([]*observation{
			obs("b", rng(1, 0, 5)),
			obs("a", rng(0, 0, 10)),
		}, 2)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"a", "b"}, ids(got))
	})

	t.Run("offset cycle returns error", func(t *testing.T) {
		// WC 0 orders J1 before J2; WC 1 orders J2 before J1.
		_, err := orderObservationsForImport([]*observation{
			obs("J1", rng(0, 0, 10), rng(1, 20, 30)),
			obs("J2", rng(0, 20, 30), rng(1, 0, 10)),
		}, 2)
		require.ErrorIs(t, err, errObservationOffsetCycle)
	})
}

func TestInitialOffsetProbing(t *testing.T) {
	ctx := context.Background()

	// probeInitialOffsets walks the shared grid (stepping back by jobSize/4 from
	// endTime) over every WC, emitting an observation whenever a WC's end offset steps
	// down, keyed by the earliest boundary-record time among the changed WCs. The
	// high-watermark bucket has no boundary record, so it's deferred. A WC with no
	// observation is seeded at its end offset via a synthetic observation at endTime.
	// Marker times sit between the 15m grid points so the strict-after lookup is
	// unambiguous.
	at := func(h, m int) time.Time { return time.Date(2025, 3, 1, h, m, 0, 0, time.UTC) }

	type wcData struct {
		markers []*offsetTime
		resume  int64
		end     int64
	}

	tests := map[string]struct {
		wcs         []wcData
		jobSize     time.Duration
		endTime     time.Time
		minScanTime time.Time
		expected    []probeObservations
	}{
		"single WC, complete buckets below the high watermark": {
			wcs: []wcData{{
				markers: []*offsetTime{{offset: 100, time: at(10, 7)}, {offset: 200, time: at(11, 7)}, {offset: 300, time: at(12, 7)}},
				resume:  100,
				end:     300,
			}},
			jobSize:     time.Hour,
			endTime:     at(13, 0),
			minScanTime: at(0, 0),
			expected: []probeObservations{
				{time: at(10, 7), offsets: map[int]int64{0: 100}},
				{time: at(11, 7), offsets: map[int]int64{0: 200}},
			},
		},
		"two aligned WCs bundle per bucket": {
			wcs: []wcData{
				{markers: []*offsetTime{{offset: 100, time: at(10, 7)}, {offset: 200, time: at(11, 7)}, {offset: 300, time: at(12, 7)}}, resume: 100, end: 300},
				{markers: []*offsetTime{{offset: 1000, time: at(10, 7)}, {offset: 1100, time: at(11, 7)}, {offset: 1200, time: at(12, 7)}}, resume: 1000, end: 1200},
			},
			jobSize:     time.Hour,
			endTime:     at(13, 0),
			minScanTime: at(0, 0),
			expected: []probeObservations{
				{time: at(10, 7), offsets: map[int]int64{0: 100, 1: 1000}},
				{time: at(11, 7), offsets: map[int]int64{0: 200, 1: 1100}},
			},
		},
		"caught-up WC seeds at its end offset via a synthetic endTime observation": {
			wcs:         []wcData{{markers: nil, resume: 2000, end: 2000}},
			jobSize:     time.Hour,
			endTime:     at(13, 0),
			minScanTime: at(0, 0),
			expected: []probeObservations{
				{time: at(13, 0), offsets: map[int]int64{0: 2000}},
			},
		},
		"caught-up WC alongside a backlog WC seeds via a synthetic endTime observation": {
			wcs: []wcData{
				{markers: []*offsetTime{{offset: 100, time: at(10, 7)}, {offset: 200, time: at(11, 7)}, {offset: 300, time: at(12, 7)}}, resume: 100, end: 300},
				{markers: nil, resume: 2000, end: 2000},
			},
			jobSize:     time.Hour,
			endTime:     at(13, 0),
			minScanTime: at(0, 0),
			expected: []probeObservations{
				{time: at(10, 7), offsets: map[int]int64{0: 100}},
				{time: at(11, 7), offsets: map[int]int64{0: 200}},
				{time: at(13, 0), offsets: map[int]int64{1: 2000}},
			},
		},
		"limited scan age starts above resume": {
			// minScanTime stops the walk before it reaches resume, so the prefix down
			// to resume (offset 100) is skipped: the earliest observed offset is 200.
			wcs: []wcData{{
				markers: []*offsetTime{{offset: 100, time: at(10, 7)}, {offset: 200, time: at(11, 7)}, {offset: 300, time: at(12, 7)}},
				resume:  100,
				end:     300,
			}},
			jobSize:     time.Hour,
			endTime:     at(13, 0),
			minScanTime: at(10, 30),
			expected: []probeObservations{
				{time: at(11, 7), offsets: map[int]int64{0: 200}},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			offs := make([]partitionOffsets, len(tt.wcs))
			stores := make([]offsetStore, len(tt.wcs))
			for wc, d := range tt.wcs {
				offs[wc] = partitionOffsets{topic: "topic", partition: 0, clusterID: wc, resume: d.resume, end: d.end}
				stores[wc] = &mockOffsetFinder{offsets: d.markers, end: d.end}
			}
			got, err := probeInitialOffsets(ctx, offs, stores, "topic", 0, tt.endTime, tt.jobSize, tt.minScanTime, test.NewTestingLogger(t))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
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
		// Like ListOffsetsAfterMilli, when there is no record after t we return the
		// end offset paired with a non-record timestamp. The real client reports
		// time.UnixMilli(-1) here (it falls back to listing the end offset, whose
		// timestamp is -1), so mirror that rather than a zero time.
		return o.end, time.UnixMilli(-1), nil
	}
	return off, mint, nil
}

var _ offsetStore = (*mockOffsetFinder)(nil)

func TestLimitNPolicy(t *testing.T) {
	existing := func(specs ...*schedulerpb.JobSpec) iter.Seq[*schedulerpb.JobSpec] {
		return slices.Values(specs)
	}

	allow1 := limitPerPartitionJobCreationPolicy{partitionLimit: 1}

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job1"},
		ptrSpec("topic", 0, 0, 0), existing()))

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job4"},
		ptrSpec("topic", 0, 0, 0),
		existing(ptrSpec("topic", 1, 0, 0))))

	require.Error(t, allow1.canCreateJob(jobKey{id: "job5"},
		ptrSpec("topic", 1, 0, 0),
		existing(ptrSpec("topic", 1, 0, 0))))

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job5"},
		ptrSpec("topic", 1, 0, 0),
		existing(
			ptrSpec("topic", 2, 0, 0),
			ptrSpec("topic", 3, 0, 0),
			ptrSpec("topic", 3, 0, 0),
		)))

	allow2 := limitPerPartitionJobCreationPolicy{partitionLimit: 2}
	require.NoError(t, allow2.canCreateJob(jobKey{id: "job6"},
		ptrSpec("topic", 1, 0, 0),
		existing(
			ptrSpec("topic", 2, 0, 0),
			ptrSpec("topic", 3, 0, 0),
		)))
	require.NoError(t, allow2.canCreateJob(jobKey{id: "job6"},
		ptrSpec("topic", 1, 0, 0),
		existing(
			ptrSpec("topic", 1, 0, 0),
			ptrSpec("topic", 2, 0, 0),
		)))
	require.Error(t, allow2.canCreateJob(jobKey{id: "job6"},
		ptrSpec("topic", 1, 0, 0),
		existing(
			ptrSpec("topic", 1, 0, 0),
			ptrSpec("topic", 1, 0, 0),
		)))
	require.Error(t, allow2.canCreateJob(jobKey{id: "job6"},
		ptrSpec("topic", 1, 0, 0),
		existing(
			ptrSpec("topic", 1, 0, 0),
			ptrSpec("topic", 1, 0, 0),
			ptrSpec("topic", 1, 0, 0),
		)))
}

func TestLimitNPolicyShortCircuits(t *testing.T) {
	// Verify that the policy stops iterating as soon as it has counted enough
	// matches to reject: with partitionLimit=2 we are about to add one, so the
	// second matching existing job is what trips rejection. The iterator must
	// not be advanced beyond that point.
	policy := limitPerPartitionJobCreationPolicy{partitionLimit: 2}
	spec := ptrSpec("topic", 0, 0, 0)

	visited := 0
	existing := func(yield func(*schedulerpb.JobSpec) bool) {
		matching := []*schedulerpb.JobSpec{
			ptrSpec("topic", 0, 0, 0),
			ptrSpec("topic", 0, 0, 0),
		}
		for _, s := range matching {
			visited++
			if !yield(s) {
				return
			}
		}
		for i := 0; i < 1000; i++ {
			visited++
			if !yield(ptrSpec("topic", 99, 0, 0)) {
				return
			}
		}
	}

	require.Error(t, policy.canCreateJob(jobKey{id: "job1"}, spec, existing))
	require.Equal(t, 2, visited, "policy should stop iterating once it has decided to reject")
}

func TestBlockBuilderScheduler_EnqueuePendingJobs(t *testing.T) {
	// Test that job detection and enqueueing work as expected w/r/t the
	// job creation policy.

	sched, _ := mustScheduler(t, 4)
	sched.cfg.MaxJobsPerPartition = 1
	sched.completeObservationMode(context.Background())

	part := int32(1)
	pt := sched.getPartitionState("ingest", part)

	pt.addPendingJob(ptrSpec("ingest", part, 10, 20))
	pt.addPendingJob(ptrSpec("ingest", part, 20, 30))
	pt.addPendingJob(ptrSpec("ingest", part, 30, 40))

	assert.Equal(t, 3, pt.pendingJobs.Len())

	sched.enqueuePendingJobs()
	assert.Equal(t, 2, pt.pendingJobs.Len())
	sched.enqueuePendingJobs()
	assert.Equal(t, 2, pt.pendingJobs.Len())

	j, spec, err := sched.jobs.assign("worker1")

	assert.NoError(t, err)
	assert.Equal(t, "ingest/1/10", j.id)
	assert.Equal(t, schedulerpb.NewNonCompartmentJobSpec("ingest", part, 10, 20), spec)

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

	pt.addPendingJob(ptrSpec("ingest", part, 10, 20))
	pt.addPendingJob(ptrSpec("ingest", part, 20, 30))
	pt.addPendingJob(ptrSpec("ingest", part, 30, 40))

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
	pt.initCommit(0, 20)
	pt.addPendingJob(ptrSpec("ingest", part, 10, 20))

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
	pt.addPendingJob(ptrSpec("ingest", part, 10, 30))

	// But the job we imported from the existing workers now being completed may be (10, 20):
	pt.initCommit(0, 20)

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
	pt.addPendingJob(ptrSpec("ingest", part, 0, 30))
	pt.addPendingJob(ptrSpec("ingest", part, 30, 40))
	pt.addPendingJob(ptrSpec("ingest", part, 40, 50))

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	assert.True(t, pt.plannedEmpty(0))
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.plannedOffset(0))

	requireGaps(t, reg, 0, 0)

	// this one introduces a gap:
	pt.addPendingJob(ptrSpec("ingest", part, 60, 70))

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.plannedOffset(0))
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(70), pt.plannedOffset(0))

	requireGaps(t, reg, 1, 0)

	// the gap may not be the first job:
	pt.addPendingJob(ptrSpec("ingest", part, 70, 80))
	// (gap)
	pt.addPendingJob(ptrSpec("ingest", part, 100, 110))
	pt.addPendingJob(ptrSpec("ingest", part, 110, 120))

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count())
	assert.Equal(t, int64(70), pt.plannedOffset(0))
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 7, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(120), pt.plannedOffset(0))

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

		if spec.Ranges()[0].StartOffset != expectedStart {
			commitGaps++
		}

		expectedStart = spec.Ranges()[0].EndOffset
		requireGaps(t, reg, 2, commitGaps, "expected %d commit gaps at job %d", commitGaps, j)
	}
}

func TestBlockBuilderScheduler_NoCommit_NoGap(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	reg := sched.register.(*prometheus.Registry)

	const part int32 = 1
	requireGaps(t, reg, 0, 0)

	pp := sched.getPartitionState("ingest", part)
	require.True(t, pp.plannedEmpty(0))
	require.True(t, pp.committedEmpty(0))

	k := jobKey{"myjob5", 5}
	spec := schedulerpb.NewNonCompartmentJobSpec("ingest", part, 10, 20)

	pp.offsets[0].planned.advance(k.id, spec.Ranges()[0])
	requireGaps(t, reg, 0, 0, "advancing an empty planned offset should not register a gap")

	pp.offsets[0].committed.advance(k.id, spec.Ranges()[0])
	requireGaps(t, reg, 0, 0, "advancing an empty committed offset should not register a gap")

	// Now create a gap:
	k2 := jobKey{"myjob7", 23}
	spec2 := schedulerpb.NewNonCompartmentJobSpec("ingest", part, 40, 50)

	pp.offsets[0].planned.advance(k2.id, spec2.Ranges()[0])
	requireGaps(t, reg, 1, 0, "a gap after a non-empty planned offset should register a gap")

	pp.offsets[0].committed.advance(k2.id, spec2.Ranges()[0])
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
		name               string
		initialStart       int64
		initialResume      int64
		initialEnd         int64
		initialTime        time.Time
		offsets            []*offsetTime
		futureObservations []endOffsetObservation
		expectedFinalEnd   int64
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sched, _ := mustScheduler(t, 1)
			sched.cfg.MaxScanAge = 1 * time.Hour
			sched.cfg.JobSize = 1 * time.Hour

			// Create a mock offset finder that returns the initial end offset
			finder := &mockOffsetFinder{
				offsets: tt.offsets,
				end:     tt.initialEnd,
			}

			consumeOffs := []partitionOffsets{
				{
					topic:     "topic",
					partition: 0,
					start:     tt.initialStart,
					resume:    tt.initialResume,
					end:       tt.initialEnd,
				},
			}

			// Call populateInitialJobs to set up initial state
			sched.populateInitialJobs(context.Background(), groupByPartition(consumeOffs...), []offsetStore{finder}, tt.initialTime)

			collectedJobs := []*schedulerpb.JobSpec{}

			// Apply future end offset observations and collect jobs returned from updateTime
			ps := sched.getPartitionState("topic", 0)

			for _, obs := range tt.futureObservations {
				ps.updateEndOffset(0, obs.offset)
				job, err := ps.updateTime(obs.timestamp, sched.cfg.JobSize)
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

			// Verify first job starts at or after resume offset
			require.Equal(t, tt.initialResume, collectedJobs[0].Ranges()[0].StartOffset,
				"first job should start at resume offset")

			// Verify last job ends at expected final end
			require.Equal(t, tt.expectedFinalEnd, collectedJobs[len(collectedJobs)-1].Ranges()[0].EndOffset,
				"last job should end at expected final end offset")

			// Verify no gaps and no overlaps between consecutive jobs
			for i := 0; i < len(collectedJobs)-1; i++ {
				current := collectedJobs[i]
				next := collectedJobs[i+1]

				require.Equal(t, current.Ranges()[0].EndOffset, next.Ranges()[0].StartOffset,
					"jobs should have no gaps: job %d ends at %d but next job starts at %d",
					i, current.Ranges()[0].EndOffset, next.Ranges()[0].StartOffset)
			}
		})
	}
}
