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

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		offs, err := fetchCommittedOffsets(ctx, sched.adminClients[0], sched.cfg.ConsumerGroup, sched.cfg.Kafka.Topic)
		require.NoError(t, err)
		o, ok := offs.Lookup(spec.Topic, spec.Partition)
		require.True(t, ok)
		require.Equal(t, spec.EndOffset, o.At)
	})
}

// TestService_MultiCluster runs the scheduler through its Service interface with compartments
// enabled, so the service builds one Kafka client per write compartment from the templated
// address.
func TestService_MultiCluster(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sched, clients := mustMultiClusterScheduler(t, 4, 3)

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

		produce := func(cli *kgo.Client, partition, n int32) {
			produceResult := cli.ProduceSync(ctx, &kgo.Record{
				Timestamp: time.Now(),
				Value:     fmt.Appendf(nil, "value-%d-%d", partition, n),
				Topic:     "ingest",
				Partition: partition,
			})
			require.NoError(t, produceResult.FirstErr())
		}

		// counts[c][p] is the number of records produced to partition p on cluster c;
		// partition 0 gets no data and must end up with no commits.
		counts := [3][4]int{
			{0, 10, 20, 30},
			{0, 40, 50, 60},
			{0, 70, 80, 90},
		}
		maxCount := 0
		for _, byPartition := range counts {
			maxCount = max(maxCount, slices.Max(byPartition[:]))
		}
		for n := range maxCount {
			<-scheduleUpdated

			for c, byPartition := range counts {
				for p, count := range byPartition {
					if n < count {
						produce(clients[c], int32(p), int32(n))
					}
				}
			}
		}

		// Complete jobs as they're created until every cluster's committed offsets reach that
		// cluster's produced totals. Cross-wired clusters would commit another cluster's counts.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			if key, spec, err := sched.assignJob("w0"); err == nil {
				assert.NoError(c, sched.updateJob(key, "w0", true, spec))
			}
			assert.NoError(c, sched.flushOffsetsToKafka(ctx))

			for clusterID, admin := range sched.adminClients {
				offs, err := fetchCommittedOffsets(ctx, admin, sched.cfg.ConsumerGroup, sched.cfg.Kafka.Topic)
				if !assert.NoError(c, err) {
					return
				}
				for p, count := range counts[clusterID] {
					o, ok := offs.Lookup("ingest", int32(p))
					if count == 0 {
						assert.False(c, ok, "cluster %d should have no commit for empty partition %d", clusterID, p)
						continue
					}
					if !assert.True(c, ok, "cluster %d should have a commit for partition %d", clusterID, p) {
						return
					}
					assert.Equal(c, int64(count), o.At, "cluster %d should commit its own totals for partition %d", clusterID, p)
				}
			}
		}, 30*time.Second, 10*time.Millisecond)
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

// TestStartup_MultiCluster verifies the observation-mode recovery flow with compartments
// enabled: jobs reported by workers carry per-cluster offset ranges (spanning all clusters or a
// subset), are reconstructed when observation mode completes, and completing them advances each
// cluster's committed offset independently.
func TestStartup_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	// (a new scheduler starts in observation mode.)

	// Committed offsets are asymmetric across clusters, and partitions 65 and 66 are committed
	// on a subset of clusters only.
	initCommits(sched, "ingest", 64, map[int]int64{0: 1000, 1: 5000, 2: 30})
	initCommits(sched, "ingest", 65, map[int]int64{0: 256, 2: 88})
	initCommits(sched, "ingest", 66, map[int]int64{1: 57})

	{
		_, _, err := sched.assignJob("w0")
		require.ErrorContains(t, err, "observation period not complete")
	}

	// Some jobs that ostensibly exist, but scheduler doesn't know about. Each job's ranges
	// resume from its clusters' committed offsets.
	j1 := observedJob(10, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 64,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 1000, EndOffset: 1100},
			1: {StartOffset: 5000, EndOffset: 5200},
			2: {StartOffset: 30, EndOffset: 90},
		},
	})
	j2 := observedJob(11, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 65,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 256, EndOffset: 300},
			2: {StartOffset: 88, EndOffset: 100},
		},
	})
	j3 := observedJob(12, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 66,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			1: {StartOffset: 57, EndOffset: 100},
		},
	})

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

	// And eventually they'll all complete, advancing each cluster's committed offset to that
	// cluster's range end, and leaving clusters outside a job's ranges untouched.
	require.NoError(t, sched.updateJob(j1.key, "w0", true, j1.spec))
	require.NoError(t, sched.updateJob(j2.key, "w0", true, j2.spec))
	require.NoError(t, sched.updateJob(j3.key, "w0", true, j3.spec))

	requireOffsets(t, sched, "ingest", 64, map[int]int64{0: 1100, 1: 5200, 2: 90})
	requireOffsets(t, sched, "ingest", 65, map[int]int64{0: 300, 2: 100})
	requireOffsets(t, sched, "ingest", 66, map[int]int64{1: 100})

	{
		_, _, err := sched.assignJob("w0")
		require.ErrorIs(t, err, errNoJobAvailable)
	}

	requireGaps(t, sched.register.(*prometheus.Registry), 0, 0)
}

// TestStartup_MultiCluster_GapOnOneCluster documents recovery truncation: an observed job whose
// ranges leave a gap on any single cluster stops the import there, and later observed jobs are
// skipped even if they are themselves contiguous. The rest of the partition's progress falls
// back to committed offsets.
func TestStartup_MultiCluster_GapOnOneCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)

	jobA := observedJob(10, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 64,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 100, EndOffset: 200},
		},
	})
	// jobB's cluster 0 range leaves a gap (200 -> 250); its cluster 1 range is contiguous.
	jobB := observedJob(11, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 64,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 250, EndOffset: 300},
			1: {StartOffset: 200, EndOffset: 300},
		},
	})
	// jobC continues jobB contiguously on both clusters.
	jobC := observedJob(12, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 64,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 300, EndOffset: 400},
			1: {StartOffset: 300, EndOffset: 400},
		},
	})

	require.NoError(t, sched.updateJob(jobA.key, "w0", false, jobA.spec))
	require.NoError(t, sched.updateJob(jobB.key, "w1", false, jobB.spec))
	require.NoError(t, sched.updateJob(jobC.key, "w2", false, jobC.spec))

	sched.completeObservationMode(context.Background())

	_, ok := sched.jobs.jobs[jobA.key.id]
	require.True(t, ok, "jobA should be imported")
	_, ok = sched.jobs.jobs[jobB.key.id]
	require.False(t, ok, "jobB should be skipped due to its gap on cluster 0")
	_, ok = sched.jobs.jobs[jobC.key.id]
	require.False(t, ok, "jobC should be skipped because it's after the gap, even though it's contiguous with jobB")

	ps := sched.getPartitionState("ingest", 64)
	require.Equal(t, int64(200), ps.plannedOffset(0))
	require.Equal(t, int64(200), ps.plannedOffset(1))
	require.True(t, ps.plannedEmpty(2), "cluster 2 was in no job's ranges")
}

// TestStartup_MultiCluster_PartiallyFlushedCommit documents recovery after a crash that
// persisted a completed job's commit on a subset of clusters
func TestStartup_MultiCluster_PartiallyFlushedCommit(t *testing.T) {
	clusters := createTestClusters(t, 3, 3)
	ctx := t.Context()

	// The pre-crash scheduler starts with every cluster's commit at jobJ's start offsets, all
	// persisted to Kafka.
	schedA, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	initCommits(schedA, "ingest", 0, map[int]int64{0: 100, 1: 100, 2: 50})
	schedA.completeObservationMode(ctx)
	require.NoError(t, schedA.flushOffsetsToKafka(ctx))

	// A worker runs jobJ to completion, advancing every cluster's in-memory commit to its
	// range end.
	jobJ := schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 100, EndOffset: 200},
			2: {StartOffset: 50, EndOffset: 150},
		},
	}
	addPendingJob(schedA, jobJ)
	schedA.enqueuePendingJobs()
	keyJ, specJ, err := schedA.assignJob("w0")
	require.NoError(t, err)
	require.Equal(t, jobJ, specJ)
	require.NoError(t, schedA.updateJob(keyJ, "w0", true, specJ))

	// The flush persists the completed commits on clusters 0 and 2 but fails on cluster 1,
	// whose Kafka commit stays at jobJ's start.
	failOffsetCommits(clusters[1].cluster)
	err = schedA.flushOffsetsToKafka(ctx)
	require.ErrorContains(t, err, "write compartment 1")
	requireCommittedInKafka(ctx, t, schedA, 0, map[int32]int64{0: 200})
	requireCommittedInKafka(ctx, t, schedA, 1, map[int32]int64{0: 100})
	requireCommittedInKafka(ctx, t, schedA, 2, map[int32]int64{0: 150})

	// The scheduler "crashes": a fresh one starts against the same clusters and loads the
	// partially flushed commits.
	schedB, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	require.NoError(t, schedB.loadInitialCommittedOffsets(ctx))
	requireOffsets(t, schedB, "ingest", 0, map[int]int64{0: 200, 1: 100, 2: 150})

	// The worker re-reports the completed jobJ during observation, along with an in-progress
	// jobK continuing jobJ contiguously on every cluster.
	require.NoError(t, schedB.updateJob(keyJ, "w0", true, specJ))
	jobK := observedJob(keyJ.epoch+1, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 200, EndOffset: 300},
			1: {StartOffset: 200, EndOffset: 300},
			2: {StartOffset: 150, EndOffset: 250},
		},
	})
	require.NoError(t, schedB.updateJob(jobK.key, "w1", false, jobK.spec))

	schedB.completeObservationMode(ctx)

	_, ok := schedB.jobs.jobs[keyJ.id]
	require.False(t, ok, "jobJ should not be imported: it's behind the flushed clusters' commits")
	_, ok = schedB.jobs.jobs[jobK.key.id]
	require.False(t, ok, "jobK should be skipped because recovery truncated at the partially flushed jobJ")

	pB := schedB.getPartitionState("ingest", 0)
	require.Equal(t, int64(200), pB.plannedOffset(0))
	require.Equal(t, int64(100), pB.plannedOffset(1), "the unflushed cluster should resume from its stale commit, re-covering jobJ's range")
	require.Equal(t, int64(150), pB.plannedOffset(2))
	requireOffsets(t, schedB, "ingest", 0, map[int]int64{0: 200, 1: 100, 2: 150})
}

// TestCompleteObservationMode_ResumesFromImportedPlan verifies that startup cuts pending jobs
// from the planned frontier established by observation import: ranges recovered from workers are
// not re-cut, only the data beyond them is.
func TestCompleteObservationMode_ResumesFromImportedPlan(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	sched, clients := mustMultiClusterScheduler(t, 3, 3)
	sched.cfg.MaxJobsPerPartition = 0
	sched.cfg.JobSize = time.Hour
	sched.cfg.MaxScanAge = 24 * time.Hour
	sched.cfg.LookbackOnNoCommit = 24 * time.Hour

	recordTime := time.Now().Add(-2 * time.Hour)
	produce := func(cli *kgo.Client, n int) {
		for i := range n {
			produceResult := cli.ProduceSync(ctx, &kgo.Record{
				Timestamp: recordTime,
				Value:     fmt.Appendf(nil, "value-%d", i),
				Topic:     "ingest",
				Partition: 0,
			})
			require.NoError(t, produceResult.FirstErr())
		}
	}
	produce(clients[0], 10)
	produce(clients[1], 8)
	produce(clients[2], 6)

	// A worker reports an in-progress job covering all of cluster 0's records and part of
	// cluster 1's; cluster 2 is not part of the job.
	observed := observedJob(10, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 10},
			1: {StartOffset: 0, EndOffset: 5},
		},
	})
	require.NoError(t, sched.updateJob(observed.key, "w0", false, observed.spec))

	sched.completeObservationMode(ctx)
	sched.enqueuePendingJobs()

	_, ok := sched.jobs.jobs[observed.key.id]
	require.True(t, ok, "the observed job should be imported")

	newJob, ok := sched.jobs.jobs["ingest/0/1:5-2:0"]
	require.True(t, ok, "the data beyond the imported plan should be planned: cluster 1 past the observed job, cluster 2 in full")
	require.Equal(t, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			1: {StartOffset: 5, EndOffset: 8},
			2: {StartOffset: 0, EndOffset: 6},
		},
	}, newJob.spec, "the new job should cover only the unplanned data; cluster 0 is fully in the observed job")
	require.Equal(t, 2, sched.jobs.count())

	ps := sched.getPartitionState("ingest", 0)
	require.Equal(t, int64(10), ps.plannedOffset(0), "cluster 0's frontier comes entirely from the imported observed job")
	require.Equal(t, int64(8), ps.plannedOffset(1))
	require.Equal(t, int64(6), ps.plannedOffset(2))
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

// TestAssignJobSkipsObsoleteOffsets_MultiCluster verifies that a job whose ranges are behind the
// committed offset on every cluster (beyondAll) is skipped as obsolete, while a job behind on only
// a subset of clusters (beyondSome) is a fatal invariant break: it should not happen for a queued
// job, so assignJob panics rather than risk dropping the lagging clusters' ranges.
func TestAssignJobSkipsObsoleteOffsets_MultiCluster(t *testing.T) {
	t.Run("fully consumed job is skipped", func(t *testing.T) {
		sched, _ := mustMultiClusterScheduler(t, 3, 3)
		sched.cfg.MaxJobsPerPartition = 0
		sched.completeObservationMode(context.Background())

		// Every cluster's committed offset reaches its range end: fully consumed.
		s1 := schedulerpb.JobSpec{
			Topic:     "ingest",
			Partition: 1,
			OffsetRanges: map[int32]schedulerpb.OffsetRange{
				0: {StartOffset: 100, EndOffset: 200},
				1: {StartOffset: 300, EndOffset: 400},
				2: {StartOffset: 500, EndOffset: 600},
			},
		}
		require.NoError(t, sched.jobs.add(jobIDForSpec(true, &s1), s1))
		initCommits(sched, "ingest", 1, map[int]int64{0: 200, 1: 400, 2: 600})

		require.Empty(t, assignAllJobs(t, sched, "w0"), "fully consumed job should be skipped")
	})

	t.Run("partially consumed job panics", func(t *testing.T) {
		sched, _ := mustMultiClusterScheduler(t, 3, 3)
		sched.cfg.MaxJobsPerPartition = 0
		sched.completeObservationMode(context.Background())

		// Consumed on cluster 0 only; clusters 1 and 2 still need it.
		s2 := schedulerpb.JobSpec{
			Topic:     "ingest",
			Partition: 2,
			OffsetRanges: map[int32]schedulerpb.OffsetRange{
				0: {StartOffset: 100, EndOffset: 200},
				1: {StartOffset: 300, EndOffset: 400},
				2: {StartOffset: 500, EndOffset: 600},
			},
		}
		require.NoError(t, sched.jobs.add(jobIDForSpec(true, &s2), s2))
		initCommits(sched, "ingest", 2, map[int]int64{0: 200, 1: 300, 2: 500})

		require.Panics(t, func() { _, _, _ = sched.assignJob("w0") })
	})
}

// TestUpdateJobPanicsOnPartiallyConsumed_MultiCluster verifies that an in-progress job update whose
// ranges are behind the committed offset on only a subset of clusters (beyondSome) panics, matching
// assignJob and enqueuePendingJobs, rather than renewing a lease on a torn job.
func TestUpdateJobPanicsOnPartiallyConsumed_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	sched.completeObservationMode(context.Background())

	// Consumed on cluster 0 only; clusters 1 and 2 still need it.
	spec := schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 2,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 300, EndOffset: 400},
			2: {StartOffset: 500, EndOffset: 600},
		},
	}
	initCommits(sched, "ingest", 2, map[int]int64{0: 200, 1: 300, 2: 500})

	key := jobKey{id: jobIDForSpec(true, &spec), epoch: 0}
	require.Panics(t, func() { _ = sched.updateJob(key, "w0", false, spec) })
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
		requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 5000}, "ingest/1 is in progress, so we should not move the offset")
		requireOffsets(t, sched, "ingest", 2, map[int]int64{0: 1400}, "ingest/2 job was complete up to 1400, so it should move the offset forward")
		requireOffsets(t, sched, "ingest", 3, map[int]int64{0: 974}, "ingest/3 should be unchanged - no updates")
		requireOffsets(t, sched, "ingest", 4, map[int]int64{0: 900}, "ingest/4 should be moved forward to account for the completed jobs")
		requireOffsets(t, sched, "ingest", 5, map[int]int64{0: 12000}, "ingest/5 has nothing new completed")
		requireOffsets(t, sched, "ingest", 6, map[int]int64{0: 600}, "ingest/6 allowed to move the commit")
		requireOffsets(t, sched, "ingest", 7, map[int]int64{}, "ingest/7 has an in-progress job, but had no commit at startup")
		requireOffsets(t, sched, "ingest", 8, map[int]int64{0: 1300}, "ingest/8 should be committed only until the gap")
		requireOffsets(t, sched, "ingest", 9, map[int]int64{0: 1300}, "ingest/9 should be committed only until the gap")
	}

	sendUpdates()
	sched.completeObservationMode(context.Background())

	verifyCommits()

	// Make sure the resumption offsets account for the gaps.
	offs, err := sched.initSingleClusterConsumptionOffsets(context.Background(), "ingest", time.Now(), 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []partitionOffsets{
		{partition: 0, resume: 0},
		{partition: 1, resume: 6000},
		{partition: 2, resume: 1600},
		{partition: 3, resume: 974},
		{partition: 4, resume: 900},
		{partition: 5, resume: 13000},
		{partition: 6, resume: 600},
		{partition: 7, resume: 93874},
		{partition: 8, resume: 1300},
		{partition: 9, resume: 1300},
	}, offs)

	require.Len(t, sched.jobs.jobs, 4, "should be 4 in-progress jobs")
	require.Equal(t, 65, int(sched.jobs.epoch))

	// Verify that the same set of updates can be sent now that we're out of
	// observation mode, and that offsets are not changed.
	sendUpdates()
	verifyCommits()
}

func TestOffsetMovement(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ps := sched.getPartitionState("ingest", 1)
	ps.initCommit(0, 5000)
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
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 5000}, "ingest/1 is in progress, so we should not move the offset")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 6000}, "ingest/1 is complete, so we should advance")
	require.NoError(t, sched.updateJob(key, "w0", true, spec))
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 6000}, "re-completing the same job shouldn't change the commit")

	p1 := sched.getPartitionState("ingest", 1)
	p1.offsets[0].committed.advance("ancient_job", schedulerpb.OffsetRange{
		StartOffset: 1000,
		EndOffset:   2000,
	})
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 6000}, "committed offsets cannot rewind")

	p2 := sched.getPartitionState("ingest", 2)
	p2.offsets[0].committed.advance("ancient_job2", schedulerpb.OffsetRange{
		StartOffset: 6000,
		EndOffset:   6222,
	})
	requireOffsets(t, sched, "ingest", 2, map[int]int64{0: 6222}, "should create knowledge of partition 2")
}

func TestKafkaFlush(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	ctx := context.Background()
	sched.completeObservationMode(ctx)

	flushAndRequireOffsets := func(topic string, offsets map[int32]int64, args ...any) {
		require.NoError(t, sched.flushOffsetsToKafka(ctx))

		offs, err := fetchCommittedOffsets(ctx, sched.adminClients[0], sched.cfg.ConsumerGroup, sched.cfg.Kafka.Topic)
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

// TestKafkaFlush_MultiCluster verifies that each cluster's Kafka receives exactly that cluster's
// committed offsets, and that a fresh scheduler against the same clusters recovers them with
// each cluster's offsets landing back in that cluster's slot.
func TestKafkaFlush_MultiCluster(t *testing.T) {
	clusters := createTestClusters(t, 3, 3)
	sched, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	ctx := t.Context()

	initCommits(sched, "ingest", 0, map[int]int64{0: 100, 1: 900, 2: 300})
	initCommits(sched, "ingest", 1, map[int]int64{0: 55})
	initCommits(sched, "ingest", 2, map[int]int64{2: 77})

	require.NoError(t, sched.flushOffsetsToKafka(ctx))

	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 100, 1: 55})
	requireCommittedInKafka(ctx, t, sched, 1, map[int32]int64{0: 900})
	requireCommittedInKafka(ctx, t, sched, 2, map[int32]int64{0: 300, 2: 77})

	reg := sched.register.(*prometheus.Registry)
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_partition_committed_offset The observed committed offset of each partition.
		# TYPE cortex_blockbuilder_scheduler_partition_committed_offset gauge
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="0",write_compartment="0"} 100
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="0",write_compartment="1"} 900
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="0",write_compartment="2"} 300
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="1",write_compartment="0"} 55
		cortex_blockbuilder_scheduler_partition_committed_offset{partition="2",write_compartment="2"} 77
	`), "cortex_blockbuilder_scheduler_partition_committed_offset"), "each cluster's commit gauge should carry its own offsets under its write_compartment label")

	restarted, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	require.NoError(t, restarted.loadInitialCommittedOffsets(ctx))

	requireOffsets(t, restarted, "ingest", 0, map[int]int64{0: 100, 1: 900, 2: 300})
	requireOffsets(t, restarted, "ingest", 1, map[int]int64{0: 55})
	requireOffsets(t, restarted, "ingest", 2, map[int]int64{2: 77})
}

// TestKafkaFlush_MultiCluster_ClustersFailToCommit verifies that clusters rejecting offset
// commits don't block the rest: the joined error identifies exactly the failing compartments,
// and every healthy cluster's offsets are still flushed.
func TestKafkaFlush_MultiCluster_ClustersFailToCommit(t *testing.T) {
	clusters := createTestClusters(t, 3, 3)
	sched, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	ctx := t.Context()

	failOffsetCommits(clusters[1].cluster)

	initCommits(sched, "ingest", 0, map[int]int64{0: 100, 1: 900, 2: 300})
	initCommits(sched, "ingest", 1, map[int]int64{0: 55})
	initCommits(sched, "ingest", 2, map[int]int64{1: 66, 2: 77})

	err := sched.flushOffsetsToKafka(ctx)
	require.ErrorContains(t, err, "write compartment 1")
	require.NotContains(t, err.Error(), "write compartment 0", "the healthy compartments should not be reported as failed")
	require.NotContains(t, err.Error(), "write compartment 2", "the healthy compartments should not be reported as failed")

	// The healthy clusters' offsets should be flushed despite cluster 1 failing.
	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 100, 1: 55})
	requireCommittedInKafka(ctx, t, sched, 2, map[int32]int64{0: 300, 2: 77})

	// Cluster 2 starts failing too. The joined error names both failing compartments, and the
	// remaining healthy cluster still flushes fresh offsets.
	failOffsetCommits(clusters[2].cluster)
	initCommits(sched, "ingest", 0, map[int]int64{0: 150, 1: 950, 2: 350})

	err = sched.flushOffsetsToKafka(ctx)
	require.ErrorContains(t, err, "write compartment 1")
	require.ErrorContains(t, err, "write compartment 2")
	require.NotContains(t, err.Error(), "write compartment 0", "the healthy compartment should not be reported as failed")

	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 150, 1: 55})
	// Cluster 2's Kafka keeps the offsets from before it started failing.
	requireCommittedInKafka(ctx, t, sched, 2, map[int32]int64{0: 300, 2: 77})
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

// TestUpdateSchedule_ProbeFailure verifies that when the only cluster's end-offset probe fails
// the tick records nothing and enqueues no jobs, and that the next successful tick picks up
// where the failed one left off.
func TestUpdateSchedule_ProbeFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	var vnet kfake.VirtualNetwork
	cluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, 1, "ingest", testkafka.WithVirtualNetwork(&vnet))
	sched, cli := mustSchedulerWithKafkaAddrAndDialer(t, kafkaAddr, vnet.DialContext)
	sched.completeObservationMode(ctx)

	produceRecords(ctx, t, cli, 0, 3)
	stopFailing := failListOffsets(cluster)

	sched.updateSchedule(ctx)

	ps := sched.getPartitionState("ingest", 0)
	require.Equal(t, int64(0), ps.offsets[0].endOffset, "a failed probe should leave the end offset at its startup value")
	require.Equal(t, 0, ps.pendingJobs.Len(), "a failed probe should not enqueue jobs")

	stopFailing()
	sched.updateSchedule(ctx)
	require.Equal(t, int64(3), ps.offsets[0].endOffset, "the next successful probe should record the produced records")
}

// TestUpdateSchedule_MultiCluster verifies that with compartments enabled the scheduler probes
// every write cluster's end offsets.
func TestUpdateSchedule_MultiCluster(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	sched, clients := mustMultiClusterScheduler(t, 3, 3)
	sched.completeObservationMode(ctx)

	// counts[c][p] is the number of records produced to partition p on cluster c.
	counts := [3][3]int{
		{3, 1, 4},
		{5, 9, 2},
		{6, 7, 8},
	}
	for c, byPartition := range counts {
		for p, n := range byPartition {
			produceRecords(ctx, t, clients[c], int32(p), n)
		}
	}

	sched.updateSchedule(ctx)

	requireEndOffsets(t, sched, "ingest", 0, map[int]int64{0: 3, 1: 5, 2: 6})
	requireEndOffsets(t, sched, "ingest", 1, map[int]int64{0: 1, 1: 9, 2: 7})
	requireEndOffsets(t, sched, "ingest", 2, map[int]int64{0: 4, 1: 2, 2: 8})
}

// TestUpdateSchedule_MultiCluster_OneClusterProbeFails verifies that when probing one cluster's
// end offsets fails, the tick proceeds without it: every other cluster's end offsets are still
// recorded and partition time still advances, cutting jobs that carry the failed cluster's
// ranges up to its last successfully probed end offset.
func TestUpdateSchedule_MultiCluster_OneClusterProbeFails(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	clusters := createTestClusters(t, 3, 3)
	sched, clients := mustMultiClusterSchedulerWithClusters(t, clusters)
	sched.completeObservationMode(ctx)

	// rounds[r][c][p] is the number of records produced to partition p on cluster c in round r.
	rounds := [2][3][3]int{
		{
			{3, 1, 4},
			{5, 9, 2},
			{6, 7, 8},
		},
		{
			{10, 20, 30},
			{40, 50, 60},
			{70, 80, 90},
		},
	}
	produceRound := func(round int) {
		for c, byPartition := range rounds[round] {
			for p, n := range byPartition {
				produceRecords(ctx, t, clients[c], int32(p), n)
			}
		}
	}

	produceRound(0)
	sched.updateSchedule(ctx)
	requireEndOffsets(t, sched, "ingest", 0, map[int]int64{0: 3, 1: 5, 2: 6}, "the healthy tick should record every cluster's ends")

	produceRound(1)
	// Backdate the job buckets so the next tick rolls over and cuts a job per partition.
	for p := range int32(3) {
		ps := sched.getPartitionState("ingest", p)
		ps.jobBucket = ps.jobBucket.Add(-2 * sched.cfg.JobSize)
	}
	stopFailing := failListOffsets(clusters[1].cluster)

	sched.updateSchedule(ctx)

	// Each end below is written as round-0 count + round-1 count. Cluster 1's probe fails this
	// tick, so it stays frozen at its round-0 count (a bare number) while clusters 0 and 2 sum both.
	msg := "the healthy clusters 0 and 2 should record both rounds; the failing cluster 1 keeps its last successfully probed ends"
	requireEndOffsets(t, sched, "ingest", 0, map[int]int64{0: 3 + 10, 1: 5, 2: 6 + 70}, msg)
	requireEndOffsets(t, sched, "ingest", 1, map[int]int64{0: 1 + 20, 1: 9, 2: 7 + 80}, msg)
	requireEndOffsets(t, sched, "ingest", 2, map[int]int64{0: 4 + 30, 1: 2, 2: 8 + 90}, msg)

	expectedRanges := map[int32]map[int32]schedulerpb.OffsetRange{
		0: {0: {StartOffset: 0, EndOffset: 3 + 10}, 1: {StartOffset: 0, EndOffset: 5}, 2: {StartOffset: 0, EndOffset: 6 + 70}},
		1: {0: {StartOffset: 0, EndOffset: 1 + 20}, 1: {StartOffset: 0, EndOffset: 9}, 2: {StartOffset: 0, EndOffset: 7 + 80}},
		2: {0: {StartOffset: 0, EndOffset: 4 + 30}, 1: {StartOffset: 0, EndOffset: 2}, 2: {StartOffset: 0, EndOffset: 8 + 90}},
	}
	for p, expected := range expectedRanges {
		ps := sched.getPartitionState("ingest", p)
		require.Equal(t, 1, ps.pendingJobs.Len(), "partition %d should still get a job cut despite cluster 1's probe failure", p)
		spec := ps.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
		require.Equal(t, expected, spec.OffsetRanges,
			"partition %d's job should bundle the healthy clusters' fresh ends with cluster 1's stale end", p)
	}

	stopFailing()
	sched.updateSchedule(ctx)

	// Cluster 1's probe succeeds now, so it too sums both rounds and catches up.
	msg = "the next successful probe should catch cluster 1 up"
	requireEndOffsets(t, sched, "ingest", 0, map[int]int64{0: 3 + 10, 1: 5 + 40, 2: 6 + 70}, msg)
	requireEndOffsets(t, sched, "ingest", 1, map[int]int64{0: 1 + 20, 1: 9 + 50, 2: 7 + 80}, msg)
	requireEndOffsets(t, sched, "ingest", 2, map[int]int64{0: 4 + 30, 1: 2 + 60, 2: 8 + 90}, msg)
}

// TestUpdateSchedule_MultiCluster_AllClusterProbesFail verifies that when every cluster's
// end-offset probe fails the tick advances nothing — no end offsets recorded, no partition time
// advance, no jobs — and the next successful tick picks up where the failed one left off.
func TestUpdateSchedule_MultiCluster_AllClusterProbesFail(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	clusters := createTestClusters(t, 3, 3)
	sched, clients := mustMultiClusterSchedulerWithClusters(t, clusters)
	sched.completeObservationMode(ctx)

	// counts[c][p] is the number of records produced to partition p on cluster c.
	counts := [3][3]int{
		{3, 1, 4},
		{5, 9, 2},
		{6, 7, 8},
	}
	for c, byPartition := range counts {
		for p, n := range byPartition {
			produceRecords(ctx, t, clients[c], int32(p), n)
		}
	}

	stops := make([]func(), len(clusters))
	for c, tc := range clusters {
		stops[c] = failListOffsets(tc.cluster)
	}

	sched.updateSchedule(ctx)

	for p := range int32(3) {
		requireEndOffsets(t, sched, "ingest", p, map[int]int64{0: 0, 1: 0, 2: 0},
			"no cluster's end offsets should be recorded when every probe fails")
		require.Equal(t, 0, sched.getPartitionState("ingest", p).pendingJobs.Len(),
			"no job should be cut when every probe fails")
	}

	for _, stop := range stops {
		stop()
	}
	sched.updateSchedule(ctx)

	msg := "the next successful tick should record every cluster's ends"
	requireEndOffsets(t, sched, "ingest", 0, map[int]int64{0: 3, 1: 5, 2: 6}, msg)
	requireEndOffsets(t, sched, "ingest", 1, map[int]int64{0: 1, 1: 9, 2: 7}, msg)
	requireEndOffsets(t, sched, "ingest", 2, map[int]int64{0: 4, 1: 2, 2: 8}, msg)
}

// TestPopulateInitialJobs_MultiCluster verifies that at startup a partition's clusters are
// probed and replayed together, producing a single job whose offset ranges bundle every cluster
// that had new data.
func TestPopulateInitialJobs_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	sched.cfg.MaxScanAge = 1 * time.Hour
	sched.cfg.JobSize = 1 * time.Hour

	recordTime := time.Date(2025, 3, 1, 10, 30, 0, 0, time.UTC)
	// ranges[p][c] is cluster c's offset range for partition p. Partition 0 has data
	// on every cluster; partitions 1 and 2 on subsets only, so their jobs must bundle just the
	// clusters that had data and leave the absent clusters out of the spec.
	ranges := map[int32]map[int32]schedulerpb.OffsetRange{
		0: {0: {StartOffset: 100, EndOffset: 200}, 1: {StartOffset: 500, EndOffset: 700}, 2: {StartOffset: 900, EndOffset: 910}},
		1: {1: {StartOffset: 710, EndOffset: 720}, 2: {StartOffset: 920, EndOffset: 930}},
		2: {2: {StartOffset: 940, EndOffset: 950}},
	}
	offsetsByPartition, stores := buildPopulateInputs(3, recordTime, ranges)

	endTime := time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)
	scanner := newOffsetScanner(stores, "ingest", endTime, sched.cfg.JobSize, sched.cfg.MaxScanAge, sched.metrics.probeRecordTimeDelta)
	sched.populateInitialJobs(context.Background(), offsetsByPartition, scanner)

	for partition, expected := range ranges {
		ps := sched.getPartitionState("ingest", partition)
		require.Equal(t, 1, ps.pendingJobs.Len(), "partition %d should get one job bundling the clusters that have data", partition)
		spec := ps.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
		require.Equal(t, expected, spec.OffsetRanges, "partition %d", partition)
	}
}

// TestInitConsumptionOffsets_PartitionOnSubsetOfClusters verifies that probing clusters with
// uneven partition counts groups offsets by partition, with a nil entry for each cluster that
// doesn't host the partition.
func TestInitConsumptionOffsets_PartitionOnSubsetOfClusters(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	clusters := createTestClustersWithPartitions(t, []int32{3, 1, 2})
	sched, clients := mustMultiClusterSchedulerWithClusters(t, clusters)

	produceRecords(ctx, t, clients[0], 0, 3)
	produceRecords(ctx, t, clients[0], 1, 1)
	produceRecords(ctx, t, clients[0], 2, 4)
	produceRecords(ctx, t, clients[1], 0, 5)
	produceRecords(ctx, t, clients[2], 0, 6)
	produceRecords(ctx, t, clients[2], 1, 7)

	offs, err := sched.initConsumptionOffsets(ctx, time.Now().Add(-time.Minute))
	require.NoError(t, err)

	// The records' timestamps predate the fallback time, so each cell resumes from its end offset.
	po := func(partition int32, offset int64) *partitionOffsets {
		return &partitionOffsets{partition: partition, start: 0, resume: offset, end: offset}
	}
	require.Equal(t, map[int32][]*partitionOffsets{
		0: {po(0, 3), po(0, 5), po(0, 6)},
		1: {po(1, 1), nil, po(1, 7)},
		2: {po(2, 4), nil, nil},
	}, offs)
}

// TestLoadInitialCommittedOffsets verifies that the consumer group's committed offsets seed
// each partition's state, and that commits for other topics in the group don't pollute it.
func TestLoadInitialCommittedOffsets(t *testing.T) {
	sched, _ := mustScheduler(t, 2)
	ctx := t.Context()
	admin := sched.adminClients[0]

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "other-topic")
	require.NoError(t, err)

	offs := make(kadm.Offsets)
	offs.AddOffset("ingest", 0, 42, -1)
	offs.AddOffset("ingest", 1, 64, -1)
	offs.AddOffset("other-topic", 0, 99, -1)
	require.NoError(t, admin.CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, offs))

	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))

	requireOffsets(t, sched, "ingest", 0, map[int]int64{0: 42}, "partition 0 should be seeded from its committed offset")
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 64}, "partition 1 should be seeded from its committed offset")
	require.Len(t, sched.partitionStates, 2, "foreign topic commits should not seed partition state")
}

// TestLoadInitialCommittedOffsets_MultiCluster verifies each cluster's committed offsets seed
// that cluster's slot in partition state and foreign topics are ignored.
func TestLoadInitialCommittedOffsets_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	ctx := t.Context()

	commit := func(clusterID int, offsets map[int32]int64) {
		offs := make(kadm.Offsets)
		for partition, offset := range offsets {
			offs.AddOffset("ingest", partition, offset, -1)
		}
		require.NoError(t, sched.adminClients[clusterID].CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, offs))
	}
	commit(0, map[int32]int64{0: 42, 1: 64, 2: 11})
	commit(1, map[int32]int64{0: 1000, 1: 7})
	commit(2, map[int32]int64{0: 5, 2: 88})

	_, err := sched.adminClients[1].CreateTopic(ctx, 1, 1, nil, "other-topic")
	require.NoError(t, err)
	foreign := make(kadm.Offsets)
	foreign.AddOffset("other-topic", 0, 99, -1)
	require.NoError(t, sched.adminClients[1].CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, foreign))

	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))

	requireOffsets(t, sched, "ingest", 0, map[int]int64{0: 42, 1: 1000, 2: 5})
	requireOffsets(t, sched, "ingest", 1, map[int]int64{0: 64, 1: 7})
	requireOffsets(t, sched, "ingest", 2, map[int]int64{0: 11, 2: 88})
	require.Len(t, sched.partitionStates, 3, "foreign topic commits should not seed partition state")
}

// TestLoadInitialCommittedOffsets_MultiCluster_OneClusterFails verifies that startup seeding
// fails fast when any single cluster's committed offsets can't be fetched, with the error
// naming the failing compartment: proceeding without them would wrongly seed that cluster's
// offset ladder.
func TestLoadInitialCommittedOffsets_MultiCluster_OneClusterFails(t *testing.T) {
	clusters := createTestClusters(t, 3, 3)
	sched, _ := mustMultiClusterSchedulerWithClusters(t, clusters)
	// fetchCommittedOffsets retries with backoff; the deadline bounds the retries so the test
	// observes the fetch error rather than waiting out all attempts.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	failOffsetFetches(clusters[1].cluster)

	err := sched.loadInitialCommittedOffsets(ctx)
	require.ErrorContains(t, err, "write compartment 1")
	require.ErrorContains(t, err, "fetch committed offsets")
}

// TestFetchCommittedOffsets_IgnoresForeignTopics verifies that committed offsets for other
// topics in the consumer group are filtered out, so they can't seed partition state.
func TestFetchCommittedOffsets_IgnoresForeignTopics(t *testing.T) {
	sched, _ := mustScheduler(t, 1)
	ctx := t.Context()
	admin := sched.adminClients[0]

	_, err := admin.CreateTopic(ctx, 1, 1, nil, "other-topic")
	require.NoError(t, err)

	offs := make(kadm.Offsets)
	offs.AddOffset("ingest", 0, 42, -1)
	offs.AddOffset("other-topic", 0, 99, -1)
	require.NoError(t, admin.CommitAllOffsets(ctx, sched.cfg.ConsumerGroup, offs))

	got, err := fetchCommittedOffsets(ctx, admin, sched.cfg.ConsumerGroup, "ingest")
	require.NoError(t, err)
	o, ok := got.Lookup("ingest", 0)
	require.True(t, ok)
	require.Equal(t, int64(42), o.At)
	_, ok = got.Lookup("other-topic", 0)
	require.False(t, ok, "foreign topic offsets should be filtered out")
}

// TestUpdateJob_ValidatesSpec verifies that specs arriving over the UpdateJob RPC are validated
// before use, so a malformed spec (e.g. an out-of-range cluster ID in its offset ranges) is
// rejected instead of panicking the scheduler.
func TestUpdateJob_ValidatesSpec(t *testing.T) {
	tests := map[string]struct {
		numClusters  int // 0 means compartments disabled.
		spec         *schedulerpb.JobSpec
		expectedCode codes.Code
	}{
		"disabled accepts start/end offsets": {
			spec:         &schedulerpb.JobSpec{Topic: "ingest", Partition: 0, StartOffset: 5, EndOffset: 10},
			expectedCode: codes.OK,
		},
		"disabled rejects offset ranges": {
			spec: &schedulerpb.JobSpec{Topic: "ingest", Partition: 0,
				OffsetRanges: map[int32]schedulerpb.OffsetRange{1: {StartOffset: 5, EndOffset: 10}}},
			expectedCode: codes.InvalidArgument,
		},
		"enabled accepts valid offset ranges": {
			numClusters: 2,
			spec: &schedulerpb.JobSpec{Topic: "ingest", Partition: 0,
				OffsetRanges: map[int32]schedulerpb.OffsetRange{0: {StartOffset: 5, EndOffset: 10}, 1: {StartOffset: 7, EndOffset: 12}}},
			expectedCode: codes.OK,
		},
		"enabled rejects an out-of-range cluster ID": {
			numClusters: 2,
			spec: &schedulerpb.JobSpec{Topic: "ingest", Partition: 0,
				OffsetRanges: map[int32]schedulerpb.OffsetRange{5: {StartOffset: 5, EndOffset: 10}}},
			expectedCode: codes.InvalidArgument,
		},
		"enabled rejects start/end offsets": {
			numClusters:  2,
			spec:         &schedulerpb.JobSpec{Topic: "ingest", Partition: 0, StartOffset: 5, EndOffset: 10},
			expectedCode: codes.InvalidArgument,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var sched *BlockBuilderScheduler
			if tc.numClusters == 0 {
				sched, _ = mustScheduler(t, 1)
			} else {
				sched, _ = mustMultiClusterScheduler(t, 1, tc.numClusters)
			}

			_, err := sched.UpdateJob(t.Context(), &schedulerpb.UpdateJobRequest{
				Key:      &schedulerpb.JobKey{Id: "job/0/1", Epoch: 1},
				WorkerId: "w0",
				Spec:     tc.spec,
			})
			if tc.expectedCode == codes.OK {
				require.NoError(t, err)
				return
			}
			require.Equal(t, tc.expectedCode, status.Code(err))
		})
	}
}

// TestPopulateInitialJobs_GroupedByPartition verifies the partition-grouped input contract:
// every partition in the map is probed and replayed independently, a partition with new data
// gets a pending job covering [resume, end), and a dormant partition (resume == end) registers
// its end offset without producing a job.
func TestPopulateInitialJobs_GroupedByPartition(t *testing.T) {
	sched, _ := mustScheduler(t, 2)
	recordTime := time.Date(2025, 3, 1, 10, 30, 0, 0, time.UTC)
	finder := &mockOffsetFinder{offsets: []*offsetTime{{offset: 100, time: recordTime}}, end: 200}

	runPopulateInitialJobs(sched, finder, map[int32][]*partitionOffsets{
		0: {{partition: 0, start: 100, resume: 100, end: 200}},
		1: {{partition: 1, start: 500, resume: 700, end: 700}},
	})

	ps0 := sched.getPartitionState("ingest", 0)
	require.Equal(t, 1, ps0.pendingJobs.Len(), "the partition with new data should get one job")
	spec := ps0.pendingJobs.Front().Value.(*schedulerpb.JobSpec)
	require.Equal(t, int64(100), spec.StartOffset)
	require.Equal(t, int64(200), spec.EndOffset)

	ps1 := sched.getPartitionState("ingest", 1)
	require.Equal(t, 0, ps1.pendingJobs.Len(), "the dormant partition should get no job")
	require.Equal(t, int64(700), ps1.offsets[0].endOffset, "the dormant partition's end offset should still be registered")
}

// TestPopulateInitialJobs_NilClusterEntry verifies that a nil entry in a partition's cluster
// slice (the partition has no offsets on that cluster) is skipped without panicking or
// producing a job.
func TestPopulateInitialJobs_NilClusterEntry(t *testing.T) {
	sched, _ := mustScheduler(t, 1)

	runPopulateInitialJobs(sched, &mockOffsetFinder{}, map[int32][]*partitionOffsets{
		0: {nil},
	})

	ps := sched.getPartitionState("ingest", 0)
	require.Equal(t, 0, ps.pendingJobs.Len())
	require.Equal(t, int64(offsetEmpty), ps.offsets[0].endOffset, "a nil cluster entry should leave the partition's offsets untouched")
}

// TestPopulateInitialJobs_ProbeErrorIsolation verifies that a probe failure on one partition
// only skips that partition: other partitions in the same call are still replayed.
func TestPopulateInitialJobs_ProbeErrorIsolation(t *testing.T) {
	sched, _ := mustScheduler(t, 2)
	finder := &mockOffsetFinder{err: errors.New("probe failed")}

	// Partition 0 has new data, so it needs a probe, which fails. Partition 1 is dormant
	// (resume == end), which needs no probe.
	runPopulateInitialJobs(sched, finder, map[int32][]*partitionOffsets{
		0: {{partition: 0, start: 100, resume: 100, end: 200}},
		1: {{partition: 1, start: 500, resume: 700, end: 700}},
	})

	ps0 := sched.getPartitionState("ingest", 0)
	require.Equal(t, 0, ps0.pendingJobs.Len(), "the failing partition should be skipped")
	require.Equal(t, int64(offsetEmpty), ps0.offsets[0].endOffset, "the failing partition's offsets should be untouched")

	ps1 := sched.getPartitionState("ingest", 1)
	require.Equal(t, int64(700), ps1.offsets[0].endOffset, "the other partition should still be replayed")
}

// TestCompartmentError verifies the write compartment is only attached to errors when
// compartments are enabled, so single-cluster errors aren't misleadingly attributed to one.
func TestCompartmentError(t *testing.T) {
	baseErr := errors.New("boom")

	t.Run("disabled leaves the error unannotated", func(t *testing.T) {
		sched, _ := mustScheduler(t, 1)
		require.Equal(t, baseErr, sched.compartmentError(0, baseErr))
	})

	t.Run("enabled annotates with the write compartment", func(t *testing.T) {
		sched, _ := mustMultiClusterScheduler(t, 1, 2)
		err := sched.compartmentError(1, baseErr)
		require.ErrorIs(t, err, baseErr)
		require.Contains(t, err.Error(), "write compartment 1")
	})
}

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

	offsetsByPartition := map[int32][]*partitionOffsets{
		0: {{partition: 0, start: 100, resume: 100, end: 1000}},
		1: {{partition: 1, start: 100, resume: 100, end: 1000}},
		2: {{partition: 2, start: 100, resume: 100, end: 1000}},
		3: {{partition: 3, start: 100, resume: 100, end: 1000}},
	}

	endTime := time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)
	scanner := newOffsetScanner([]offsetStore{finder}, "ingest", endTime, sched.cfg.JobSize, sched.cfg.MaxScanAge, sched.metrics.probeRecordTimeDelta)
	sched.populateInitialJobs(context.Background(), offsetsByPartition, scanner)
	require.Len(t, finder.distinctTimes, 4, "four partitions will each be probed at the same times, so the probe times should be shared across partitions")
}

func TestLimitNPolicy(t *testing.T) {
	existing := func(specs ...*schedulerpb.JobSpec) iter.Seq[*schedulerpb.JobSpec] {
		return slices.Values(specs)
	}

	allow1 := limitPerPartitionJobCreationPolicy{partitionLimit: 1}

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job1"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 0}, existing()))

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job4"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 0},
		existing(&schedulerpb.JobSpec{Topic: "topic", Partition: 1})))

	require.Error(t, allow1.canCreateJob(jobKey{id: "job5"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(&schedulerpb.JobSpec{Topic: "topic", Partition: 1})))

	require.NoError(t, allow1.canCreateJob(jobKey{id: "job5"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(
			&schedulerpb.JobSpec{Topic: "topic", Partition: 2},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 3},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 3},
		)))

	allow2 := limitPerPartitionJobCreationPolicy{partitionLimit: 2}
	require.NoError(t, allow2.canCreateJob(jobKey{id: "job6"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(
			&schedulerpb.JobSpec{Topic: "topic", Partition: 2},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 3},
		)))
	require.NoError(t, allow2.canCreateJob(jobKey{id: "job6"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 2},
		)))
	require.Error(t, allow2.canCreateJob(jobKey{id: "job6"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		)))
	require.Error(t, allow2.canCreateJob(jobKey{id: "job6"},
		&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		existing(
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
			&schedulerpb.JobSpec{Topic: "topic", Partition: 1},
		)))
}

func TestLimitNPolicyShortCircuits(t *testing.T) {
	// Verify that the policy stops iterating as soon as it has counted enough
	// matches to reject: with partitionLimit=2 we are about to add one, so the
	// second matching existing job is what trips rejection. The iterator must
	// not be advanced beyond that point.
	policy := limitPerPartitionJobCreationPolicy{partitionLimit: 2}
	spec := &schedulerpb.JobSpec{Topic: "topic", Partition: 0}

	visited := 0
	existing := func(yield func(*schedulerpb.JobSpec) bool) {
		matching := []*schedulerpb.JobSpec{
			{Topic: "topic", Partition: 0},
			{Topic: "topic", Partition: 0},
		}
		for _, s := range matching {
			visited++
			if !yield(s) {
				return
			}
		}
		for i := 0; i < 1000; i++ {
			visited++
			if !yield(&schedulerpb.JobSpec{Topic: "topic", Partition: 99}) {
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
	pt.initCommit(0, 20)
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

// TestBlockBuilderScheduler_EnqueuePendingJobs_CommitRace_MultiCluster verifies enqueue-time
// handling of pending jobs stale relative to per-cluster progress: a job behind every cluster's
// committed offset is dropped, while a job stale on only a subset of clusters panics.
func TestBlockBuilderScheduler_EnqueuePendingJobs_CommitRace_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	// Partition 2's pending job is behind every cluster's committed offset: dropped.
	initCommits(sched, "ingest", 2, map[int]int64{0: 20, 1: 15})
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 2,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 10, EndOffset: 20},
			1: {StartOffset: 5, EndOffset: 15},
		},
	})

	sched.enqueuePendingJobs()
	assert.Equal(t, 0, sched.getPartitionState("ingest", 2).pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count(), "the job should have been ignored because it's behind every cluster's committed offset")

	// Partition 1's pending job is behind cluster 0's progress but still needed by cluster 1.
	initCommits(sched, "ingest", 1, map[int]int64{0: 20, 1: 5})
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 1,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 10, EndOffset: 20},
			1: {StartOffset: 5, EndOffset: 15},
		},
	})

	require.Panics(t, sched.enqueuePendingJobs)
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
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 0, EndOffset: 30})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 30, EndOffset: 40})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 40, EndOffset: 50})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 0, sched.jobs.count())
	assert.True(t, pt.offsets[0].planned.empty())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.offsets[0].planned.offset())

	requireGaps(t, reg, 0, 0)

	// this one introduces a gap:
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 60, EndOffset: 70})

	assert.Equal(t, 1, pt.pendingJobs.Len())
	assert.Equal(t, 3, sched.jobs.count())
	assert.Equal(t, int64(50), pt.offsets[0].planned.offset())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(70), pt.offsets[0].planned.offset())

	requireGaps(t, reg, 1, 0)

	// the gap may not be the first job:
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 70, EndOffset: 80})
	// (gap)
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 100, EndOffset: 110})
	pt.addPendingJob(&schedulerpb.JobSpec{Topic: "ingest", Partition: part, StartOffset: 110, EndOffset: 120})

	assert.Equal(t, 3, pt.pendingJobs.Len())
	assert.Equal(t, 4, sched.jobs.count())
	assert.Equal(t, int64(70), pt.offsets[0].planned.offset())
	sched.enqueuePendingJobs()
	assert.Equal(t, 0, pt.pendingJobs.Len())
	assert.Equal(t, 7, sched.jobs.count(), "a gap should not interfere with job queueing")
	assert.Equal(t, int64(120), pt.offsets[0].planned.offset())

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

// TestBlockBuilderScheduler_EnqueuePendingJobs_GapDetection_MultiCluster verifies gap accounting
// with per-cluster offset ranges: a gap in a single cluster's range is detected even when the
// other cluster's range is contiguous, and doesn't block job queueing.
func TestBlockBuilderScheduler_EnqueuePendingJobs_GapDetection_MultiCluster(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 3, 3)
	sched.cfg.MaxJobsPerPartition = 0
	sched.completeObservationMode(context.Background())

	reg := sched.register.(*prometheus.Registry)

	// Partition 0: every cluster's ranges are contiguous.
	p0 := sched.getPartitionState("ingest", 0)
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 30},
			1: {StartOffset: 0, EndOffset: 100},
			2: {StartOffset: 0, EndOffset: 10},
		},
	})
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 0,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 30, EndOffset: 40},
			1: {StartOffset: 100, EndOffset: 200},
			2: {StartOffset: 10, EndOffset: 20},
		},
	})
	sched.enqueuePendingJobs()
	require.Equal(t, 2, sched.jobs.count())
	require.Equal(t, int64(40), p0.plannedOffset(0))
	require.Equal(t, int64(200), p0.plannedOffset(1))
	require.Equal(t, int64(20), p0.plannedOffset(2))
	requireGaps(t, reg, 0, 0)

	// Partition 1: a gap in cluster 1's ranges only; the other clusters' are contiguous.
	p1 := sched.getPartitionState("ingest", 1)
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 1,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 30},
			1: {StartOffset: 0, EndOffset: 100},
			2: {StartOffset: 0, EndOffset: 10},
		},
	})
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 1,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 30, EndOffset: 40},
			1: {StartOffset: 150, EndOffset: 200},
			2: {StartOffset: 10, EndOffset: 20},
		},
	})
	sched.enqueuePendingJobs()
	require.Equal(t, 4, sched.jobs.count(), "a gap should not interfere with job queueing")
	require.Equal(t, int64(40), p1.plannedOffset(0))
	require.Equal(t, int64(200), p1.plannedOffset(1))
	require.Equal(t, int64(20), p1.plannedOffset(2))
	requireGaps(t, reg, 1, 0, "the gap in cluster 1's ranges should be detected")

	// Partition 2: gaps in all three clusters' ranges count once per cluster.
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 2,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 0, EndOffset: 30},
			1: {StartOffset: 0, EndOffset: 100},
			2: {StartOffset: 0, EndOffset: 10},
		},
	})
	addPendingJob(sched, schedulerpb.JobSpec{
		Topic:     "ingest",
		Partition: 2,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 50, EndOffset: 60},
			1: {StartOffset: 150, EndOffset: 200},
			2: {StartOffset: 30, EndOffset: 40},
		},
	})
	sched.enqueuePendingJobs()
	require.Equal(t, 6, sched.jobs.count())
	requireGaps(t, reg, 4, 0, "gaps in all clusters' ranges should each be detected")
}

func TestBlockBuilderScheduler_NoCommit_NoGap(t *testing.T) {
	sched, _ := mustScheduler(t, 4)
	reg := sched.register.(*prometheus.Registry)

	const part int32 = 1
	requireGaps(t, reg, 0, 0)

	pp := sched.getPartitionState("ingest", part)
	require.True(t, pp.offsets[0].planned.empty())
	require.True(t, pp.offsets[0].committed.empty())

	k := jobKey{"myjob5", 5}
	spec := schedulerpb.OffsetRange{
		StartOffset: 10,
		EndOffset:   20,
	}

	pp.offsets[0].planned.advance(k.id, spec)
	requireGaps(t, reg, 0, 0, "advancing an empty planned offset should not register a gap")

	pp.offsets[0].committed.advance(k.id, spec)
	requireGaps(t, reg, 0, 0, "advancing an empty committed offset should not register a gap")

	// Now create a gap:
	k2 := jobKey{"myjob7", 23}
	spec2 := schedulerpb.OffsetRange{
		StartOffset: 40,
		EndOffset:   50,
	}

	pp.offsets[0].planned.advance(k2.id, spec2)
	requireGaps(t, reg, 1, 0, "a gap after a non-empty planned offset should register a gap")

	pp.offsets[0].committed.advance(k2.id, spec2)
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
				offsets:       tt.offsets,
				end:           tt.initialEnd,
				distinctTimes: make(map[time.Time]struct{}),
			}

			offsetsByPartition := map[int32][]*partitionOffsets{
				0: {
					{
						partition: 0,
						start:     tt.initialStart,
						resume:    tt.initialResume,
						end:       tt.initialEnd,
					},
				},
			}

			// Call populateInitialJobs to set up initial state
			scanner := newOffsetScanner([]offsetStore{finder}, "ingest", tt.initialTime, sched.cfg.JobSize, sched.cfg.MaxScanAge, sched.metrics.probeRecordTimeDelta)
			sched.populateInitialJobs(context.Background(), offsetsByPartition, scanner)

			collectedJobs := []*schedulerpb.JobSpec{}

			// Apply future end offset observations and collect jobs returned from updateTime
			ps := sched.getPartitionState("ingest", 0)

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

// runPopulateInitialJobs runs populateInitialJobs over a scanner backed by finder, with 1h
// JobSize/MaxScanAge and buckets ending at 2025-03-01 11:00 UTC.
func runPopulateInitialJobs(sched *BlockBuilderScheduler, finder offsetStore, offsetsByPartition map[int32][]*partitionOffsets) {
	sched.cfg.MaxScanAge = 1 * time.Hour
	sched.cfg.JobSize = 1 * time.Hour
	endTime := time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC)
	scanner := newOffsetScanner([]offsetStore{finder}, "ingest", endTime, sched.cfg.JobSize, sched.cfg.MaxScanAge, sched.metrics.probeRecordTimeDelta)
	sched.populateInitialJobs(context.Background(), offsetsByPartition, scanner)
}

// failListOffsets makes the cluster answer ListOffsets requests with UNKNOWN_SERVER_ERROR
// until the returned stop function is called.
func failListOffsets(cluster *kfake.Cluster) (stop func()) {
	failing := atomic.NewBool(true)
	cluster.ControlKey(kmsg.ListOffsets.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if !failing.Load() {
			return nil, nil, false
		}
		listR := req.(*kmsg.ListOffsetsRequest)
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		resp.Default()
		for _, reqTopic := range listR.Topics {
			respTopic := kmsg.ListOffsetsResponseTopic{Topic: reqTopic.Topic}
			for _, reqPartition := range reqTopic.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ListOffsetsResponseTopicPartition{
					Partition: reqPartition.Partition,
					ErrorCode: kerr.UnknownServerError.Code,
				})
			}
			resp.Topics = append(resp.Topics, respTopic)
		}
		return resp, nil, true
	})
	return func() { failing.Store(false) }
}

// failOffsetCommits makes the cluster reject all offset commits until stop is called.
func failOffsetCommits(cluster *kfake.Cluster) (stop func()) {
	failing := atomic.NewBool(true)
	cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if !failing.Load() {
			return nil, nil, false
		}
		commitR := req.(*kmsg.OffsetCommitRequest)
		resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
		resp.Default()
		for _, reqTopic := range commitR.Topics {
			respTopic := kmsg.OffsetCommitResponseTopic{Topic: reqTopic.Topic, TopicID: reqTopic.TopicID}
			for _, reqPartition := range reqTopic.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.OffsetCommitResponseTopicPartition{
					Partition: reqPartition.Partition,
					ErrorCode: kerr.GroupAuthorizationFailed.Code,
				})
			}
			resp.Topics = append(resp.Topics, respTopic)
		}
		return resp, nil, true
	})
	return func() { failing.Store(false) }
}

// failOffsetFetches makes the cluster reject all offset fetches.
func failOffsetFetches(cluster *kfake.Cluster) {
	cluster.ControlKey(kmsg.OffsetFetch.Int16(), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		fetchR := req.(*kmsg.OffsetFetchRequest)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		resp.Default()
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		for _, g := range fetchR.Groups {
			resp.Groups = append(resp.Groups, kmsg.OffsetFetchResponseGroup{
				Group:     g.Group,
				ErrorCode: kerr.GroupAuthorizationFailed.Code,
			})
		}
		return resp, nil, true
	})
}

// produceRecords produces n records to the given partition of the "ingest" topic.
func produceRecords(ctx context.Context, t *testing.T, cli *kgo.Client, partition int32, n int) {
	t.Helper()
	for i := range n {
		produceResult := cli.ProduceSync(ctx, &kgo.Record{
			Timestamp: time.Unix(int64(i), 0),
			Value:     []byte("value"),
			Topic:     "ingest",
			Partition: partition,
		})
		require.NoError(t, produceResult.FirstErr())
	}
}

// testCluster is one fake Kafka cluster and the coordinates needed to connect more clients to it.
type testCluster struct {
	cluster *kfake.Cluster
	addr    string
	dialer  dialerFunc
}

// Write compartment c's fake cluster listens at port testClusterBasePort+c on a shared in-memory
// network, so that the compartment ID placeholder in testClusterAddressTemplate resolves to that
// compartment's cluster. Assumes single-digit compartment IDs.
const (
	testClusterBasePort        = 10090
	testClusterAddressTemplate = "localhost:1009" + compartments.WriteCompartmentIDPlaceholder
)

// createTestClusters creates n independent fake Kafka clusters, one per write compartment.
func createTestClusters(t *testing.T, partitions int32, n int) []testCluster {
	partitionsByCluster := make([]int32, n)
	for c := range partitionsByCluster {
		partitionsByCluster[c] = partitions
	}
	return createTestClustersWithPartitions(t, partitionsByCluster)
}

// createTestClustersWithPartitions creates one fake Kafka cluster per entry of
// partitionsByCluster, with cluster c hosting partitionsByCluster[c] partitions.
func createTestClustersWithPartitions(t *testing.T, partitionsByCluster []int32) []testCluster {
	vnet := &kfake.VirtualNetwork{}
	clusters := make([]testCluster, len(partitionsByCluster))
	for c, partitions := range partitionsByCluster {
		cluster, kafkaAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitions, "ingest",
			testkafka.WithVirtualNetwork(vnet), testkafka.WithPort(testClusterBasePort+c))
		clusters[c] = testCluster{cluster: cluster, addr: kafkaAddr, dialer: vnet.DialContext}
	}
	return clusters
}

// mustMultiClusterSchedulerWithClusters returns a compartments-enabled scheduler backed by the
// given Kafka clusters, along with a producer client for each. Multiple schedulers can be created
// against the same clusters to exercise restart scenarios.
func mustMultiClusterSchedulerWithClusters(t *testing.T, clusters []testCluster) (*BlockBuilderScheduler, []*kgo.Client) {
	cfg := Config{
		Kafka: ingest.KafkaConfig{
			Address:      flagext.StringSliceCSV{testClusterAddressTemplate},
			Dialer:       clusters[0].dialer, // All clusters share one in-memory network.
			Topic:        "ingest",
			FetchMaxWait: 10 * time.Millisecond,
		},
		ConsumerGroup:       "test-builder",
		SchedulingInterval:  1000000 * time.Hour,
		JobSize:             1 * time.Hour,
		MaxJobsPerPartition: 1,
		Compartments: compartments.Config{
			Enabled: true,
			Write:   compartments.WriteConfig{NumCompartments: len(clusters)},
		},
	}

	reg := prometheus.NewPedanticRegistry()
	sched, err := New(cfg, test.NewTestingLogger(t), reg)
	require.NoError(t, err)

	clients := make([]*kgo.Client, len(clusters))
	admins := make([]*kadm.Client, len(clusters))
	for c, tc := range clusters {
		cli, err := kgo.NewClient(
			kgo.SeedBrokers(tc.addr),
			kgo.Dialer(tc.dialer),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		require.NoError(t, err)
		t.Cleanup(cli.Close)
		clients[c] = cli
		admins[c] = kadm.NewClient(cli)
	}
	sched.adminClients = admins
	return sched, clients
}

// mustMultiClusterScheduler returns a compartments-enabled scheduler backed by numClusters
// independent Kafka clusters, one per write compartment, along with a producer client for each.
func mustMultiClusterScheduler(t *testing.T, partitions int32, numClusters int) (*BlockBuilderScheduler, []*kgo.Client) {
	return mustMultiClusterSchedulerWithClusters(t, createTestClusters(t, partitions, numClusters))
}

// observedJob builds a worker-reported job for observation-mode tests, deriving the job ID
// from the spec the same way the planner does.
func observedJob(epoch int64, spec schedulerpb.JobSpec) job[schedulerpb.JobSpec] {
	return job[schedulerpb.JobSpec]{key: jobKey{id: jobIDForSpec(true, &spec), epoch: epoch}, spec: spec}
}

// initCommits seeds a partition's per-cluster committed offsets, keyed by cluster ID like
// requireOffsets.
func initCommits(sched *BlockBuilderScheduler, topic string, partition int32, offsets map[int]int64) {
	ps := sched.getPartitionState(topic, partition)
	for clusterID, offset := range offsets {
		ps.initCommit(clusterID, offset)
	}
}

// addPendingJob appends the spec to its partition's pending queue.
func addPendingJob(sched *BlockBuilderScheduler, spec schedulerpb.JobSpec) {
	sched.getPartitionState(spec.Topic, spec.Partition).addPendingJob(&spec)
}

// assignAllJobs assigns jobs to the worker until the queue is drained, returning the assigned
// specs.
func assignAllJobs(t *testing.T, sched *BlockBuilderScheduler, workerID string) []*schedulerpb.JobSpec {
	t.Helper()
	var specs []*schedulerpb.JobSpec
	for {
		_, spec, err := sched.assignJob(workerID)
		if errors.Is(err, errNoJobAvailable) {
			return specs
		}
		require.NoError(t, err)
		specs = append(specs, &spec)
	}
}

// requireOffsets asserts the partition's committed offsets equal expected. Unlike compartment
// data in production code, which is slice-indexed, the expectations are keyed by cluster ID so
// each expected offset names its cluster; clusters without a commit are simply absent.
func requireOffsets(t *testing.T, sched *BlockBuilderScheduler, topic string, partition int32, expected map[int]int64, msgAndArgs ...any) {
	t.Helper()
	ps := sched.getPartitionState(topic, partition)
	actual := make(map[int]int64, len(ps.offsets))
	for clusterID := range ps.offsets {
		if o := ps.offsets[clusterID].committed.offset(); o != offsetEmpty {
			actual[clusterID] = o
		}
	}
	require.Equal(t, expected, actual, msgAndArgs...)
}

// requireEndOffsets asserts the partition's probed end offsets equal expected, keyed by cluster
// ID like requireOffsets. Every cluster needs an entry: unlike commits, an end offset always
// exists, and an unprobed cluster's is 0.
func requireEndOffsets(t *testing.T, sched *BlockBuilderScheduler, topic string, partition int32, expected map[int]int64, msgAndArgs ...any) {
	t.Helper()
	ps := sched.getPartitionState(topic, partition)
	actual := make(map[int]int64, len(ps.offsets))
	for clusterID := range ps.offsets {
		actual[clusterID] = ps.offsets[clusterID].endOffset
	}
	require.Equal(t, expected, actual, msgAndArgs...)
}

// requireCommittedInKafka asserts the cluster's Kafka holds exactly the expected committed
// offsets for the scheduler's consumer group and topic.
func requireCommittedInKafka(ctx context.Context, t *testing.T, sched *BlockBuilderScheduler, clusterID int, expected map[int32]int64) {
	t.Helper()
	offs, err := fetchCommittedOffsets(ctx, sched.adminClients[clusterID], sched.cfg.ConsumerGroup, sched.cfg.Kafka.Topic)
	require.NoError(t, err)
	got := make(map[int32]int64)
	offs.Each(func(o kadm.Offset) {
		got[o.Partition] = o.At
	})
	require.Equal(t, expected, got, "cluster %d should hold exactly its own committed offsets", clusterID)
}

// partitionedOffsetFinder dispatches offset probes to a per-partition mock finder.
type partitionedOffsetFinder struct {
	byPartition map[int32]*mockOffsetFinder
}

func (f *partitionedOffsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, time.Time, bool, error) {
	finder, ok := f.byPartition[partition]
	if !ok {
		return 0, time.Time{}, false, fmt.Errorf("unexpected probe for partition %d", partition)
	}
	return finder.offsetAfterTime(ctx, topic, partition, t)
}

// buildPopulateInputs converts per-partition, per-cluster offset ranges into populateInitialJobs
// inputs: offsets grouped by partition (nil entries for clusters without the partition) and one
// finder per cluster reporting a single record at recordTime within each of its ranges.
func buildPopulateInputs(numClusters int, recordTime time.Time, ranges map[int32]map[int32]schedulerpb.OffsetRange) (map[int32][]*partitionOffsets, []offsetStore) {
	offsetsByPartition := make(map[int32][]*partitionOffsets, len(ranges))
	finders := make([]*partitionedOffsetFinder, numClusters)
	for c := range finders {
		finders[c] = &partitionedOffsetFinder{byPartition: make(map[int32]*mockOffsetFinder)}
	}
	for partition, byCluster := range ranges {
		slots := make([]*partitionOffsets, numClusters)
		for clusterID, r := range byCluster {
			slots[clusterID] = &partitionOffsets{partition: partition, start: r.StartOffset, resume: r.StartOffset, end: r.EndOffset}
			finders[clusterID].byPartition[partition] = &mockOffsetFinder{
				offsets: []*offsetTime{{offset: r.StartOffset, time: recordTime}},
				end:     r.EndOffset,
			}
		}
		offsetsByPartition[partition] = slots
	}
	stores := make([]offsetStore, numClusters)
	for c := range finders {
		stores[c] = finders[c]
	}
	return offsetsByPartition, stores
}
