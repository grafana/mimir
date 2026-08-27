// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestScheduler_JobLifecycleMetrics(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	scheduler, reg := newTestScheduler(t, bkt, newTestSchedulerConfig())
	ctx := context.Background()

	assertCompleted := func(msg string, planCount, compactionCount int) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_scheduler_jobs_completed_total Total number of jobs successfully completed by workers.
			# TYPE cortex_compactor_scheduler_jobs_completed_total counter
			cortex_compactor_scheduler_jobs_completed_total{job_type="compaction"} %d
			cortex_compactor_scheduler_jobs_completed_total{job_type="plan"} %d
		`, compactionCount, planCount)), "cortex_compactor_scheduler_jobs_completed_total"), msg)
	}
	assertIncompleteBytes := func(msg string, splitBytes, mergeBytes float64) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_scheduler_incomplete_compaction_jobs_bytes The total bytes of blocks in compaction jobs that have not yet completed (pending or active).
			# TYPE cortex_compactor_scheduler_incomplete_compaction_jobs_bytes gauge
			cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="merge",lane="compaction"} %g
			cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="split",lane="compaction"} %g
		`, mergeBytes, splitBytes)), "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes"), msg)
	}

	// Trigger maintenance so tenant's plan job is enqueued.
	scheduler.rotator.Maintenance(ctx, false, true)

	leaseResp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)
	_, err = scheduler.UpdatePlanJob(ctx, &compactorschedulerpb.UpdatePlanJobRequest{
		Tenant: leaseResp.Spec.Tenant,
		Key:    leaseResp.Key,
		Update: compactorschedulerpb.UPDATE_TYPE_ABANDON,
	})
	require.NoError(t, err)
	assertCompleted("plan job abandoned", 0, 0)

	// Re-trigger maintenance to re-enqueue the plan job after abandonment.
	scheduler.rotator.Maintenance(ctx, false, true)

	leaseResp, err = scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)
	_, err = scheduler.PlannedJobs(ctx, &compactorschedulerpb.PlannedJobsRequest{
		Key:    leaseResp.Key,
		Tenant: leaseResp.Spec.Tenant,
		Jobs: []*compactorschedulerpb.PlannedCompactionJob{
			// Two compaction jobs offered: one to be abandoned, one to be completed below.
			{Id: "compaction-job-1", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-a")}, Split: true, TotalBlocksBytes: 100}},
			{Id: "compaction-job-2", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-b")}, TotalBlocksBytes: 200}},
		},
	})
	require.NoError(t, err)
	assertCompleted("plan job completed", 1, 0)
	assertIncompleteBytes("two compaction jobs pending", 100, 200)

	leaseResp, err = scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)
	assertIncompleteBytes("first compaction job leased (still incomplete)", 100, 200)
	_, err = scheduler.UpdateCompactionJob(ctx, &compactorschedulerpb.UpdateCompactionJobRequest{
		Tenant: leaseResp.Spec.Tenant,
		Key:    leaseResp.Key,
		Update: compactorschedulerpb.UPDATE_TYPE_ABANDON,
	})
	require.NoError(t, err)
	assertCompleted("compaction job abandoned", 1, 0)
	assertIncompleteBytes("first compaction job abandoned (bytes removed)", 0, 200)

	leaseResp, err = scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)
	_, err = scheduler.UpdateCompactionJob(ctx, &compactorschedulerpb.UpdateCompactionJobRequest{
		Tenant: leaseResp.Spec.Tenant,
		Key:    leaseResp.Key,
		Update: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
	})
	require.NoError(t, err)
	assertCompleted("compaction job completed", 1, 1)
	assertIncompleteBytes("all compaction jobs complete", 0, 0)
}

func TestScheduler_RepeatedJobFailures(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	assertCounter := func(t *testing.T, reg *prometheus.Registry, expected int) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_scheduler_repeated_job_failures_total Total number of failures for jobs that exceeded the repeated failure threshold.
			# TYPE cortex_compactor_scheduler_repeated_job_failures_total counter
			cortex_compactor_scheduler_repeated_job_failures_total %d
		`, expected)), "cortex_compactor_scheduler_repeated_job_failures_total"))
	}

	t.Run("via job reassign", func(t *testing.T) {
		scheduler, reg := newTestScheduler(t, bkt, newTestSchedulerConfig())
		ctx := context.Background()
		scheduler.rotator.Maintenance(ctx, false, true)

		leaseAndCancel := func() {
			resp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
			require.NoError(t, err)
			require.NotNil(t, resp.Key)
			_, err = scheduler.UpdatePlanJob(ctx, &compactorschedulerpb.UpdatePlanJobRequest{
				Tenant: resp.Spec.Tenant,
				Key:    resp.Key,
				Update: compactorschedulerpb.UPDATE_TYPE_REASSIGN,
			})
			require.NoError(t, err)
		}

		for range scheduler.cfg.RepeatedFailureReportThreshold {
			leaseAndCancel()
		}
		assertCounter(t, reg, 0)

		leaseAndCancel()
		assertCounter(t, reg, 1)
	})

	t.Run("via lease expiration", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := newTestSchedulerConfig()
			cfg.LeaseDuration = time.Minute
			scheduler, reg := newTestScheduler(t, bkt, cfg)
			ctx := context.Background()
			scheduler.rotator.Maintenance(ctx, false, true)

			leaseAndExpire := func() {
				resp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
				require.NoError(t, err)
				require.NotNil(t, resp.Key)
				synctest.Wait()
				time.Sleep(cfg.LeaseDuration + time.Nanosecond)
				scheduler.rotator.Maintenance(ctx, true, false)
			}

			for range scheduler.cfg.RepeatedFailureReportThreshold {
				leaseAndExpire()
			}
			assertCounter(t, reg, 0)

			leaseAndExpire()
			assertCounter(t, reg, 1)
		})
	})
}

func TestScheduler_IncompleteBytesByLane(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	cfg := newTestSchedulerConfig()
	cfg.LanePolicy.Policy = lanePolicyUrgency
	scheduler, reg := newTestScheduler(t, bkt, cfg)
	ctx := context.Background()

	scheduler.rotator.Maintenance(ctx, false, true)
	leaseResp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)

	_, err = scheduler.PlannedJobs(ctx, &compactorschedulerpb.PlannedJobsRequest{
		Key:    leaseResp.Key,
		Tenant: leaseResp.Spec.Tenant,
		Jobs: []*compactorschedulerpb.PlannedCompactionJob{
			{Id: "fresh-2h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-a")}, TotalBlocksBytes: 100, MinTime: 0, MaxTime: 2 * hour,
			}},
			{Id: "periodic-24h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-b")}, TotalBlocksBytes: 900, MinTime: 0, MaxTime: 24 * hour,
			}},
			{Id: "out-of-order-24h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-c")}, TotalBlocksBytes: 50, MinTime: 0, MaxTime: 24 * hour, OutOfOrder: true,
			}},
		},
	})
	require.NoError(t, err)

	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_incomplete_compaction_jobs_bytes The total bytes of blocks in compaction jobs that have not yet completed (pending or active).
		# TYPE cortex_compactor_scheduler_incomplete_compaction_jobs_bytes gauge
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="merge",lane="compaction-urgent"} 150
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="merge",lane="compaction-defer"} 900
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="split",lane="compaction-urgent"} 0
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="split",lane="compaction-defer"} 0
	`), "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes"))
}

func TestScheduler_UrgencyLanes(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	cfg := newTestSchedulerConfig()
	cfg.LanePolicy.Policy = lanePolicyUrgency
	scheduler, _ := newTestScheduler(t, bkt, cfg)
	ctx := context.Background()

	scheduler.rotator.Maintenance(ctx, false, true)
	leaseResp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
	require.NoError(t, err)
	require.NotNil(t, leaseResp.Key)

	// Offered the deferrable job first, so passing requires lane ordering rather than FIFO.
	_, err = scheduler.PlannedJobs(ctx, &compactorschedulerpb.PlannedJobsRequest{
		Key:    leaseResp.Key,
		Tenant: leaseResp.Spec.Tenant,
		Jobs: []*compactorschedulerpb.PlannedCompactionJob{
			{Id: "periodic-24h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-defer")}, TotalBlocksBytes: 900, MinTime: 0, MaxTime: 24 * hour,
			}},
			{Id: "fresh-2h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-urgent")}, TotalBlocksBytes: 100, MinTime: 0, MaxTime: 2 * hour,
			}},
		},
	})
	require.NoError(t, err)

	leaseIDs := func() []string {
		var ids []string
		for {
			resp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"})
			require.NoError(t, err)
			if resp.Key == nil {
				return ids
			}
			ids = append(ids, resp.Key.Id)
		}
	}
	require.Equal(t, []string{"fresh-2h", "periodic-24h"}, leaseIDs(), "the urgent lane should be served before the defer lane")
}

// A worker dedicated to the defer lane makes progress even while urgent work is pending, which is
// what lets the two be served by separately sized fleets.
func TestScheduler_UrgencyLanes_DedicatedWorker(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)

	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	cfg := newTestSchedulerConfig()
	cfg.LanePolicy.Policy = lanePolicyUrgency
	scheduler, _ := newTestScheduler(t, bkt, cfg)
	ctx := context.Background()

	scheduler.rotator.Maintenance(ctx, false, true)
	planResp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "planner"})
	require.NoError(t, err)
	require.NotNil(t, planResp.Key)

	_, err = scheduler.PlannedJobs(ctx, &compactorschedulerpb.PlannedJobsRequest{
		Key:    planResp.Key,
		Tenant: planResp.Spec.Tenant,
		Jobs: []*compactorschedulerpb.PlannedCompactionJob{
			{Id: "fresh-2h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-urgent")}, TotalBlocksBytes: 100, MinTime: 0, MaxTime: 2 * hour,
			}},
			{Id: "periodic-24h", Job: &compactorschedulerpb.CompactionJob{
				BlockIds: [][]byte{[]byte("block-defer")}, TotalBlocksBytes: 900, MinTime: 0, MaxTime: 24 * hour,
			}},
		},
	})
	require.NoError(t, err)

	leaseUrgency := func(urgency compactorschedulerpb.CompactionUrgency) string {
		resp, err := scheduler.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{
			WorkerId: "worker",
			LaneRequests: []*compactorschedulerpb.LaneRequest{
				{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, CompactionUrgency: urgency},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Key)
		return resp.Key.Id
	}

	// The urgent job is still pending, so a shared worker would take it first.
	require.Equal(t, "periodic-24h", leaseUrgency(compactorschedulerpb.COMPACTION_URGENCY_DEFER))
	require.Equal(t, "fresh-2h", leaseUrgency(compactorschedulerpb.COMPACTION_URGENCY_URGENT))
}

func newTestSchedulerConfig() Config {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.ContinueOnError))
	cfg.TenantDiscoveryInterval = time.Hour // avoid repeated discovery ticks
	return cfg
}

// newTestScheduler creates and starts a Scheduler backed by bkt
func newTestScheduler(t *testing.T, bkt objstore.Bucket, cfg Config) (*Scheduler, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	compactorCfg := compactor.Config{CompactionWaitPeriod: 15 * time.Minute}

	scheduler, err := newCompactorScheduler(compactorCfg, cfg, util.NewAllowList(nil, nil), bkt, &NopJobPersistenceManager{}, reg, log.NewNopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = services.StopAndAwaitTerminated(context.Background(), scheduler)
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, scheduler))

	return scheduler, reg
}
