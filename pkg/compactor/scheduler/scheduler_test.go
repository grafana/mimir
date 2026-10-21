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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func TestScheduler_PlannedJobs_Rejections(t *testing.T) {
	backfillID := []byte("01K7ZQ3W8V9C2N5R6T4Y1HXJ0M")
	compactionJob := &compactorschedulerpb.PlannedCompactionJob{
		Id:  "0@17241709254077376921-merge--1761004800000-1761012000000",
		Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-a")}},
	}
	backfillCompactionJob := &compactorschedulerpb.PlannedCompactionJob{
		Id:  "0@17241709254077376921-merge--1761004800000-1761012000000",
		Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-a")}, BackfillId: backfillID},
	}

	tests := map[string]struct {
		req          *compactorschedulerpb.PlannedJobsRequest
		expectedCode codes.Code
	}{
		"plan job with a backfill ID": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:  &compactorschedulerpb.JobKey{Id: planJobId},
				Jobs: []*compactorschedulerpb.PlannedCompactionJob{backfillCompactionJob},
			},
			expectedCode: codes.InvalidArgument,
		},
		"plan job with unchanged": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:       &compactorschedulerpb.JobKey{Id: planJobId},
				Unchanged: true,
			},
			expectedCode: codes.InvalidArgument,
		},
		"plan job with backfill jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: planJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job without a backfill ID": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:  &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Jobs: []*compactorschedulerpb.PlannedCompactionJob{compactionJob},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job with unchanged": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:       &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Unchanged: true,
			},
			expectedCode: codes.Unimplemented,
		},
		"key that is not a plan job key": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:  &compactorschedulerpb.JobKey{Id: compactionJob.Id},
				Jobs: []*compactorschedulerpb.PlannedCompactionJob{compactionJob},
			},
			expectedCode: codes.InvalidArgument,
		},
	}

	scheduler, _ := newTestScheduler(t, objstore.NewInMemBucket(), newTestSchedulerConfig())
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.req.Tenant = "tenant1"
			_, err := scheduler.PlannedJobs(t.Context(), tc.req)
			require.Equal(t, tc.expectedCode, status.Code(err), err)
		})
	}
}

func TestScheduler_PlannedJobs_BackfillCompaction(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(t.Context(), backfillPhasesPrefix+"tenant1/backfill", strings.NewReader("")))

	cfg := newTestSchedulerConfig()
	cfg.DiscoveryPolls = []string{discoveryPollBackfills}
	cfg.LanePolicy.Policy = lanePolicyBackfill
	scheduler, _ := newTestScheduler(t, bkt, cfg)
	scheduler.rotator.Maintenance(t.Context(), false, true)

	planLease, err := scheduler.LeaseJob(t.Context(), &compactorschedulerpb.LeaseJobRequest{
		WorkerId:     "worker1",
		LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING}},
	})
	require.NoError(t, err)
	require.Equal(t, backfillPhasePlanJobId, planLease.Key.Id)

	const plannedID = "0@17241709254077376921-merge--1761004800000-1761012000000"
	backfillID := []byte("01K7ZQ3W8V9C2N5R6T4Y1HXJ0M")
	_, err = scheduler.PlannedJobs(t.Context(), &compactorschedulerpb.PlannedJobsRequest{
		Key:    planLease.Key,
		Tenant: planLease.Spec.Tenant,
		Jobs: []*compactorschedulerpb.PlannedCompactionJob{
			{Id: plannedID, Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-a")}, BackfillId: backfillID}},
		},
	})
	require.NoError(t, err)

	compactionLease, err := scheduler.LeaseJob(t.Context(), &compactorschedulerpb.LeaseJobRequest{
		WorkerId:     "worker1",
		LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION}},
	})
	require.NoError(t, err)
	require.Equal(t, backfillCompactionJobKeyPrefix+plannedID, compactionLease.Key.Id)
	require.Equal(t, compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION, compactionLease.Spec.JobType)
	require.Equal(t, backfillID, compactionLease.Spec.Job.BackfillId)
}

func TestScheduler_UpdateJob_RejectsKeysOfOtherJobTypes(t *testing.T) {
	const compactionKey = "0@17241709254077376921-merge--1761004800000-1761012000000"

	scheduler, _ := newTestScheduler(t, objstore.NewInMemBucket(), newTestSchedulerConfig())
	updatePlanJob := func(key string) error {
		_, err := scheduler.UpdatePlanJob(t.Context(), &compactorschedulerpb.UpdatePlanJobRequest{
			Tenant: "tenant1",
			Key:    &compactorschedulerpb.JobKey{Id: key},
			Update: compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS,
		})
		return err
	}
	updateCompactionJob := func(key string) error {
		_, err := scheduler.UpdateCompactionJob(t.Context(), &compactorschedulerpb.UpdateCompactionJobRequest{
			Tenant: "tenant1",
			Key:    &compactorschedulerpb.JobKey{Id: key},
			Update: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		})
		return err
	}

	tests := map[string]struct {
		update func(key string) error
		key    string
	}{
		"plan job update with a compaction key":                  {update: updatePlanJob, key: compactionKey},
		"plan job update with a backfill compaction key":         {update: updatePlanJob, key: backfillCompactionJobKeyPrefix + compactionKey},
		"compaction job update with the plan job key":            {update: updateCompactionJob, key: planJobId},
		"compaction job update with the backfill phase plan key": {update: updateCompactionJob, key: backfillPhasePlanJobId},
		"compaction job update with an unknown typed key":        {update: updateCompactionJob, key: typedJobKeyPrefix + "z" + compactionKey},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.update(tc.key)
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
		})
	}
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

func TestConfig_Validate_BackfillLanePolicy(t *testing.T) {
	tests := map[string]struct {
		discoveryPolls []string
		lanePolicy     string
		expectErr      bool
	}{
		"simple policy without the backfills poll": {
			discoveryPolls: []string{discoveryPollBlocks},
			lanePolicy:     lanePolicySimple,
		},
		"backfill policy with the backfills poll": {
			discoveryPolls: []string{discoveryPollBlocks, discoveryPollBackfills},
			lanePolicy:     lanePolicyBackfill,
		},
		"simple policy with the backfills poll": {
			discoveryPolls: []string{discoveryPollBlocks, discoveryPollBackfills},
			lanePolicy:     lanePolicySimple,
			expectErr:      true,
		},
		"backfill policy without the backfills poll": {
			discoveryPolls: []string{discoveryPollBlocks},
			lanePolicy:     lanePolicyBackfill,
			expectErr:      true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := newTestSchedulerConfig()
			cfg.PersistenceType = "none"
			cfg.DiscoveryPolls = tc.discoveryPolls
			cfg.LanePolicy.Policy = tc.lanePolicy

			err := cfg.Validate()
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
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
