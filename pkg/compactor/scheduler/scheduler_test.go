// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"flag"
	"fmt"
	"path"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestScheduler_JobLifecycleMetrics(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	scheduler, reg := startedTestScheduler(t, bkt, newTestSchedulerConfig())
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
	require.NoError(t, scheduler.tenantDiscoverer.discoverTenants(ctx))
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
			{Id: "compaction-job-1", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testULID(1).Bytes()}, Split: true, TotalBlocksBytes: 100}},
			{Id: "compaction-job-2", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testULID(2).Bytes()}, TotalBlocksBytes: 200}},
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
	backfillID := []byte(testULID(1000).String())
	compactionJob := &compactorschedulerpb.PlannedCompactionJob{
		Id:  "compaction-job",
		Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testULID(1).Bytes()}},
	}
	backfillCompactionJob := &compactorschedulerpb.PlannedCompactionJob{
		Id:  "compaction-job",
		Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testULID(1).Bytes()}, BackfillId: backfillID},
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
				Key:      &compactorschedulerpb.JobKey{Id: planJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{Unchanged: true},
			},
			expectedCode: codes.InvalidArgument,
		},
		"plan job with clear": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: planJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{Clear: true},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job with unchanged and clear": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{Unchanged: true, Clear: true},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job with clear and backfill jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID, Clear: true},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job with clear and compaction jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Jobs:     []*compactorschedulerpb.PlannedCompactionJob{backfillCompactionJob},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{Clear: true},
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
		"backfill phase plan job with unchanged and backfill jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID, Unchanged: true},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill phase plan job with compaction and backfill jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Jobs:     []*compactorschedulerpb.PlannedCompactionJob{backfillCompactionJob},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill jobs without a backfill ID": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BlockIds: [][]byte{testULID(1).Bytes()}},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill jobs of a job type that is not planned as backfill jobs": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION, BackfillId: backfillID, BlockIds: [][]byte{testULID(1).Bytes()}},
			},
			expectedCode: codes.InvalidArgument,
		},
		"backfill jobs with an invalid block ID": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:      &compactorschedulerpb.JobKey{Id: backfillPhasePlanJobId},
				Backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, BackfillId: backfillID, BlockIds: [][]byte{[]byte(testULID(1).String())}},
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
		"key that is not a plan job key": {
			req: &compactorschedulerpb.PlannedJobsRequest{
				Key:  &compactorschedulerpb.JobKey{Id: compactionJob.Id},
				Jobs: []*compactorschedulerpb.PlannedCompactionJob{compactionJob},
			},
			expectedCode: codes.InvalidArgument,
		},
	}

	scheduler, _ := startedTestScheduler(t, objstore.NewInMemBucket(), newTestSchedulerConfig())
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.req.Tenant = "tenant1"
			_, err := scheduler.PlannedJobs(t.Context(), tc.req)
			require.Equal(t, tc.expectedCode, status.Code(err), err)
		})
	}
}

func TestScheduler_BackfillLifecycle(t *testing.T) {
	const tenant = "tenant1"
	backfillID := []byte(testULID(1000).String())
	block := testULID(1)

	completeCompactionJob := func(t *testing.T, scheduler *Scheduler, key *compactorschedulerpb.JobKey) {
		_, err := scheduler.UpdateCompactionJob(t.Context(), &compactorschedulerpb.UpdateCompactionJobRequest{Tenant: tenant, Key: key, Update: compactorschedulerpb.UPDATE_TYPE_COMPLETE})
		require.NoError(t, err)
	}
	completeBackfillJob := func(t *testing.T, scheduler *Scheduler, key *compactorschedulerpb.JobKey) {
		_, err := scheduler.UpdateBackfillJob(t.Context(), &compactorschedulerpb.UpdateBackfillJobRequest{Tenant: tenant, Key: key, Update: compactorschedulerpb.UPDATE_TYPE_COMPLETE})
		require.NoError(t, err)
	}

	tests := map[string]struct {
		planned      *compactorschedulerpb.PlannedJobsRequest // the key and tenant are filled in
		expectedID   string
		expectedSpec *compactorschedulerpb.JobSpec
		complete     func(t *testing.T, scheduler *Scheduler, key *compactorschedulerpb.JobKey)
	}{
		"compaction": {
			planned: &compactorschedulerpb.PlannedJobsRequest{Jobs: []*compactorschedulerpb.PlannedCompactionJob{
				{Id: "compaction-job", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{block.Bytes()}, BackfillId: backfillID}},
			}},
			expectedID: backfillCompactionJobIDPrefix + "compaction-job",
			expectedSpec: &compactorschedulerpb.JobSpec{
				Tenant:  tenant,
				JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION,
				Job:     &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{block.Bytes()}, BackfillId: backfillID},
			},
			complete: completeCompactionJob,
		},
		"validate": {
			planned: &compactorschedulerpb.PlannedJobsRequest{Backfill: &compactorschedulerpb.PlannedBackfillJobs{
				JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID, BlockIds: [][]byte{block.Bytes()},
			}},
			expectedID: backfillValidateJobIDPrefix + block.String(),
			expectedSpec: &compactorschedulerpb.JobSpec{
				Tenant:        tenant,
				JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE,
				BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID, BlockId: block.Bytes()},
			},
			complete: completeBackfillJob,
		},
		"copy": {
			planned: &compactorschedulerpb.PlannedJobsRequest{Backfill: &compactorschedulerpb.PlannedBackfillJobs{
				JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, BackfillId: backfillID, BlockIds: [][]byte{block.Bytes()},
			}},
			expectedID: backfillCopyJobIDPrefix + block.String(),
			expectedSpec: &compactorschedulerpb.JobSpec{
				Tenant:        tenant,
				JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_COPY,
				BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID, BlockId: block.Bytes()},
			},
			complete: completeBackfillJob,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			require.NoError(t, bkt.Upload(t.Context(), path.Join(backfill.BucketPrefix, backfill.PhaseMarkerPath(tenant, uuid.New(), backfill.PhaseBackfill)), strings.NewReader("{}")))

			cfg := newTestSchedulerConfig()
			cfg.DiscoveryPolls = []string{discoveryPollBackfills}
			cfg.LanePolicy.Policy = lanePolicyBackfill
			scheduler, _ := startedTestScheduler(t, bkt, cfg)

			// Discover the tenant through the backfills poll, then inject a backfill phase plan job
			require.NoError(t, scheduler.tenantDiscoverer.discoverTenants(t.Context()))
			scheduler.rotator.Maintenance(t.Context(), false, true)

			lease := func(jobType compactorschedulerpb.JobType) *compactorschedulerpb.LeaseJobResponse {
				t.Helper()
				resp, err := scheduler.LeaseJob(t.Context(), &compactorschedulerpb.LeaseJobRequest{
					WorkerId:     "worker",
					LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: jobType}},
				})
				require.NoError(t, err)
				return resp
			}

			// Lease the backfill phase plan job and return fake results for it
			planLease := lease(compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING)
			require.Equal(t, backfillPhasePlanJobId, planLease.Key.Id)
			tc.planned.Key = planLease.Key
			tc.planned.Tenant = tenant
			_, err := scheduler.PlannedJobs(t.Context(), tc.planned)
			require.NoError(t, err)

			// Lease the fake results back out and complete them
			jobType := tc.expectedSpec.JobType
			planned := lease(jobType)
			require.Equal(t, tc.expectedID, planned.Key.Id)
			require.Equal(t, tc.expectedSpec, planned.Spec)
			tc.complete(t, scheduler, planned.Key)
			require.Equal(t, 1.0, prom_testutil.ToFloat64(scheduler.metrics.jobsCompleted.WithLabelValues(jobTypeLabel(jobType))))

			// Lease again and expect an empty result
			planned = lease(jobType)
			require.Nil(t, planned.Key)
			require.Nil(t, planned.Spec)
		})
	}
}

func TestScheduler_PlannedJobs_BackfillUnchanged(t *testing.T) {
	const tenant = "tenant1"
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(t.Context(), path.Join(backfill.BucketPrefix, backfill.PhaseMarkerPath(tenant, uuid.New(), backfill.PhaseBackfill)), strings.NewReader("{}")))

	cfg := newTestSchedulerConfig()
	cfg.DiscoveryPolls = []string{discoveryPollBackfills}
	cfg.LanePolicy.Policy = lanePolicyBackfill
	scheduler, _ := startedTestScheduler(t, bkt, cfg)
	require.NoError(t, scheduler.tenantDiscoverer.discoverTenants(t.Context()))
	scheduler.rotator.Maintenance(t.Context(), false, true)

	planLease, err := scheduler.LeaseJob(t.Context(), &compactorschedulerpb.LeaseJobRequest{
		WorkerId:     "worker",
		LaneRequests: []*compactorschedulerpb.LaneRequest{{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING}},
	})
	require.NoError(t, err)
	require.Equal(t, backfillPhasePlanJobId, planLease.Key.Id)

	_, err = scheduler.PlannedJobs(t.Context(), &compactorschedulerpb.PlannedJobsRequest{Key: planLease.Key, Tenant: tenant, Backfill: &compactorschedulerpb.PlannedBackfillJobs{Unchanged: true}})
	require.NoError(t, err)
	require.Equal(t, 1.0, prom_testutil.ToFloat64(scheduler.metrics.jobsCompleted.WithLabelValues(jobTypeBackfillPhasePlan)))

	// The plan job is complete, so its lease is gone
	_, err = scheduler.UpdatePlanJob(t.Context(), &compactorschedulerpb.UpdatePlanJobRequest{Key: planLease.Key, Tenant: tenant, Update: compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS})
	require.ErrorIs(t, err, errLeaseNotFound)
}

func TestScheduler_UpdateJob_RejectsKeysOfOtherJobTypes(t *testing.T) {
	const compactionKey = "compaction-job"

	scheduler, _ := startedTestScheduler(t, objstore.NewInMemBucket(), newTestSchedulerConfig())
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
	updateBackfillJob := func(key string) error {
		_, err := scheduler.UpdateBackfillJob(t.Context(), &compactorschedulerpb.UpdateBackfillJobRequest{
			Tenant: "tenant1",
			Key:    &compactorschedulerpb.JobKey{Id: key},
			Update: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		})
		return err
	}
	validateKey := backfillValidateJobIDPrefix + testULID(1).String()

	tests := map[string]struct {
		update func(key string) error
		key    string
	}{
		"compaction job update with a backfill validate key":     {update: updateCompactionJob, key: validateKey},
		"backfill job update with a compaction key":              {update: updateBackfillJob, key: compactionKey},
		"backfill job update with a backfill compaction key":     {update: updateBackfillJob, key: backfillCompactionJobIDPrefix + compactionKey},
		"backfill job update with the backfill phase plan key":   {update: updateBackfillJob, key: backfillPhasePlanJobId},
		"plan job update with a backfill compaction key":         {update: updatePlanJob, key: backfillCompactionJobIDPrefix + compactionKey},
		"compaction job update with the plan job key":            {update: updateCompactionJob, key: planJobId},
		"compaction job update with the backfill phase plan key": {update: updateCompactionJob, key: backfillPhasePlanJobId},
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
		scheduler, reg := startedTestScheduler(t, bkt, newTestSchedulerConfig())
		ctx := context.Background()
		require.NoError(t, scheduler.tenantDiscoverer.discoverTenants(ctx))
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
			scheduler, reg := startedTestScheduler(t, bkt, cfg)
			ctx := context.Background()
			require.NoError(t, scheduler.tenantDiscoverer.discoverTenants(ctx))
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
	return cfg
}

// startedTestScheduler creates and starts a Scheduler backed by bkt
func startedTestScheduler(t *testing.T, bkt objstore.Bucket, cfg Config) (*Scheduler, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	compactorCfg := compactor.Config{CompactionWaitPeriod: 15 * time.Minute}

	scheduler, err := newCompactorScheduler(compactorCfg, cfg, util.NewAllowList(nil, nil), bkt, &NopJobPersistenceManager{}, reg, log.NewNopLogger())
	require.NoError(t, err)

	// Tests drive the tenant discoverer and rotator directly, since running them in the background would be hard to follow
	scheduler.tenantDiscoverer.Service = services.NewIdleService(nil, nil)
	scheduler.rotator.Service = services.NewIdleService(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = services.StopAndAwaitTerminated(context.Background(), scheduler)
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, scheduler))

	return scheduler, reg
}
