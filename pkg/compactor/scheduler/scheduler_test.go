// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestScheduler_LeaseJob_JobsLeasedMetric(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	scheduler, reg := newTestScheduler(t, bkt)
	ctx := context.Background()

	// Trigger maintenance so tenants' plan jobs are enqueued.
	scheduler.rotator.Maintenance(ctx, false, true)

	t.Run("no calls", func(t *testing.T) {
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_compactor_scheduler_jobs_leased_total Total number of jobs leased to workers by the scheduler.
			# TYPE cortex_compactor_scheduler_jobs_leased_total counter
			cortex_compactor_scheduler_jobs_leased_total 0
		`), "cortex_compactor_scheduler_jobs_leased_total"))
	})

	t.Run("increments when job is leased", func(t *testing.T) {
		req := &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"}
		resp, err := scheduler.LeaseJob(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.Key)

		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_compactor_scheduler_jobs_leased_total Total number of jobs leased to workers by the scheduler.
			# TYPE cortex_compactor_scheduler_jobs_leased_total counter
			cortex_compactor_scheduler_jobs_leased_total 1
		`), "cortex_compactor_scheduler_jobs_leased_total"))
	})

	t.Run("does not increment when no job is available", func(t *testing.T) {
		// We've already leased the only job we had, so we expect no new leases
		req := &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker2"}
		resp, err := scheduler.LeaseJob(ctx, req)
		require.NoError(t, err)
		require.Nil(t, resp.Key)

		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_compactor_scheduler_jobs_leased_total Total number of jobs leased to workers by the scheduler.
			# TYPE cortex_compactor_scheduler_jobs_leased_total counter
			cortex_compactor_scheduler_jobs_leased_total 1
		`), "cortex_compactor_scheduler_jobs_leased_total"))
	})
}

func TestScheduler_UpdatePlanJob_PersistentFailure(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	scheduler, reg := newTestScheduler(t, bkt)
	jobFailuresAllowed := scheduler.cfg.JobFailuresAllowed
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

	for range jobFailuresAllowed {
		leaseAndCancel()
	}
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_persistent_job_failures_total Total number of jobs that have failed more than the allowed number of times.
		# TYPE cortex_compactor_scheduler_persistent_job_failures_total counter
		cortex_compactor_scheduler_persistent_job_failures_total{user="tenant1"} 0
	`), "cortex_compactor_scheduler_persistent_job_failures_total"))

	leaseAndCancel()
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_persistent_job_failures_total Total number of jobs that have failed more than the allowed number of times.
		# TYPE cortex_compactor_scheduler_persistent_job_failures_total counter
		cortex_compactor_scheduler_persistent_job_failures_total{user="tenant1"} 1
	`), "cortex_compactor_scheduler_persistent_job_failures_total"))
}

func newTestSchedulerConfig() Config {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.ContinueOnError))
	cfg.TenantDiscoveryInterval = time.Hour // avoid repeated discovery ticks
	return cfg
}

// newTestScheduler creates and starts a Scheduler backed by bkt
func newTestScheduler(t *testing.T, bkt objstore.Bucket) (*Scheduler, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg)
	compactorCfg := compactor.Config{CompactionWaitPeriod: 15 * time.Minute}

	scheduler, err := newCompactorScheduler(compactorCfg, newTestSchedulerConfig(), util.NewAllowList(nil, nil), bkt, &NopJobPersistenceManager{}, metrics, log.NewNopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = services.StopAndAwaitTerminated(context.Background(), scheduler)
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, scheduler))

	return scheduler, reg
}
