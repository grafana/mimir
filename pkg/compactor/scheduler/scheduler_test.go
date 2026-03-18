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

func TestScheduler_LeaseJob_JobsLeasedMetric(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	scheduler, reg := newTestScheduler(t, bkt, newTestSchedulerConfig())
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

func TestScheduler_RepeatedJobFailures(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(context.Background(), "tenant1/placeholder", strings.NewReader("")))

	assertCounter := func(t *testing.T, reg *prometheus.Registry, expected int) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_scheduler_repeated_job_failures_total Total number of jobs that have failed more than the allowed number of times.
			# TYPE cortex_compactor_scheduler_repeated_job_failures_total counter
			cortex_compactor_scheduler_repeated_job_failures_total{user="tenant1"} %d
		`, expected)), "cortex_compactor_scheduler_repeated_job_failures_total"))
	}

	t.Run("via job reassign", func(t *testing.T) {
		scheduler, reg := newTestScheduler(t, bkt, newTestSchedulerConfig())
		repeatedFailureThreshold := scheduler.cfg.RepeatedFailureThreshold
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

		for range repeatedFailureThreshold {
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
			repeatedFailureThreshold := scheduler.cfg.RepeatedFailureThreshold
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

			for range repeatedFailureThreshold {
				leaseAndExpire()
			}
			assertCounter(t, reg, 0)

			leaseAndExpire()
			assertCounter(t, reg, 1)
		})
	})
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
	metrics := newSchedulerMetrics(reg)
	compactorCfg := compactor.Config{CompactionWaitPeriod: 15 * time.Minute}

	scheduler, err := newCompactorScheduler(compactorCfg, cfg, util.NewAllowList(nil, nil), bkt, &NopJobPersistenceManager{}, metrics, log.NewNopLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = services.StopAndAwaitTerminated(context.Background(), scheduler)
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, scheduler))

	return scheduler, reg
}
