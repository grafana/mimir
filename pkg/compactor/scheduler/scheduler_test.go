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
		// The plan job is now active (leased above), no pending jobs.
		req := &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker2"}
		resp, err := scheduler.LeaseJob(ctx, req)
		require.NoError(t, err)
		require.Nil(t, resp.Key)

		// Counter should still be 1 from the previous sub-test.
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_compactor_scheduler_jobs_leased_total Total number of jobs leased to workers by the scheduler.
			# TYPE cortex_compactor_scheduler_jobs_leased_total counter
			cortex_compactor_scheduler_jobs_leased_total 1
		`), "cortex_compactor_scheduler_jobs_leased_total"))
	})
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
