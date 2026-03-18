// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

func TestScheduler_LeaseJob_JobsLeasedMetric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg)
	clk := clock.NewMock()

	rotator := NewRotator(time.Minute, time.Hour, 15*time.Minute, 2*time.Minute, 3, 4, metrics, log.NewNopLogger())
	trackerMetrics := metrics.newTrackerMetricsForTenant("tenant1")
	tracker := NewJobTracker(&NopJobPersister{}, "tenant1", clk, infiniteLeases, infiniteLeases, trackerMetrics)

	// Trigger planning: a fresh tracker immediately enqueues a plan job.
	_, err := tracker.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute)
	require.NoError(t, err)

	rotator.AddTenant("tenant1", tracker)

	scheduler := &Scheduler{
		running: atomic.NewBool(true),
		rotator: rotator,
		metrics: metrics,
		logger:  log.NewNopLogger(),
	}

	t.Run("increments when job is leased", func(t *testing.T) {
		req := &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker1"}
		resp, err := scheduler.LeaseJob(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp.Key)

		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_compactor_scheduler_jobs_leased_total Total number of jobs leased to workers by the scheduler.
			# TYPE cortex_compactor_scheduler_jobs_leased_total counter
			cortex_compactor_scheduler_jobs_leased_total 1
		`), "cortex_compactor_scheduler_jobs_leased_total"))
	})

	t.Run("does not increment when no job is available", func(t *testing.T) {
		// All jobs are now active (leased above), no pending jobs.
		req := &compactorschedulerpb.LeaseJobRequest{WorkerId: "worker2"}
		resp, err := scheduler.LeaseJob(context.Background(), req)
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
