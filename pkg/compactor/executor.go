// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/compactor/compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/compactor/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

// CompactionExecutor defines how compaction work gets executed.
type CompactionExecutor interface {
	// Run executes the compaction strategy. This method should block until
	// the context is cancelled or an error occurs.
	Run(ctx context.Context, compactor *MultitenantCompactor) error
	// Stop performs cleanup when the executor is being shut down.
	Stop() error
}

// StandaloneExecutor runs compaction on a timer without external scheduling.
type StandaloneExecutor struct{}

// Run implements CompactionExecutor for standalone mode.
func (e *StandaloneExecutor) Run(ctx context.Context, c *MultitenantCompactor) error {
	// Run an initial compaction before starting the interval.
	c.compactUsers(ctx)

	ticker := time.NewTicker(util.DurationWithJitter(c.compactorCfg.CompactionInterval, 0.05))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.compactUsers(ctx)
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "compactor subservice failed")
		}
	}
}

// Stop implements CompactionExecutor for standalone mode.
func (e *StandaloneExecutor) Stop() error {
	// StandaloneExecutor has no resources to clean up
	return nil
}

// SchedulerExecutor requests compaction jobs from a scheduler service.
type SchedulerExecutor struct {
	cfg                      Config
	logger                   log.Logger
	schedulerClient          schedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
}

// NewSchedulerExecutor creates a new SchedulerExecutor with the provided config.
func NewSchedulerExecutor(cfg Config, logger log.Logger, invalidClusterValidation *prometheus.CounterVec) (*SchedulerExecutor, error) {
	executor := &SchedulerExecutor{
		cfg:                      cfg,
		logger:                   logger,
		invalidClusterValidation: invalidClusterValidation,
	}

	// Initialize scheduler client - this will succeed even if scheduler is unreachable
	// since grpc.Dial() creates the connection object immediately and connects lazily
	var err error
	executor.schedulerClient, executor.schedulerConn, err = executor.makeSchedulerClient()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create scheduler client")
	}

	return executor, nil
}

// Run implements CompactionExecutor for scheduler mode.
func (e *SchedulerExecutor) Run(ctx context.Context, c *MultitenantCompactor) error {
	// Scheduler client is already initialized in NewSchedulerExecutor

	// Generate a unique worker ID for this compactor instance
	workerID := fmt.Sprintf("compactor-%s", c.ringLifecycler.GetInstanceID())
	level.Info(e.logger).Log("msg", "compactor running in worker mode", "scheduler_endpoint", e.cfg.SchedulerAddress, "worker_id", workerID)

	b := backoff.New(ctx, backoff.Config{
		MinBackoff: e.cfg.SchedulerMinBackoff,
		MaxBackoff: e.cfg.SchedulerMaxBackoff,
	})

	for {
		work, err := e.leaseAndExecuteJob(ctx, c, workerID)
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to lease or execute job", "err", err)
		}

		if work {
			b.Reset()
		}

		select {
		case <-time.After(b.NextDelay()):
			continue
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "compactor subservice failed")
		}
	}
}

func (e *SchedulerExecutor) makeSchedulerClient() (schedulerpb.CompactorSchedulerClient, *grpc.ClientConn, error) {
	invalidClusterReporter := util.NewInvalidClusterValidationReporter(e.cfg.GRPCClientConfig.ClusterValidation.Label, e.invalidClusterValidation, e.logger)
	opts, err := e.cfg.GRPCClientConfig.DialOption(nil, nil, invalidClusterReporter)
	if err != nil {
		return nil, nil, err
	}

	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(e.cfg.SchedulerAddress, opts...)
	if err != nil {
		return nil, nil, err
	}

	client := schedulerpb.NewCompactorSchedulerClient(conn)
	return client, conn, nil
}

func (e *SchedulerExecutor) leaseAndExecuteJob(ctx context.Context, c *MultitenantCompactor, workerID string) (bool, error) {
	req := &schedulerpb.LeaseJobRequest{
		WorkerId: workerID,
	}

	resp, err := e.schedulerClient.LeaseJob(ctx, req)
	if err != nil {
		return false, err
	}

	level.Info(e.logger).Log("msg", "leased job from scheduler", "job_id", resp.Key.Id, "tenant", resp.Spec.Tenant)

	// Send initial IN_PROGRESS status update
	if err := e.updateJobStatus(ctx, resp.Key, resp.Spec.Tenant, schedulerpb.IN_PROGRESS); err != nil {
		level.Warn(e.logger).Log("msg", "failed to mark job as in progress", "job_id", resp.Key.Id, "err", err)
	}

	// Create context with timeout for job execution based on max job duration
	jobCtx, jobCancel := context.WithTimeout(ctx, e.cfg.SchedulerMaxJobDuration)
	defer jobCancel()

	// Create done channel to signal job completion to status update goroutine
	done := make(chan struct{})
	defer close(done)

	// Start per-job status update goroutine
	go func() {
		ticker := time.NewTicker(e.cfg.SchedulerUpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return // Job completed, stop sending updates
			case <-ticker.C:
				if err := e.updateJobStatus(ctx, resp.Key, resp.Spec.Tenant, schedulerpb.IN_PROGRESS); err != nil {
					level.Warn(e.logger).Log("msg", "failed to send keep-alive status update", "job_id", resp.Key.Id, "err", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Execute the actual compaction job with timeout context
	updType, err := e.executeCompactionJob(jobCtx, c, resp.Spec)
	if err != nil {
		// Check if the error is due to job timeout
		if errors.Is(err, context.DeadlineExceeded) {
			level.Warn(e.logger).Log("msg", "compaction job exceeded max duration", "job_id", resp.Key.Id, "tenant", resp.Spec.Tenant, "max_duration", e.cfg.SchedulerMaxJobDuration)
			// Send failure status for timeout
			if statusErr := e.updateJobStatus(ctx, resp.Key, resp.Spec.Tenant, schedulerpb.ABANDON); statusErr != nil {
				level.Warn(e.logger).Log("msg", "failed to update job status after timeout", "job_id", resp.Key.Id, "err", statusErr)
			}
		}
		return true, err
	}

	// Send final status update
	if err := e.updateJobStatus(ctx, resp.Key, resp.Spec.Tenant, updType); err != nil {
		return true, err
	}
	return true, nil
}

func (e *SchedulerExecutor) updateJobStatus(ctx context.Context, key *schedulerpb.JobKey, tenant string, updType schedulerpb.UpdateType) error {
	req := &schedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: tenant, Update: updType}
	_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
	return err
}

func (e *SchedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, spec *schedulerpb.JobSpec) (schedulerpb.UpdateType, error) {
	level.Info(e.logger).Log("msg", "executing compaction job", "tenant", spec.Tenant)
	return schedulerpb.COMPLETE, nil
}

// Stop implements CompactionExecutor for scheduler mode.
func (e *SchedulerExecutor) Stop() error {
	if e.schedulerConn != nil {
		return e.schedulerConn.Close()
	}
	return nil
}
