// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/timeutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

// compactionExecutor defines how compaction work is executed.
type compactionExecutor interface {
	run(ctx context.Context, compactor *MultitenantCompactor) error
	stop() error
}

// standaloneExecutor runs compaction on a timer without external scheduling.
type standaloneExecutor struct{}

func (e *standaloneExecutor) run(ctx context.Context, c *MultitenantCompactor) error {
	// Run an initial compaction before starting the interval.
	c.compactUsers(ctx)

	// Apply 0-100% jitter to the first tick to spread compactions when multiple
	// compactors start at the same time and the initial run completes quickly.
	firstInterval := util.DurationWithNegativeJitter(c.compactorCfg.CompactionInterval, 1.0) + 1
	standardInterval := util.DurationWithJitter(c.compactorCfg.CompactionInterval, 0.05)
	stopTicker, tickerChan := timeutil.NewVariableTicker(firstInterval, standardInterval)
	defer stopTicker()

	for {
		select {
		case <-tickerChan:
			c.compactUsers(ctx)
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return fmt.Errorf("compactor subservice failed: %w", err)
		}
	}
}

func (e *standaloneExecutor) stop() error {
	return nil
}

// schedulerExecutor requests compaction jobs from an external scheduler.
type schedulerExecutor struct {
	cfg                      Config
	logger                   log.Logger
	schedulerClient          compactorschedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
	retryable                failsafe.Executor[any]
}

func newSchedulerExecutor(cfg Config, logger log.Logger, invalidClusterValidation *prometheus.CounterVec) (*schedulerExecutor, error) {
	executor := &schedulerExecutor{
		cfg:                      cfg,
		logger:                   logger,
		invalidClusterValidation: invalidClusterValidation,
	}

	executor.retryable = failsafe.With(retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool {
			if errStatus, ok := grpcutil.ErrorToStatus(err); ok {
				switch errStatus.Code() {
				case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
					// client will generate UNAVAILABLE if some data transmitted (e.g., request metadata written
					// to TCP connection) before connection breaks.
					return true
				}
			}
			return false
		}).
		WithBackoff(cfg.ExecutorRetryMinBackoff, cfg.ExecutorRetryMaxBackoff).
		WithJitter(500 * time.Millisecond).
		WithMaxAttempts(-1).
		Build())

	// Initialize scheduler client. This call will succeed even if scheduler is unreachable
	// since grpc.Dial() creates the connection immediately and connects lazily.
	var err error
	executor.schedulerClient, executor.schedulerConn, err = executor.makeSchedulerClient()
	if err != nil {
		return nil, err
	}

	return executor, nil
}

func (e *schedulerExecutor) run(ctx context.Context, c *MultitenantCompactor) error {
	workerID := fmt.Sprintf("compactor-%s", c.ringLifecycler.GetInstanceID())
	level.Info(e.logger).Log("msg", "compactor running in scheduler mode", "scheduler_endpoint", e.cfg.SchedulerAddress, "worker_id", workerID)

	b := backoff.New(ctx, backoff.Config{
		MinBackoff: e.cfg.SchedulerMinLeasingBackoff,
		MaxBackoff: e.cfg.SchedulerMaxLeasingBackoff,
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
		}
	}
}

func (e *schedulerExecutor) stop() error {
	if e.schedulerConn != nil {
		return e.schedulerConn.Close()
	}
	return nil
}

// startJobStatusUpdater starts a goroutine that sends periodic IN_PROGRESS keep-alive updates
func (e *schedulerExecutor) startJobStatusUpdater(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, cancelJob context.CancelCauseFunc) {
	ticker := time.NewTicker(e.cfg.SchedulerUpdateInterval)
	defer ticker.Stop()

	jobId := key.Id
	jobTenant := spec.Tenant

	for {
		select {
		case <-ticker.C:
			if err := e.updateJobStatus(ctx, key, spec, compactorschedulerpb.IN_PROGRESS); err != nil {
				// Check if the job was canceled from the scheduler side (not found response)
				if errStatus, ok := grpcutil.ErrorToStatus(err); ok && errStatus.Code() == codes.NotFound {
					level.Info(e.logger).Log("msg", "job canceled by scheduler, stopping work", "job_id", jobId, "tenant", jobTenant)
					cancelJob(err) // Cancel the job context to stop the main work
					return
				}
				level.Warn(e.logger).Log("msg", "failed to send keep-alive update", "job_id", jobId, "tenant", jobTenant, "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendFinalJobStatus sends a final status update to the scheduler with retry policy.
// Compaction jobs send final statuses on completion, planning jobs only on failure for reassignment.
func (e *schedulerExecutor) sendFinalJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, status compactorschedulerpb.UpdateType) {
	jobId := key.Id
	jobTenant := spec.Tenant

	var err error
	switch spec.JobType {
	case compactorschedulerpb.COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: status}
		err = e.retryable.WithContext(ctx).Run(func() error {
			_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
			return err
		})
	case compactorschedulerpb.PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Update: status}
		err = e.retryable.WithContext(ctx).Run(func() error {
			_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
			return err
		})
	default:
		err = fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", spec.JobType.String())
	}

	if err != nil {
		level.Error(e.logger).Log("msg", "failed to send final status update", "job_id", jobId, "tenant", jobTenant, "status", status, "err", err)
	}
}

func (e *schedulerExecutor) makeSchedulerClient() (compactorschedulerpb.CompactorSchedulerClient, *grpc.ClientConn, error) {
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

	client := compactorschedulerpb.NewCompactorSchedulerClient(conn)
	return client, conn, nil
}

func (e *schedulerExecutor) leaseAndExecuteJob(ctx context.Context, c *MultitenantCompactor, workerID string) (bool, error) {
	req := &compactorschedulerpb.LeaseJobRequest{
		WorkerId: workerID,
	}

	resp, err := e.schedulerClient.LeaseJob(ctx, req)
	if err != nil {
		return false, err
	}

	jobID := resp.Key.Id
	jobTenant := resp.Spec.Tenant
	jobType := resp.Spec.JobType

	// Create a cancellable context for this job that can be canceled if the scheduler cancels the job
	jobCtx, cancelJob := context.WithCancelCause(ctx)

	// Start async keep-alive updater for periodic IN_PROGRESS messages
	go e.startJobStatusUpdater(jobCtx, resp.Key, resp.Spec, cancelJob)

	switch jobType {
	case compactorschedulerpb.COMPACTION:
		status, err := e.executeCompactionJob(jobCtx, c, resp.Spec)
		cancelJob(err)
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to execute job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", err)
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, compactorschedulerpb.REASSIGN)
			return true, err
		}
		e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
		return true, nil
	case compactorschedulerpb.PLANNING:
		plannedJobs, planErr := e.executePlanningJob(jobCtx, c, resp.Spec)
		cancelJob(planErr)
		if planErr != nil {
			level.Warn(e.logger).Log("msg", "failed to execute planning job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", planErr)
			// Planning jobs only send final status updates on failure
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, compactorschedulerpb.REASSIGN)
			return true, planErr
		}

		// For planning jobs, no final status update is sent - results are communicated via PlannedJobs
		if err := e.sendPlannedJobs(ctx, resp.Spec, resp.Key, plannedJobs); err != nil {
			level.Warn(e.logger).Log("msg", "failed to send planned jobs", "job_id", jobID, "tenant", jobTenant, "num_jobs", len(plannedJobs), "err", err)
			return true, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", jobType.String())
	}
}

func (e *schedulerExecutor) updateJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, updType compactorschedulerpb.UpdateType) error {
	switch spec.JobType {
	case compactorschedulerpb.COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: updType}
		_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
		return err
	case compactorschedulerpb.PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Update: updType}
		_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
		return err
	default:
		return fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", spec.JobType.String())
	}
}

func (e *schedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	return compactorschedulerpb.COMPLETE, nil
}

func (e *schedulerExecutor) executePlanningJob(ctx context.Context, c *MultitenantCompactor, spec *compactorschedulerpb.JobSpec) ([]*compactorschedulerpb.PlannedCompactionJob, error) {
	randBlockID, err := ulid.New(ulid.Timestamp(time.Unix(0, 0)), rand.Reader)
	if err != nil {
		return nil, err
	}

	plannedJob := &compactorschedulerpb.PlannedCompactionJob{
		Id: randBlockID.String(),
		Job: &compactorschedulerpb.CompactionJob{
			BlockIds: [][]byte{[]byte(randBlockID.String())},
			Split:    false,
		},
	}

	return []*compactorschedulerpb.PlannedCompactionJob{plannedJob}, nil
}

// sendPlannedJobs sends the planned compaction jobs back to the scheduler with retries.
func (e *schedulerExecutor) sendPlannedJobs(ctx context.Context, spec *compactorschedulerpb.JobSpec, key *compactorschedulerpb.JobKey, plannedJobs []*compactorschedulerpb.PlannedCompactionJob) error {
	req := &compactorschedulerpb.PlannedJobsRequest{
		Key:  key,
		Jobs: plannedJobs,
	}

	return e.retryable.WithContext(ctx).Run(func() error {
		_, err := e.schedulerClient.PlannedJobs(ctx, req)
		return err
	})
}
