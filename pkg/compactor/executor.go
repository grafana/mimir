// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/compactor/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

// compactionExecutor defines how compaction work is executed.
type compactionExecutor interface {
	run(ctx context.Context, compactor *MultitenantCompactor) error
	createShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy
	stop() error
}

// standaloneExecutor runs compaction on a timer without external scheduling.
type standaloneExecutor struct{}

func (e *standaloneExecutor) run(ctx context.Context, c *MultitenantCompactor) error {

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

func (e *standaloneExecutor) stop() error {
	return nil
}

func (e *standaloneExecutor) createShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy {
	allowedTenants := util.NewAllowList(enabledTenants, disabledTenants)
	return newSplitAndMergeShardingStrategy(allowedTenants, ring, ringLifecycler, cfgProvider)
}

// schedulerExecutor requests compaction jobs from an external scheduler.
type schedulerExecutor struct {
	cfg                      Config
	logger                   log.Logger
	schedulerClient          schedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
}

func newSchedulerExecutor(cfg Config, logger log.Logger, invalidClusterValidation *prometheus.CounterVec) (*schedulerExecutor, error) {

	executor := &schedulerExecutor{
		cfg:                      cfg,
		logger:                   logger,
		invalidClusterValidation: invalidClusterValidation,
	}

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
func (e *schedulerExecutor) startJobStatusUpdater(ctx context.Context, key *schedulerpb.JobKey, spec *schedulerpb.JobSpec) {
	ticker := time.NewTicker(e.cfg.SchedulerUpdateInterval)
	defer ticker.Stop()

	jobId := key.Id
	jobTenant := spec.Tenant

	for {
		select {
		case <-ticker.C:
			if err := e.updateJobStatus(ctx, key, spec, schedulerpb.IN_PROGRESS); err != nil {
				level.Warn(e.logger).Log("msg", "failed to send keep-alive update", "job_id", jobId, "tenant", jobTenant, "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendFinalJobStatus sends a final status update to the scheduler
func (e *schedulerExecutor) sendFinalJobStatus(ctx context.Context, key *schedulerpb.JobKey, spec *schedulerpb.JobSpec, status schedulerpb.UpdateType) {
	jobId := key.Id
	jobTenant := spec.Tenant

	if err := e.updateJobStatus(ctx, key, spec, status); err != nil {
		level.Warn(e.logger).Log("msg", "failed to send final status update", "job_id", jobId, "tenant", jobTenant, "status", status, "err", err)
	}
}

func (e *schedulerExecutor) createShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy {
	allowedTenants := util.NewAllowList(enabledTenants, disabledTenants)
	return &schedulerShardingStrategy{
		allowedTenants: allowedTenants,
		ring:           ring,
		ringLifecycler: ringLifecycler,
		configProvider: cfgProvider,
	}
}

func (e *schedulerExecutor) makeSchedulerClient() (schedulerpb.CompactorSchedulerClient, *grpc.ClientConn, error) {
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

func (e *schedulerExecutor) leaseAndExecuteJob(ctx context.Context, c *MultitenantCompactor, workerID string) (bool, error) {
	req := &schedulerpb.LeaseJobRequest{
		WorkerId: workerID,
	}

	resp, err := e.schedulerClient.LeaseJob(ctx, req)
	if err != nil {
		return false, err
	}

	jobID := resp.Key.Id
	jobTenant := resp.Spec.Tenant
	jobType := resp.Spec.JobType

	// Create cancellable context for keep-alive updater
	keepAliveCtx, cancelKeepAlive := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelKeepAlive() // Ensure keep-alive is cancelled when job finishes

	// Start async keep-alive updater for periodic IN_PROGRESS messages
	go e.startJobStatusUpdater(keepAliveCtx, resp.Key, resp.Spec)

	if jobType == schedulerpb.COMPACTION {
		status, err := e.executeCompactionJob(ctx, c, resp.Spec)
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to execute job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", err)
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, schedulerpb.REASSIGN)
			return true, err
		}
		e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
		return true, nil
	}

	if jobType == schedulerpb.PLANNING {
		plannedJobs, planErr := e.executePlanningJob(ctx, c, resp.Spec)
		if planErr != nil {
			level.Warn(e.logger).Log("msg", "failed to execute planning job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", planErr)
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, schedulerpb.REASSIGN)
			return true, planErr
		}

		// For planning jobs, no final status update is sent - results are communicated via PlannedJobs
		if err := e.sendPlannedJobs(ctx, resp.Spec, resp.Key, plannedJobs); err != nil {
			level.Warn(e.logger).Log("msg", "failed to send planned jobs", "job_id", jobID, "tenant", jobTenant, "num_jobs", len(plannedJobs), "err", err)
			return true, err
		}
		return true, nil
	}

	return true, nil
}

func (e *schedulerExecutor) updateJobStatus(ctx context.Context, key *schedulerpb.JobKey, spec *schedulerpb.JobSpec, updType schedulerpb.UpdateType) error {
	switch spec.JobType {
	case schedulerpb.COMPACTION:
		req := &schedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: updType}
		_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
		return err
	default:
		req := &schedulerpb.UpdatePlanJobRequest{Key: key, Update: updType}
		_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
		return err
	}
}

func (e *schedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, spec *schedulerpb.JobSpec) (schedulerpb.UpdateType, error) {
	return schedulerpb.COMPLETE, nil
}

func (e *schedulerExecutor) executePlanningJob(ctx context.Context, c *MultitenantCompactor, spec *schedulerpb.JobSpec) ([]*schedulerpb.PlannedCompactionJob, error) {
	randBlockID, err := ulid.New(ulid.Timestamp(time.Unix(0, 0)), rand.Reader)
	if err != nil {
		return nil, err
	}

	plannedJob := &schedulerpb.PlannedCompactionJob{
		Id: randBlockID.String(),
		Job: &schedulerpb.CompactionJob{
			BlockIds: [][]byte{[]byte(randBlockID.String())},
			Split:    false,
		},
	}

	return []*schedulerpb.PlannedCompactionJob{plannedJob}, nil
}

// sendPlannedJobs sends the planned compaction jobs back to the scheduler.
func (e *schedulerExecutor) sendPlannedJobs(ctx context.Context, spec *schedulerpb.JobSpec, key *schedulerpb.JobKey, plannedJobs []*schedulerpb.PlannedCompactionJob) error {

	req := &schedulerpb.PlannedJobsRequest{
		Key:  key,
		Jobs: plannedJobs,
	}

	_, err := e.schedulerClient.PlannedJobs(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
