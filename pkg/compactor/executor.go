// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/compactor/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

// CompactionExecutor defines how compaction work is executed.
type CompactionExecutor interface {
	Run(ctx context.Context, compactor *MultitenantCompactor) error
	CreateShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy
	Stop() error
}

// StandaloneExecutor runs compaction on a timer without external scheduling.
type StandaloneExecutor struct{}

func (e *StandaloneExecutor) Run(ctx context.Context, c *MultitenantCompactor) error {

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

func (e *StandaloneExecutor) Stop() error {
	return nil
}

func (e *StandaloneExecutor) CreateShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy {
	allowedTenants := util.NewAllowList(enabledTenants, disabledTenants)
	return newSplitAndMergeShardingStrategy(allowedTenants, ring, ringLifecycler, cfgProvider)
}

// SchedulerExecutor requests compaction jobs from an external scheduler.
type SchedulerExecutor struct {
	cfg                      Config
	logger                   log.Logger
	schedulerClient          schedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
}

type schedulerShardingStrategy struct {
	allowedTenants *util.AllowList
}

func (s *schedulerShardingStrategy) compactorOwnsUser(userID string) (bool, error) {
	return s.allowedTenants.IsAllowed(userID), nil
}

func (s *schedulerShardingStrategy) blocksCleanerOwnsUser(userID string) (bool, error) {
	return s.allowedTenants.IsAllowed(userID), nil
}

func (s *schedulerShardingStrategy) ownJob(job *Job) (bool, error) {
	return true, nil
}

func (s *schedulerShardingStrategy) instanceOwningJob(job *Job) (ring.InstanceDesc, error) {
	return ring.InstanceDesc{}, nil
}

func NewSchedulerExecutor(cfg Config, logger log.Logger, invalidClusterValidation *prometheus.CounterVec) (*SchedulerExecutor, error) {

	executor := &SchedulerExecutor{
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

func (e *SchedulerExecutor) Run(ctx context.Context, c *MultitenantCompactor) error {
	workerID := fmt.Sprintf("compactor-%s", c.ringLifecycler.GetInstanceID())
	level.Info(e.logger).Log("msg", "compactor running in scheduler mode", "scheduler_endpoint", e.cfg.SchedulerAddress, "worker_id", workerID)

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

func (e *SchedulerExecutor) Stop() error {
	if e.schedulerConn != nil {
		return e.schedulerConn.Close()
	}
	return nil
}

func (e *SchedulerExecutor) CreateShardingStrategy(enabledTenants, disabledTenants []string, ring *ring.Ring, ringLifecycler *ring.BasicLifecycler, cfgProvider ConfigProvider) shardingStrategy {
	allowedTenants := util.NewAllowList(enabledTenants, disabledTenants)
	return &schedulerShardingStrategy{allowedTenants: allowedTenants}
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

	// TODO: Consider retry logic for status update failures. We currently only log a warning
	// if the scheduler is unreachable. First update will be retried when ticker.C fires, but how
	// to handle scheduler down for a final status update?
	if err := e.updateJobStatus(ctx, resp.Key, resp.Spec.Tenant, schedulerpb.IN_PROGRESS); err != nil {
		level.Warn(e.logger).Log("msg", "failed to mark job as in progress", "job_id", resp.Key.Id, "err", err)
	}

	statusCtx := context.WithoutCancel(ctx)
	statusChan := make(chan schedulerpb.UpdateType, 1)

	// Status updater goroutine handles both keep-alive status updates and reports final job status
	// Use WithoutCancel to ensure status updates can succeed even during shutdown
	go func() {
		ticker := time.NewTicker(e.cfg.SchedulerUpdateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := e.updateJobStatus(statusCtx, resp.Key, resp.Spec.Tenant, schedulerpb.IN_PROGRESS); err != nil {
					level.Warn(e.logger).Log("msg", "failed to send status update", "job_id", resp.Key.Id, "tenant", resp.Spec.Tenant, "status", schedulerpb.IN_PROGRESS, "err", err)
				}
			case status := <-statusChan:
				// TODO: Consider retry logic for status update failures.
				if err := e.updateJobStatus(statusCtx, resp.Key, resp.Spec.Tenant, status); err != nil {
					level.Warn(e.logger).Log("msg", "failed to send final status update", "job_id", resp.Key.Id, "tenant", resp.Spec.Tenant, "status", status, "err", err)
				}
				return
			}
		}
	}()

	status, err := e.executeCompactionJob(ctx, c, resp.Spec)
	if err != nil {
		statusChan <- schedulerpb.ABANDON
		return true, err
	}
	statusChan <- status
	return true, nil
}

func (e *SchedulerExecutor) updateJobStatus(ctx context.Context, key *schedulerpb.JobKey, tenant string, updType schedulerpb.UpdateType) error {
	req := &schedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: tenant, Update: updType}
	_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
	return err
}

// executeCompactionJob executes the compaction work for a scheduled job.
//
// NOTE: This is a stubbed implementation. For the time being, assume jobs will only have a spec.Tenant set,
// we will not differentiate between planning and compaction.
func (e *SchedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, spec *schedulerpb.JobSpec) (schedulerpb.UpdateType, error) {

	tenant := spec.Tenant
	if err := c.compactUser(ctx, tenant); err != nil {
		level.Error(e.logger).Log("msg", "compaction failed for tenant", "tenant", tenant, "err", err)
		return schedulerpb.ABANDON, err
	}
	return schedulerpb.COMPLETE, nil
}
