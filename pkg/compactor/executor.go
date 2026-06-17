// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/timeutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	errCompactionJobHasNoBlocks      = errors.New("compaction job has no blocks")
	errNoBlockMetadataProvided       = errors.New("no block metadata provided")
	errJobCanceledByScheduler        = errors.New("job canceled by scheduler")
	errFinalStatusGracePeriodTimeout = errors.New("final status grace period timed out")
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

var (
	errInvalidSchedulerEndpoint                      = fmt.Errorf("invalid compactor.scheduler-client.scheduler-endpoint, required when compactor.scheduler-client.enabled is true")
	errInvalidSchedulerUpdateInterval                = fmt.Errorf("invalid compactor.scheduler-client.update-interval, interval must be positive")
	errInvalidSchedulerLeasingMinBackoff             = fmt.Errorf("invalid compactor.scheduler-client.leasing-min-backoff, must be positive")
	errInvalidSchedulerLeasingMaxBackoff             = fmt.Errorf("invalid compactor.scheduler-client.leasing-max-backoff, must be greater than min backoff")
	errInvalidSchedulerUpdateMinBackoff              = fmt.Errorf("invalid compactor.scheduler-client.update-min-backoff, must be positive")
	errInvalidSchedulerUpdateMaxBackoff              = fmt.Errorf("invalid compactor.scheduler-client.update-max-backoff, must be greater than min backoff")
	errInvalidSchedulerTerminatingFinalStatusTimeout = fmt.Errorf("invalid compactor.scheduler-client.terminating-final-status-timeout, must be positive")
)

type SchedulerClientConfig struct {
	Enabled                       bool                           `yaml:"enabled" category:"experimental"`
	SchedulerEndpoint             string                         `yaml:"scheduler_endpoint" category:"experimental"`
	GRPCClientConfig              grpcclient.Config              `yaml:"grpc_client_config" category:"experimental"`
	LeasingMinBackoff             time.Duration                  `yaml:"leasing_min_backoff" category:"experimental"`
	LeasingMaxBackoff             time.Duration                  `yaml:"leasing_max_backoff" category:"experimental"`
	UpdateInterval                time.Duration                  `yaml:"update_interval" category:"experimental"`
	UpdateMinBackoff              time.Duration                  `yaml:"update_min_backoff" category:"experimental"`
	UpdateMaxBackoff              time.Duration                  `yaml:"update_max_backoff" category:"experimental"`
	CompactionDirCleanupInterval  time.Duration                  `yaml:"compaction_dir_cleanup_interval" category:"experimental"`
	MetadataCacheConfig           mimir_tsdb.MetadataCacheConfig `yaml:"metadata_cache" category:"experimental"`
	TerminatingFinalStatusTimeout time.Duration                  `yaml:"terminating_final_status_timeout" category:"experimental"`
}

func (cfg *SchedulerClientConfig) RegisterFlags(f *flag.FlagSet) {
	flagPrefix := "compactor.scheduler-client."
	f.BoolVar(&cfg.Enabled, flagPrefix+"enabled", false, "Controls whether compactors should contact a scheduler to request work.")
	f.StringVar(&cfg.SchedulerEndpoint, flagPrefix+"scheduler-endpoint", "", "Compactor scheduler endpoint.")
	f.DurationVar(&cfg.UpdateInterval, flagPrefix+"update-interval", 15*time.Second, "Interval between scheduler job lease updates.")
	f.DurationVar(&cfg.LeasingMinBackoff, flagPrefix+"leasing-min-backoff", 100*time.Millisecond, "Minimum backoff time between scheduler job lease requests.")
	f.DurationVar(&cfg.LeasingMaxBackoff, flagPrefix+"leasing-max-backoff", 2*time.Minute, "Maximum backoff time between scheduler job lease requests.")
	f.DurationVar(&cfg.UpdateMinBackoff, flagPrefix+"update-min-backoff", 1*time.Second, "Minimum backoff time for compaction executor retries when sending scheduler status updates.")
	f.DurationVar(&cfg.UpdateMaxBackoff, flagPrefix+"update-max-backoff", 32*time.Second, "Maximum backoff time for compaction executor retries when sending scheduler status updates.")
	f.DurationVar(&cfg.CompactionDirCleanupInterval, flagPrefix+"compaction-dir-cleanup-interval", 30*time.Minute, "Defines how frequently to clean up the compaction working directory. The directory is cleaned on startup and then only when this interval has elapsed since the last cleanup. Set to 0 to disable periodic cleanup.")
	f.DurationVar(&cfg.TerminatingFinalStatusTimeout, flagPrefix+"terminating-final-status-timeout", 30*time.Second, "Timeout for sending a final job status update to the scheduler when the parent context is canceled (e.g. during shutdown).")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix(flagPrefix+"grpc-client-config", f)
	cfg.MetadataCacheConfig.RegisterFlagsWithPrefix(f, flagPrefix+"metadata-cache.")
}

func (cfg *SchedulerClientConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.SchedulerEndpoint) == "" {
		return errInvalidSchedulerEndpoint
	}
	if cfg.UpdateInterval <= 0 {
		return errInvalidSchedulerUpdateInterval
	}
	if cfg.LeasingMinBackoff <= 0 {
		return errInvalidSchedulerLeasingMinBackoff
	}
	if cfg.LeasingMaxBackoff <= cfg.LeasingMinBackoff {
		return errInvalidSchedulerLeasingMaxBackoff
	}
	if cfg.UpdateMinBackoff <= 0 {
		return errInvalidSchedulerUpdateMinBackoff
	}
	if cfg.UpdateMaxBackoff <= cfg.UpdateMinBackoff {
		return errInvalidSchedulerUpdateMaxBackoff
	}
	if err := cfg.MetadataCacheConfig.Validate(); err != nil {
		return err
	}
	if cfg.TerminatingFinalStatusTimeout <= 0 {
		return errInvalidSchedulerTerminatingFinalStatusTimeout
	}
	return nil
}

const (
	jobTypePlan         = "plan"
	jobTypeCompaction   = "compaction"
	compactionTypeSplit = "split"
	compactionTypeMerge = "merge"
)

// schedulerExecutor requests compaction jobs from an external scheduler.
type schedulerExecutor struct {
	cfg                      SchedulerClientConfig
	logger                   log.Logger
	schedulerClient          compactorschedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
	retryable                failsafe.Executor[any]
	lastCleanupTime          time.Time
	metadataCache            cache.Cache
}

func newSchedulerExecutor(cfg SchedulerClientConfig, logger log.Logger, invalidClusterValidation *prometheus.CounterVec, reg prometheus.Registerer) (*schedulerExecutor, error) {
	cacheReg := prometheus.WrapRegistererWithPrefix("thanos_", prometheus.WrapRegistererWith(prometheus.Labels{"component": "compactor"}, reg))
	metadataCache, err := cache.CreateClient("metadata-cache", cfg.MetadataCacheConfig.BackendConfig, logger, cacheReg)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata cache: %w", err)
	}

	executor := &schedulerExecutor{
		cfg:                      cfg,
		logger:                   logger,
		invalidClusterValidation: invalidClusterValidation,
		metadataCache:            metadataCache,
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
		WithBackoff(cfg.UpdateMinBackoff, cfg.UpdateMaxBackoff).
		WithJitter(500 * time.Millisecond).
		WithMaxAttempts(-1).
		Build())

	// Initialize scheduler client. This call will succeed even if scheduler is unreachable
	// since grpc.Dial() creates the connection immediately and connects lazily.
	executor.schedulerClient, executor.schedulerConn, err = executor.makeSchedulerClient()
	if err != nil {
		if metadataCache != nil {
			metadataCache.Stop()
		}
		return nil, err
	}

	return executor, nil
}

func (e *schedulerExecutor) run(ctx context.Context, c *MultitenantCompactor) error {
	workerID := fmt.Sprintf("compactor-%s", c.ringLifecycler.GetInstanceID())
	level.Info(e.logger).Log("msg", "compactor running in scheduler mode", "scheduler_endpoint", e.cfg.SchedulerEndpoint, "worker_id", workerID)

	// Scheduler mode compactors work on jobs for arbitrary tenants, so unlike standalone mode they
	// do not cache metadata on disk. Pass nil for ownedUsers to delete any meta sync directories
	// that may have been left over from standalone mode.
	c.deleteUnownedMetaSyncDirs(nil)

	compactDir := filepath.Join(c.compactorCfg.DataDir, "compact")

	b := backoff.New(ctx, backoff.Config{
		MinBackoff: e.cfg.LeasingMinBackoff,
		MaxBackoff: e.cfg.LeasingMaxBackoff,
	})

	for {
		// Clean up the compaction directory before leasing work if interval is configured.
		if e.cfg.CompactionDirCleanupInterval > 0 {
			if err := e.cleanupCompactionDir(compactDir); err != nil {
				level.Warn(e.logger).Log("msg", "failed to cleanup compaction directory", "path", compactDir, "err", err)
			}
		}

		ok, err := e.leaseAndExecuteJob(ctx, c, workerID)
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to lease or execute job", "err", err)
		}
		if ok {
			b.Reset()
		}

		select {
		case <-time.After(b.NextDelay()):
			continue
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return fmt.Errorf("compactor subservice failed: %w", err)
		}
	}
}

func (e *schedulerExecutor) stop() error {
	if e.metadataCache != nil {
		e.metadataCache.Stop()
	}
	if e.schedulerConn != nil {
		return e.schedulerConn.Close()
	}
	return nil
}

// emptyCompactionDir removes all contents from the compaction directory without deleting the directory itself.
// If the directory does not exist, it will be created.
func emptyCompactionDir(compactDir string) error {
	// Ensure directory exists first
	if err := os.MkdirAll(compactDir, 0750); err != nil {
		return fmt.Errorf("failed to create compaction directory: %w", err)
	}

	// Read all entries in the directory
	entries, err := os.ReadDir(compactDir)
	if err != nil {
		return fmt.Errorf("failed to read compaction directory: %w", err)
	}

	// Remove each entry
	for _, entry := range entries {
		path := filepath.Join(compactDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove %q: %w", path, err)
		}
	}
	return nil
}

// cleanupCompactionDir cleans up the compaction directory if the configured
// cleanup interval has elapsed since the last cleanup.
func (e *schedulerExecutor) cleanupCompactionDir(compactDir string) error {
	elapsed := time.Since(e.lastCleanupTime)
	shouldCleanup := elapsed >= e.cfg.CompactionDirCleanupInterval

	if !shouldCleanup {
		return nil
	}

	if err := emptyCompactionDir(compactDir); err != nil {
		return err
	}

	e.lastCleanupTime = time.Now()
	return nil
}

// startJobStatusUpdater starts a goroutine that sends periodic IN_PROGRESS keep-alive updates
func (e *schedulerExecutor) startJobStatusUpdater(ctx context.Context, c *MultitenantCompactor, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, cancelJob context.CancelCauseFunc) {
	ticker := time.NewTicker(e.cfg.UpdateInterval)
	defer ticker.Stop()

	jobId := key.Id
	jobTenant := spec.Tenant

	for {
		select {
		case <-ticker.C:
			if err := e.updateJobStatus(ctx, key, spec, compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS); err != nil {
				// Check if the job was canceled from the scheduler side (not found response)
				if grpcutil.ErrorToStatusCode(err) == codes.NotFound {
					level.Info(e.logger).Log("msg", "job canceled by scheduler, stopping work", "job_id", jobId, "tenant", jobTenant)
					cancelJob(errJobCanceledByScheduler) // Cancel the job context to stop the main work
					return
				}
				level.Warn(e.logger).Log("msg", "failed to send keep-alive update", "job_id", jobId, "tenant", jobTenant, "err", err)
			} else {
				// Update scheduler contact timestamp on successful heartbeat
				c.schedulerLastContact.SetToCurrentTime()
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendFinalJobStatus sends a final status update to the scheduler with a retry policy.
// Compaction jobs send final statuses on completion, planning jobs only on failure for reassignment.
// If ctx is canceled (e.g. during shutdown), attempting to send a final status can continue for up to TerminatingFinalStatusTimeout.
func (e *schedulerExecutor) sendFinalJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, status compactorschedulerpb.UpdateType) {
	graceCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancel(nil)
	stop := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(e.cfg.TerminatingFinalStatusTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel(errFinalStatusGracePeriodTimeout)
		case <-graceCtx.Done():
		}
	})
	defer stop()

	jobId := key.Id
	jobTenant := spec.Tenant

	var err error
	switch spec.JobType {
	case compactorschedulerpb.JOB_TYPE_COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: status}
		err = e.retryable.WithContext(graceCtx).Run(func() error {
			_, err := e.schedulerClient.UpdateCompactionJob(graceCtx, req)
			return err
		})
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Tenant: spec.Tenant, Update: status}
		err = e.retryable.WithContext(graceCtx).Run(func() error {
			_, err := e.schedulerClient.UpdatePlanJob(graceCtx, req)
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
	conn, err := grpc.Dial(e.cfg.SchedulerEndpoint, opts...)
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

	c.schedulerLastContact.SetToCurrentTime()

	if resp.Key == nil || resp.Spec == nil {
		// No available job
		return false, nil
	}

	jobID := resp.Key.Id
	jobTenant := resp.Spec.Tenant
	jobType := resp.Spec.JobType

	if !jobTypeValid(jobType) {
		return false, fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", jobType.String())
	}

	// Create a cancellable context for this job that can be canceled if the scheduler cancels the job
	jobCtx, cancelJob := context.WithCancelCause(ctx)
	wg := sync.WaitGroup{}
	// Start async keep-alive updater for periodic IN_PROGRESS messages
	wg.Go(func() {
		e.startJobStatusUpdater(jobCtx, c, resp.Key, resp.Spec, cancelJob)
	})

	switch jobType {
	case compactorschedulerpb.JOB_TYPE_COMPACTION:
		status, err := e.executeCompactionJob(jobCtx, c, resp.Key, resp.Spec)
		cancelJob(err)
		wg.Wait()
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to execute job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", err)
			if !errors.Is(context.Cause(jobCtx), errJobCanceledByScheduler) {
				e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
			}
			return true, err
		}
		e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
		return true, nil
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		planStartTime := time.Now()
		plannedJobs, planErr := e.executePlanningJob(jobCtx, c, jobTenant)
		cancelJob(planErr)
		wg.Wait()
		if planErr != nil {
			level.Warn(e.logger).Log("msg", "failed to execute planning job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", planErr)
			if !errors.Is(context.Cause(jobCtx), errJobCanceledByScheduler) {
				// Only send an update on failure if the scheduler still thinks we own the job.
				e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, compactorschedulerpb.UPDATE_TYPE_REASSIGN)
			}
			return true, planErr
		}

		// For planning jobs, no final status update is sent on success. Completion is communicated via PlannedJobs.
		if err := e.sendPlannedJobs(ctx, resp.Key, resp.Spec, plannedJobs); err != nil {
			level.Warn(e.logger).Log("msg", "failed to send planned jobs", "job_id", jobID, "tenant", jobTenant, "num_jobs", len(plannedJobs), "err", err)
			return true, err
		}
		c.jobDuration.WithLabelValues(jobTypePlan, "").Observe(time.Since(planStartTime).Seconds())
		return true, nil
	default:
		// Should not happen because this case is caught above.
		return false, fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", jobType.String())
	}
}

func jobTypeValid(jobType compactorschedulerpb.JobType) bool {
	switch jobType {
	case compactorschedulerpb.JOB_TYPE_COMPACTION, compactorschedulerpb.JOB_TYPE_PLANNING:
		return true
	default:
		return false
	}
}

func (e *schedulerExecutor) updateJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, updType compactorschedulerpb.UpdateType) error {
	switch spec.JobType {
	case compactorschedulerpb.JOB_TYPE_COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: updType}
		_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
		return err
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Tenant: spec.Tenant, Update: updType}
		_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
		return err
	default:
		return fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", spec.JobType.String())
	}
}

func (e *schedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	if spec.Job == nil || len(spec.Job.BlockIds) == 0 {
		level.Error(e.logger).Log("msg", "invalid compaction plan, abandoning job", "tenant", spec.Tenant)
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, errCompactionJobHasNoBlocks
	}

	userID := spec.Tenant
	userLogger := util_log.WithUserID(userID, e.logger)

	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg, userLogger)

	userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)

	blockIDs := make([]ulid.ULID, len(spec.Job.BlockIds))
	for i, id := range spec.Job.BlockIds {
		if err := blockIDs[i].UnmarshalBinary(id); err != nil {
			level.Error(userLogger).Log("msg", "invalid block ID, abandoning job", "tenant", userID, "err", err)
			return compactorschedulerpb.UPDATE_TYPE_ABANDON, fmt.Errorf("failed to parse block ID: %w", err)
		}
	}

	startTime := time.Now()
	filters := []block.MetadataFilter{
		NewLabelRemoverFilter(compactionIgnoredLabels),
	}
	fetcher := newBatchCachingMetaFetcher(userBucket, e.metadataCache, userLogger, userID, c.compactorCfg.MetaSyncConcurrency, e.cfg.MetadataCacheConfig.MetafileContentTTL)
	metaMap, err := fetcher.fetchMetasFromIDs(ctx, blockIDs, filters)
	if err != nil {
		// Abandon the job if a block metadata was not found or corrupt
		if errors.Is(err, block.ErrorSyncMetaNotFound) || errors.Is(err, block.ErrorSyncMetaCorrupted) {
			level.Warn(userLogger).Log("msg", "block metadata missing or corrupt in object storage, abandoning job", "err", err)
			return compactorschedulerpb.UPDATE_TYPE_ABANDON, err
		}
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to sync metas: %w", err)
	}

	jobMetas, err := getJobMetas(metaMap, blockIDs)
	if err != nil {
		level.Error(userLogger).Log("msg", "blocks not found after sync, abandoning job", "err", err)
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, err
	}

	var splitNumShards uint32
	if spec.Job.Split {
		splitNumShards = uint32(c.cfgProvider.CompactorSplitAndMergeShards(userID))
	}

	job, err := buildCompactionJobFromMetas(userID, key.Id, jobMetas, spec, splitNumShards)
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
	}

	compactor, err := c.newBucketCompactor(ctx, userID, userLogger, userBucket, reg)
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to create bucket compactor: %w", err)
	}

	// Track that a compaction job has started
	compactor.metrics.groupCompactionRunsStarted.Inc()

	level.Info(userLogger).Log("msg", "executing compaction job from scheduler", "tenant", userID, "blocks", len(blockIDs), "split", spec.Job.Split)
	_, compactedBlockIDs, err := compactor.runCompactionJob(ctx, job)
	if err == nil {
		compactor.metrics.groupCompactionRunsCompleted.Inc()
		if hasNonZeroULIDs(compactedBlockIDs) {
			compactor.metrics.groupCompactions.Inc()
		}
		compactionType := compactionTypeMerge
		if spec.Job.Split {
			compactionType = compactionTypeSplit
		}
		elapsed := time.Since(startTime).Seconds()
		c.jobDuration.WithLabelValues(jobTypeCompaction, compactionType).Observe(elapsed)
		if totalBytes := spec.Job.TotalBlocksBytes; totalBytes > 0 {
			c.compactionJobBytes.WithLabelValues(compactionType).Observe(float64(totalBytes))
		}
		level.Info(userLogger).Log("msg", "compaction job completed", "tenant", userID, "compacted_blocks", len(compactedBlockIDs))
		return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
	}

	// At this point the compaction has failed. Track the failure.
	compactor.metrics.groupCompactionRunsFailed.Inc()

	if errors.Is(err, syscall.ENOSPC) {
		c.outOfSpace.Inc()
	}

	if handleErr := compactor.handleKnownCompactionErrors(ctx, job, err); handleErr == nil {
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, err
	}

	// All other errors should be reassigned
	level.Warn(userLogger).Log("msg", "compaction job failed with unhandled error, reassigning", "err", err)
	return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
}

func (e *schedulerExecutor) executePlanningJob(ctx context.Context, c *MultitenantCompactor, tenant string) ([]*compactorschedulerpb.PlannedCompactionJob, error) {
	userBucket := bucket.NewUserBucketClient(tenant, c.bucketClient, c.cfgProvider)
	userLogger := log.With(e.logger, "user", tenant)

	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg, userLogger)

	bucketCompactor, err := c.newBucketCompactor(ctx, tenant, userLogger, userBucket, reg)
	if err != nil {
		return nil, fmt.Errorf("creating bucket compactor: %w", err)
	}

	maxLookback := c.cfgProvider.CompactorMaxLookback(tenant)
	if c.cfgProvider.CompactorBlockUploadEnabled(tenant) {
		maxLookback = 0
	}

	// The BatchCachingMetaFetcher handles marker filtering on its own
	deduplicateBlocksFilter := NewShardAwareDeduplicateFilter()
	fetcher := newBatchCachingMetaFetcher(userBucket, e.metadataCache, userLogger, tenant, c.compactorCfg.MetaSyncConcurrency, e.cfg.MetadataCacheConfig.MetafileContentTTL)

	level.Info(userLogger).Log("msg", "start sync of metas")
	metas, err := fetcher.fetchCompactableMetasFromListing(ctx, maxLookback, []block.MetadataFilter{
		NewLabelRemoverFilter(compactionIgnoredLabels),
		deduplicateBlocksFilter,
	}, block.NewFetcherMetrics(reg, nil))
	if err != nil {
		return nil, fmt.Errorf("meta fetch failed: %w", err)
	}

	level.Info(userLogger).Log("msg", "start of GC")
	gcMetrics := newSyncerMetrics(reg, c.blocksMarkedForDeletion)
	if err := garbageCollectBlocks(ctx, userLogger, userBucket, deduplicateBlocksFilter.DuplicateIDs(), gcMetrics, metas); err != nil {
		return nil, fmt.Errorf("blocks garbage collect: %w", err)
	}

	jobs, err := bucketCompactor.grouper.Groups(metas)
	if err != nil {
		return nil, fmt.Errorf("group compaction jobs: %w", err)
	}

	if c.jobsOrder != nil {
		jobs = c.jobsOrder(jobs)
	} else {
		level.Info(userLogger).Log("msg", "unknown sorting, jobs will be unsorted")
	}

	jobs = bucketCompactor.filterJobsByWaitPeriod(ctx, jobs)

	now := time.Now()
	for _, delta := range bucketCompactor.blockMaxTimeDeltas(now, jobs) {
		bucketCompactor.metrics.blocksMaxTimeDelta.Observe(delta)
	}

	plannedJobs := make([]*compactorschedulerpb.PlannedCompactionJob, 0, len(jobs))
	for _, job := range jobs {
		toCompact, err := c.blocksPlanner.Plan(ctx, job.metasByMinTime)
		if err != nil {
			// Planning is pass-through for the split-merge compactor besides a range double check. If one group of blocks fails it will continue
			// to fail without intervention. In order to not stop all planning if that somehow occurs warn and continue
			level.Warn(userLogger).Log("msg", "failed to plan job", "err", err)
			continue
		}
		if len(toCompact) == 0 {
			continue
		}

		plannedJob := &compactorschedulerpb.PlannedCompactionJob{
			Id: job.key,
			Job: &compactorschedulerpb.CompactionJob{
				Split:            job.useSplitting,
				BlockIds:         serializeBlockIds(toCompact),
				TotalBlocksBytes: sumBlockBytes(toCompact),
			},
		}
		plannedJobs = append(plannedJobs, plannedJob)
	}

	level.Info(userLogger).Log("msg", "job planning completed", "num_jobs", len(plannedJobs))
	return plannedJobs, nil
}

func serializeBlockIds(metas []*block.Meta) [][]byte {
	ids := make([][]byte, 0, len(metas))
	for _, meta := range metas {
		ids = append(ids, meta.ULID.Bytes())
	}
	return ids
}

func sumBlockBytes(metas []*block.Meta) uint64 {
	var total uint64
	for _, meta := range metas {
		total += uint64(meta.BlockBytes())
	}
	return total
}

// sendPlannedJobs sends the planned compaction jobs back to the scheduler with retries.
func (e *schedulerExecutor) sendPlannedJobs(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, plannedJobs []*compactorschedulerpb.PlannedCompactionJob) error {
	req := &compactorschedulerpb.PlannedJobsRequest{
		Key:    key,
		Tenant: spec.Tenant,
		Jobs:   plannedJobs,
	}

	return e.retryable.WithContext(ctx).Run(func() error {
		_, err := e.schedulerClient.PlannedJobs(ctx, req)
		return err
	})
}

func getJobMetas(metas map[ulid.ULID]*block.Meta, blockIDs []ulid.ULID) ([]*block.Meta, error) {
	jobMetas := make([]*block.Meta, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		meta, ok := metas[blockID]
		if !ok {
			// A block not being found in the retrieved metadata means it was filtered. The job is no longer valid.
			return nil, fmt.Errorf("block %s not found in retrieved metadata", blockID.String())
		}
		jobMetas = append(jobMetas, meta)
	}
	return jobMetas, nil
}

func buildCompactionJobFromMetas(userID string, groupKey string, jobMetas []*block.Meta, spec *compactorschedulerpb.JobSpec, splitNumShards uint32) (*Job, error) {
	if len(jobMetas) == 0 {
		return nil, errNoBlockMetadataProvided
	}

	// Blocks are already sorted by MinTime from planning
	var rangeStart, rangeEnd = int64(math.MaxInt64), int64(math.MinInt64)
	for _, meta := range jobMetas {
		if meta.MinTime < rangeStart {
			rangeStart = meta.MinTime
		}
		if meta.MaxTime > rangeEnd {
			rangeEnd = meta.MaxTime
		}
	}

	resolution := jobMetas[0].Thanos.Downsample.Resolution
	externalLabels := labels.FromMap(jobMetas[0].Thanos.Labels)

	stage := stageMerge
	if spec.Job.Split {
		stage = stageSplit
	}

	var shardID string
	if jobMetas[0].Thanos.Labels != nil {
		shardID = jobMetas[0].Thanos.Labels[block.CompactorShardIDExternalLabel]
	}

	// groupKey is provided by the scheduler and is already constructed
	shardingKey := fmt.Sprintf("%s-%s-%d-%d-%s", userID, stage, rangeStart, rangeEnd, shardID)

	job := newJob(userID, groupKey, externalLabels, resolution, spec.Job.Split, splitNumShards, shardingKey)
	for _, meta := range jobMetas {
		if err := job.AppendMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to append block metadata to job: %w", err)
		}
	}

	return job, nil
}
