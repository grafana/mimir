// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	SnapshotsStoragePrefix = "usage-tracker-snapshots"

	writerMetricsPrefix = "cortex_usage_tracker_kafka_writer_"
	readerMetricsPrefix = "cortex_usage_tracker_kafka_reader_"

	eventsKafkaWriterComponent    = "usage-tracker-events-writer"
	eventsKafkaReaderComponent    = "usage-tracker-events-reader"
	snapshotsKafkaWriterComponent = "usage-tracker-snapshots-metadata-writer"
	snapshotsKafkaReaderComponent = "usage-tracker-snapshots-metadata-reader"

	snapshotCleanupIdleTimeoutFactor = 10
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	DoNotApplySeriesLimits bool `yaml:"do_not_apply_series_limits"`
	UseGlobalSeriesLimits  bool `yaml:"use_global_series_limits"`

	Partitions                        int           `yaml:"partitions"`
	PartitionReconcileInterval        time.Duration `yaml:"partition_reconcile_interval"`
	LostPartitionsShutdownGracePeriod time.Duration `yaml:"lost_partitions_shutdown_grace_period"`
	MaxPartitionsToCreatePerReconcile int           `yaml:"max_partitions_to_create_per_reconcile"`

	InstanceRing  InstanceRingConfig  `yaml:"instance_ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`

	EventsStorageWriter ingest.KafkaConfig `yaml:"events_storage_writer"`
	EventsStorageReader ingest.KafkaConfig `yaml:"events_storage_reader"`

	SnapshotsMetadataWriter ingest.KafkaConfig `yaml:"snapshots_metadata_writer"`
	SnapshotsMetadataReader ingest.KafkaConfig `yaml:"snapshots_metadata_reader"`
	SnapshotsStorage        bucket.Config      `yaml:"snapshots_storage"`
	SnapshotsLoadBackoff    backoff.Config     `yaml:"snapshots_load_backoff"`

	IdleTimeout                           time.Duration `yaml:"idle_timeout"`
	CreatedSeriesEventsMaxPending         int           `yaml:"max_pending_created_series_events"`
	CreatedSeriesEventsMaxBatchSizeBytes  int           `yaml:"created_series_events_max_batch_size_bytes"`
	CreatedSeriesEventsBatchTTL           time.Duration `yaml:"created_series_events_batch_ttl"`
	CreatedSeriesEventsPublishConcurrency int           `yaml:"created_series_events_publish_concurrency"`

	SkipSnapshotLoadingAtStartup bool    `yaml:"skip_snapshot_loading_at_startup" category:"experimental"`
	SnapshotIntervalJitter       float64 `yaml:"snapshot_interval_jitter"`
	TargetSnapshotFileSizeBytes  int     `yaml:"target_snapshot_file_size_bytes"`

	SnapshotCleanupInterval       time.Duration `yaml:"snapshot_cleanup_interval"`
	SnapshotCleanupIntervalJitter float64       `yaml:"snapshot_cleanup_interval_jitter"`

	MaxEventsFetchSize int `yaml:"max_events_fetch_size"`

	UserCloseToLimitPercentageThreshold int `yaml:"user_close_to_limit_percentage_threshold"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.BoolVar(&c.Enabled, "usage-tracker.enabled", false, "True to enable the usage-tracker.")

	f.BoolVar(&c.UseGlobalSeriesLimits, "usage-tracker.use-global-series-limits", false, "If true, the usage-tracker service uses global in-memory series limits instead of the active series limits. This is useful for testing purposes only.") // TODO: Remove in Mimir 3.0

	f.IntVar(&c.Partitions, "usage-tracker.partitions", 64, "Number of partitions to use for the usage-tracker. This number isn't expected to change after you're already using the usage-tracker.")
	f.DurationVar(&c.PartitionReconcileInterval, "usage-tracker.partition-reconcile-interval", 10*time.Second, "Interval to reconcile partitions.")
	f.DurationVar(&c.LostPartitionsShutdownGracePeriod, "usage-tracker.lost-partitions-shutdown-grace-period", 30*time.Second, "Time to wait before shutting down a partition handler that is no longer owned by this instance.")
	f.IntVar(&c.MaxPartitionsToCreatePerReconcile, "usage-tracker.max-partitions-to-create-per-reconcile", 1, "Maximum number of partitions to create per reconcile interval. This avoids load avalanches and prevents shuffling when adding new instances.")

	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)

	c.EventsStorageWriter.RegisterFlagsWithPrefix("usage-tracker.events-storage.writer.", f)
	c.EventsStorageReader.RegisterFlagsWithPrefix("usage-tracker.events-storage.reader.", f)

	c.SnapshotsMetadataWriter.RegisterFlagsWithPrefix("usage-tracker.snapshots-metadata.writer.", f)
	c.SnapshotsMetadataReader.RegisterFlagsWithPrefix("usage-tracker.snapshots-metadata.reader.", f)

	c.SnapshotsStorage.RegisterFlagsWithPrefixAndDefaultDirectory("usage-tracker.snapshots-storage.", "usagetrackersnapshots", f)
	c.SnapshotsLoadBackoff.RegisterFlagsWithPrefix("usage-tracker.snapshots-load-backoff", f)

	f.DurationVar(&c.IdleTimeout, "usage-tracker.idle-timeout", 20*time.Minute, "The time after which series are considered idle and not active anymore. Must be greater than 0 and less than 1 hour.")
	f.IntVar(&c.CreatedSeriesEventsMaxPending, "usage-tracker.created-series-events-max-pending", 10_000, "Maximum number of pending created series events waiting to be published.")
	f.IntVar(&c.CreatedSeriesEventsMaxBatchSizeBytes, "usage-tracker.created-series-events-max-batch-size-bytes", 1<<20, "Maximum size of a batch of created series events to be published.")
	f.DurationVar(&c.CreatedSeriesEventsBatchTTL, "usage-tracker.created-series-events-batch-ttl", 250*time.Millisecond, "Time after which a batch of created series events is published even if it's not full.")
	f.IntVar(&c.CreatedSeriesEventsPublishConcurrency, "usage-tracker.created-series-events-publish-concurrency", 10, "Number of concurrent workers publishing created series events.")

	f.Float64Var(&c.SnapshotIntervalJitter, "usage-tracker.snapshot-interval-jitter", 0.1, "Jitter to apply to the snapshot interval. This is a percentage of the snapshot interval, e.g. 0.1 means 10% jitter. It should be between 0 and 1.")
	f.IntVar(&c.TargetSnapshotFileSizeBytes, "usage-tracker.target-snapshot-file-size-bytes", 100*1024*1024, "Target size of a snapshot file in bytes. This is used to determine when to create a new snapshot file. It should be greater than 0.")

	f.BoolVar(&c.SkipSnapshotLoadingAtStartup, "usage-tracker.skip-snapshot-loading-at-startup", false, "If true, the usage-tracker will not load snapshots at startup. This means that the full state will not be reloaded during partition handler startup. Useful to skip corrupted snapshots or for testing purposes only.")
	f.DurationVar(&c.SnapshotCleanupInterval, "usage-tracker.snapshot-cleanup-interval", time.Hour, "Interval to clean up old snapshots.")
	f.Float64Var(&c.SnapshotCleanupIntervalJitter, "usage-tracker.snapshot-cleanup-interval-jitter", 0.25, "Jitter to apply to the snapshot cleanup interval. This is a percentage of the snapshot cleanup interval, e.g. 0.1 means 10% jitter. It should be between 0 and 1.")

	f.IntVar(&c.MaxEventsFetchSize, "usage-tracker.max-events-fetch-size", 100, "Maximum number of events to fetch from Kafka in a single request. This is used to limit the memory usage when fetching events.")

	f.IntVar(&c.UserCloseToLimitPercentageThreshold, "usage-tracker.user-close-to-limit-percentage-threshold", 90, "Percentage of the local series limit after which a user is considered close to the limit. A user is close to the limit if their series count is above this percentage of their local limit.")
}

func (c *Config) ValidateForClient() error {
	// Skip validation if not enabled.
	if !c.Enabled {
		return nil
	}

	return c.validateCommon()
}

func (c *Config) validateCommon() error {
	if !isPowerOfTwo(c.Partitions) {
		return fmt.Errorf("invalid number of partitions %d, must be a power of 2", c.Partitions)
	}

	return nil
}

func (c *Config) ValidateForUsageTracker() error {
	if err := c.validateCommon(); err != nil {
		return err
	}

	if err := c.EventsStorageWriter.Validate(); err != nil {
		return errors.Wrap(err, "Events Kafka writer")
	}
	if err := c.EventsStorageReader.Validate(); err != nil {
		return errors.Wrap(err, "Events Kafka reader")
	}

	if err := c.SnapshotsMetadataWriter.Validate(); err != nil {
		return errors.Wrap(err, "Snapshots metadata Kafka writer")
	}
	if err := c.SnapshotsMetadataReader.Validate(); err != nil {
		return errors.Wrap(err, "Snapshots metadata Kafka reader")
	}

	if c.EventsStorageWriter.AutoCreateTopicEnabled && c.EventsStorageWriter.AutoCreateTopicDefaultPartitions < c.Partitions {
		return fmt.Errorf("number of configured partitions for events storage %d must be less or equal than the default number of partitions to be auto created %d for the Kafka topic", c.Partitions, c.EventsStorageWriter.AutoCreateTopicDefaultPartitions)
	}
	if c.SnapshotsMetadataWriter.AutoCreateTopicEnabled && c.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions < c.Partitions {
		return fmt.Errorf("number of configured partitions for snapshots metadata %d must be less or equal than the default number of partitions to be auto created %d for the Kafka topic", c.Partitions, c.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions)
	}

	if err := c.SnapshotsStorage.Validate(); err != nil {
		return err
	}

	if c.IdleTimeout <= 0 || c.IdleTimeout > time.Hour {
		return fmt.Errorf("invalid usage-tracker idle timeout %q, should be greater than 0 and less than 1 hour", c.IdleTimeout)
	}

	if c.SnapshotIntervalJitter < 0 || c.SnapshotIntervalJitter > 1 {
		return fmt.Errorf("invalid usage-tracker snapshot interval jitter %f, should be between 0 and 1", c.SnapshotIntervalJitter)
	}

	return nil
}

type UsageTracker struct {
	services.Service

	instanceID int32

	cfg        Config
	overrides  *validation.Overrides
	logger     log.Logger
	registerer prometheus.Registerer

	// Partition and instance ring.
	partitionKVClient  kv.Client
	instanceRing       *ring.Ring
	partitionRing      *ring.MultiPartitionInstanceRing
	instanceLifecycler *ring.BasicLifecycler

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client

	// Snapshots metadata storage (Kafka).
	snapshotsMetadataKafkaWriter *kgo.Client
	snapshotsBucket              objstore.InstrumentedBucket

	partitionsMtx sync.RWMutex
	partitions    map[int32]*partitionHandler

	lostPartitions map[int32]time.Time

	// Dependencies.
	subservicesWatcher *services.FailureWatcher

	// Metrics
	snapshotCleanupsTotal         prometheus.Counter
	snapshotCleanupsFailed        prometheus.Counter
	snapshotCleanupsFailedFiles   prometheus.Counter
	snapshotCleanupsDeletedFiles  prometheus.Counter
	snapshotsRemainingInTheBucket prometheus.Gauge
}

func NewUsageTracker(cfg Config, instanceRing *ring.Ring, partitionRing *ring.MultiPartitionInstanceRing, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		cfg:           cfg,
		instanceRing:  instanceRing,
		partitionRing: partitionRing,
		overrides:     overrides,
		logger:        logger,
		registerer:    registerer,

		partitions:     make(map[int32]*partitionHandler, 64),
		lostPartitions: make(map[int32]time.Time),
	}

	// Init instance ring lifecycler.
	var err error
	t.instanceID, err = parseInstanceID(t.cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "parsing instance ID")
	}

	if int(t.instanceID) >= t.cfg.Partitions {
		return nil, errors.Wrap(err, "instance ID is greater than the number of partitions")
	}

	t.instanceLifecycler, err = NewInstanceRingLifecycler(cfg.InstanceRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Init the partition ring lifecycler.
	t.partitionKVClient, err = NewPartitionRingKVClient(cfg.PartitionRing, "lifecycler", logger, registerer)
	if err != nil {
		return nil, err
	}

	bkt, err := bucket.NewClient(context.Background(), cfg.SnapshotsStorage, "usage-tracker-snapshots", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create usage-tracker snapshots storage client")
	}
	t.snapshotsBucket = bkt

	// Create Kafka writer for events storage.
	if t.cfg.EventsStorageWriter.AutoCreateTopicEnabled {
		if err := ingest.CreateTopic(t.cfg.EventsStorageWriter, t.logger); err != nil {
			return nil, errors.Wrap(err, "failed to create Kafka topic for usage-tracker events")
		}
	}

	eventsWriterReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, prometheus.WrapRegistererWith(prometheus.Labels{"component": eventsKafkaWriterComponent}, t.registerer))
	t.eventsKafkaWriter, err = ingest.NewKafkaWriterClient(t.cfg.EventsStorageWriter, 20, t.logger, eventsWriterReg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka writer client for usage-tracker")
	}

	// Create Kafka writer for snapshots metadata storage.
	if t.cfg.SnapshotsMetadataWriter.AutoCreateTopicEnabled {
		if err := ingest.CreateTopic(t.cfg.SnapshotsMetadataWriter, t.logger); err != nil {
			return nil, errors.Wrap(err, "failed to create Kafka topic for usage-tracker snapshots metadata")
		}
	}
	snapshotsWriterReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, prometheus.WrapRegistererWith(prometheus.Labels{"component": snapshotsKafkaWriterComponent}, t.registerer))
	t.snapshotsMetadataKafkaWriter, err = ingest.NewKafkaWriterClient(t.cfg.SnapshotsMetadataWriter, 20, t.logger, snapshotsWriterReg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka writer client for usage-tracker snapshots metadata")
	}

	// Only instance 0 performs cleanups of the snapshots bucket, it doesn't make sense to export the metric on the rest of the instances.
	if t.shouldPerformCleanups() {
		t.snapshotCleanupsTotal = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_cleanups_total",
			Help: "Total number of performed snapshots cleanups.",
		})
		t.snapshotCleanupsFailed = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_cleanups_failed_total",
			Help: "Total number of snapshots cleanups that failed.",
		})
		t.snapshotCleanupsFailedFiles = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_cleanups_failed_files_total",
			Help: "Total number of files that failed to be deleted during snapshots cleanup.",
		})
		t.snapshotCleanupsDeletedFiles = promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_cleanups_deleted_files_total",
			Help: "Total number of snapshots deleted during snapshots cleanup.",
		})
		t.snapshotsRemainingInTheBucket = promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_usage_tracker_snapshots_remaining_in_the_bucket",
			Help: "Number of snapshots remaining in the bucket.",
		})
		t.snapshotsRemainingInTheBucket.Set(-1) // We don't know yet.
	}

	t.Service = services.NewBasicService(t.start, t.run, t.stop)
	return t, nil
}

func (t *UsageTracker) shouldPerformCleanups() bool {
	return t.instanceID == 0
}

// start implements services.StartingFn.
func (t *UsageTracker) start(ctx context.Context) error {
	// Start dependencies.
	t.subservicesWatcher = services.NewFailureWatcher()
	t.subservicesWatcher.WatchService(t.instanceLifecycler)
	if err := services.StartAndAwaitRunning(ctx, t.instanceLifecycler); err != nil {
		return errors.Wrap(err, "unable to start instance lifecycler")
	}

	return nil
}

// run implements services.RunningFn.
func (t *UsageTracker) run(ctx context.Context) error {
	// TODO: check here if all partitions already have owners, in which case we should reconcilePartitions immediately (no need to wait).
	// If there are no owners yet, this is a cold start, so wait until all instances have joined the ring to avoid re-shuffling.

	var snapshotsCleanupTickerChan <-chan time.Time
	if t.shouldPerformCleanups() {
		ticker := time.NewTicker(util.DurationWithJitter(t.cfg.SnapshotCleanupInterval, t.cfg.SnapshotCleanupIntervalJitter))
		defer ticker.Stop()
		snapshotsCleanupTickerChan = ticker.C
	}

	for {
		select {
		case <-time.After(t.cfg.PartitionReconcileInterval):
			if err := t.reconcilePartitions(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
		case <-ctx.Done():
			return nil
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		case <-snapshotsCleanupTickerChan:
			if err := t.cleanupSnapshots(ctx); err != nil {
				level.Error(t.logger).Log("msg", "failed to clean up old snapshots", "err", err)
			}
		}
	}
}

func (t *UsageTracker) reconcilePartitions(ctx context.Context) error {
	// We're going to hold the read lock for the whole function, as we're the only function that can write (we assume stop() can't be called because we're still running)
	// We'll take the write mutex when we want to modify t.partitions.
	t.partitionsMtx.RLock()
	defer t.partitionsMtx.RUnlock()
	// locked will run f while write-locked
	locked := func(f func()) {
		t.partitionsMtx.RUnlock()
		t.partitionsMtx.Lock()
		f()
		t.partitionsMtx.Unlock()
		t.partitionsMtx.RLock()
	}

	logger := log.With(t.logger, "reconciliation_time", time.Now().UTC().Truncate(time.Second).Format(time.RFC3339))
	start, end, totalInstances := t.instancePartitions()

	if err := t.removeStalePartitionOwnership(ctx, start, end); err != nil {
		level.Error(logger).Log("msg", "unable to remove stale partition ownership, cannot reconcile", "err", err)
		return err
	}

	current := int32(len(t.partitions))

	level.Debug(logger).Log("msg", "serving partitions", "current", current, "new", end-start, "start", start, "end", end, "instance", t.instanceID, "total_instances", totalInstances, "partitions", t.cfg.Partitions)

	unmarkedAsLost := 0
	for p, lostAt := range t.lostPartitions {
		// We don't check whether p>=start because that should always be true: we never lose starting partitions.
		if p < end {
			level.Info(logger).Log("msg", "partition was previously lost but we own it again", "partition", p, "lost_at", lostAt)
			delete(t.lostPartitions, p)
			unmarkedAsLost++
		}
	}

	// Stop the partitions that are not needed anymore.
	// The only way to lose partitions is for a higher instance ID to take them.
	// We should wait until the new instance has taken each one of the partitions before we delete them,
	// and then we should wait a grace period to make sure distributors also know about the new ownership information.
	// Okay, so how do we figure out that the new instance has taken the partitions?
	// We'll get the replication set just like the client would do, and we check whether we still belong to it.
	deleted := 0
	markedAsLost := 0
losingPartitions:
	for pid, p := range t.partitions {
		if pid < end {
			continue
		}

		logger := log.With(logger, "action", "losing", "partition", pid)
		if lostAt, ok := t.lostPartitions[pid]; ok {
			// We have already lost this partition, check whether grace period has expired already.
			if gracePeriodRemaining := t.cfg.LostPartitionsShutdownGracePeriod - time.Since(lostAt); gracePeriodRemaining > 0 {
				level.Info(logger).Log("msg", "partition already marked as lost, still waiting grace period", "lost_at", lostAt, "grace_period_remaining", gracePeriodRemaining)
				continue
			}
			level.Info(logger).Log("msg", "partition already marked as lost, grace period expired, shutting down")

			// Delete it first, so it doesn't receive any requests while we're shutting it down.
			locked(func() { delete(t.partitions, pid) })
			delete(t.lostPartitions, pid)
			p.setRemoveOwnerOnShutdown(true)
			if err := services.StopAndAwaitTerminated(context.Background(), p); err != nil {
				level.Error(logger).Log("msg", "unable to stop partition handler", "err", err)
				return errors.Wrapf(err, "unable to stop partition handler %d", pid)
			}
			deleted++
			level.Info(logger).Log("msg", "partition handler stopped")
			continue
		}

		replicationSet, err := t.partitionRing.GetReplicationSetForPartitionAndOperation(pid, usagetrackerclient.TrackSeriesOp)
		if err != nil {
			level.Error(logger).Log("msg", "unable to get replication set for partition", "err", err)
			continue
		}
		for _, instance := range replicationSet.Instances {
			if instance.Id == t.cfg.InstanceRing.InstanceID {
				level.Info(logger).Log("msg", "we're still serving partition, not shutting down")
				continue losingPartitions
			}
		}

		level.Info(logger).Log("msg", "partition has a higher owner now, marking as lost and waiting grace period before shutting down")
		t.lostPartitions[pid] = time.Now()
		markedAsLost++
	}

	added := 0
	skipped := 0
	started := make(chan error, max(0, end-start))
	for pid := start; pid < end; pid++ {
		if _, ok := t.partitions[pid]; ok {
			// We already have this partition.
			continue
		}
		if added >= t.cfg.MaxPartitionsToCreatePerReconcile {
			skipped++
			continue
		}

		logger := log.With(logger, "action", "adding", "partition", pid)

		level.Info(logger).Log("msg", "creating new partition handler")
		p, err := newPartitionHandler(pid, t.cfg, t.partitionKVClient, t.eventsKafkaWriter, t.snapshotsMetadataKafkaWriter, t.snapshotsBucket, t, t.logger, t.registerer)
		if err != nil {
			return errors.Wrapf(err, "unable to create partition handler %d", pid)
		}
		level.Info(logger).Log("msg", "starting partition handler")
		if err := p.StartAsync(ctx); err != nil {
			return errors.Wrapf(err, "unable to start partition handler %d ", pid)
		}
		go func() {
			if err := p.AwaitRunning(ctx); err != nil {
				started <- errors.Wrapf(err, "unable to run partition handler %d", pid)
			} else {
				started <- nil
			}
		}()

		locked(func() { t.partitions[pid] = p })
		level.Info(logger).Log("msg", "partition handler started")
		added++
	}

	if added > 0 {
		level.Info(logger).Log("msg", "waiting for partitions to start", "added", added)

		// Wait until all partitions have started.
		for range added {
			select {
			case err := <-started:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if skipped > 0 {
		level.Info(logger).Log("msg", "max partitions to create reached, skipping the rest until next reconcile", "added", added, "skipped", skipped)
	} else if t.instanceLifecycler.GetState() == ring.JOINING {
		level.Info(logger).Log("msg", "all partitions created, becoming ACTIVE")
		if err := t.instanceLifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
			return errors.Wrap(err, "unable to change instance lifecycler state to ACTIVE")
		}
	}

	logLevel := level.Debug
	if added+skipped+markedAsLost+unmarkedAsLost+deleted > 0 {
		// Only log at info level if something happened.
		logLevel = level.Info
	}

	logLevel(logger).Log(
		"msg", "end of partition reconciliation",
		"start", start, "end", end,
		"instance", t.instanceID, "total_instances", totalInstances, "partitions", t.cfg.Partitions,
		"added", added, "skipped", skipped,
		"marked_as_lost", markedAsLost, "deleted", deleted,
		"unmarked_as_lost", unmarkedAsLost,
	)
	return nil
}

func (t *UsageTracker) removeStalePartitionOwnership(ctx context.Context, start, end int32) error {
	partitionRing := t.partitionRing.PartitionRing()
	partitionRingEditor := ring.NewPartitionRingEditor(PartitionRingKey, t.partitionKVClient)

	var partitionOwners []string
	for p := int32(0); p < int32(t.cfg.Partitions); p++ {
		if p >= start && p < end {
			// We (are supposed to) own this one, no need to remove it.
			continue
		}
		// We are not supposed to own this partition.
		if _, ok := t.partitions[p]; ok {
			// We have a lifecycler running for this partition, it will take care of removing the ownership.
			continue
		}

		partitionOwners = partitionRing.MultiPartitionOwnerIDs(p, partitionOwners[:0])
		if !slices.Contains(partitionOwners, t.cfg.InstanceRing.InstanceID) {
			// We are not the owner of this partition, all ok.
			continue
		}

		level.Warn(t.logger).Log("msg", "removing stale partition ownership", "partition", p, "instance_id", t.cfg.InstanceRing.InstanceID)
		err := partitionRingEditor.RemoveMultiPartitionOwner(ctx, t.cfg.InstanceRing.InstanceID, p)
		if err != nil {
			return errors.Wrapf(err, "unable to remove stale partition ownership for partition %d", p)
		}
	}

	return nil
}

// PrepareInstanceRingDownscaleHandler prepares the instance ring entry for downscaling.
// It can mark usage-tracker as read-only or set it back to read-write mode.
//
// Note that a usage-tracker in read-only mode doesn't mean it's in read-only,
// it's just a signal for other instances to take ownership of its partitions.
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when instance ring entry was switched to read-only mode, or 0, if ring entry is not in read-only mode.
//
//   - POST
//     Switches the instance ring entry to read-only mode (if it isn't yet), and returns the timestamp when the switch to
//     read-only mode happened.
//
//   - DELETE
//     Sets ingester ring entry back to read-write mode.
func (t *UsageTracker) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if t.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Calling this repeatedly doesn't update the read-only timestamp, if instance is already in read-only mode.
		err := t.instanceLifecycler.ChangeReadOnlyState(r.Context(), true)
		if err != nil {
			level.Error(t.logger).Log("msg", "failed to set ingester to read-only mode in the ring", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		t.instanceLifecycler.SetKeepInstanceInTheRingOnShutdown(false)
		t.setAllPartitionsToRemoveOwnerOnShutdown(true)

	case http.MethodDelete:
		// Clear the read-only status.
		err := t.instanceLifecycler.ChangeReadOnlyState(r.Context(), false)
		if err != nil {
			level.Error(t.logger).Log("msg", "failed to clear ingester's read-only mode", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		t.instanceLifecycler.SetKeepInstanceInTheRingOnShutdown(true)
		t.setAllPartitionsToRemoveOwnerOnShutdown(false)
	}

	ro, rots := t.instanceLifecycler.GetReadOnlyState()
	if ro {
		util.WriteJSONResponse(w, map[string]any{"timestamp": rots.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}

func (t *UsageTracker) setAllPartitionsToRemoveOwnerOnShutdown(removeOwnerOnShutdown bool) {
	t.partitionsMtx.RLock()
	defer t.partitionsMtx.RUnlock()
	for _, p := range t.partitions {
		p.setRemoveOwnerOnShutdown(removeOwnerOnShutdown)
	}
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	errs := multierror.New()
	// Stop dependencies.
	t.partitionsMtx.RLock()
	defer t.partitionsMtx.RUnlock()
	for _, p := range t.partitions {
		p.StopAsync()
	}
	for _, p := range t.partitions {
		if err := p.AwaitTerminated(context.Background()); err != nil {
			errs.Add(errors.Wrapf(err, "unable to stop partition handler %d", p.partitionID))
		}
	}

	if err := services.StopAndAwaitTerminated(context.Background(), t.instanceLifecycler); err != nil {
		errs.Add(errors.Wrap(err, "unable to stop instance lifecycler"))
	}

	// Close Kafka clients.
	t.eventsKafkaWriter.Close()
	t.snapshotsMetadataKafkaWriter.Close()

	return errs.Err()
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	partition := req.Partition
	p, err := t.runningPartition(partition)
	if err != nil {
		return nil, err
	}

	rejected, err := p.store.trackSeries(context.Background(), req.UserID, req.SeriesHashes, time.Now())
	if err != nil {
		return nil, err
	}
	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

// GetUsersCloseToLimit implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) GetUsersCloseToLimit(_ context.Context, req *usagetrackerpb.GetUsersCloseToLimitRequest) (*usagetrackerpb.GetUsersCloseToLimitResponse, error) {
	partition := req.Partition
	p, err := t.runningPartition(partition)
	if err != nil {
		return nil, err
	}

	userIDs := p.store.getSortedUsersCloseToLimit()
	return &usagetrackerpb.GetUsersCloseToLimitResponse{
		SortedUserIds: userIDs,
		Partition:     partition,
	}, nil
}

func (t *UsageTracker) runningPartition(partition int32) (*partitionHandler, error) {
	t.partitionsMtx.RLock()
	p, ok := t.partitions[partition]
	t.partitionsMtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("partition handler %d not found", partition)
	}
	if p.State() != services.Running {
		return nil, fmt.Errorf("partition handler %d is not running (state: %s)", partition, p.State())
	}
	return p, nil
}

// CheckReady performs a readiness check.
// An instance is ready when it has instantiated all the partitions that should belong to it according to the ring.
func (t *UsageTracker) CheckReady(_ context.Context) error {
	if t.instanceLifecycler.GetState() != ring.ACTIVE {
		return fmt.Errorf("instance is not active, current state: %s", t.instanceLifecycler.GetState())
	}
	return nil
}

func (t *UsageTracker) instancePartitions() (start, end, totalInstances int32) {
	totalInstances = t.instancesServingPartitions()
	if readOnly, _ := t.instanceLifecycler.GetReadOnlyState(); readOnly {
		// If we're in read-only mode, we don't serve any partitions.
		// We're going to gradually lose the ones we still own.
		return 0, -1, totalInstances
	}
	if t.instanceID >= totalInstances {
		// We're usage-tracker-zone-x-N but there are M < N+1 replicas in the ring.
		// Let's just assume they'll start later instead of failing here.
		totalInstances = t.instanceID + 1
	}

	start, end = instancePartitions(t.instanceID, totalInstances, int32(t.cfg.Partitions))
	return start, end, totalInstances
}

// instancesServingPartitions returns the number of instances that are responsible for serving partitions.
func (t *UsageTracker) instancesServingPartitions() int32 {
	// This is the number of active instances that are not read-only, i.e. WriteableInstances in this zone.
	return int32(t.instanceRing.WritableInstancesWithTokensInZoneCount(t.cfg.InstanceRing.InstanceZone))
}

func (t *UsageTracker) localSeriesLimit(userID string) uint64 {
	var globalLimit int
	if t.cfg.UseGlobalSeriesLimits {
		globalLimit = t.overrides.MaxGlobalSeriesPerUser(userID)
	} else {
		globalLimit = t.overrides.MaxActiveSeriesPerUser(userID)
	}
	if globalLimit <= 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return uint64(float64(globalLimit) / float64(t.partitionRing.PartitionRing().ActivePartitionsCount()))
}

func (t *UsageTracker) zonesCount() uint64 {
	return uint64(t.instanceRing.ZonesCount())
}

type chanEventsPublisher struct {
	logger log.Logger
	events chan []byte
}

func (p chanEventsPublisher) publishCreatedSeries(ctx context.Context, userID string, series []uint64, timestamp time.Time) error {
	defer refsPool.Put(series)

	ev := usagetrackerpb.SeriesCreatedEvent{
		UserID:       userID,
		Timestamp:    timestamp.Unix(),
		SeriesHashes: series,
	}
	data, err := proto.Marshal(&ev)
	if err != nil {
		return errors.Wrap(err, "marshaling series created event")
	}
	select {
	case p.events <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// instancePartitions returns the the interval [start, end) of partitions that the instance is responsible for.
func instancePartitions(instance, instances, partitions int32) (start, end int32) {
	if instance >= instances {
		panic(fmt.Errorf("instance %d >= instances %d", instance, instances))
	}
	start = instancePartitionsStart(instance, partitions)
	end = partitions
	// I couldn't find a general formula for the end, so we just check the instances that steal ranges from us and find the one that starts earlier (if any).
	for i := int32(0); i < instances; i++ {
		if thatInstanceStart := instancePartitionsStart(i, partitions); thatInstanceStart > start && thatInstanceStart < end {
			end = thatInstanceStart
		}
	}
	return start, end
}

func instancePartitionsStart(instance, partitions int32) (start int32) {
	// This is how 16 partitions are distributed across instanceIDs as the number of totalInstances grows:
	//
	//  0   1   2   3   4   5   6   7   8   9   0   1   2   3   4   5
	//                                  0
	//  [==============================================================)
	//                  0               |               1
	//  [==============================)[==============================)
	//          0       |       2       |               1
	//  [==============)[==============)[==============================)
	//          0       |       2       |       1       |	    3
	//  [==============)[==============)[==============)[==============)
	//      0   |   4   |       2       |       1       |	    3
	//  [======)[======)[==============)[==============================)
	//      0   |   4   |   2   |   5   |       1       |	    3
	//  [======)[======)[======)[======)[==============================)
	//      0   |   4   |   2   |   5   |   1   |   6   |	    3
	//  [======)[======)[======)[======)[======)[======================)
	//      0   |   4   |   2   |   5   |   1   |   6   |   3   |   7
	//  [======)[======)[======)[======)[======)[======)[======)[======)
	//  etc.
	//
	// How to build this:
	// Each instance starts at a shift which is half step plus the amount of steps since last power of two:
	//  0   1   2   3   4   5   6   7   8   9   0   1   2   3   4   5
	//                                  0
	//  [==============================================================)
	//
	//  > We want to add 1,
	//  1 is a power of two, so we divide the initial step of partitions*2 by 2,
	//  step=partitions=16, shift=step/2=8
	//  {-------------shift-------------} + 0 steps
	//                  0               |               1
	//  [==============================)[==============================)
	//
	//  > We want to add 2,
	//  2 is a power of two, so we divide step by 2:
	//  step=partitions/2=8, shift=step/4=4
	//  {-----shift-----} + 0 steps
	//          0       |       2       |               1
	//  [==============)[==============)[==============================)
	//
	//  > We want to add 3,
	//  3 is not a power of two so we don't update shift/step:
	//  {-----shift-----}{---------- 1 step ------------}
	//          0       |       2       |       1       |	    3
	//  [==============)[==============)[==============)[==============)
	//  etc.
	//
	// Let's now generate the code that does this.
	// First we go with the start, this doesn't even depend on totalInstances:
	if instance == 0 {
		// Instance 0 is a special case in the formula, as we can't take log2(0)
		// It always starts at 0.
		start = 0
	} else {
		// For the rest, the start can be deduced from the example above.
		previousPowerOf2 := int32(1) << log2(instance)
		step := partitions / previousPowerOf2
		shift := step / 2
		start = shift + (instance-previousPowerOf2)*step
	}

	return start
}

func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}

// log2 calculates a simple int log2 of n, without doing any kind of bit hacks
func log2(n int32) int32 {
	var log int32
	for n > 1 {
		n >>= 1
		log++
	}
	return log
}

// parseInstanceID returns the instance ID number from the instance ID string.
func parseInstanceID(instanceID string) (int32, error) {
	match := instanceIDRegexp.FindStringSubmatch(instanceID)
	if len(match) == 0 {
		return 0, fmt.Errorf("instance ID %s doesn't match regular expression %q", instanceID, instanceIDRegexp.String())
	}

	// Parse the instance sequence number.
	seq, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("no sequence number in instance ID %s", instanceID)
	}

	return int32(seq), nil //nolint:gosec
}

func snapshotFilename(time time.Time, instanceID string, partitionID int32) string {
	return fmt.Sprintf("snapshot-%d-p%d-%s.bin", time.UnixMilli(), partitionID, instanceID)
}

var snapshotRegexp = regexp.MustCompile(`^snapshot-(\d+)-p(\d+)-([a-zA-Z0-9_-]+)\.bin$`)

func (t *UsageTracker) cleanupSnapshots(ctx context.Context) error {
	const (
		snapshotRegexpGroupAll = iota
		snapshotRegexpGroupTimestamp
		snapshotRegexpGroupPartitionID
		snapshotRegexpGroupInstanceID
	)

	defer t.snapshotCleanupsTotal.Inc()
	watermark := time.Now().Add(-t.cfg.IdleTimeout * snapshotCleanupIdleTimeoutFactor)

	var deleted, remaining int
	t0 := time.Now()
	err := t.snapshotsBucket.Iter(ctx, "", func(name string) error {
		// Use snapshotRegexp to extract the timestamp from the name.
		matches := snapshotRegexp.FindStringSubmatch(name)
		if len(matches) == 0 {
			level.Error(t.logger).Log("msg", "skipping snapshot file with unexpected name", "name", name)
			t.snapshotCleanupsFailedFiles.Inc()
			remaining++
			return nil // Continue the cleanup.
		}
		timestampMillis, err := strconv.ParseInt(matches[snapshotRegexpGroupTimestamp], 10, 64)
		if err != nil {
			t.snapshotCleanupsFailedFiles.Inc()
			remaining++
			return fmt.Errorf("parsing timestamp from snapshot file name %s: %w", name, err)
		}
		snapshotTime := time.UnixMilli(timestampMillis)
		if snapshotTime.After(watermark) {
			remaining++
			return nil
		}

		// This snapshot is older than the watermark, delete it.
		level.Info(t.logger).Log("msg", "deleting old snapshot file", "name", name, "timestamp", snapshotTime)
		if err := t.snapshotsBucket.Delete(ctx, name); err != nil {
			t.snapshotCleanupsFailedFiles.Inc()
			remaining++
			return fmt.Errorf("deleting old snapshot file %s: %w", name, err)
		}
		t.snapshotCleanupsDeletedFiles.Inc()
		return nil
	})
	t.snapshotsRemainingInTheBucket.Set(float64(remaining))

	if err != nil {
		t.snapshotCleanupsFailed.Inc()
		return errors.Wrap(err, "iterating over snapshot files for deletion")
	}

	level.Info(t.logger).Log("msg", "snapshot files cleanup completed", "deleted", deleted, "duration", time.Since(t0))
	return nil
}
