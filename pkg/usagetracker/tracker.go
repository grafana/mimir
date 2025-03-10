// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

	eventsKafkaWriterMetricsPrefix = "cortex_usage_tracker_events_writer"
	eventsKafkaReaderMetricsPrefix = "cortex_usage_tracker_events_reader"
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	Partitions                        int           `yaml:"partitions"`
	PartitionReconcileInterval        time.Duration `yaml:"partition_reconcile_interval"`
	LostPartitionsShutdownGracePeriod time.Duration `yaml:"lost_partitions_shutdown_grace_period"`
	MaxPartitionsToCreatePerReconcile int           `yaml:"max_partitions_to_create_per_reconcile"`

	InstanceRing  InstanceRingConfig  `yaml:"instance_ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`

	EventsStorage    EventsStorageConfig `yaml:"events_storage"`
	SnapshotsStorage bucket.Config       `yaml:"snapshots_storage"`

	IdleTimeout                           time.Duration `yaml:"idle_timeout"`
	CreatedSeriesEventsMaxPending         int           `yaml:"max_pending_created_series_events"`
	CreatedSeriesEventsMaxBatchSizeBytes  int           `yaml:"created_series_events_max_batch_size_bytes"`
	CreatedSeriesEventsBatchTTL           time.Duration `yaml:"created_series_events_batch_ttl"`
	CreatedSeriesEventsPublishConcurrency int           `yaml:"created_series_events_publish_concurrency"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.BoolVar(&c.Enabled, "usage-tracker.enabled", false, "True to enable the usage-tracker.")

	f.IntVar(&c.Partitions, "usage-tracker.partitions", 64, "Number of partitions to use for the usage-tracker. This number isn't expected to change once usage-tracker is already being used.")
	f.DurationVar(&c.PartitionReconcileInterval, "usage-tracker.partition-reconcile-interval", 10*time.Second, "Interval to reconcile partitions.")
	f.DurationVar(&c.LostPartitionsShutdownGracePeriod, "usage-tracker.lost-partitions-shutdown-grace-period", 30*time.Second, "Time to wait beforeshutting down a partition that is no longer owned by this instance.")
	f.IntVar(&c.MaxPartitionsToCreatePerReconcile, "usage-tracker.max-partitions-to-create-per-reconcile", 1, "Maximum number of partitions to create per reconcile interval. This avoids load avalanches and prevents shuffling when adding new instances.")

	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
	c.EventsStorage.RegisterFlags(f)
	c.SnapshotsStorage.RegisterFlagsWithPrefixAndDefaultDirectory("usage-tracker.snapshot-storage.", "usagetrackersnapshots", f)

	f.DurationVar(&c.IdleTimeout, "usage-tracker.idle-timeout", 20*time.Minute, "The time after which series are considered idle and not active anymore. Must be greater than 0 and less than 1 hour.")
	f.IntVar(&c.CreatedSeriesEventsMaxPending, "usage-tracker.created-series-events-max-pending", 10_000, "Maximum number of pending created series events waiting to be published.")
	f.IntVar(&c.CreatedSeriesEventsMaxBatchSizeBytes, "usage-tracker.created-series-events-max-batch-size-bytes", 1<<20, "Maximum size of a batch of created series events to be published.")
	f.DurationVar(&c.CreatedSeriesEventsBatchTTL, "usage-tracker.created-series-events-batch-ttl", 250*time.Millisecond, "Time after which a batch of created series events is published even if it's not full.")
	f.IntVar(&c.CreatedSeriesEventsPublishConcurrency, "usage-tracker.created-series-events-publish-concurrency", 10, "Number of concurrent workers publishing created series events.")
}

func (c *Config) Validate() error {
	// Skip validation if not enabled.
	if !c.Enabled {
		return nil
	}

	if !isPowerOfTwo(c.Partitions) {
		return fmt.Errorf("invalid number of partitions %d, must be a power of 2", c.Partitions)
	}

	if err := c.EventsStorage.Validate(); err != nil {
		return err
	}
	if err := c.SnapshotsStorage.Validate(); err != nil {
		return err
	}
	if c.IdleTimeout <= 0 || c.IdleTimeout > time.Hour {
		return fmt.Errorf("invalid usage-tracker idle timeout %q, should be greater than 0 and less than 1 hour", c.IdleTimeout)
	}
	if c.EventsStorage.Writer.AutoCreateTopicEnabled && c.EventsStorage.Writer.AutoCreateTopicDefaultPartitions < c.Partitions {
		return fmt.Errorf("number of configured partitions %d must be less or equal than the default number of partitions to be auto created %d for the Kafka topic", c.Partitions, c.EventsStorage.Writer.AutoCreateTopicDefaultPartitions)
	}

	return nil
}

type UsageTracker struct {
	services.Service

	instanceID int32

	cfg        Config
	bucket     objstore.InstrumentedBucket
	overrides  *validation.Overrides
	logger     log.Logger
	registerer prometheus.Registerer

	// Partition and instance ring.
	partitionKVClient  kv.Client
	instanceRing       *ring.Ring
	partitionRing      *ring.PartitionInstanceRing
	instanceLifecycler *ring.BasicLifecycler

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client

	mtx        sync.RWMutex
	partitions map[int32]*partition

	lostPartitions map[int32]time.Time

	// Dependencies.
	subservicesWatcher *services.FailureWatcher
}

func NewUsageTracker(cfg Config, instanceRing *ring.Ring, partitionRing *ring.PartitionInstanceRing, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		cfg:           cfg,
		instanceRing:  instanceRing,
		partitionRing: partitionRing,
		overrides:     overrides,
		logger:        logger,
		registerer:    registerer,

		partitions:     make(map[int32]*partition, 64),
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
	t.bucket = bkt

	// Create Kafka writer for events storage.
	if t.cfg.EventsStorage.Writer.AutoCreateTopicEnabled {
		if err := ingest.CreateTopic(t.cfg.EventsStorage.Writer, t.logger); err != nil {
			return nil, errors.Wrap(err, "failed to create Kafka topic for usage-tracker events")
		}
	}
	t.eventsKafkaWriter, err = ingest.NewKafkaWriterClient(t.cfg.EventsStorage.Writer, 20, t.logger, prometheus.WrapRegistererWithPrefix(eventsKafkaWriterMetricsPrefix, t.registerer))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka writer client for usage-tracker")
	}

	t.Service = services.NewBasicService(t.start, t.run, t.stop)
	return t, nil
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

func (t *UsageTracker) run(ctx context.Context) error {
	// TODO: check here if all partitions already have owners, in which case we should reconcilePartitions immediately (no need to wait).
	// If there are no owners yet, this is a cold start, so wait until all instances have joined the ring to avoid re-shuffling.
	for {
		select {
		case <-time.After(t.cfg.PartitionReconcileInterval):
			if err := t.reconcilePartitions(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

func (t *UsageTracker) reconcilePartitions(ctx context.Context) error {
	// We're going to hold the read lock for the whole function, as we're the only function that can write (we assume stop() can't be called because we're still running)
	// We'll take the write mutex when we want to modify t.partitions.
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	// locked will run f while write-locked
	locked := func(f func()) {
		t.mtx.RUnlock()
		t.mtx.Lock()
		f()
		t.mtx.Unlock()
		t.mtx.RLock()
	}

	logger := log.With(t.logger, "reconciliation_time", time.Now().UTC().Truncate(time.Second).Format(time.RFC3339))
	totalInstances := t.instancesServingPartitions()
	start, end, err := instancePartitions(t.instanceID, totalInstances, int32(t.cfg.Partitions))
	if err != nil {
		level.Error(logger).Log("msg", "unable to calculate partitions, skipping reconcile", "err", err)
		return nil
	}
	current := int32(len(t.partitions))

	level.Info(logger).Log("msg", "serving partitions", "current", current, "new", end-start, "start", start, "end", end, "instance", t.instanceID, "total_instances", totalInstances, "partitions", t.cfg.Partitions)

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
			if err := services.StopAndAwaitTerminated(context.Background(), p); err != nil {
				level.Error(logger).Log("msg", "unable to stop partition", "err", err)
				return errors.Wrapf(err, "unable to stop partition %d", pid)
			}
			deleted++
			level.Info(logger).Log("msg", "partition stopped")
			continue
		}

		replicationSet, err := t.partitionRing.GetReplicationSetForPartitionAndOperation(pid, usagetrackerclient.TrackSeriesOp, true)
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

		level.Info(logger).Log("msg", "creating new partition")
		p, err := newPartition(pid, t.cfg, t.partitionKVClient, t.partitionRing, t.eventsKafkaWriter, t.bucket, t, t.logger, t.registerer)
		if err != nil {
			return errors.Wrap(err, "unable to create partition")
		}
		level.Info(logger).Log("msg", "starting partition")
		if err := services.StartAndAwaitRunning(ctx, p); err != nil {
			return errors.Wrap(err, "unable to start partition")
		}
		locked(func() { t.partitions[pid] = p })
		level.Info(logger).Log("msg", "partition started")
		added++
	}
	if skipped > 0 {
		level.Info(logger).Log("msg", "max partitions to create reached, skipping the rest until next reconcile", "added", added, "skipped", skipped)
	}

	level.Info(logger).Log(
		"msg", "partitions reconciled",
		"start", start, "end", end,
		"instance", t.instanceID, "total_instances", totalInstances, "partitions", t.cfg.Partitions,
		"added", added, "skipped", skipped,
		"marked_as_lost", markedAsLost, "deleted", deleted,
		"unmarked_as_lost", unmarkedAsLost,
	)
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
func (i *UsageTracker) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if i.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Calling this repeatedly doesn't update the read-only timestamp, if instance is already in read-only mode.
		err := i.instanceLifecycler.ChangeReadOnlyState(r.Context(), true)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to set ingester to read-only mode in the ring", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case http.MethodDelete:
		// Clear the read-only status.
		err := i.instanceLifecycler.ChangeReadOnlyState(r.Context(), false)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to clear ingester's read-only mode", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ro, rots := i.instanceLifecycler.GetReadOnlyState()
	if ro {
		util.WriteJSONResponse(w, map[string]any{"timestamp": rots.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	errs := multierror.New()
	// Stop dependencies.
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	for _, p := range t.partitions {
		p.StopAsync()
	}
	for _, p := range t.partitions {
		if err := services.StopAndAwaitTerminated(context.Background(), p); err != nil {
			errs.Add(errors.Wrapf(err, "unable to stop partition %d", p.partitionID))
		}
	}

	if err := services.StopAndAwaitTerminated(context.Background(), t.instanceLifecycler); err != nil {
		errs.Add(errors.Wrap(err, "unable to stop instance lifecycler"))
	}

	// Close Kafka clients.
	t.eventsKafkaWriter.Close()

	return errs.Err()
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	t.mtx.RLock()
	p, ok := t.partitions[req.Partition]
	t.mtx.RUnlock()
	if !ok {
		return nil, fmt.Errorf("partition %d not found", req.Partition)
	}
	rejected, err := p.store.trackSeries(context.Background(), req.UserID, req.SeriesHashes, time.Now())
	if err != nil {
		return nil, err
	}
	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

// instancesServingPartitions returns the number of instances that are responsible for serving partitions.
func (t *UsageTracker) instancesServingPartitions() int32 {
	// This is the number of active instances that are not read-only, i.e. WriteableInstances in this zone.
	return int32(t.instanceRing.WritableInstancesWithTokensInZoneCount(t.cfg.InstanceRing.InstanceZone))
}

func (t *UsageTracker) localSeriesLimit(userID string) uint64 {
	globalLimit := t.overrides.MaxGlobalSeriesPerUser(userID) // TODO: use a new active series limit.
	if globalLimit <= 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return uint64(float64(globalLimit) / float64(t.partitionRing.PartitionRing().ActivePartitionsCount()))
}

type chanEventsPublisher struct {
	logger log.Logger
	events chan []byte
}

func (p chanEventsPublisher) publishCreatedSeries(ctx context.Context, userID string, series []uint64, timestamp time.Time) error {
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
func instancePartitions(instance, instances, partitions int32) (start, end int32, err error) {
	if instance >= instances {
		return 0, 0, fmt.Errorf("instance %d >= instances %d", instance, instances)
	}
	start = instancePartitionsStart(instance, partitions)
	end = partitions
	// I couldn't find a general formula for the end, so we just check the instances that steal ranges from us and find the one that starts earlier (if any).
	for i := int32(0); i < instances; i++ {
		if thatInstanceStart := instancePartitionsStart(i, partitions); thatInstanceStart > start && thatInstanceStart < end {
			end = thatInstanceStart
		}
	}
	return start, end, nil
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

// parseInstanceID returns the partition ID from the instance ID.
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
