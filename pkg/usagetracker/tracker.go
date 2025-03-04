// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	SnapshotsStoragePrefix = "usage-tracker-snapshots"

	eventsKafkaWriterMetricsPrefix = "cortex_usage_tracker_events_writer"
	eventsKafkaReaderMetricsPrefix = "cortex_usage_tracker_events_reader"
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	Partitions int `yaml:"partitions"`

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

		partitions: make(map[int32]*partition, 64),
	}

	// Init instance ring lifecycler.
	var err error
	t.instanceID, err = parseInstanceID(t.cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "parsing instance ID")
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
	for {
		select {
		case <-time.After(10 * time.Second):
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
	instancesServingPartitions := t.instancesServingPartitions()
	start, end, err := instancePartitions(t.instanceID, instancesServingPartitions, int32(t.cfg.Partitions))
	if err != nil {
		level.Error(t.logger).Log("msg", "unable to calculate partitions, skipping reconcile", "err", err)
		return nil
	}
	t.mtx.RLock()
	current := int32(len(t.partitions))
	t.mtx.RUnlock()
	if current == end-start {
		level.Debug(t.logger).Log("msg", "partitions already reconciled", "start", start, "end", end, "instance", t.instanceID, "serving_instances", instancesServingPartitions, "partitions", t.cfg.Partitions)
		return nil
	}

	level.Info(t.logger).Log("msg", "serving partitions", "current", current, "new", end-start, "start", start, "end", end, "instance", t.instanceID, "serving_instances", instancesServingPartitions, "partitions", t.cfg.Partitions)

	if current > end-start {
		// We need to stop some partitions.
		// TODO: do this in a more graceful way.
		for partitionID := start + current; partitionID >= end; partitionID-- {
			level.Info(t.logger).Log("msg", "stopping partition", "partition", partitionID)
			t.mtx.Lock()
			p, ok := t.partitions[partitionID]
			t.mtx.Unlock()
			if !ok {
				level.Error(t.logger).Log("msg", "partition not found to stop, implementation error", "partition", partitionID)
				continue
			}
			if err := services.StopAndAwaitTerminated(context.Background(), p); err != nil {
				level.Error(t.logger).Log("msg", "unable to stop partition", "partition", partitionID, "err", err)
				return errors.Wrapf(err, "unable to stop partition %d", p.partitionID)
			}
			t.mtx.Lock()
			// TODO: don't delete yet, delete later.
			delete(t.partitions, partitionID)
			t.mtx.Unlock()
		}
	}

	for partitionID := start + current; partitionID < end; partitionID++ {
		level.Info(t.logger).Log("msg", "creating partition", "partition", partitionID)
		p, err := newPartition(partitionID, t.cfg, t.partitionKVClient, t.partitionRing, t.eventsKafkaWriter, t.bucket, t, t.logger, t.registerer)
		if err != nil {
			return errors.Wrap(err, "unable to create partition")
		}
		level.Info(t.logger).Log("msg", "starting partition", "partition", partitionID)
		if err := services.StartAndAwaitRunning(ctx, p); err != nil {
			return errors.Wrap(err, "unable to start partition")
		}
		t.mtx.Lock()
		t.partitions[partitionID] = p
		t.mtx.Unlock()
		level.Info(t.logger).Log("msg", "started partition", "partition", partitionID)
	}

	level.Info(t.logger).Log("msg", "partitions reconciled", "start", start, "end", end, "instance", t.instanceID, "serving_instances", instancesServingPartitions, "partitions", t.cfg.Partitions)
	return nil
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
	for i := instance + 1; i < instances; i++ {
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
