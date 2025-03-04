// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"math/bits"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv"
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

	if bits.OnesCount64(uint64(c.Partitions)) != 1 {
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

	cfg        Config
	bucket     objstore.InstrumentedBucket
	overrides  *validation.Overrides
	logger     log.Logger
	registerer prometheus.Registerer

	// Partition and instance ring.
	partitionKVClient  kv.Client
	partitionRing      *ring.PartitionInstanceRing
	instanceLifecycler *ring.BasicLifecycler

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client

	mtx        sync.RWMutex
	partitions map[int32]*partition

	// Dependencies.
	subservicesWatcher *services.FailureWatcher
}

func NewUsageTracker(cfg Config, partitionRing *ring.PartitionInstanceRing, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		cfg:           cfg,
		partitionRing: partitionRing,
		overrides:     overrides,
		logger:        logger,
		registerer:    registerer,

		partitions: make(map[int32]*partition, 64),
	}

	// Init instance ring lifecycler.
	var err error
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
	// prometheus.WrapRegistererWith(prometheus.Labels{"component": component}, reg)
	partitionID, err := partitionIDFromInstanceID(t.cfg.InstanceRing.InstanceID)
	if err != nil {
		return errors.Wrap(err, "calculating usage-tracker partition ID")
	}
	p, err := newPartition(partitionID, t.cfg, t.partitionKVClient, t.partitionRing, t.eventsKafkaWriter, t.bucket, t, t.logger, t.registerer)
	if err != nil {
		return errors.Wrap(err, "unable to create partition")
	}
	if err := services.StartAndAwaitRunning(ctx, p); err != nil {
		return errors.Wrap(err, "unable to start partition")
	}
	t.mtx.Lock()
	t.partitions[partitionID] = p
	t.mtx.Unlock()

	partitionWatcher := services.NewFailureWatcher()
	partitionWatcher.WatchService(p)

	defer func() {
		if err := services.StopAndAwaitTerminated(context.Background(), p); err != nil {
			level.Error(t.logger).Log("msg", "failed to stop partition", "err", err)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-t.subservicesWatcher.Chan():
		return errors.Wrap(err, "usage-tracker dependency failed")
	case err := <-partitionWatcher.Chan():
		return errors.Wrap(err, "partition failed")
	}
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	// Stop dependencies.
	err := services.StopAndAwaitTerminated(context.Background(), t.instanceLifecycler)

	// Close Kafka clients.
	t.eventsKafkaWriter.Close()

	return err
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	t.mtx.RLock()
	p, ok := t.partitions[req.Partition]
	for _, p = range t.partitions {
		break // take the first one
	}
	if !ok {
		return nil, errors.New("no partitions available")
	}
	rejected, err := p.store.trackSeries(context.Background(), req.UserID, req.SeriesHashes, time.Now())
	if err != nil {
		return nil, err
	}
	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
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
