// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
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
	eventsTopic = "usage-tracker-events"

	SnapshotsStoragePrefix = "usage-tracker-snapshots"

	eventsKafkaWriterMetricsPrefix = "cortex_usage_tracker_events_writer"
	eventsKafkaReaderMetricsPrefix = "cortex_usage_tracker_events_reader"
)

type Config struct {
	Enabled       bool                `yaml:"enabled"`
	InstanceRing  InstanceRingConfig  `yaml:"instance_ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`

	EventsStorage    EventsStorageConfig `yaml:"events_storage"`
	SnapshotsStorage bucket.Config       `yaml:"snapshots_storage"`

	IdleTimeout time.Duration `yaml:"idle_timeout"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.BoolVar(&c.Enabled, "usage-tracker.enabled", false, "True to enable the usage-tracker.")

	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
	c.EventsStorage.RegisterFlags(f)
	c.SnapshotsStorage.RegisterFlagsWithPrefixAndDefaultDirectory("usage-tracker.snapshot-storage.", "usagetrackersnapshots", f)

	f.DurationVar(&c.IdleTimeout, "usage-tracker.idle-timeout", 20*time.Minute, "The time after which series are considered idle and not active anymore. Must be greater than 0 and less than 1 hour.")
}

func (c *Config) Validate() error {
	// Skip validation if not enabled.
	if !c.Enabled {
		return nil
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

	store *trackerStore

	cfg        Config
	bucket     objstore.InstrumentedBucket
	overrides  *validation.Overrides
	logger     log.Logger
	registerer prometheus.Registerer

	partitionID int32

	// Partition and instance ring.
	partitionLifecycler *ring.PartitionInstanceLifecycler
	partitionRing       *ring.PartitionInstanceRing
	instanceLifecycler  *ring.BasicLifecycler

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client
	eventsKafkaReader *kgo.Client

	// Dependencies.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewUsageTracker(cfg Config, partitionRing *ring.PartitionInstanceRing, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		cfg:           cfg,
		partitionRing: partitionRing,
		overrides:     overrides,
		logger:        logger,
		registerer:    registerer,
	}

	// Get the partition ID.
	var err error
	t.partitionID, err = partitionIDFromInstanceID(cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating usage-tracker partition ID")
	}

	// Init instance ring lifecycler.
	t.instanceLifecycler, err = NewInstanceRingLifecycler(cfg.InstanceRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Init the partition ring lifecycler.
	partitionKVClient, err := NewPartitionRingKVClient(cfg.PartitionRing, "lifecycler", logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, t.partitionID, cfg.InstanceRing.InstanceID, partitionKVClient, logger, registerer)
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

	// Create Kafka reader for events storage.
	t.eventsKafkaReader, err = ingest.NewKafkaReaderClient(t.cfg.EventsStorage.Reader, ingest.NewKafkaReaderClientMetrics(eventsKafkaReaderMetricsPrefix, "usage-tracker", t.registerer), t.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka reader client for usage-tracker")
	}

	eventsPublisher := kafkaEventsPublisher{kafka: t.eventsKafkaWriter, partition: t.partitionID, logger: logger}
	t.store = newTrackerStore(cfg.IdleTimeout, logger, t, eventsPublisher)

	t.Service = services.NewBasicService(t.start, t.run, t.stop)

	return t, nil
}

// start implements services.StartingFn.
func (t *UsageTracker) start(ctx context.Context) error {
	var err error

	// Start dependencies.
	if t.subservices, err = services.NewManager(t.instanceLifecycler, t.partitionLifecycler); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker dependencies")
	}

	t.subservicesWatcher = services.NewFailureWatcher()
	t.subservicesWatcher.WatchManager(t.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, t.subservices); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker subservices")
	}

	startConsumingAtMilli := time.Now().Add(-t.cfg.IdleTimeout).UnixMilli()
	t.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		eventsTopic: {t.partitionID: kgo.NewOffset().AfterMilli(startConsumingAtMilli)},
	})

	return nil
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	// Stop dependencies.
	if t.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), t.subservices)
	}

	// Close Kafka clients.
	t.eventsKafkaWriter.Close()
	t.eventsKafkaReader.Close()

	return nil
}

// run implements services.RunningFn.
func (t *UsageTracker) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	wg := &sync.WaitGroup{}
	go t.consumeSeriesCreatedEvents(ctx, wg)
	defer wg.Wait()

	for {
		select {
		case now := <-ticker.C:
			t.store.cleanup(now)
		case <-ctx.Done():
			return nil
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

func (t *UsageTracker) consumeSeriesCreatedEvents(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	for ctx.Err() != nil {
		fetches := t.eventsKafkaReader.PollRecords(ctx, 1)
		fetches.EachError(func(_ string, p int32, err error) {
			if !errors.Is(err, context.Canceled) { // Ignore when we're shutting down.
				// TODO: observability? Handle this?
				level.Error(t.logger).Log("msg", "failed to fetch records", "partition", p, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var ev usagetrackerpb.SeriesCreatedEvent
			if err := ev.Unmarshal(r.Value); err != nil {
				level.Error(t.logger).Log("msg", "failed to unmarshal series created event", "err", err, "partition", r.Partition, "offset", r.Offset, "value_len", len(r.Value))
				return
			}
			// TODO: maybe ignore our own events?
			t.store.processCreatedSeriesEvent(ev.UserID, ev.SeriesHashes, time.Unix(ev.Timestamp, 0), time.Now())
			level.Debug(t.logger).Log("msg", "processed series created event", "partition", r.Partition, "offset", r.Offset, "series", len(ev.SeriesHashes))
		})
	}
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	rejected, err := t.store.trackSeries(context.Background(), req.UserID, req.SeriesHashes, time.Now())
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

type kafkaEventsPublisher struct {
	logger    log.Logger
	kafka     *kgo.Client
	partition int32
}

func (p kafkaEventsPublisher) publishCreatedSeries(ctx context.Context, userID string, series []uint64, timestamp time.Time) error {
	ev := usagetrackerpb.SeriesCreatedEvent{
		UserID:       userID,
		Timestamp:    timestamp.Unix(),
		SeriesHashes: series,
	}
	data, err := proto.Marshal(&ev)
	if err != nil {
		return errors.Wrap(err, "marshaling series created event")
	}
	// TODO: usage Produce() here to avoid ProduceSync's allocations
	res := p.kafka.ProduceSync(ctx, &kgo.Record{
		Topic:     eventsTopic,
		Value:     data,
		Partition: p.partition,
	})
	r, err := res.First()
	if err != nil {
		return errors.Wrap(err, "producing series created event")
	}
	level.Debug(p.logger).Log("msg", "published series created event", "partition", r.Partition, "offset", r.Offset, "series", len(ev.SeriesHashes))
	return nil
}
