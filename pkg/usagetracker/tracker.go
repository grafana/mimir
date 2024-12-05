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

	IdleTimeout                           time.Duration `yaml:"idle_timeout"`
	CreatedSeriesEventsMaxPending         int           `yaml:"max_pending_created_series_events"`
	CreatedSeriesEventsMaxBatchSizeBytes  int           `yaml:"created_series_events_max_batch_size_bytes"`
	CreatedSeriesEventsBatchTTL           time.Duration `yaml:"created_series_events_batch_ttl"`
	CreatedSeriesEventsPublishConcurrency int           `yaml:"created_series_events_publish_concurrency"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.BoolVar(&c.Enabled, "usage-tracker.enabled", false, "True to enable the usage-tracker.")

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

	pendingCreatedSeriesMarshaledEvents chan []byte

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

		pendingCreatedSeriesMarshaledEvents: make(chan []byte, cfg.CreatedSeriesEventsMaxPending),
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

	eventsPublisher := chanEventsPublisher{events: t.pendingCreatedSeriesMarshaledEvents, logger: logger}
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

	startConsumingEventsAtMillis := time.Now().Add(-t.cfg.IdleTimeout).UnixMilli()
	t.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		eventsTopic: {t.partitionID: kgo.NewOffset().AfterMilli(startConsumingEventsAtMillis)},
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
	go t.publishSeriesCreatedEvents(ctx, wg)
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
	defer wg.Done()
	for ctx.Err() == nil {
		fetches := t.eventsKafkaReader.PollRecords(ctx, 0)
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

func (t *UsageTracker) publishSeriesCreatedEvents(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	publish := make(chan []*kgo.Record)
	defer close(publish)

	for w := 0; w < t.cfg.CreatedSeriesEventsPublishConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range publish {
				level.Debug(t.logger).Log("msg", "producing batch of series created events", "size", len(batch), "partition", t.partitionID)
				// TODO: maybe use a slightly longer-deadline context here, to avoid dropping the pending events from the queue?
				res := t.eventsKafkaWriter.ProduceSync(ctx, batch...)
				for _, r := range res {
					if r.Err != nil {
						// TODO: what should we do here?
						level.Error(t.logger).Log("msg", "failed to publish series created event", "err", r.Err, "partition", t.partitionID)
						continue
					}
					level.Debug(t.logger).Log("msg", "produced series created event", "partition", r.Record.Partition, "offset", r.Record.Offset)
				}
			}
		}()
	}

	// empty timer with nil C chan
	timer := new(time.Timer)
	defer func() {
		if timer.C != nil {
			timer.Stop()
		}
	}()

	var batch []*kgo.Record
	var batchSize int
	publishBatch := func() {
		select {
		case publish <- batch:
		case <-ctx.Done():
			return
		}

		batch = nil
		batchSize = 0
		timer.Stop()
		timer.C = nil // Just in case timer already fired.
	}

	for {
		select {
		case <-ctx.Done():
			return

		case data := <-t.pendingCreatedSeriesMarshaledEvents:
			if len(batch) == 0 {
				timer = time.NewTimer(t.cfg.CreatedSeriesEventsBatchTTL)
			}
			level.Debug(t.logger).Log("msg", "batching series created event", "size", len(data), "partition", t.partitionID)
			batch = append(batch, &kgo.Record{Topic: eventsTopic, Value: data, Partition: t.partitionID})
			batchSize += len(data)

			if batchSize >= t.cfg.CreatedSeriesEventsMaxBatchSizeBytes {
				level.Debug(t.logger).Log("msg", "publishing batch due to size", "size", batchSize, "partition", t.partitionID)
				publishBatch()
			}

		case <-timer.C:
			level.Debug(t.logger).Log("msg", "publishing batch due to TTL", "partition", t.partitionID)
			publishBatch()
		}
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
