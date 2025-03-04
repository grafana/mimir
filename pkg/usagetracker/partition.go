package usagetracker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

type partition struct {
	services.Service

	store *trackerStore

	cfg        Config
	logger     log.Logger
	registerer prometheus.Registerer

	partitionID int32

	// Partition and instance ring.
	partitionLifecycler *ring.PartitionInstanceLifecycler
	partitionRing       *ring.PartitionInstanceRing

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client
	eventsKafkaReader *kgo.Client

	// Snapshots
	snapshotsBucket objstore.Bucket

	pendingCreatedSeriesMarshaledEvents chan []byte

	// Dependencies.
	subservicesWatcher *services.FailureWatcher
}

func newPartition(
	partitionID int32,
	cfg Config,
	partitionKVClient kv.Client,
	partitionRing *ring.PartitionInstanceRing,
	eventsKafkaWriter *kgo.Client,
	snapshotsBucket objstore.InstrumentedBucket,
	lim limiter,
	logger log.Logger,
	registerer prometheus.Registerer,
) (*partition, error) {
	registerer = prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.FormatInt(int64(partitionID), 10)}, registerer)

	p := &partition{
		cfg:           cfg,
		partitionRing: partitionRing,
		logger:        logger,
		registerer:    registerer,

		partitionID: partitionID,

		eventsKafkaWriter: eventsKafkaWriter,

		snapshotsBucket: objstore.NewPrefixedBucket(snapshotsBucket, fmt.Sprintf("partition-%d", partitionID)),

		pendingCreatedSeriesMarshaledEvents: make(chan []byte, cfg.CreatedSeriesEventsMaxPending),
	}

	var err error
	p.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, p.partitionID, cfg.InstanceRing.InstanceID, partitionKVClient, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Create Kafka reader for events storage.
	p.eventsKafkaReader, err = ingest.NewKafkaReaderClient(p.cfg.EventsStorage.Reader, ingest.NewKafkaReaderClientMetrics(eventsKafkaReaderMetricsPrefix, "usage-tracker", p.registerer), p.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka reader client for usage-tracker")
	}

	eventsPublisher := chanEventsPublisher{events: p.pendingCreatedSeriesMarshaledEvents, logger: logger}
	p.store = newTrackerStore(cfg.IdleTimeout, logger, lim, eventsPublisher)
	if err := registerer.Register(p.store); err != nil {
		return nil, errors.Wrap(err, "unable to register usage-tracker store as Prometheus collector")
	}
	p.Service = services.NewBasicService(p.start, p.run, p.stop)

	return p, nil
}

// start implements services.StartingFn.
func (p *partition) start(ctx context.Context) error {
	startConsumingEventsAtMillis := time.Now().Add(-p.cfg.IdleTimeout).UnixMilli()
	p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		p.cfg.EventsStorage.TopicName: {p.partitionID: kgo.NewOffset().AfterMilli(startConsumingEventsAtMillis)},
	})

	// Do this only once ready
	if err := services.StartAndAwaitRunning(ctx, p.partitionLifecycler); err != nil {
		return errors.Wrap(err, "unable to start partition lifecycler")
	}

	p.subservicesWatcher = services.NewFailureWatcher()
	p.subservicesWatcher.WatchService(p.partitionLifecycler)
	return nil
}

// run implements services.RunningFn.
func (p *partition) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	wg := &sync.WaitGroup{}
	go p.consumeSeriesCreatedEvents(ctx, wg)
	go p.publishSeriesCreatedEvents(ctx, wg)
	defer wg.Wait()

	for {
		select {
		case now := <-ticker.C:
			p.store.cleanup(now)
		case <-ctx.Done():
			return nil
		case err := <-p.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

// stop implements services.StoppingFn.
func (p *partition) stop(_ error) error {
	p.registerer.Unregister(p.store)
	// Stop dependencies.
	err := services.StopAndAwaitTerminated(context.Background(), p.partitionLifecycler)

	// Close our read client, don't close the write client.
	p.eventsKafkaReader.Close()
	return err
}

func (p *partition) consumeSeriesCreatedEvents(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for ctx.Err() == nil {
		fetches := p.eventsKafkaReader.PollRecords(ctx, 0)
		fetches.EachError(func(_ string, part int32, err error) {
			if !errors.Is(err, context.Canceled) { // Ignore when we're shutting down.
				// TODO: observability? Handle this?
				level.Error(p.logger).Log("msg", "failed to fetch records", "partition", part, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var ev usagetrackerpb.SeriesCreatedEvent
			if err := ev.Unmarshal(r.Value); err != nil {
				level.Error(p.logger).Log("msg", "failed to unmarshal series created event", "err", err, "partition", r.Partition, "offset", r.Offset, "value_len", len(r.Value))
				return
			}
			// TODO: maybe ignore our own events?
			p.store.processCreatedSeriesEvent(ev.UserID, ev.SeriesHashes, time.Unix(ev.Timestamp, 0), time.Now())
			level.Debug(p.logger).Log("msg", "processed series created event", "partition", r.Partition, "offset", r.Offset, "series", len(ev.SeriesHashes))
		})
	}
}

func (p *partition) publishSeriesCreatedEvents(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	publish := make(chan []*kgo.Record)
	defer close(publish)

	for w := 0; w < p.cfg.CreatedSeriesEventsPublishConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range publish {
				level.Debug(p.logger).Log("msg", "producing batch of series created events", "size", len(batch), "partition", p.partitionID)
				// TODO: maybe use a slightly longer-deadline context here, to avoid dropping the pending events from the queue?
				res := p.eventsKafkaWriter.ProduceSync(ctx, batch...)
				for _, r := range res {
					if r.Err != nil {
						// TODO: what should we do here?
						level.Error(p.logger).Log("msg", "failed to publish series created event", "err", r.Err, "partition", p.partitionID)
						continue
					}
					level.Debug(p.logger).Log("msg", "produced series created event", "partition", r.Record.Partition, "offset", r.Record.Offset)
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

		case data := <-p.pendingCreatedSeriesMarshaledEvents:
			if len(batch) == 0 {
				timer = time.NewTimer(p.cfg.CreatedSeriesEventsBatchTTL)
			}
			level.Debug(p.logger).Log("msg", "batching series created event", "size", len(data), "partition", p.partitionID)
			batch = append(batch, &kgo.Record{Topic: p.cfg.EventsStorage.TopicName, Value: data, Partition: p.partitionID})
			batchSize += len(data)

			if batchSize >= p.cfg.CreatedSeriesEventsMaxBatchSizeBytes {
				level.Debug(p.logger).Log("msg", "publishing batch due to size", "size", batchSize, "partition", p.partitionID)
				publishBatch()
			}

		case <-timer.C:
			level.Debug(p.logger).Log("msg", "publishing batch due to TTL", "partition", p.partitionID)
			publishBatch()
		}
	}
}
