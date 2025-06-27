// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util"
)

const (
	instanceRecordHeader  = "instance"
	eventTypeRecordHeader = "event_type"

	eventTypeSeriesCreated = "series_created"
	eventTypeSnapshot      = "snapshot"
)

var (
	eventTypeSeriesCreatedBytes = []byte(eventTypeSeriesCreated)
	eventTypeSnapshotBytes      = []byte(eventTypeSnapshot)
)

type partition struct {
	services.Service

	store *trackerStore

	cfg        Config
	logger     log.Logger
	registerer prometheus.Registerer
	collector  prometheus.Collector

	partitionID int32

	// Partition and instance ring.
	partitionLifecycler *ring.PartitionInstanceLifecycler
	partitionRing       *ring.PartitionInstanceRing

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client
	eventsKafkaReader *kgo.Client

	// Snapshots metadata storage (Kafka).
	snapshotsKafkaReader *kgo.Client

	// Snapshots
	snapshotsBucket objstore.Bucket

	// ignoreSnapshotEventsUntil is the offset of the last snapshot event that was included in the snapshot we loaded at startup.
	ignoreSnapshotEventsUntil int64
	lastPublishedEventOffset  atomic.Int64

	snapshotsFromEvents chan *usagetrackerpb.SnapshotEvent

	pendingCreatedSeriesMarshaledEvents chan []byte

	// Dependencies.
	subservicesWatcher *services.FailureWatcher

	// Misc
	instanceIDBytes []byte
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
	reg := prometheus.NewRegistry()
	logger = log.With(logger, "partition", partitionID)
	p := &partition{
		cfg:           cfg,
		partitionRing: partitionRing,
		logger:        logger,
		registerer:    registerer,
		collector:     prometheus.WrapCollectorWith(prometheus.Labels{"partition": strconv.FormatInt(int64(partitionID), 10)}, reg),

		partitionID: partitionID,

		eventsKafkaWriter: eventsKafkaWriter,

		snapshotsBucket: objstore.NewPrefixedBucket(snapshotsBucket, fmt.Sprintf("partition-%d", partitionID)),

		snapshotsFromEvents: make(chan *usagetrackerpb.SnapshotEvent, shards),

		pendingCreatedSeriesMarshaledEvents: make(chan []byte, cfg.CreatedSeriesEventsMaxPending),

		instanceIDBytes: []byte(cfg.InstanceRing.InstanceID),
	}

	// In the partition ring lifecycler one owner can only have one partition, so we create a sub-owner for this partition, because we (may) own multiple partitions.
	instanceID := fmt.Sprintf("%s/%d", cfg.InstanceRing.InstanceID, partitionID)

	var err error
	p.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, p.partitionID, instanceID, partitionKVClient, logger, reg)
	if err != nil {
		return nil, err
	}
	p.partitionLifecycler.SetRemoveOwnerOnShutdown(true)

	// Create Kafka reader for events storage.
	p.eventsKafkaReader, err = ingest.NewKafkaReaderClient(p.cfg.EventsStorageReader, ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, eventsKafkaReaderComponent, reg), p.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka reader client for usage-tracker")
	}

	p.snapshotsKafkaReader, err = ingest.NewKafkaReaderClient(p.cfg.SnapshotsMetadataReader, ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, snapshotsKafkaReaderComponent, reg), p.logger,
		kgo.ConsumeTopics(p.cfg.SnapshotsMetadataReader.Topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd().Relative(-1)),
	)

	eventsPublisher := chanEventsPublisher{events: p.pendingCreatedSeriesMarshaledEvents, logger: logger}
	p.store = newTrackerStore(cfg.IdleTimeout, logger, lim, eventsPublisher)
	if err := reg.Register(p.store); err != nil {
		return nil, errors.Wrap(err, "unable to register usage-tracker store as Prometheus collector")
	}
	p.Service = services.NewBasicService(p.start, p.run, p.stop)

	return p, nil
}

// start implements services.StartingFn.
func (p *partition) start(ctx context.Context) error {
	if err := p.registerer.Register(p.collector); err != nil {
		return errors.Wrap(err, "unable to register usage-tracker partition collector as Prometheus collector")
	}

	eventsOffset, err := p.loadSnapshotAndPrepareKafkaEventsReader(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to load snapshot for usage-tracker partition")
	}
	if err := p.loadEvents(ctx, eventsOffset); err != nil {
		return errors.Wrap(err, "unable to load events for usage-tracker partition")
	}

	// Do this only once ready
	if err := services.StartAndAwaitRunning(ctx, p.partitionLifecycler); err != nil {
		return errors.Wrap(err, "unable to start partition lifecycler")
	}

	p.subservicesWatcher = services.NewFailureWatcher()
	p.subservicesWatcher.WatchService(p.partitionLifecycler)
	return nil
}

func (p *partition) loadSnapshotAndPrepareKafkaEventsReader(ctx context.Context) (int64, error) {
	// Default offset to start, to avoid returning nonsense.
	snapshotsTopic := p.cfg.SnapshotsMetadataReader.Topic

	offset, err := findEndOffset(ctx, p.snapshotsKafkaReader, snapshotsTopic)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to find end offset for topic %s", snapshotsTopic)
	}
	if offset == 0 {
		level.Warn(p.logger).Log("msg", "end offset is 0, no snapshots found", "topic", snapshotsTopic)
		return 0, nil
	}

	// Start reading the last snapshot.
	fetches := p.snapshotsKafkaReader.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		return 0, fmt.Errorf("failed to retrieve last snapshot: %w", err)
	}
	if fetches.Empty() {
		return 0, fmt.Errorf("didn't get any snapshot records from topic %s", snapshotsTopic)
	}
	// Get the last record from the fetches.
	record := fetches.Records()[len(fetches.Records())-1]
	// Log it and continue
	level.Info(p.logger).Log("msg", "got last snapshot record", "offset", record.Offset, "bytes", len(record.Value))

	if time.Since(record.Timestamp) > p.cfg.IdleTimeout {
		level.Warn(p.logger).Log("msg", "last snapshot record is too old, ignoring it", "offset", record.Offset, "timestamp", record.Timestamp, "idle_timeout", p.cfg.IdleTimeout)
		return 0, nil
	}

	var snapshot usagetrackerpb.SnapshotRecord
	if err := snapshot.Unmarshal(record.Value); err != nil {
		return 0, errors.Wrapf(err, "failed to unmarshal snapshot record from topic %s", snapshotsTopic)
	}

	level.Info(p.logger).Log("msg", "found snapshot record", "offset", record.Offset, "record_timestamp", record.Timestamp, "snapshot_timestamp", snapshot.Timestamp)
	if err := p.loadAllSnapshotShards(ctx, snapshot.Filenames); err != nil {
		return 0, errors.Wrapf(err, "failed to load snapshot")
	}

	p.ignoreSnapshotEventsUntil = snapshot.LastSnapshotEventOffset

	return snapshot.LastEventOffsetPublishedBeforeSnapshot, nil
}

func (p *partition) loadAllSnapshotShards(ctx context.Context, files []string) error {
	t0 := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type downloadedSnapshot struct {
		filename string
		data     [][]byte
	}

	// One is being downloaded, one is buffered, one is being processed.
	downloaded := make(chan downloadedSnapshot, 1)
	errs := make(chan error, 1)
	go func() {
		t0 := time.Now()
		i := 0
		for snapshot := range downloaded {
			for j, data := range snapshot.data {
				if err := p.store.loadSnapshot(data, time.Now()); err != nil {
					errs <- errors.Wrapf(err, "failed to load snapshot data %d from file %q", j, snapshot.filename)
					return
				}
			}
			if i == 0 {
				level.Info(p.logger).Log("msg", "loaded the first snapshot file", "shards", len(snapshot.data), "load_time", time.Since(t0))
			}
			i++
		}
		errs <- nil
	}()

	go func() {
		defer close(downloaded)
		t0 := time.Now()
		for _, filename := range files {
			// TODO: Maybe retry with backoff here?
			rc, err := p.snapshotsBucket.Get(ctx, filename)
			if err != nil {
				errs <- errors.Wrapf(err, "failed to get snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}
			data, err := io.ReadAll(rc)
			if err != nil {
				errs <- errors.Wrapf(err, "failed to read snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}
			if err := rc.Close(); err != nil {
				errs <- errors.Wrapf(err, "failed to close snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}
			var file usagetrackerpb.SnapshotFile
			if err := file.Unmarshal(data); err != nil {
				errs <- errors.Wrapf(err, "failed to unmarshal snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}

			level.Info(p.logger).Log("msg", "downloaded first snapshot file", "elapsed", time.Since(t0), "bytes", len(data))
			select {
			case downloaded <- downloadedSnapshot{filename, file.Data}:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-errs:
		if err == nil {
			level.Info(p.logger).Log("msg", "all snapshot shards loaded successfully", "elapsed", time.Since(t0))
		}
		return err
	}
}

func (p *partition) loadEvents(ctx context.Context, eventsOffset int64) error {
	endOffset, err := findEndOffset(ctx, p.eventsKafkaReader, p.cfg.EventsStorageReader.Topic)
	if err != nil {
		return errors.Wrapf(err, "failed to find end offset for topic %s", p.cfg.EventsStorageReader.Topic)
	}

	if endOffset == 0 {
		// There are no events in the topic.
		level.Warn(p.logger).Log("msg", "end offset is 0, no events found to load", "topic", p.cfg.EventsStorageReader.Topic)
		startConsumingEventsAtMillis := time.Now().Add(-p.cfg.IdleTimeout).UnixMilli()
		p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.EventsStorageWriter.Topic: {p.partitionID: kgo.NewOffset().AfterMilli(startConsumingEventsAtMillis)},
		})
		return nil
	}

	if endOffset < eventsOffset-1 {
		level.Error(p.logger).Log("msg", "snapshot suggested to start consuming events at an offset that is greater than the end offset of the topic", "end_offset", endOffset, "snapshot_offset", eventsOffset, "topic", p.cfg.EventsStorageReader.Topic)
		return nil
	}

	if eventsOffset == 0 {
		// It doesn't make sense to start consuming events from the beginning of the topic, we should read only the latest IdleTimeout minutes.
		startConsumingEventsAtMillis := time.Now().Add(-p.cfg.IdleTimeout).UnixMilli()
		p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.EventsStorageWriter.Topic: {p.partitionID: kgo.NewOffset().AfterMilli(startConsumingEventsAtMillis)},
		})
		level.Info(p.logger).Log("msg", "events offset from snapshot was zero, starting consumption at idle timeout", "topic", p.cfg.EventsStorageReader.Topic, "end_offset", startConsumingEventsAtMillis)
	} else {
		level.Info(p.logger).Log("msg", "starting to consume events from snapshot offset", "topic", p.cfg.EventsStorageReader.Topic, "snapshot_offset", eventsOffset, "end_offset", endOffset)
		// If the end offset is greater than the snapshot offset, we need to start consuming events from the snapshot offset.
		p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.EventsStorageWriter.Topic: {p.partitionID: kgo.NewOffset().At(eventsOffset)},
		})
	}

	for {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := p.eventsKafkaReader.PollFetches(ctx) // TODO: maybe configure the max amount of records to fetch here?
		cancel()
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				level.Info(p.logger).Log("msg", "didn't receive events to load within timeout, retrying")
				continue
			}
			return errors.Wrap(err, "failed to load events")
		}

		fetches.EachRecord(func(r *kgo.Record) {
			// We don't ignore our own events here, because they potentially were sent after we made the snapshot.
			eventType, ok := recordHeader(r, eventTypeRecordHeader)
			if !ok {
				level.Error(p.logger).Log("msg", "record does not have event_type header, ignoring", "offset", r.Offset, "value_len", len(r.Value))
			}

			switch eventType {
			case eventTypeSeriesCreated:
				p.processSeriesCreatedEventRecord(r)
			case eventTypeSnapshot:
				p.processShardSnapshotEventSync(ctx, r)
			}
		})

	}
}

// run implements services.RunningFn.
func (p *partition) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	wg := &goroutineWaitGroup{}
	wg.Go(func() { p.consumeSeriesCreatedEvents(ctx) })
	wg.Go(func() { p.publishSeriesCreatedEvents(ctx) })
	wg.Go(func() { p.loadSnapshotsFromEvents(ctx) })
	wg.Go(func() { p.publishSnapshots(ctx) })
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
	p.registerer.Unregister(p.collector)
	// Stop dependencies.
	err := services.StopAndAwaitTerminated(context.Background(), p.partitionLifecycler)

	// Close our read client, don't close the write client.
	p.eventsKafkaReader.Close()
	return err
}

func (p *partition) consumeSeriesCreatedEvents(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := p.eventsKafkaReader.PollRecords(ctx, 0)
		fetches.EachError(func(_ string, part int32, err error) {
			if !errors.Is(err, context.Canceled) { // Ignore when we're shutting down.
				// TODO: observability? Handle this?
				level.Error(p.logger).Log("msg", "failed to fetch records", "fetch_partition", part, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			instanceID, eventType, err := eventRecordHeaders(r)
			if err != nil {
				level.Error(p.logger).Log("msg", "record does not have all required headers, ignoring", "offset", r.Offset, "value_len", len(r.Value), "err", err)
				return
			}
			if instanceID == p.cfg.InstanceRing.InstanceID {
				level.Debug(p.logger).Log("msg", "ignoring our own event", "offset", r.Offset)
				return
			}
			switch eventType {
			case eventTypeSeriesCreated:
				p.processSeriesCreatedEventRecord(r)
			case eventTypeSnapshot:
				p.processShardSnapshotEventAsync(r)
			}
		})
	}
}

func (p *partition) processSeriesCreatedEventRecord(r *kgo.Record) {
	var ev usagetrackerpb.SeriesCreatedEvent
	if err := ev.Unmarshal(r.Value); err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to unmarshal series created event", "err", err, "offset", r.Offset, "value_len", len(r.Value))
		return
	}

	p.store.processCreatedSeriesEvent(ev.UserID, ev.SeriesHashes, time.Unix(ev.Timestamp, 0), time.Now())
	level.Debug(p.logger).Log("msg", "processed series created event", "offset", r.Offset, "series", len(ev.SeriesHashes))
}

func (p *partition) processShardSnapshotEventSync(ctx context.Context, r *kgo.Record) {
	p.processShardSnapshotEvent(r, func(ev *usagetrackerpb.SnapshotEvent) {
		p.loadSnapshot(ctx, ev)
	})
}

func (p *partition) processShardSnapshotEventAsync(r *kgo.Record) {
	p.processShardSnapshotEvent(r, func(ev *usagetrackerpb.SnapshotEvent) {
		select {
		case p.snapshotsFromEvents <- ev:
			return
		default:
			p.logger.Log("msg", "dropping shard snapshot event, channel is full", "offset", r.Offset, "timestamp", time.UnixMilli(ev.Timestamp))
		}
	})
}

func (p *partition) processShardSnapshotEvent(r *kgo.Record, process func(event *usagetrackerpb.SnapshotEvent)) {
	if r.Offset < p.ignoreSnapshotEventsUntil {
		// Ignore snapshot events that were made before the last snapshot we loaded.
		level.Info(p.logger).Log("msg", "ignoring shard snapshot event", "offset", r.Offset, "ignore_until", p.ignoreSnapshotEventsUntil)
		return
	}

	var ev usagetrackerpb.SnapshotEvent
	if err := ev.Unmarshal(r.Value); err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to unmarshal shard snapshot event", "err", err, "offset", r.Offset, "value_len", len(r.Value))
		return
	}

	eventTimestamp := time.UnixMilli(ev.Timestamp)
	if time.Since(eventTimestamp) > p.cfg.IdleTimeout {
		// We shouldn't really see this, so let's warn.
		level.Warn(p.logger).Log("msg", "ignoring shard snapshot event, it is too old", "offset", r.Offset, "timestamp", eventTimestamp, "idle_timeout", p.cfg.IdleTimeout)
		return
	}

	process(&ev)
}

func (p *partition) loadSnapshotsFromEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-p.snapshotsFromEvents:
			p.loadSnapshot(ctx, r)
		}
	}
}

func (p *partition) loadSnapshot(ctx context.Context, r *usagetrackerpb.SnapshotEvent) {
	t0 := time.Now()
	rc, err := p.snapshotsBucket.Get(ctx, r.Filename)
	if err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to get snapshot file from bucket", "filename", r.Filename, "err", err)
		return
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to read snapshot file from bucket", "filename", r.Filename, "err", err)
		return
	}

	if err := rc.Close(); err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to close snapshot file from bucket", "filename", r.Filename, "err", err)
		return
	}
	td := time.Now()

	var file usagetrackerpb.SnapshotFile
	if err := file.Unmarshal(data); err != nil {
		// TODO increment a metric here, alert on it.
		level.Error(p.logger).Log("msg", "failed to unmarshal snapshot file", "filename", r.Filename, "err", err)
		return
	}

	for i, data := range file.Data {
		if err := p.store.loadSnapshot(data, time.Now()); err != nil {
			// TODO increment a metric here, alert on it.
			level.Error(p.logger).Log("msg", "failed to load snapshot data", "filename", r.Filename, "shard_index", i, "err", err)
			return
		}
		level.Debug(p.logger).Log("msg", "loaded snapshot shard", "filename", r.Filename, "shard_index", i, "bytes", len(data))
	}

	level.Info(p.logger).Log("msg", "loaded snapshot from events", "filename", r.Filename, "shards", len(file.Data), "bytes", len(data), "elapsed_download", time.Since(t0), "elapsed_total", time.Since(td))
}

func (p *partition) publishSeriesCreatedEvents(ctx context.Context) {
	wg := &goroutineWaitGroup{}
	defer wg.Wait()

	publish := make(chan []*kgo.Record)
	defer close(publish)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for w := 0; w < p.cfg.CreatedSeriesEventsPublishConcurrency; w++ {
		wg.Go(func() {
			for batch := range publish {
				level.Debug(p.logger).Log("msg", "producing batch of series created events", "size", len(batch))
				// TODO: maybe use a slightly longer-deadline context here, to avoid dropping the pending events from the queue?
				// TODO: make sure we don't send the traceparent header here.
				res := p.eventsKafkaWriter.ProduceSync(ctx, batch...)
				for _, r := range res {
					if r.Err != nil {
						// TODO: what should we do here?
						level.Error(p.logger).Log("msg", "failed to publish series created event", "err", r.Err)
						continue
					}
					level.Debug(p.logger).Log("msg", "produced series created event", "offset", r.Record.Offset)
					p.lastPublishedEventOffset.Store(r.Record.Offset)
				}
			}
		})
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
			level.Debug(p.logger).Log("msg", "batching series created event", "bytes", len(data))
			batch = append(batch, p.newEventKafkaRecord(eventTypeSeriesCreatedBytes, data))
			batchSize += len(data)

			if batchSize >= p.cfg.CreatedSeriesEventsMaxBatchSizeBytes {
				level.Debug(p.logger).Log("msg", "publishing batch due to size", "size", batchSize)
				publishBatch()
			}

		case <-timer.C:
			level.Debug(p.logger).Log("msg", "publishing batch due to TTL")
			publishBatch()
		}
	}
}

func (p *partition) newEventKafkaRecord(eventTypeBytes []byte, data []byte) *kgo.Record {
	return &kgo.Record{
		Topic: p.cfg.EventsStorageWriter.Topic,
		Headers: []kgo.RecordHeader{
			{Key: eventTypeRecordHeader, Value: eventTypeBytes},
			{Key: instanceRecordHeader, Value: p.instanceIDBytes},
		},
		Value:     data,
		Partition: p.partitionID,
	}
}

func (p *partition) publishSnapshots(ctx context.Context) {
	for {
		delay := util.DurationWithJitter(p.cfg.IdleTimeout/2, p.cfg.SnapshotIntervalJitter)
		level.Info(p.logger).Log("msg", "next snapshot scheduled", "delay", delay, "next_snapshot", time.Now().Add(delay))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			if err := p.publishSnapshot(ctx); err != nil {
				// TODO: increment a metric and alert on it.
				level.Error(p.logger).Log("msg", "failed to publish snapshot", "err", err)
			}
		}
	}
}

func (p *partition) publishSnapshot(ctx context.Context) error {
	level.Info(p.logger).Log("msg", "publishing snapshot")

	var filenames []string
	var bufs [][]byte
	t0 := time.Now()
	lastShardSize := 1024 * 1024 // Start with 1MB, this is just to allocate same size for all shards.
	var file usagetrackerpb.SnapshotFile
	fileDataLen := 0
	totalDataLen := 0

	firstSnapshotEventOffset := p.lastPublishedEventOffset.Load()
	lastSnapshotEventOffset := int64(0)

	writeFileAndPublishEvent := func() error {
		if len(file.Data) == 0 {
			return nil
		}

		// TODO Optimization: we could use an io.Pipe() here to avoid allocating the whole file in memory again.
		fileData, err := file.Marshal()
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to marshal snapshot file", "err", err)
			return errors.Wrap(err, "failed to marshal snapshot file")
		}
		filename := fmt.Sprintf("snapshot-%d-partition-%d-instance-%s.bin", time.Now().UnixMilli(), p.partitionID, p.cfg.InstanceRing.InstanceID)
		if err := p.snapshotsBucket.Upload(ctx, filename, bytes.NewReader(fileData)); err != nil {
			level.Error(p.logger).Log("msg", "failed to upload snapshot file to bucket", "filename", filename, "err", err)
			return errors.Wrap(err, "failed to upload snapshot file to bucket")
		}

		event := &usagetrackerpb.SnapshotEvent{
			Timestamp: time.Now().UnixMilli(),
			Filename:  filename,
		}
		eventData, err := event.Marshal()
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to marshal snapshot event", "err", err)
			return errors.Wrap(err, "failed to marshal snapshot event")
		}

		r, err := p.eventsKafkaWriter.ProduceSync(ctx, p.newEventKafkaRecord(eventTypeSnapshotBytes, eventData)).First()
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to publish snapshot event", "err", err)
			return errors.Wrap(err, "failed to publish snapshot event")
		}
		// Store the latest event we published, so next startup doesn't need to replay these snapshot events.
		lastSnapshotEventOffset = r.Offset

		// Store the buffers.
		for _, data := range file.Data {
			bufs = append(bufs, data[:0])
		}
		// Reset file and counters
		file = usagetrackerpb.SnapshotFile{}
		totalDataLen += fileDataLen
		fileDataLen = 0

		return nil
	}

	for s := range shards {
		var buf []byte
		if len(bufs) > 0 {
			buf, bufs = bufs[len(bufs)-1], bufs[:len(bufs)-1]
		} else {
			buf = make([]byte, 0, lastShardSize+lastShardSize/4) // Allocate 25% more than the last shard size to avoid reallocations.
		}

		shardSnapshot := p.store.snapshot(uint8(s), time.Now(), buf)
		lastShardSize = len(shardSnapshot)
		fileDataLen += len(shardSnapshot)
		file.Data = append(file.Data, shardSnapshot)

		if fileDataLen > p.cfg.TargetSnapshotFileSizeBytes {
			if err := writeFileAndPublishEvent(); err != nil {
				return err
			}
		}
	}
	if err := writeFileAndPublishEvent(); err != nil {
		return err
	}

	level.Info(p.logger).Log("msg", "wrote snapshot files and published events", "files", len(filenames), "filenames", filenames, "elapsed", time.Since(t0), "total_bytes", totalDataLen)

	record := &usagetrackerpb.SnapshotRecord{
		Timestamp:                              t0.UnixMilli(),
		Filenames:                              filenames,
		LastEventOffsetPublishedBeforeSnapshot: firstSnapshotEventOffset,
		LastSnapshotEventOffset:                lastSnapshotEventOffset,
	}
	data, err := record.Marshal()
	if err != nil {
		level.Error(p.logger).Log("msg", "failed to marshal snapshot record", "err", err)
		return errors.Wrap(err, "failed to marshal snapshot record")
	}
	snapshotRecord := &kgo.Record{
		Topic: p.cfg.EventsStorageWriter.Topic,
		Headers: []kgo.RecordHeader{
			{Key: instanceRecordHeader, Value: p.instanceIDBytes},
		},
		Value:     data,
		Partition: p.partitionID,
	}
	r, err := p.eventsKafkaWriter.ProduceSync(ctx, snapshotRecord).First()
	if err != nil {
		level.Error(p.logger).Log("msg", "failed to publish snapshot record", "err", err)
		return errors.Wrap(err, "failed to publish snapshot record")
	}
	level.Info(p.logger).Log("msg", "published snapshot record", "offset", r.Offset)

	return nil
}

// findEndOffset retrieves the end offset for the given topic using the provided Kafka client.
// This offset is the offset of the "next" message in the topic, so if it's 0, there are no messages yet.
// https://github.com/grafana/mimir/blob/392588b98e386c0fd7cdd549a54e5f06d3155bc2/pkg/storage/ingest/DESIGN.md
func findEndOffset(ctx context.Context, client *kgo.Client, topic string) (int64, error) {
	readAdmin := kadm.NewClient(client)

	offsets, err := readAdmin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get end offsets: %w", err)
	}
	if offsets.Error() != nil {
		return 0, fmt.Errorf("failed to get end offsets, offsets error: %w", offsets.Error())
	}
	topicOffset, ok := offsets.Lookup(topic, 0)
	if !ok {
		return 0, fmt.Errorf("failed to get end offsets, topic not found")
	}

	return topicOffset.Offset, nil
}

func eventRecordHeaders(r *kgo.Record) (instanceID, eventType string, err error) {
	instanceID, ok := recordHeader(r, instanceRecordHeader)
	if !ok {
		return "", "", fmt.Errorf("record does not have %q header, headers: %v", instanceRecordHeader, r.Headers)
	}
	eventType, ok = recordHeader(r, eventTypeRecordHeader)
	if !ok {
		return "", "", fmt.Errorf("record does not have %q header, headers: %v", eventTypeRecordHeader, r.Headers)
	}
	return instanceID, eventType, nil
}

func recordHeader(r *kgo.Record, headerName string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == headerName {
			return string(h.Value), true
		}
	}
	return "", false
}

type goroutineWaitGroup struct {
	sync.WaitGroup
}

func (wg *goroutineWaitGroup) Go(f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
