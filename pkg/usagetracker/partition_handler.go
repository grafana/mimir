// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

type partitionHandler struct {
	services.Service

	store *trackerStore

	cfg    Config
	logger log.Logger

	// registerer is the global usage-tracker registered
	registerer prometheus.Registerer
	// partitionRegisterer is registerer wrapped with partition label,
	// to register and un-register the tracker store at the correct moment.
	partitionRegisterer prometheus.Registerer
	// collector is a prometheus registerer wrapped with partition label,
	// this holds all the metrics, especially the ones for Kafka client.
	// It's used to overcome the Kafka client limitation as it's impossible to un-register its metrics,
	// so we un-register the entire collector instead.
	collector prometheus.Collector

	partitionID int32

	// Partition and instance ring.
	partitionLifecycler *ring.PartitionInstanceLifecycler

	// Events storage (Kafka).
	eventsKafkaWriter *kgo.Client
	eventsKafkaReader *kgo.Client

	// Snapshots metadata storage (Kafka).
	snapshotsKafkaReader *kgo.Client
	snapshotsKafkaWriter *kgo.Client
	snapshotsBucket      objstore.Bucket
	snapshotsFromEvents  chan *usagetrackerpb.SnapshotEvent

	// ignoreSnapshotEventsUntil is the offset of the last snapshot event that was included in the snapshot we loaded at startup.
	ignoreSnapshotEventsUntil int64
	lastPublishedEventOffset  atomic.Int64
	// loadedLastSnapshotTimestamp is the timestamp of the last snapshot we loaded at startup.
	// it's set only once and is not updated.
	loadedLastSnapshotTimestamp time.Time

	pendingCreatedSeriesMarshaledEvents chan []byte

	eventFetchFailures prometheus.Counter

	seriesCreatedEventsReceived        prometheus.Counter
	seriesCreatedEventsTotalErrors     *prometheus.CounterVec
	seriesCreatedEventsPublishFailures prometheus.Counter

	snapshotEventsReceived        prometheus.Counter
	snapshotEventsTotalErrors     *prometheus.CounterVec
	snapshotEventsPublishFailures prometheus.Counter

	// Dependencies.
	subservicesWatcher *services.FailureWatcher

	// Misc
	instanceIDBytes []byte

	// Testing
	onConsumeEvent func(eventType string)

	forceUpdateLimitsForTests chan chan struct{}
}

func newPartitionHandler(
	partitionID int32,
	cfg Config,
	partitionKVClient kv.Client,
	eventsKafkaWriter *kgo.Client,
	snapshotsKafkaWriter *kgo.Client,
	snapshotsBucket objstore.InstrumentedBucket,
	lim limiter,
	logger log.Logger,
	registerer prometheus.Registerer,
) (*partitionHandler, error) {
	reg := prometheus.NewRegistry()
	logger = log.With(logger, "partition", partitionID)
	partitionLabels := prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}
	p := &partitionHandler{
		cfg:                 cfg,
		logger:              logger,
		registerer:          registerer,
		partitionRegisterer: prometheus.WrapRegistererWith(partitionLabels, registerer),
		collector:           prometheus.WrapCollectorWith(partitionLabels, reg),

		partitionID: partitionID,

		eventsKafkaWriter: eventsKafkaWriter,

		snapshotsKafkaWriter: snapshotsKafkaWriter,
		snapshotsBucket:      snapshotsBucket,
		snapshotsFromEvents:  make(chan *usagetrackerpb.SnapshotEvent, shards),

		pendingCreatedSeriesMarshaledEvents: make(chan []byte, cfg.CreatedSeriesEventsMaxPending),

		eventFetchFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_event_fetch_failures_total",
			Help: "Total number of failures while fetching events from Kafka.",
		}),

		seriesCreatedEventsReceived: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_series_created_events_received_total",
			Help: "Total number of series created events received from Kafka.",
		}),
		seriesCreatedEventsTotalErrors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_series_created_events_errors_total",
			Help: "Total number of errors while processing series created events from Kafka.",
		}, []string{"error"}),
		seriesCreatedEventsPublishFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_series_created_events_publish_failures_total",
			Help: "Total number of failures while publishing series created events to Kafka.",
		}),

		snapshotEventsReceived: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_events_received_total",
			Help: "Total number of snapshot events received from Kafka.",
		}),
		snapshotEventsTotalErrors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_events_errors_total",
			Help: "Total number of errors while processing snapshot events from Kafka.",
		}, []string{"error"}),
		snapshotEventsPublishFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_snapshot_events_publish_failures_total",
			Help: "Total number of failures while publishing snapshot events to Kafka.",
		}),

		instanceIDBytes: []byte(cfg.InstanceRing.InstanceID),

		forceUpdateLimitsForTests: make(chan chan struct{}),
	}

	// In the partition ring lifecycler one owner can only have one partition, so we create a sub-owner for this partition, because we (may) own multiple partitions.
	instanceID := fmt.Sprintf("%s/%d", cfg.InstanceRing.InstanceID, partitionID)

	var err error
	p.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, p.partitionID, instanceID, partitionKVClient, logger, reg)
	if err != nil {
		return nil, err
	}

	// Create Kafka reader for events storage.
	p.eventsKafkaReader, err = ingest.NewKafkaReaderClient(p.cfg.EventsStorageReader, ingest.NewKafkaReaderClientMetrics(readerMetricsPrefix, eventsKafkaReaderComponent, reg), p.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka reader client for usage-tracker")
	}

	p.snapshotsKafkaReader, err = ingest.NewKafkaReaderClient(p.cfg.SnapshotsMetadataReader, ingest.NewKafkaReaderClientMetrics(readerMetricsPrefix, snapshotsKafkaReaderComponent, reg), p.logger,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.SnapshotsMetadataReader.Topic: {p.partitionID: kgo.NewOffset().AtEnd().Relative(-1)},
		}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka reader client for usage-tracker snapshots")
	}

	eventsPublisher := chanEventsPublisher{events: p.pendingCreatedSeriesMarshaledEvents, logger: logger}
	p.store = newTrackerStore(cfg.IdleTimeout, cfg.UserCloseToLimitPercentageThreshold, logger, lim, eventsPublisher)
	p.Service = services.NewBasicService(p.start, p.run, p.stop)
	return p, nil
}

func (p *partitionHandler) setRemoveOwnerOnShutdown(removeOwnerOnShutdown bool) {
	p.partitionLifecycler.SetRemoveOwnerOnShutdown(removeOwnerOnShutdown)
}

// start implements services.StartingFn.
func (p *partitionHandler) start(ctx context.Context) error {
	if err := p.registerer.Register(p.collector); err != nil {
		return errors.Wrap(err, "unable to register partition handler collector as Prometheus collector")
	}

	eventsOffset, err := p.loadLastSnapshotRecordAndGetEventsOffset(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to load snapshot")
	}
	if err := p.loadEvents(ctx, eventsOffset); err != nil {
		return errors.Wrap(err, "unable to load events")
	}
	// Initial limits are lower than real limits, and we've just loaded the snapshots and processed events,
	// so update the limits to let users track more series.
	p.store.updateLimits()

	// Register the tracker store only after loading snapshots & events.
	if err := p.partitionRegisterer.Register(p.store); err != nil {
		return errors.Wrap(err, "unable to register tracker store as Prometheus collector")
	}

	// Do this only once ready
	if err := services.StartAndAwaitRunning(ctx, p.partitionLifecycler); err != nil {
		return errors.Wrap(err, "unable to start partition lifecycler")
	}

	p.subservicesWatcher = services.NewFailureWatcher()
	p.subservicesWatcher.WatchService(p.partitionLifecycler)
	return nil
}

func (p *partitionHandler) loadLastSnapshotRecordAndGetEventsOffset(ctx context.Context) (int64, error) {
	// Default offset to start, to avoid returning nonsense.
	snapshotsTopic := p.cfg.SnapshotsMetadataReader.Topic

	if p.cfg.SkipSnapshotLoadingAtStartup {
		level.Warn(p.logger).Log("msg", "configured to skip snapshot loading at startup")
		return 0, nil
	}

	offset, err := findEndOffset(ctx, p.snapshotsKafkaReader, snapshotsTopic, p.partitionID)
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

	level.Info(p.logger).Log("msg", "loading snapshot record", "offset", record.Offset, "record_timestamp", record.Timestamp, "snapshot_timestamp", snapshot.Timestamp)
	if err := p.loadAllSnapshotShards(ctx, snapshot.Filenames); err != nil {
		return 0, errors.Wrapf(err, "failed to load snapshot")
	}

	p.loadedLastSnapshotTimestamp = time.UnixMilli(snapshot.Timestamp)
	p.ignoreSnapshotEventsUntil = snapshot.LastSnapshotEventOffset

	return snapshot.LastEventOffsetPublishedBeforeSnapshot, nil
}

func (p *partitionHandler) loadAllSnapshotShards(ctx context.Context, files []string) error {
	t0 := time.Now()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	type downloadedSnapshot struct {
		filename string
		data     [][]byte
	}

	// One is being downloaded, one is buffered, one is being processed.
	downloaded := make(chan downloadedSnapshot, 1)
	errs := make(chan error, 1)
	go func() {
		for snapshot := range downloaded {
			snapshotT0 := time.Now()
			if err := p.store.loadSnapshots(snapshot.data, time.Now()); err != nil {
				errs <- errors.Wrapf(err, "failed to load snapshot data from file %q", snapshot.filename)
				return
			}
			level.Info(p.logger).Log("msg", "loaded snapshot file", "filename", snapshot.filename, "shards", len(snapshot.data), "snapshot_load_time", time.Since(snapshotT0), "total_elapsed", time.Since(t0))
		}
		errs <- nil
	}()

	go func() {
		defer close(downloaded)
		buf := new(bytes.Buffer)
		for i, filename := range files {
			fileT0 := time.Now()
			n, err := p.loadSnapshotIntoBufferWithBackoff(ctx, filename, buf)
			if err != nil {
				errs <- errors.Wrapf(err, "failed to load snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}

			var file usagetrackerpb.SnapshotFile
			if err := file.Unmarshal(buf.Bytes()); err != nil {
				errs <- errors.Wrapf(err, "failed to unmarshal snapshot file %s from bucket %s", filename, p.snapshotsBucket.Name())
				return
			}

			level.Info(p.logger).Log("msg", "downloaded snapshot file", "file_index", i, "filename", filename, "file_download_time", time.Since(fileT0), "total_elapsed", time.Since(t0), "bytes", n)
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
		cancel(err)
		if err == nil {
			level.Info(p.logger).Log("msg", "all snapshot shards loaded successfully", "elapsed", time.Since(t0))
		}
		return err
	}
}

func (p *partitionHandler) loadSnapshotIntoBufferWithBackoff(ctx context.Context, filename string, buf *bytes.Buffer) (int64, error) {
	backoff := backoff.New(ctx, p.cfg.SnapshotsLoadBackoff)
	var err error
	for backoff.Ongoing() {
		var rc io.ReadCloser
		rc, err = p.snapshotsBucket.Get(ctx, filename)
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to get snapshot file from bucket", "filename", filename, "err", err, "retries", backoff.NumRetries())
			err = errors.Wrapf(err, "failed to get snapshot file %s from bucket %s (%d retries)", filename, p.snapshotsBucket.Name(), backoff.NumRetries())
			backoff.Wait()
			continue
		}
		buf.Reset()
		var n int64
		n, err = buf.ReadFrom(rc)
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to read snapshot file from bucket", "filename", filename, "err", err, "retries", backoff.NumRetries())
			err = errors.Wrapf(err, "failed to read snapshot file %s from bucket %s (%d retries)", filename, p.snapshotsBucket.Name(), backoff.NumRetries())
			backoff.Wait()
			continue
		}
		if err := rc.Close(); err != nil {
			level.Error(p.logger).Log("msg", "failed to close snapshot file from bucket", "filename", filename, "err", err, "retries", backoff.NumRetries())
			// Retrying here would only make the situation worse.
		}

		return n, nil
	}
	return 0, fmt.Errorf("%w (%s)", err, backoff.Err())
}

// loadEvents loads the events from the last snapshot (or from the idle timeout) up to the last one.
// In this method:
// eventsOffset:
// - Is the offset of the last event published before the loaded snapshot was taken.
// - If it's zero, then no snapshot was loaded. If it's non-zero, we should start consuming events from that offset.
// endOffset:
// - Is the end offset of the events topic, which is the offset of the next record that will be published.
// lastExpectedEventOffset:
// - Is endOffset - 1: we expect to load events up to this offset.
func (p *partitionHandler) loadEvents(ctx context.Context, eventsOffset int64) error {
	endOffset, err := findEndOffset(ctx, p.eventsKafkaReader, p.cfg.EventsStorageReader.Topic, p.partitionID)
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
	lastExpectedEventOffset := endOffset - 1 // Because endOffset is the "next" offset.
	level.Info(p.logger).Log("msg", "end offset for events topic", "topic", p.cfg.EventsStorageReader.Topic, "end_offset", endOffset, "last_expected_event_offset", lastExpectedEventOffset)

	if p.cfg.SkipSnapshotLoadingAtStartup {
		p.ignoreSnapshotEventsUntil = lastExpectedEventOffset
	}

	if lastExpectedEventOffset < eventsOffset {
		level.Error(p.logger).Log("msg", "snapshot suggested to start consuming events at an offset that is greater than the last expected offset of the topic", "last_expected_event_offset", lastExpectedEventOffset, "snapshot_offset", eventsOffset, "topic", p.cfg.EventsStorageReader.Topic)
		return nil
	}

	if eventsOffset == 0 {
		firstOffsetAfterIdleTimeout, err := findOffsetAfter(ctx, p.eventsKafkaReader, p.cfg.EventsStorageReader.Topic, p.partitionID, time.Now().Add(-p.cfg.IdleTimeout))
		if err != nil {
			return errors.Wrapf(err, "failed to find offset after idle timeout for topic %s", p.cfg.EventsStorageReader.Topic)
		}

		// It doesn't make sense to start consuming events from the beginning of the topic, we should read only the latest IdleTimeout minutes.
		startConsumingEventsAtMillis := time.Now().Add(-p.cfg.IdleTimeout).UnixMilli()
		p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.EventsStorageWriter.Topic: {p.partitionID: kgo.NewOffset().AfterMilli(startConsumingEventsAtMillis)},
		})

		if firstOffsetAfterIdleTimeout >= endOffset {
			level.Info(p.logger).Log("msg", "events offset from snapshot was zero, and there are no events after idle timeout, so not loading any event", "topic", p.cfg.EventsStorageReader.Topic, "first_offset_after_idle_timeout", firstOffsetAfterIdleTimeout, "end_offset", endOffset)
			return nil
		}

		level.Info(p.logger).Log("msg", "events offset from snapshot was zero, starting consumption at idle timeout", "topic", p.cfg.EventsStorageReader.Topic, "first_offset_after_idle_timeout", firstOffsetAfterIdleTimeout, "end_offset", endOffset)
	} else {
		level.Info(p.logger).Log("msg", "starting to consume events from snapshot offset", "topic", p.cfg.EventsStorageReader.Topic, "snapshot_offset", eventsOffset, "end_offset", endOffset)
		// If the end offset is greater than the snapshot offset, we need to start consuming events from the snapshot offset.
		p.eventsKafkaReader.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			p.cfg.EventsStorageWriter.Topic: {p.partitionID: kgo.NewOffset().At(eventsOffset + 1)}, // Start at the next offset after the snapshot offset.
		})
	}

	lastEventOffset := int64(-1)
	gotEvents := 0
	for lastEventOffset < lastExpectedEventOffset && ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := p.eventsKafkaReader.PollRecords(pollCtx, p.cfg.MaxEventsFetchSize)
		cancel()

		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				level.Info(p.logger).Log("msg", "didn't receive events to load within timeout, retrying", "start_offset", eventsOffset, "last_event_offset", lastEventOffset, "last_expected_event_offset", lastExpectedEventOffset)
				continue
			}
			return errors.Wrap(err, "failed to load events")
		}

		fetches.EachRecord(func(r *kgo.Record) {
			gotEvents++
			// We don't ignore our own events here, because they potentially were sent after we made the snapshot.
			eventType, ok := recordHeader(r, eventTypeRecordHeader)
			if !ok {
				level.Error(p.logger).Log("msg", "record does not have event_type header, ignoring", "offset", r.Offset, "value_len", len(r.Value))
			}

			switch eventType {
			case eventTypeSeriesCreated:
				p.processSeriesCreatedEventRecord(r)
			case eventTypeSnapshot:
				p.processSnapshotEventSync(ctx, r)
			}
			lastEventOffset = r.Offset
		})
	}

	level.Info(p.logger).Log("msg", "finished loading events", "topic", p.cfg.EventsStorageReader.Topic, "start_offset", eventsOffset, "last_event_offset", lastEventOffset, "last_expected_event_offset", lastExpectedEventOffset, "got_events", gotEvents)
	return ctx.Err()
}

// run implements services.RunningFn.
func (p *partitionHandler) run(ctx context.Context) error {
	cleanup := time.NewTicker(time.Minute)
	defer cleanup.Stop()

	updateLimits := time.NewTicker(time.Second)
	defer updateLimits.Stop()

	wg := &goroutineWaitGroup{}
	wg.Go(func() { p.consumeEvents(ctx) })
	wg.Go(func() { p.publishSeriesCreatedEvents(ctx) })
	wg.Go(func() { p.loadSnapshotsFromEvents(ctx) })
	wg.Go(func() { p.publishSnapshots(ctx) })
	defer wg.Wait()

	for {
		select {
		case now := <-cleanup.C:
			p.store.cleanup(now)
		case <-updateLimits.C:
			p.store.updateLimits()
		case done := <-p.forceUpdateLimitsForTests:
			p.store.updateLimits()
			close(done)
		case <-ctx.Done():
			return nil
		case err := <-p.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

// stop implements services.StoppingFn.
func (p *partitionHandler) stop(_ error) error {
	p.registerer.Unregister(p.collector)
	p.partitionRegisterer.Unregister(p.store)
	// Stop dependencies.
	err := services.StopAndAwaitTerminated(context.Background(), p.partitionLifecycler)

	// Close our read client, don't close the write client.
	p.eventsKafkaReader.Close()
	p.snapshotsKafkaReader.Close()
	return err
}

func (p *partitionHandler) consumeEvents(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := p.eventsKafkaReader.PollRecords(ctx, p.cfg.MaxEventsFetchSize)
		fetches.EachError(func(_ string, part int32, err error) {
			if !errors.Is(err, context.Canceled) { // Ignore when we're shutting down.
				p.eventFetchFailures.Inc()
				level.Error(p.logger).Log("msg", "failed to fetch event records", "fetch_partition", part, "err", err)
			}
		})

		fetches.EachRecord(func(r *kgo.Record) {
			instanceID, eventType, err := eventRecordHeaders(r)
			if err != nil {
				level.Error(p.logger).Log("msg", "record does not have all required headers, ignoring", "offset", r.Offset, "value_len", len(r.Value), "err", err)
				return
			}
			if instanceID == p.cfg.InstanceRing.InstanceID {
				level.Debug(p.logger).Log("msg", "ignoring our own event", "offset", r.Offset, "type", eventType)
				return
			}
			switch eventType {
			case eventTypeSeriesCreated:
				p.processSeriesCreatedEventRecord(r)
			case eventTypeSnapshot:
				p.processSnapshotEventAsync(r)
			}
			if p.onConsumeEvent != nil {
				p.onConsumeEvent(eventType) // For testing purposes, to know that we consumed an event.
			}
		})
	}
}

func (p *partitionHandler) processSeriesCreatedEventRecord(r *kgo.Record) {
	p.seriesCreatedEventsReceived.Inc()

	var ev usagetrackerpb.SeriesCreatedEvent
	if err := ev.Unmarshal(r.Value); err != nil {
		p.seriesCreatedEventsTotalErrors.WithLabelValues("unmarshal").Inc()
		level.Error(p.logger).Log("msg", "failed to unmarshal series created event", "err", err, "offset", r.Offset, "value_len", len(r.Value))
		return
	}

	p.store.processCreatedSeriesEvent(ev.UserID, ev.SeriesHashes, time.Unix(ev.Timestamp, 0), time.Now())
	level.Debug(p.logger).Log("msg", "processed series created event", "offset", r.Offset, "series", len(ev.SeriesHashes))
}

func (p *partitionHandler) processSnapshotEventSync(ctx context.Context, r *kgo.Record) {
	p.processSnapshotEvent(r, func(ev *usagetrackerpb.SnapshotEvent) {
		p.loadSnapshot(ctx, ev, new(bytes.Buffer))
	})
}

func (p *partitionHandler) processSnapshotEventAsync(r *kgo.Record) {
	p.processSnapshotEvent(r, func(ev *usagetrackerpb.SnapshotEvent) {
		select {
		case p.snapshotsFromEvents <- ev:
			return
		default:
			p.logger.Log("msg", "dropping shard snapshot event, channel is full", "offset", r.Offset, "timestamp", time.UnixMilli(ev.Timestamp))
		}
	})
}

func (p *partitionHandler) processSnapshotEvent(r *kgo.Record, process func(event *usagetrackerpb.SnapshotEvent)) {
	if r.Offset <= p.ignoreSnapshotEventsUntil {
		// Ignore snapshot events that were made before the last snapshot we loaded.
		level.Info(p.logger).Log("msg", "ignoring shard snapshot event", "offset", r.Offset, "ignore_until", p.ignoreSnapshotEventsUntil)
		return
	}

	var ev usagetrackerpb.SnapshotEvent
	if err := ev.Unmarshal(r.Value); err != nil {
		p.snapshotEventsTotalErrors.WithLabelValues("unmarshal_event").Inc()
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

func (p *partitionHandler) loadSnapshotsFromEvents(ctx context.Context) {
	buf := bytes.NewBuffer(nil)
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-p.snapshotsFromEvents:
			p.loadSnapshot(ctx, r, buf)
		}
	}
}

func (p *partitionHandler) loadSnapshot(ctx context.Context, r *usagetrackerpb.SnapshotEvent, buf *bytes.Buffer) {
	p.snapshotEventsReceived.Inc()

	t0 := time.Now()
	n, err := p.loadSnapshotIntoBufferWithBackoff(ctx, r.Filename, buf)
	if err != nil {
		p.snapshotEventsTotalErrors.WithLabelValues("download").Inc()
		level.Error(p.logger).Log("msg", "failed to load snapshot file", "filename", r.Filename, "err", err)
		return
	}

	tl := time.Now()

	var file usagetrackerpb.SnapshotFile
	if err := file.Unmarshal(buf.Bytes()); err != nil {
		p.snapshotEventsTotalErrors.WithLabelValues("unmarshal_file").Inc()
		level.Error(p.logger).Log("msg", "failed to unmarshal snapshot file", "filename", r.Filename, "err", err)
		return
	}

	if err := p.store.loadSnapshots(file.Data, time.Now()); err != nil {
		p.snapshotEventsTotalErrors.WithLabelValues("load").Inc()
		level.Error(p.logger).Log("msg", "failed to load snapshot data", "filename", r.Filename, "err", err)
		return
	}

	level.Info(p.logger).Log("msg", "loaded snapshot from events", "filename", r.Filename, "shards", len(file.Data), "bytes", n, "elapsed_total", time.Since(t0), "elapsed_load", time.Since(tl))
}

func (p *partitionHandler) publishSeriesCreatedEvents(ctx context.Context) {
	wg := &goroutineWaitGroup{}
	defer wg.Wait()

	publish := make(chan []*kgo.Record)
	defer close(publish)

	for w := 0; w < p.cfg.CreatedSeriesEventsPublishConcurrency; w++ {
		wg.Go(func() {
			for batch := range publish {
				level.Debug(p.logger).Log("msg", "producing batch of series created events", "size", len(batch))
				// TODO: make sure we don't send the traceparent header here.
				res := p.eventsKafkaWriter.ProduceSync(ctx, batch...)
				for _, r := range res {
					if r.Err != nil {
						lvl := level.Error
						if errors.Is(r.Err, context.Canceled) {
							lvl = level.Debug
						}

						p.seriesCreatedEventsPublishFailures.Inc()
						lvl(p.logger).Log("msg", "failed to publish series created event", "err", r.Err)
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

func (p *partitionHandler) newEventKafkaRecord(eventTypeBytes []byte, data []byte) *kgo.Record {
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

func (p *partitionHandler) publishSnapshots(ctx context.Context) {
	delay := util.DurationWithJitter(p.cfg.IdleTimeout/2, p.cfg.SnapshotIntervalJitter)
	var firstSnapshot <-chan time.Time
	if !p.loadedLastSnapshotTimestamp.IsZero() {
		nextExpectedSnapshot := p.loadedLastSnapshotTimestamp.Add(delay)
		delayUntilFirstSnapshot := max(time.Until(nextExpectedSnapshot), time.Second)
		firstSnapshot = time.After(delayUntilFirstSnapshot)
		level.Info(p.logger).Log("msg", "first snapshot scheduled", "delay", delay, "first_snapshot", time.Now().Add(delay), "loaded_last_snapshot_timestamp", p.loadedLastSnapshotTimestamp.UTC().Format(time.RFC3339))
	}

	for {
		level.Info(p.logger).Log("msg", "next snapshot scheduled", "delay", delay, "next_snapshot", time.Now().UTC().Add(delay).Format(time.RFC3339))
		select {
		case <-ctx.Done():
			return
		case <-firstSnapshot:
			if err := p.publishSnapshot(ctx); err != nil {
				p.snapshotEventsPublishFailures.Inc()
				level.Error(p.logger).Log("msg", "failed to publish snapshot", "err", err)
			}
		case <-time.After(delay):
			// If this happens before firstSnapshot, there's no need to do that one.
			firstSnapshot = nil

			if err := p.publishSnapshot(ctx); err != nil {
				p.snapshotEventsPublishFailures.Inc()
				level.Error(p.logger).Log("msg", "failed to publish snapshot", "err", err)
			}
		}
	}
}

func (p *partitionHandler) publishSnapshot(ctx context.Context) error {
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

		fileData, err := file.Marshal()
		if err != nil {
			level.Error(p.logger).Log("msg", "failed to marshal snapshot file", "err", err)
			return errors.Wrap(err, "failed to marshal snapshot file")
		}
		filename := snapshotFilename(time.Now(), p.cfg.InstanceRing.InstanceID, p.partitionID)
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
		filenames = append(filenames, filename)

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

	level.Info(p.logger).Log("msg", "wrote snapshot files and published events", "files", len(filenames), "filenames", strings.Join(filenames, ","), "elapsed", time.Since(t0), "total_bytes", totalDataLen)

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
		Topic: p.cfg.SnapshotsMetadataWriter.Topic,
		Headers: []kgo.RecordHeader{
			{Key: instanceRecordHeader, Value: p.instanceIDBytes},
		},
		Value:     data,
		Partition: p.partitionID,
	}
	r, err := p.snapshotsKafkaWriter.ProduceSync(ctx, snapshotRecord).First()
	if err != nil {
		level.Error(p.logger).Log("msg", "failed to publish snapshot record", "err", err)
		return errors.Wrap(err, "failed to publish snapshot record")
	}
	level.Info(p.logger).Log("msg", "published snapshot record", "offset", r.Offset, "last_event_offset_published_before_snapshot", firstSnapshotEventOffset, "last_snapshot_event_offset", lastSnapshotEventOffset)

	return nil
}

// findEndOffset retrieves the end offset for the given topic using the provided Kafka client.
// This offset is the offset of the "next" message in the topic, so if it's 0, there are no messages yet.
// https://github.com/grafana/mimir/blob/392588b98e386c0fd7cdd549a54e5f06d3155bc2/pkg/storage/ingest/DESIGN.md
func findEndOffset(ctx context.Context, client *kgo.Client, topic string, partition int32) (int64, error) {
	readAdmin := kadm.NewClient(client)

	offsets, err := readAdmin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get end offsets: %w", err)
	}
	if offsets.Error() != nil {
		return 0, fmt.Errorf("failed to get end offsets, offsets error: %w", offsets.Error())
	}
	topicOffset, ok := offsets.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("failed to get end offsets, topic or partition not found, make sure the partition exists. Got offsets: %#v", offsets)
	}

	return topicOffset.Offset, nil
}

// findOffsetAfter retrieves the offset of the first message after the given time for the specified topic and partition.
// Similarly to findEndOffset, if there are no such offsets in the partition, it returns the offset of the next message that is going to be produced.
func findOffsetAfter(ctx context.Context, client *kgo.Client, topic string, partition int32, t time.Time) (int64, error) {
	readAdmin := kadm.NewClient(client)

	offsets, err := readAdmin.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get offsets after %s: %w", t, err)
	}
	if offsets.Error() != nil {
		return 0, fmt.Errorf("failed to get offsets after %s, offsets error: %w", t, offsets.Error())
	}
	topicOffset, ok := offsets.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("failed to get offsets after %s, topic or partition not found, offsets: %#v", t, offsets)
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
