// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

type recordConsumer interface {
	// consume should return an error only if there is a recoverable error. Returning an error will cause consumption to slow down.
	consume(context.Context, *Segment) error
}

type PartitionReader struct {
	services.Service
	dependencies *services.Manager

	segmentReader *SegmentReader
	metadataDB    MetadataStoreDatabase
	metadataStore *MetadataStore
	bucketClient  objstore.InstrumentedBucket

	config        Config
	partitionID   int32
	consumerGroup string

	consumer recordConsumer
	metrics  readerMetrics

	committer      *partitionCommitter
	commitInterval time.Duration

	// consumedOffsetWatcher is used to wait until a given offset has been consumed.
	// This gets initialised with -1 which means nothing has been consumed from the partition yet.
	consumedOffsetWatcher *partitionOffsetWatcher
	offsetReader          *partitionOffsetReader

	logger log.Logger
	reg    prometheus.Registerer
}

func NewPartitionReaderForPusher(config Config, partitionID int32, consumerGroup string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	consumer := newPusherConsumer(pusher, reg, logger)
	metadataDB := NewMetadataStorePostgresql(config.PostgresConfig)

	return newPartitionReader(config, metadataDB, partitionID, consumerGroup, consumer, logger, reg)
}

func newPartitionReader(config Config, metadataDB MetadataStoreDatabase, partitionID int32, consumerGroup string, consumer recordConsumer, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	bucketClient, err := bucket.NewClient(context.Background(), config.Bucket, "segment-store", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create segment store bucket client")
	}

	r := &PartitionReader{
		config:                config,
		bucketClient:          bucketClient,
		metadataDB:            metadataDB,
		partitionID:           partitionID,
		consumer:              consumer,
		consumerGroup:         consumerGroup,
		metrics:               newReaderMetrics(reg),
		commitInterval:        time.Second,
		consumedOffsetWatcher: newPartitionOffsetWatcher(),
		logger:                log.With(logger, "partition", partitionID),
		reg:                   prometheus.WrapRegistererWith(prometheus.Labels{"component": "partition-reader"}, reg),
	}

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) error {
	// We need to start the metadata store in order to fetch the last committed offset.
	r.metadataStore = NewMetadataStore(r.metadataDB, r.logger)
	if err := services.StartAndAwaitRunning(ctx, r.metadataStore); err != nil {
		return errors.Wrap(err, "failed to start metadata store")
	}

	// Fetch the last committed offset.
	startFromOffset, err := r.fetchLastCommittedOffsetWithRetries(ctx)
	if err != nil {
		return err
	}
	r.consumedOffsetWatcher.Notify(startFromOffset - 1)
	level.Info(r.logger).Log("msg", "resuming consumption from offset", "offset", startFromOffset)

	// Start dependency services.
	r.committer = newConsumerCommitter(r.metadataStore, r.partitionID, r.consumerGroup, r.commitInterval, r.logger)
	r.offsetReader = newPartitionOffsetReader(r.metadataStore, r.partitionID, r.config.LastProducedOffsetPollInterval, r.reg, r.logger)
	r.segmentReader = NewSegmentReader(r.bucketClient, r.metadataStore, r.partitionID, startFromOffset, r.config.BufferSize, r.reg, r.logger)

	r.dependencies, err = services.NewManager(r.committer, r.offsetReader, r.consumedOffsetWatcher, r.segmentReader)
	if err != nil {
		return errors.Wrap(err, "creating service manager")
	}
	err = services.StartManagerAndAwaitHealthy(ctx, r.dependencies)
	if err != nil {
		return errors.Wrap(err, "starting service manager")
	}
	return nil
}

func (r *PartitionReader) stop(error) error {
	level.Info(r.logger).Log("msg", "stopping partition reader")

	if err := services.StopManagerAndAwaitStopped(context.Background(), r.dependencies); err != nil {
		return errors.Wrap(err, "stopping dependencies")
	}

	if err := services.StopAndAwaitTerminated(context.Background(), r.metadataStore); err != nil {
		return errors.Wrap(err, "stopping metadata store")
	}

	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	for ctx.Err() == nil {
		segment, err := r.segmentReader.WaitNextSegment(ctx)
		if err != nil {
			level.Error(r.logger).Log("msg", "failed waiting next segment", "err", err)
			r.metrics.pollErrors.Add(1)
			continue
		}

		r.recordSegmentMetrics(segment)

		// TODO consumeSegment() may get interrupted in the middle because of ctx canceled due to PartitionReader stopped.
		// 		We should improve it, but we shouldn't just pass a context.Background() because if consumption is stuck
		// 		then PartitionReader will never stop.
		r.consumeSegment(ctx, segment)
		r.committer.enqueueOffset(segment.Ref.OffsetID)
		r.consumedOffsetWatcher.Notify(segment.Ref.OffsetID)
	}

	return nil
}

func (r *PartitionReader) consumeSegment(ctx context.Context, segment *Segment) {
	if len(segment.Data.Pieces) == 0 {
		return
	}

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	for boff.Ongoing() {
		err := r.consumer.consume(ctx, segment)
		if err == nil {
			break
		}
		level.Error(r.logger).Log(
			"msg", "encountered error while ingesting segment; will retry",
			"err", err,
			"segment_offset", segment.Ref.OffsetID,
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}
}

func (r *PartitionReader) recordSegmentMetrics(segment *Segment) {
	// TODO record receiveDelay for each piece
	// Need to add timestamp to pieces when pushed

	r.metrics.segmentsTotal.Add(1)
	r.metrics.piecesPerSegment.Observe(float64(len(segment.Data.Pieces)))
}

func (r *PartitionReader) fetchLastCommittedOffsetWithRetries(ctx context.Context) (offset int64, err error) {
	var (
		retry = backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 2 * time.Second,
			MaxRetries: 10,
		})
	)

	for retry.Ongoing() {
		offset, err = r.metadataStore.GetLastConsumedOffsetID(ctx, r.partitionID, r.consumerGroup)
		if err == nil {
			return offset, nil
		}

		level.Warn(r.logger).Log("msg", "failed to fetch last committed offset", "err", err)
		retry.Wait()
	}

	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = retry.Err()
	}

	return offset, err
}

// WaitReadConsistency waits until all data produced up until now has been consumed by the reader.
func (r *PartitionReader) WaitReadConsistency(ctx context.Context) (returnErr error) {
	startTime := time.Now()
	r.metrics.strongConsistencyRequests.Inc()

	defer func() {
		// Do not track failure or latency if the request was canceled (because the tracking would be incorrect).
		if errors.Is(returnErr, context.Canceled) {
			return
		}

		// Track latency for failures too, so that we have a better measurement of latency if
		// backend latency is high and requests fail because of timeouts.
		r.metrics.strongConsistencyLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			r.metrics.strongConsistencyFailures.Inc()
		}
	}()

	// Ensure the service is running. Some subservices used below are created when starting
	// so they're not available before that.
	if state := r.Service.State(); state != services.Running {
		return fmt.Errorf("partition reader service is not running (state: %s)", state.String())
	}

	// Get the last produced offset.
	lastProducedOffset, err := r.offsetReader.FetchLastProducedOffset(ctx)
	if err != nil {
		return err
	}

	// Then wait for it.
	return r.consumedOffsetWatcher.Wait(ctx, lastProducedOffset)
}

type partitionCommitter struct {
	services.Service

	metadataStore  *MetadataStore
	commitInterval time.Duration
	partitionID    int32
	consumerGroup  string

	toCommit *atomic.Int64

	logger log.Logger
}

func newConsumerCommitter(metadataStore *MetadataStore, partitionID int32, consumerGroup string, commitInterval time.Duration, logger log.Logger) *partitionCommitter {
	c := &partitionCommitter{
		metadataStore:  metadataStore,
		logger:         logger,
		partitionID:    partitionID,
		consumerGroup:  consumerGroup,
		toCommit:       atomic.NewInt64(-1),
		commitInterval: commitInterval,
	}
	c.Service = services.NewBasicService(nil, c.run, c.stop)
	return c
}

func (r *partitionCommitter) enqueueOffset(o int64) {
	r.toCommit.Store(o)
}

func (r *partitionCommitter) run(ctx context.Context) error {
	commitTicker := time.NewTicker(r.commitInterval)
	defer commitTicker.Stop()

	previousOffset := r.toCommit.Load()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-commitTicker.C:
			currOffset := r.toCommit.Load()
			if currOffset == previousOffset {
				continue
			}
			previousOffset = currOffset
			r.commit(ctx, currOffset)
		}
	}
}

func (r *partitionCommitter) commit(ctx context.Context, offset int64) {
	err := r.metadataStore.CommitLastConsumedOffset(ctx, r.partitionID, r.consumerGroup, offset)
	if err != nil {
		level.Error(r.logger).Log("msg", "encountered error while committing offsets", "err", err, "offset", offset)
	} else {
		level.Debug(r.logger).Log("msg", "committed offset", "offset", offset)
	}
}

func (r *partitionCommitter) stop(error) error {
	offset := r.toCommit.Load()
	if offset < 0 {
		return nil
	}
	// Commit has internal timeouts, so this call shouldn't block for too long.
	r.commit(context.Background(), offset)
	return nil
}

type readerMetrics struct {
	receiveDelay              prometheus.Summary
	piecesPerSegment          prometheus.Histogram
	pollErrors                prometheus.Counter
	segmentsTotal             prometheus.Counter
	strongConsistencyRequests prometheus.Counter
	strongConsistencyFailures prometheus.Counter
	strongConsistencyLatency  prometheus.Summary
}

func newReaderMetrics(reg prometheus.Registerer) readerMetrics {
	factory := promauto.With(reg)

	return readerMetrics{
		receiveDelay: factory.NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_reader_receive_delay_seconds",
			Help:       "Delay between producing a record and receiving it in the consumer.",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		segmentsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_segments_total",
			Help: "Total number of segments received by the consumer.",
		}),
		piecesPerSegment: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_reader_pieces_per_segment",
			Help:    "The number of pieces received by the consumer in a single segment.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		}),
		pollErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_poll_errors_total",
			Help: "The number of poll errors encountered by the consumer.",
		}),
		strongConsistencyRequests: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_strong_consistency_requests_total",
			Help: "Total number of requests for which strong consistency has been requested.",
		}),
		strongConsistencyFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_strong_consistency_failures_total",
			Help: "Total number of failures while waiting for strong consistency to be enforced.",
		}),
		strongConsistencyLatency: factory.NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
			Help:       "How long a request spent waiting for strong consistency to be guaranteed.",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
	}
}
