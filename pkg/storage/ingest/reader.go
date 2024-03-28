// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"
)

const (
	// kafkaStartOffset is a special offset value that means the beginning of the partition.
	kafkaStartOffset = int64(-2)

	// kafkaEndOffset is a special offset value that means the end of the partition.
	kafkaEndOffset = int64(-1)
)

type record struct {
	tenantID string
	content  []byte
}

type recordConsumer interface {
	// consume should return an error only if there is a recoverable error. Returning an error will cause consumption to slow down.
	consume(context.Context, []record) error
}

type PartitionReader struct {
	services.Service
	dependencies *services.Manager

	kafkaCfg      KafkaConfig
	partitionID   int32
	consumerGroup string

	client *kgo.Client

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

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, consumerGroup string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	consumer := newPusherConsumer(pusher, reg, logger)
	return newPartitionReader(kafkaCfg, partitionID, consumerGroup, consumer, logger, reg)
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, consumerGroup string, consumer recordConsumer, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:              kafkaCfg,
		partitionID:           partitionID,
		consumer:              consumer,
		consumerGroup:         consumerGroup,
		metrics:               newReaderMetrics(partitionID, reg),
		commitInterval:        time.Second,
		consumedOffsetWatcher: newPartitionOffsetWatcher(),
		logger:                log.With(logger, "partition", partitionID),
		reg:                   reg,
	}

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) (returnErr error) {
	// Stop dependencies if the start() fails.
	defer func() {
		if returnErr != nil {
			_ = r.stopDependencies()
		}
	}()

	var (
		lastConsumedOffset int64
		startOffset        int64
		err                error
	)

	// Find the offset from which we should start consuming.
	switch r.kafkaCfg.ConsumeFromPositionAtStartup {
	case consumeFromStart:
		lastConsumedOffset = -1
		startOffset = kafkaStartOffset
		level.Info(r.logger).Log("msg", "starting consumption from partition start", "start_offset", startOffset, "consumer_group", r.consumerGroup)

	case consumeFromEnd:
		lastConsumedOffset = -1
		startOffset = kafkaEndOffset
		level.Warn(r.logger).Log("msg", "starting consumption from partition end (may cause data loss)", "start_offset", startOffset, "consumer_group", r.consumerGroup)

	default:
		var exists bool
		lastConsumedOffset, exists, err = r.fetchLastCommittedOffsetWithRetries(ctx)

		if err != nil {
			return err
		} else if exists {
			startOffset = lastConsumedOffset + 1 // We'll have to start consuming from the next offset (included).
			level.Info(r.logger).Log("msg", "starting consumption from last consumed offset", "last_consumed_offset", lastConsumedOffset, "start_offset", startOffset, "consumer_group", r.consumerGroup)
		} else {
			lastConsumedOffset = -1
			startOffset = kafkaStartOffset
			level.Info(r.logger).Log("msg", "starting consumption from partition start because no committed offset has been found", "start_offset", startOffset, "consumer_group", r.consumerGroup)
		}
	}

	// Initialise the last consumed offset only if we've got an actual offset from the consumer group.
	if lastConsumedOffset >= 0 {
		r.consumedOffsetWatcher.Notify(lastConsumedOffset)
	}

	r.client, err = r.newKafkaReader(kgo.NewOffset().At(startOffset))
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	r.committer = newPartitionCommitter(r.kafkaCfg, kadm.NewClient(r.client), r.partitionID, r.consumerGroup, r.commitInterval, r.logger, r.reg)

	r.offsetReader = newPartitionOffsetReader(r.client, r.kafkaCfg.Topic, r.partitionID, r.kafkaCfg.LastProducedOffsetPollInterval, r.reg, r.logger)

	r.dependencies, err = services.NewManager(r.committer, r.offsetReader, r.consumedOffsetWatcher)
	if err != nil {
		return errors.Wrap(err, "creating service manager")
	}
	err = services.StartManagerAndAwaitHealthy(ctx, r.dependencies)
	if err != nil {
		return errors.Wrap(err, "starting service manager")
	}

	// Enforce the max consumer lag (if enabled).
	if maxLag := r.kafkaCfg.MaxConsumerLagAtStartup; maxLag > 0 {
		if startOffset != kafkaEndOffset {
			if err := r.processNextFetchesUntilMaxLagHonored(ctx, maxLag); err != nil {
				return err
			}
		} else {
			level.Info(r.logger).Log("msg", "partition reader is skipping to consume partition until max consumer lag is honored because it's going to consume the partition from the end")
		}
	}

	return nil
}

func (r *PartitionReader) stop(error) error {
	level.Info(r.logger).Log("msg", "stopping partition reader")

	return r.stopDependencies()
}

func (r *PartitionReader) stopDependencies() error {
	if r.dependencies != nil {
		if err := services.StopManagerAndAwaitStopped(context.Background(), r.dependencies); err != nil {
			return errors.Wrap(err, "stopping service manager")
		}
	}

	if r.client != nil {
		r.client.Close()
	}

	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	for ctx.Err() == nil {
		r.processNextFetches(ctx, r.metrics.receiveDelayWhenRunning)
	}

	return nil
}

func (r *PartitionReader) processNextFetches(ctx context.Context, delayObserver prometheus.Observer) {
	fetches := r.client.PollFetches(ctx)
	r.recordFetchesMetrics(fetches, delayObserver)
	r.logFetchErrors(fetches)
	fetches = filterOutErrFetches(fetches)

	// TODO consumeFetches() may get interrupted in the middle because of ctx canceled due to PartitionReader stopped.
	// 		We should improve it, but we shouldn't just pass a context.Background() because if consumption is stuck
	// 		then PartitionReader will never stop.
	r.consumeFetches(ctx, fetches)
	r.enqueueCommit(fetches)
	r.notifyLastConsumedOffset(fetches)
}

func (r *PartitionReader) processNextFetchesUntilMaxLagHonored(ctx context.Context, maxLag time.Duration) error {
	level.Info(r.logger).Log("msg", "partition reader is starting to consume partition until max consumer lag is honored", "max_lag", maxLag)

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	for boff.Ongoing() {
		// Send a direct request to the Kafka backend to fetch the partition start offset.
		partitionStartOffset, err := r.offsetReader.FetchPartitionStartOffset(ctx)
		if err != nil {
			level.Warn(r.logger).Log("msg", "partition reader failed to fetch partition start offset", "err", err)
			boff.Wait()
			continue
		}

		// Send a direct request to the Kafka backend to fetch the last produced offset.
		// We intentionally don't use WaitNextFetchLastProducedOffset() to not introduce further
		// latency.
		lastProducedOffset, err := r.offsetReader.FetchLastProducedOffset(ctx)
		if err != nil {
			level.Warn(r.logger).Log("msg", "partition reader failed to fetch last produced offset", "err", err)
			boff.Wait()
			continue
		}

		lastProducedOffsetFetchedAt := time.Now()

		// Ensure there're some records to consume. For example, if the partition has been inactive for a long
		// time and all its records have been deleted, the partition start offset may be > 0 but there are no
		// records to actually consume.
		if partitionStartOffset > lastProducedOffset {
			level.Info(r.logger).Log("msg", "partition reader found no records to consume because partition is empty", "partition_start_offset", partitionStartOffset, "last_produced_offset", lastProducedOffset)
			return nil
		}

		// This message is NOT expected to be logged with a very high rate.
		level.Info(r.logger).Log("msg", "partition reader is consuming records to honor max consumer lag", "partition_start_offset", partitionStartOffset, "last_produced_offset", lastProducedOffset)

		for boff.Ongoing() {
			// Continue reading until we reached the desired offset.
			lastConsumedOffset := r.consumedOffsetWatcher.LastConsumedOffset()
			if lastProducedOffset <= lastConsumedOffset {
				break
			}

			r.processNextFetches(ctx, r.metrics.receiveDelayWhenStarting)
		}

		if boff.Err() != nil {
			return boff.Err()
		}

		// If it took less than the max desired lag to replay the partition
		// then we can stop here, otherwise we'll have to redo it.
		if currLag := time.Since(lastProducedOffsetFetchedAt); currLag <= maxLag {
			level.Info(r.logger).Log("msg", "partition reader consumed partition and current lag is less than configured max consumer lag", "last_consumed_offset", r.consumedOffsetWatcher.LastConsumedOffset(), "current_lag", currLag, "max_lag", maxLag)
			return nil
		}
	}

	return boff.Err()
}

func filterOutErrFetches(fetches kgo.Fetches) kgo.Fetches {
	filtered := make(kgo.Fetches, 0, len(fetches))
	for i, fetch := range fetches {
		if !isErrFetch(fetch) {
			filtered = append(filtered, fetches[i])
		}
	}

	return filtered
}

func isErrFetch(fetch kgo.Fetch) bool {
	for _, t := range fetch.Topics {
		for _, p := range t.Partitions {
			if p.Err != nil {
				return true
			}
		}
	}
	return false
}

func (r *PartitionReader) logFetchErrors(fetches kgo.Fetches) {
	mErr := multierror.New()
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}

		// kgo advises to "restart" the kafka client if the returned error is a kerr.Error.
		// Recreating the client would cause duplicate metrics registration, so we don't do it for now.
		mErr.Add(fmt.Errorf("topic %q, partition %d: %w", topic, partition, err))
	})
	if len(mErr) == 0 {
		return
	}
	r.metrics.fetchesErrors.Add(float64(len(mErr)))
	level.Error(r.logger).Log("msg", "encountered error while fetching", "err", mErr.Err())
}

func (r *PartitionReader) enqueueCommit(fetches kgo.Fetches) {
	if fetches.NumRecords() == 0 {
		return
	}
	lastOffset := int64(0)
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if partition.Partition != r.partitionID {
			level.Error(r.logger).Log("msg", "asked to commit wrong partition", "partition", partition.Partition, "expected_partition", r.partitionID)
			return
		}
		lastOffset = partition.Records[len(partition.Records)-1].Offset
	})
	r.committer.enqueueOffset(lastOffset)
}

func (r *PartitionReader) consumeFetches(ctx context.Context, fetches kgo.Fetches) {
	if fetches.NumRecords() == 0 {
		return
	}
	records := make([]record, 0, fetches.NumRecords())

	var (
		minOffset = math.MaxInt
		maxOffset = 0
	)
	fetches.EachRecord(func(r *kgo.Record) {
		minOffset = min(minOffset, int(r.Offset))
		maxOffset = max(maxOffset, int(r.Offset))
		records = append(records, record{
			content:  r.Value,
			tenantID: string(r.Key),
		})
	})

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	for boff.Ongoing() {
		err := r.consumer.consume(ctx, records)
		if err == nil {
			break
		}
		level.Error(r.logger).Log(
			"msg", "encountered error while ingesting data from Kafka; will retry",
			"err", err,
			"record_min_offset", minOffset,
			"record_max_offset", maxOffset,
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}

}

func (r *PartitionReader) notifyLastConsumedOffset(fetches kgo.Fetches) {
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		// We expect all records to belong to the partition consumed by this reader,
		// but we double check it here.
		if partition.Partition != r.partitionID {
			return
		}

		if len(partition.Records) == 0 {
			return
		}

		// Records are expected to be sorted by offsets, so we can simply look at the last one.
		rec := partition.Records[len(partition.Records)-1]
		r.consumedOffsetWatcher.Notify(rec.Offset)

		r.metrics.lastConsumedOffset.Set(float64(rec.Offset))
	})
}

func (r *PartitionReader) recordFetchesMetrics(fetches kgo.Fetches, delayObserver prometheus.Observer) {
	var (
		now        = time.Now()
		numRecords = 0
	)

	fetches.EachRecord(func(record *kgo.Record) {
		numRecords++
		delayObserver.Observe(now.Sub(record.Timestamp).Seconds())
	})

	r.metrics.fetchesTotal.Add(float64(len(fetches)))
	r.metrics.recordsPerFetch.Observe(float64(numRecords))
}

func (r *PartitionReader) newKafkaReader(at kgo.Offset) (*kgo.Client, error) {
	const fetchMaxBytes = 100_000_000

	opts := append(
		commonKafkaClientOptions(r.kafkaCfg, r.metrics.kprom, r.logger),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaCfg.Topic: {r.partitionID: at},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),

		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2*fetchMaxBytes),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating kafka client")
	}

	return client, nil
}

func (r *PartitionReader) fetchLastCommittedOffsetWithRetries(ctx context.Context) (offset int64, exists bool, err error) {
	var (
		retry = backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 2 * time.Second,
			MaxRetries: 10,
		})
	)

	for retry.Ongoing() {
		offset, exists, err = r.fetchLastCommittedOffset(ctx)
		if err == nil {
			return offset, exists, nil
		}

		level.Warn(r.logger).Log("msg", "failed to fetch last committed offset", "err", err)
		retry.Wait()
	}

	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = retry.Err()
	}

	return 0, false, err
}

// fetchLastCommittedOffset returns the last consumed offset which has been committed by the PartitionReader
// to the consumer group.
func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context) (offset int64, exists bool, _ error) {
	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(commonKafkaClientOptions(r.kafkaCfg, r.metrics.kprom, r.logger)...)
	if err != nil {
		return 0, false, errors.Wrap(err, "unable to create admin client")
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	offsets, err := adm.FetchOffsets(ctx, r.consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errors.Wrap(err, "unable to fetch group offsets")
	}

	offsetRes, exists := offsets.Lookup(r.kafkaCfg.Topic, r.partitionID)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, offsetRes.Err
	}

	return offsetRes.At, true, nil
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
	lastProducedOffset, err := r.offsetReader.WaitNextFetchLastProducedOffset(ctx)
	if err != nil {
		return err
	}

	// Then wait for it.
	return r.consumedOffsetWatcher.Wait(ctx, lastProducedOffset)
}

type partitionCommitter struct {
	services.Service

	kafkaCfg       KafkaConfig
	commitInterval time.Duration
	partitionID    int32
	consumerGroup  string

	toCommit  *atomic.Int64
	admClient *kadm.Client

	logger log.Logger

	// Metrics.
	commitRequestsTotal   prometheus.Counter
	commitFailuresTotal   prometheus.Counter
	commitRequestsLatency prometheus.Histogram
	lastCommittedOffset   prometheus.Gauge
}

func newPartitionCommitter(kafkaCfg KafkaConfig, admClient *kadm.Client, partitionID int32, consumerGroup string, commitInterval time.Duration, logger log.Logger, reg prometheus.Registerer) *partitionCommitter {
	c := &partitionCommitter{
		logger:         logger,
		kafkaCfg:       kafkaCfg,
		partitionID:    partitionID,
		consumerGroup:  consumerGroup,
		toCommit:       atomic.NewInt64(-1),
		admClient:      admClient,
		commitInterval: commitInterval,

		commitRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_offset_commit_requests_total",
			Help:        "Total number of requests issued to commit the last consumed offset (includes both successful and failed requests).",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		commitFailuresTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_reader_offset_commit_failures_total",
			Help:        "Total number of failed requests to commit the last consumed offset.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
		commitRequestsLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_offset_commit_request_duration_seconds",
			Help:                            "The duration of requests to commit the last consumed offset.",
			ConstLabels:                     prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		lastCommittedOffset: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name:        "cortex_ingest_storage_reader_last_committed_offset",
			Help:        "The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.",
			ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
		}),
	}
	c.Service = services.NewBasicService(nil, c.run, c.stop)

	// Initialise the last committed offset metric to -1 to signal no offset has been committed yet (0 is a valid offset).
	c.lastCommittedOffset.Set(-1)

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

			if err := r.commit(ctx, currOffset); err == nil {
				previousOffset = currOffset
			}
		}
	}
}

func (r *partitionCommitter) commit(ctx context.Context, offset int64) (returnErr error) {
	startTime := time.Now()
	r.commitRequestsTotal.Inc()

	defer func() {
		r.commitRequestsLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			level.Error(r.logger).Log("msg", "failed to commit last consumed offset to Kafka", "err", returnErr, "offset", offset)
			r.commitFailuresTotal.Inc()
		}
	}()

	// Commit the last consumed offset.
	toCommit := kadm.Offsets{}
	toCommit.AddOffset(r.kafkaCfg.Topic, r.partitionID, offset, -1)

	committed, err := r.admClient.CommitOffsets(ctx, r.consumerGroup, toCommit)
	if err != nil {
		return err
	} else if !committed.Ok() {
		return committed.Error()
	}

	committedOffset, _ := committed.Lookup(r.kafkaCfg.Topic, r.partitionID)
	level.Debug(r.logger).Log("msg", "last commit offset successfully committed to Kafka", "offset", committedOffset.At)
	r.lastCommittedOffset.Set(float64(committedOffset.At))

	return nil
}

func (r *partitionCommitter) stop(error) error {
	offset := r.toCommit.Load()
	if offset < 0 {
		return nil
	}

	// Commit has internal timeouts, so this call shouldn't block for too long.
	_ = r.commit(context.Background(), offset)

	return nil
}

type readerMetrics struct {
	receiveDelayWhenStarting  prometheus.Observer
	receiveDelayWhenRunning   prometheus.Observer
	recordsPerFetch           prometheus.Histogram
	fetchesErrors             prometheus.Counter
	fetchesTotal              prometheus.Counter
	strongConsistencyRequests prometheus.Counter
	strongConsistencyFailures prometheus.Counter
	strongConsistencyLatency  prometheus.Histogram
	lastConsumedOffset        prometheus.Gauge
	kprom                     *kprom.Metrics
}

func newReaderMetrics(partitionID int32, reg prometheus.Registerer) readerMetrics {
	receiveDelay := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_ingest_storage_reader_receive_delay_seconds",
		Help:                            "Delay between producing a record and receiving it in the consumer.",
		NativeHistogramZeroThreshold:    math.Pow(2, -10), // Values below this will be considered to be 0. Equals to 0.0009765625, or about 1ms.
		NativeHistogramBucketFactor:     1.2,              // We use higher factor (scheme=2) to have wider spread of buckets.
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		Buckets:                         prometheus.ExponentialBuckets(0.125, 2, 18), // Buckets between 125ms and 9h.
	}, []string{"phase"})

	lastConsumedOffset := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_ingest_storage_reader_last_consumed_offset",
		Help:        "The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.",
		ConstLabels: prometheus.Labels{"partition": strconv.Itoa(int(partitionID))},
	})

	// Initialise the last consumed offset metric to -1 to signal no offset has been consumed yet (0 is a valid offset).
	lastConsumedOffset.Set(-1)

	return readerMetrics{
		receiveDelayWhenStarting: receiveDelay.WithLabelValues("starting"),
		receiveDelayWhenRunning:  receiveDelay.WithLabelValues("running"),
		recordsPerFetch: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_reader_records_per_fetch",
			Help:    "The number of records received by the consumer in a single fetch operation.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		}),
		fetchesErrors: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetch_errors_total",
			Help: "The number of fetch errors encountered by the consumer.",
		}),
		fetchesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetches_total",
			Help: "Total number of Kafka fetches received by the consumer.",
		}),
		strongConsistencyRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_strong_consistency_requests_total",
			Help: "Total number of requests for which strong consistency has been requested.",
		}),
		strongConsistencyFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_strong_consistency_failures_total",
			Help: "Total number of failures while waiting for strong consistency to be enforced.",
		}),
		strongConsistencyLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
			Help:                            "How long a request spent waiting for strong consistency to be guaranteed.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		lastConsumedOffset: lastConsumedOffset,
		kprom: kprom.NewMetrics("cortex_ingest_storage_reader",
			kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}, reg)),
			// Do not export the client ID, because we use it to specify options to the backend.
			kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes)),
	}
}
