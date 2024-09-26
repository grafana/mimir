// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// kafkaOffsetStart is a special offset value that means the beginning of the partition.
	kafkaOffsetStart = int64(-2)

	// kafkaOffsetEnd is a special offset value that means the end of the partition.
	kafkaOffsetEnd = int64(-1)

	// defaultMinBytesMaxWaitTime is the time the Kafka broker can wait for MinBytes to be filled.
	// This is usually used when there aren't enough records available to fulfil MinBytes, so the broker waits for more records to be produced.
	// Warpstream clamps this between 5s and 30s.
	defaultMinBytesMaxWaitTime = 5 * time.Second
)

var (
	errWaitStrongReadConsistencyTimeoutExceeded = errors.Wrap(context.DeadlineExceeded, "wait strong read consistency timeout exceeded")
	errWaitTargetLagDeadlineExceeded            = errors.Wrap(context.DeadlineExceeded, "target lag deadline exceeded")
	errUnknownPartitionLeader                   = fmt.Errorf("unknown partition leader")
)

type record struct {
	// Context holds the tracing (and potentially other) info, that the record was enriched with on fetch from Kafka.
	ctx      context.Context
	tenantID string
	content  []byte
}

type recordConsumer interface {
	// Consume consumes the given records in the order they are provided. We need this as samples that will be ingested,
	// are also needed to be in order to avoid ingesting samples out of order.
	// The function is expected to be idempotent and incremental, meaning that it can be called multiple times with the same records, and it won't respond to context cancellation.
	Consume(context.Context, []record) error
}

type fetcher interface {
	pollFetches(context.Context) (kgo.Fetches, context.Context)
}

type PartitionReader struct {
	services.Service
	dependencies *services.Manager

	kafkaCfg                              KafkaConfig
	partitionID                           int32
	consumerGroup                         string
	concurrentFetchersMinBytesMaxWaitTime time.Duration

	client  *kgo.Client
	fetcher fetcher

	newConsumer consumerFactory
	metrics     readerMetrics

	committer *partitionCommitter

	// consumedOffsetWatcher is used to wait until a given offset has been consumed.
	// This gets initialised with -1 which means nothing has been consumed from the partition yet.
	consumedOffsetWatcher *partitionOffsetWatcher
	offsetReader          *partitionOffsetReader

	logger log.Logger
	reg    prometheus.Registerer
}

type consumerFactoryFunc func() recordConsumer

func (c consumerFactoryFunc) consumer() recordConsumer {
	return c()
}

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, instanceID string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	metrics := newPusherConsumerMetrics(reg)
	factory := consumerFactoryFunc(func() recordConsumer {
		return newPusherConsumer(pusher, kafkaCfg, metrics, logger)
	})
	return newPartitionReader(kafkaCfg, partitionID, instanceID, factory, logger, reg)
}

type consumerFactory interface {
	consumer() recordConsumer
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, instanceID string, consumer consumerFactory, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:                              kafkaCfg,
		partitionID:                           partitionID,
		newConsumer:                           consumer,
		consumerGroup:                         kafkaCfg.GetConsumerGroup(instanceID, partitionID),
		metrics:                               newReaderMetrics(partitionID, reg),
		consumedOffsetWatcher:                 newPartitionOffsetWatcher(),
		concurrentFetchersMinBytesMaxWaitTime: defaultMinBytesMaxWaitTime,
		logger:                                log.With(logger, "partition", partitionID),
		reg:                                   reg,
	}

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) (returnErr error) {
	if r.kafkaCfg.AutoCreateTopicEnabled {
		setDefaultNumberOfPartitionsForAutocreatedTopics(r.kafkaCfg, r.logger)
	}

	// Stop dependencies if the start() fails.
	defer func() {
		if returnErr != nil {
			_ = r.stopDependencies()
		}
	}()

	startOffset, lastConsumedOffset, err := r.getStartOffset(ctx)
	if err != nil {
		return err
	}
	// Initialise the last consumed offset only if we've got an actual offset from the consumer group.
	if lastConsumedOffset >= 0 {
		r.consumedOffsetWatcher.Notify(lastConsumedOffset)
	}

	r.client, err = r.newKafkaReader(kgo.NewOffset().At(startOffset))
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	r.committer = newPartitionCommitter(r.kafkaCfg, kadm.NewClient(r.client), r.partitionID, r.consumerGroup, r.logger, r.reg)

	offsetsClient := newPartitionOffsetClient(r.client, r.kafkaCfg.Topic, r.reg, r.logger)

	// It's ok to have the start offset slightly outdated.
	// We only need this offset accurate if we fall behind or if we start and the log gets truncated from beneath us.
	// In both cases we should recover after receiving one updated value.
	// In the more common case where this offset is used when we're fetching from after the end, there we don't need an accurate value.
	const startOffsetReaderRefreshDuration = 10 * time.Second
	getPartitionStart := func(ctx context.Context) (int64, error) {
		return offsetsClient.FetchPartitionStartOffset(ctx, r.partitionID)
	}
	startOffsetReader := newGenericOffsetReader(getPartitionStart, startOffsetReaderRefreshDuration, r.logger)

	r.offsetReader = newPartitionOffsetReaderWithOffsetClient(offsetsClient, r.partitionID, r.kafkaCfg.LastProducedOffsetPollInterval, r.logger)

	r.dependencies, err = services.NewManager(r.committer, r.offsetReader, r.consumedOffsetWatcher, startOffsetReader)
	if err != nil {
		return errors.Wrap(err, "creating service manager")
	}
	err = services.StartManagerAndAwaitHealthy(ctx, r.dependencies)
	if err != nil {
		return errors.Wrap(err, "starting service manager")
	}

	if r.kafkaCfg.FetchConcurrency > 1 {
		r.fetcher, err = newConcurrentFetchers(ctx, r.client, r.logger, r.kafkaCfg.Topic, r.partitionID, startOffset, r.kafkaCfg.FetchConcurrency, r.kafkaCfg.RecordsPerFetch, r.kafkaCfg.UseCompressedBytesAsFetchMaxBytes, r.concurrentFetchersMinBytesMaxWaitTime, offsetsClient, startOffsetReader, &r.metrics)
		if err != nil {
			return errors.Wrap(err, "creating concurrent fetchers")
		}
	} else {
		r.fetcher = r
	}

	// Enforce the max consumer lag (if enabled).
	if targetLag, maxLag := r.kafkaCfg.TargetConsumerLagAtStartup, r.kafkaCfg.MaxConsumerLagAtStartup; targetLag > 0 && maxLag > 0 {
		if startOffset != kafkaOffsetEnd {
			if err := r.processNextFetchesUntilTargetOrMaxLagHonored(ctx, targetLag, maxLag); err != nil {
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
		err := r.processNextFetches(ctx, r.metrics.receiveDelayWhenRunning)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Fail the whole service in case of a non-recoverable error.
			return err
		}
	}

	return nil
}

func (r *PartitionReader) processNextFetches(ctx context.Context, delayObserver prometheus.Observer) error {
	fetches, fetchCtx := r.fetcher.pollFetches(ctx)
	// Propagate the fetching span to consuming the records.
	ctx = opentracing.ContextWithSpan(ctx, opentracing.SpanFromContext(fetchCtx))
	r.recordFetchesMetrics(fetches, delayObserver)
	r.logFetchErrors(fetches)
	fetches = filterOutErrFetches(fetches)

	err := r.consumeFetches(ctx, fetches)
	if err != nil {
		return fmt.Errorf("consume %d records: %w", fetches.NumRecords(), err)
	}
	r.enqueueCommit(fetches)
	r.notifyLastConsumedOffset(fetches)
	return nil
}

// processNextFetchesUntilTargetOrMaxLagHonored process records from Kafka until at least the maxLag is honored.
// This function does a best-effort to get lag below targetLag, but it's not guaranteed that it will be
// reached once this function successfully returns (only maxLag is guaranteed).
func (r *PartitionReader) processNextFetchesUntilTargetOrMaxLagHonored(ctx context.Context, targetLag, maxLag time.Duration) error {
	logger := log.With(r.logger, "target_lag", targetLag, "max_lag", maxLag)
	level.Info(logger).Log("msg", "partition reader is starting to consume partition until target and max consumer lag is honored")

	attempts := []func() (currLag time.Duration, _ error){
		// First process fetches until at least the max lag is honored.
		func() (time.Duration, error) {
			return r.processNextFetchesUntilLagHonored(ctx, maxLag, logger)
		},

		// If the target lag hasn't been reached with the first attempt (which stops once at least the max lag
		// is honored) then we try to reach the (lower) target lag within a fixed time (best-effort).
		// The timeout is equal to the max lag. This is done because we expect at least a 2x replay speed
		// from Kafka (which means at most it takes 1s to ingest 2s of data): assuming new data is continuously
		// written to the partition, we give the reader maxLag time to replay the backlog + ingest the new data
		// written in the meanwhile.
		func() (time.Duration, error) {
			timedCtx, cancel := context.WithTimeoutCause(ctx, maxLag, errWaitTargetLagDeadlineExceeded)
			defer cancel()

			return r.processNextFetchesUntilLagHonored(timedCtx, targetLag, logger)
		},

		// If the target lag hasn't been reached with the previous attempt that we'll move on. However,
		// we still need to guarantee that in the meanwhile the lag didn't increase and max lag is still honored.
		func() (time.Duration, error) {
			return r.processNextFetchesUntilLagHonored(ctx, maxLag, logger)
		},
	}

	var currLag time.Duration
	for _, attempt := range attempts {
		var err error

		currLag, err = attempt()
		if errors.Is(err, errWaitTargetLagDeadlineExceeded) {
			continue
		}
		if err != nil {
			return err
		}
		if currLag <= targetLag {
			level.Info(logger).Log(
				"msg", "partition reader consumed partition and current lag is lower than configured target consumer lag",
				"last_consumed_offset", r.consumedOffsetWatcher.LastConsumedOffset(),
				"current_lag", currLag,
			)
			return nil
		}
	}

	level.Warn(logger).Log(
		"msg", "partition reader consumed partition and current lag is lower than configured max consumer lag but higher than target consumer lag",
		"last_consumed_offset", r.consumedOffsetWatcher.LastConsumedOffset(),
		"current_lag", currLag,
	)
	return nil
}

func (r *PartitionReader) processNextFetchesUntilLagHonored(ctx context.Context, maxLag time.Duration, logger log.Logger) (currLag time.Duration, _ error) {
	// clean-up resources spun up from this function
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(fmt.Errorf("partition reader stopped consuming partition until max consumer lag is honored"))

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0, // Retry forever (unless context is canceled / deadline exceeded).
	})

	for boff.Ongoing() {
		// Send a direct request to the Kafka backend to fetch the partition start offset.
		partitionStartOffset, err := r.offsetReader.FetchPartitionStartOffset(ctx)
		if err != nil {
			level.Warn(logger).Log("msg", "partition reader failed to fetch partition start offset", "err", err)
			boff.Wait()
			continue
		}

		// Send a direct request to the Kafka backend to fetch the last produced offset.
		// We intentionally don't use WaitNextFetchLastProducedOffset() to not introduce further
		// latency.
		lastProducedOffsetRequestedAt := time.Now()
		lastProducedOffset, err := r.offsetReader.FetchLastProducedOffset(ctx)
		if err != nil {
			level.Warn(logger).Log("msg", "partition reader failed to fetch last produced offset", "err", err)
			boff.Wait()
			continue
		}

		// Ensure there are some records to consume. For example, if the partition has been inactive for a long
		// time and all its records have been deleted, the partition start offset may be > 0 but there are no
		// records to actually consume.
		if partitionStartOffset > lastProducedOffset {
			level.Info(logger).Log("msg", "partition reader found no records to consume because partition is empty", "partition_start_offset", partitionStartOffset, "last_produced_offset", lastProducedOffset)
			return 0, nil
		}

		// This message is NOT expected to be logged with a very high rate. In this log we display the last measured
		// lag. If we don't have it (lag is zero value), then it will not be logged.
		level.Info(loggerWithCurrentLagIfSet(logger, currLag)).Log("msg", "partition reader is consuming records to honor target and max consumer lag", "partition_start_offset", partitionStartOffset, "last_produced_offset", lastProducedOffset)
		for boff.Ongoing() {
			// Continue reading until we reached the desired offset.
			lastConsumedOffset := r.consumedOffsetWatcher.LastConsumedOffset()
			if lastProducedOffset <= lastConsumedOffset {
				break
			}
			err := r.processNextFetches(ctx, r.metrics.receiveDelayWhenStarting)
			if err != nil {
				return 0, err
			}
		}
		if boff.Err() != nil {
			return 0, boff.ErrCause()
		}

		// If it took less than the max desired lag to replay the partition
		// then we can stop here, otherwise we'll have to redo it.
		if currLag = time.Since(lastProducedOffsetRequestedAt); currLag <= maxLag {
			return currLag, nil
		}
	}

	return 0, boff.ErrCause()
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

func loggerWithCurrentLagIfSet(logger log.Logger, currLag time.Duration) log.Logger {
	if currLag <= 0 {
		return logger
	}

	return log.With(logger, "current_lag", currLag)
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

func (r *PartitionReader) consumeFetches(ctx context.Context, fetches kgo.Fetches) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PartitionReader.consumeFetches")
	defer span.Finish()

	if fetches.NumRecords() == 0 {
		return nil
	}
	records := make([]record, 0, fetches.NumRecords())

	var (
		minOffset = math.MaxInt
		maxOffset = 0
	)
	fetches.EachRecord(func(rec *kgo.Record) {
		minOffset = min(minOffset, int(rec.Offset))
		maxOffset = max(maxOffset, int(rec.Offset))
		records = append(records, record{
			// This context carries the tracing data for this individual record;
			// kotel populates this data when it fetches the messages.
			ctx:      rec.Context,
			tenantID: string(rec.Key),
			content:  rec.Value,
		})
	})

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})
	defer func(consumeStart time.Time) {
		r.metrics.consumeLatency.Observe(time.Since(consumeStart).Seconds())
	}(time.Now())

	logger := spanlogger.FromContext(ctx, r.logger)

	for boff.Ongoing() {
		// We instantiate the consumer on each iteration because it is stateful, and we can't reuse it after closing.
		consumer := r.newConsumer.consumer()
		// If the PartitionReader is stopping and the ctx was cancelled, we don't want to interrupt the in-flight
		// processing midway. Instead, we let it finish, assuming it'll succeed.
		// If the processing fails while stopping, we log the error and let the backoff stop and bail out.
		// There is an edge-case when the processing gets stuck and doesn't let the stopping process. In such a case,
		// we expect the infrastructure (e.g. k8s) to eventually kill the process.
		consumeCtx := context.WithoutCancel(ctx)
		err := consumer.Consume(consumeCtx, records)
		if err == nil {
			level.Debug(logger).Log("msg", "closing consumer after successful consumption")
			break
		}
		level.Error(logger).Log(
			"msg", "encountered error while ingesting data from Kafka; should retry",
			"err", err,
			"record_min_offset", minOffset,
			"record_max_offset", maxOffset,
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}
	// Because boff is set to retry forever, the only error here is when the context is cancelled.
	return boff.ErrCause()
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
	return NewKafkaReaderClient(r.kafkaCfg, r.metrics.kprom, r.logger,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaCfg.Topic: {r.partitionID: at},
		}),
	)
}

func (r *PartitionReader) getStartOffset(ctx context.Context) (startOffset, lastConsumedOffset int64, err error) {
	switch r.kafkaCfg.ConsumeFromPositionAtStartup {
	case consumeFromStart:
		startOffset = kafkaOffsetStart
		level.Info(r.logger).Log("msg", "starting consumption from partition start", "start_offset", startOffset, "consumer_group", r.consumerGroup)
		return startOffset, -1, nil

	case consumeFromEnd:
		startOffset = kafkaOffsetEnd
		level.Warn(r.logger).Log("msg", "starting consumption from partition end (may cause data loss)", "start_offset", startOffset, "consumer_group", r.consumerGroup)
		return startOffset, -1, nil
	}

	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(commonKafkaClientOptions(r.kafkaCfg, r.metrics.kprom, r.logger)...)
	if err != nil {
		return 0, -1, fmt.Errorf("unable to create bootstrap client: %w", err)
	}
	defer cl.Close()

	fetchOffset := func(ctx context.Context) (offset, lastConsumedOffset int64, err error) {
		if r.kafkaCfg.ConsumeFromPositionAtStartup == consumeFromTimestamp {
			ts := time.UnixMilli(r.kafkaCfg.ConsumeFromTimestampAtStartup)
			offset, exists, err := r.fetchFirstOffsetAfterTime(ctx, cl, ts)
			if err != nil {
				return 0, -1, err
			}
			if exists {
				lastConsumedOffset = offset - 1 // Offset before the one we'll start the consumption from
				level.Info(r.logger).Log("msg", "starting consumption from timestamp", "timestamp", ts.UnixMilli(), "last_consumed_offset", lastConsumedOffset, "start_offset", offset, "consumer_group", r.consumerGroup)
				return offset, lastConsumedOffset, nil
			}
		} else {
			offset, exists, err := r.fetchLastCommittedOffset(ctx, cl)
			if err != nil {
				return 0, -1, err
			}
			if exists {
				lastConsumedOffset = offset
				offset = lastConsumedOffset + 1 // We'll start consuming from the next offset after the last consumed.
				level.Info(r.logger).Log("msg", "starting consumption from last consumed offset", "last_consumed_offset", lastConsumedOffset, "start_offset", offset, "consumer_group", r.consumerGroup)
				return offset, lastConsumedOffset, nil
			}
		}

		offset = kafkaOffsetStart
		level.Info(r.logger).Log("msg", "starting consumption from partition start because no offset has been found", "start_offset", offset, "consumer_group", r.consumerGroup)

		return offset, -1, err
	}

	retry := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 10,
	})
	for retry.Ongoing() {
		startOffset, lastConsumedOffset, err = fetchOffset(ctx)
		if err == nil {
			return startOffset, lastConsumedOffset, nil
		}

		level.Warn(r.logger).Log("msg", "failed to fetch offset", "err", err)
		retry.Wait()
	}

	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = retry.Err()
	}

	return 0, -1, err
}

// fetchLastCommittedOffset returns the last consumed offset which has been committed by the PartitionReader
// to the consumer group.
func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context, cl *kgo.Client) (offset int64, exists bool, _ error) {
	offsets, err := kadm.NewClient(cl).FetchOffsets(ctx, r.consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("unable to fetch group offsets: %w", err)
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

// fetchFirstOffsetAfterMilli returns the first offset after the requested millisecond timestamp.
func (r *PartitionReader) fetchFirstOffsetAfterTime(ctx context.Context, cl *kgo.Client, ts time.Time) (offset int64, exists bool, _ error) {
	offsets, err := kadm.NewClient(cl).ListOffsetsAfterMilli(ctx, ts.UnixMilli(), r.kafkaCfg.Topic)
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("unable to list topic offsets: %w", err)
	}

	offsetRes, exists := offsets.Lookup(r.kafkaCfg.Topic, r.partitionID)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, offsetRes.Err
	}

	return offsetRes.Offset, true, nil
}

// WaitReadConsistencyUntilLastProducedOffset waits until all data produced up until now has been consumed by the reader.
func (r *PartitionReader) WaitReadConsistencyUntilLastProducedOffset(ctx context.Context) (returnErr error) {
	return r.waitReadConsistency(ctx, false, func(ctx context.Context) (int64, error) {
		return r.offsetReader.WaitNextFetchLastProducedOffset(ctx)
	})
}

// WaitReadConsistencyUntilOffset waits until all data up until input offset has been consumed by the reader.
func (r *PartitionReader) WaitReadConsistencyUntilOffset(ctx context.Context, offset int64) (returnErr error) {
	return r.waitReadConsistency(ctx, true, func(_ context.Context) (int64, error) {
		return offset, nil
	})
}

func (r *PartitionReader) waitReadConsistency(ctx context.Context, withOffset bool, getOffset func(context.Context) (int64, error)) error {
	_, err := r.metrics.strongConsistencyInstrumentation.Observe(withOffset, func() (struct{}, error) {
		spanLog := spanlogger.FromContext(ctx, r.logger)
		spanLog.DebugLog("msg", "waiting for read consistency")

		// Honor the configured wait timeout.
		if r.kafkaCfg.WaitStrongReadConsistencyTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeoutCause(ctx, r.kafkaCfg.WaitStrongReadConsistencyTimeout, errWaitStrongReadConsistencyTimeoutExceeded)
			defer cancel()
		}

		// Ensure the service is running. Some subservices used below are created when starting
		// so they're not available before that.
		if state := r.Service.State(); state != services.Running {
			return struct{}{}, fmt.Errorf("partition reader service is not running (state: %s)", state.String())
		}

		// Get the offset to wait for.
		offset, err := getOffset(ctx)
		if err != nil {
			return struct{}{}, err
		}

		spanLog.DebugLog("msg", "catching up with offset", "offset", offset)

		return struct{}{}, r.consumedOffsetWatcher.Wait(ctx, offset)
	})

	return err
}

func (r *PartitionReader) pollFetches(ctx context.Context) (result kgo.Fetches, ctx2 context.Context) {
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	return r.client.PollFetches(ctx), ctx
}

// fetchWant represents a range of offsets to fetch.
// Based on a given number of records, it tries to estimate how many bytes we need to fetch, given there's no support for fetching offsets directly.
// fetchWant also contains the channel on which to send the fetched records for the offset range.
type fetchWant struct {
	startOffset    int64 // inclusive
	endOffset      int64 // exclusive
	bytesPerRecord int

	// result should be closed when there are no more fetches for this partition. It is ok to send multiple times on the channel.
	result chan fetchResult
}

func fetchWantFrom(offset int64, recordsPerFetch int) fetchWant {
	return fetchWant{
		startOffset: offset,
		endOffset:   offset + int64(recordsPerFetch),
		result:      make(chan fetchResult),
	}
}

// Next returns the fetchWant for the next numRecords starting from the last known offset.
func (w fetchWant) Next(numRecords int) fetchWant {
	n := fetchWantFrom(w.endOffset, numRecords)
	n.bytesPerRecord = w.bytesPerRecord
	return n.trimIfTooBig()
}

// MaxBytes returns the maximum number of bytes we can fetch in a single request.
// It's capped at math.MaxInt32 to avoid overflow, and it'll always fetch a minimum of 1MB.
func (w fetchWant) MaxBytes() int32 {
	fetchBytes := w.expectedBytes()
	if fetchBytes > math.MaxInt32 {
		// This shouldn't happen because w should have been trimmed before sending the request.
		// But we definitely don't want to request negative bytes by casting to int32, so add this safeguard.
		return math.MaxInt32
	}
	fetchBytes = max(1_000_000, fetchBytes) // when we're fetching few records, we can afford to over-fetch to avoid more requests.
	return int32(fetchBytes)
}

// UpdateBytesPerRecord updates the expected bytes per record based on the results of the last fetch and trims the fetchWant if MaxBytes() would now exceed math.MaxInt32.
func (w fetchWant) UpdateBytesPerRecord(lastFetchBytes int, lastFetchNumberOfRecords int) fetchWant {
	// Smooth over the estimation to avoid having outlier fetches from throwing off the estimation.
	// We don't want a fetch of 5 records to determine how we fetch the next fetch of 6000 records.
	// Ideally we weigh the estimation on the number of records observed, but it's simpler to smooth it over with a constant factor.
	const currentEstimateWeight = 0.8

	actualBytesPerRecord := float64(lastFetchBytes) / float64(lastFetchNumberOfRecords)
	w.bytesPerRecord = int(currentEstimateWeight*float64(w.bytesPerRecord) + (1-currentEstimateWeight)*actualBytesPerRecord)

	return w.trimIfTooBig()
}

// expectedBytes returns how many bytes we'd need to accommodate the range of offsets using bytesPerRecord.
// They may be more than the kafka protocol supports (> MaxInt32). Use MaxBytes.
func (w fetchWant) expectedBytes() int {
	// We over-fetch bytes to reduce the likelihood of under-fetching and having to run another request.
	// Based on some testing 65% of under-estimations are by less than 5%. So we account for that.
	const overFetchBytesFactor = 1.05
	return int(overFetchBytesFactor * float64(w.bytesPerRecord*int(w.endOffset-w.startOffset)))
}

// trimIfTooBig adjusts the end offset if we expect to fetch too many bytes.
// It's capped at math.MaxInt32 bytes.
func (w fetchWant) trimIfTooBig() fetchWant {
	if w.expectedBytes() <= math.MaxInt32 {
		return w
	}
	// We are overflowing, so we need to trim the end offset.
	// We do this by calculating how many records we can fetch with the max bytes, and then setting the end offset to that.
	w.endOffset = w.startOffset + int64(math.MaxInt32/w.bytesPerRecord)
	return w
}

type fetchResult struct {
	kgo.FetchPartition
	ctx          context.Context
	fetchedBytes int

	waitingToBePickedUpFromOrderedFetchesSpan opentracing.Span
}

func (fr *fetchResult) logCompletedFetch(fetchStartTime time.Time, w fetchWant) {
	var logger log.Logger = spanlogger.FromContext(fr.ctx, log.NewNopLogger())

	msg := "fetched records"
	if fr.Err != nil {
		msg = "received an error while fetching records; will retry after processing received records (if any)"
	}
	var (
		gotRecords   = int64(len(fr.Records))
		askedRecords = w.endOffset - w.startOffset
	)
	switch {
	case fr.Err == nil, errors.Is(fr.Err, kerr.OffsetOutOfRange):
		logger = level.Debug(logger)
	default:
		logger = level.Error(logger)
	}
	var firstTimestamp, lastTimestamp string
	if gotRecords > 0 {
		firstTimestamp = fr.Records[0].Timestamp.String()
		lastTimestamp = fr.Records[gotRecords-1].Timestamp.String()
	}
	logger.Log(
		"msg", msg,
		"duration", time.Since(fetchStartTime),
		"start_offset", w.startOffset,
		"end_offset", w.endOffset,
		"asked_records", askedRecords,
		"got_records", gotRecords,
		"diff_records", askedRecords-gotRecords,
		"asked_bytes", w.MaxBytes(),
		"got_bytes", fr.fetchedBytes,
		"diff_bytes", int(w.MaxBytes())-fr.fetchedBytes,
		"first_timestamp", firstTimestamp,
		"last_timestamp", lastTimestamp,
		"hwm", fr.HighWatermark,
		"lso", fr.LogStartOffset,
		"err", fr.Err,
	)
}

func (fr *fetchResult) startWaitingForConsumption() {
	fr.waitingToBePickedUpFromOrderedFetchesSpan, fr.ctx = opentracing.StartSpanFromContext(fr.ctx, "fetchResult.waitingForConsumption")
}

func (fr *fetchResult) finishWaitingForConsumption() {
	if fr.waitingToBePickedUpFromOrderedFetchesSpan == nil {
		fr.waitingToBePickedUpFromOrderedFetchesSpan, fr.ctx = opentracing.StartSpanFromContext(fr.ctx, "fetchResult.noWaitingForConsumption")
	}
	fr.waitingToBePickedUpFromOrderedFetchesSpan.Finish()
}

// Merge merges other with an older fetchResult. mergedWith keeps most of the fields of fr and assumes they are more up to date then other's.
func (fr *fetchResult) Merge(older fetchResult) fetchResult {
	if older.ctx != nil {
		level.Debug(spanlogger.FromContext(older.ctx, log.NewNopLogger())).Log("msg", "merged fetch result with the next result")
	}

	// older.Records are older than fr.Records, so we append them first.
	fr.Records = append(older.Records, fr.Records...)

	// We ignore HighWatermark, LogStartOffset, LastStableOffset because this result should be more up to date.
	fr.fetchedBytes += older.fetchedBytes
	return *fr
}

func newEmptyFetchResult(ctx context.Context, err error) fetchResult {
	return fetchResult{
		ctx:            ctx,
		fetchedBytes:   0,
		FetchPartition: kgo.FetchPartition{Err: err},
	}
}

type concurrentFetchers struct {
	client      *kgo.Client
	logger      log.Logger
	partitionID int32
	topicID     [16]byte
	topicName   string
	metrics     *readerMetrics
	tracer      *kotel.Tracer

	concurrency      int
	recordsPerFetch  int
	minBytesWaitTime time.Duration

	orderedFetches     chan fetchResult
	lastReturnedRecord int64
	startOffsets       *genericOffsetReader[int64]

	// trackCompressedBytes controls whether to calculate MaxBytes for fetch requests based on previous responses' compressed or uncompressed bytes.
	trackCompressedBytes bool
}

// newConcurrentFetchers creates a new concurrentFetchers. startOffset can be kafkaOffsetStart, kafkaOffsetEnd or a specific offset.
func newConcurrentFetchers(
	ctx context.Context,
	client *kgo.Client,
	logger log.Logger,
	topic string,
	partition int32,
	startOffset int64,
	concurrency int,
	recordsPerFetch int,
	trackCompressedBytes bool,
	minBytesWaitTime time.Duration,
	offsetReader *partitionOffsetClient,
	startOffsetsReader *genericOffsetReader[int64],
	metrics *readerMetrics,
) (*concurrentFetchers, error) {

	const noReturnedRecords = -1 // we still haven't returned the 0 offset.
	f := &concurrentFetchers{
		client:               client,
		logger:               logger,
		concurrency:          concurrency,
		topicName:            topic,
		partitionID:          partition,
		metrics:              metrics,
		recordsPerFetch:      recordsPerFetch,
		minBytesWaitTime:     minBytesWaitTime,
		lastReturnedRecord:   noReturnedRecords,
		startOffsets:         startOffsetsReader,
		trackCompressedBytes: trackCompressedBytes,
		tracer:               recordsTracer(),
		orderedFetches:       make(chan fetchResult),
	}

	var err error
	switch startOffset {
	case kafkaOffsetStart:
		startOffset, err = offsetReader.FetchPartitionStartOffset(ctx, partition)
	case kafkaOffsetEnd:
		startOffset, err = offsetReader.FetchPartitionLastProducedOffset(ctx, partition)
		// End (-1) means "ignore all existing records". FetchPartitionLastProducedOffset returns the offset of an existing record.
		// We need to start from the next one, which is still not produced.
		startOffset++
	}
	if err != nil {
		return nil, fmt.Errorf("resolving offset to start consuming from: %w", err)
	}

	topics, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find topic ID: %w", err)
	}
	if !topics.Has(topic) {
		return nil, fmt.Errorf("failed to find topic ID: topic not found")
	}
	f.topicID = topics[topic].ID

	go f.runFetchers(ctx, startOffset)

	return f, nil
}

func (r *concurrentFetchers) pollFetches(ctx context.Context) (kgo.Fetches, context.Context) {
	waitStartTime := time.Now()
	select {
	case <-ctx.Done():
		return kgo.Fetches{}, ctx
	case f := <-r.orderedFetches:
		firstUnreturnedRecordIdx := recordIndexAfterOffset(f.Records, r.lastReturnedRecord)
		r.recordOrderedFetchTelemetry(f, firstUnreturnedRecordIdx, waitStartTime)

		f.Records = f.Records[firstUnreturnedRecordIdx:]
		if len(f.Records) > 0 {
			r.lastReturnedRecord = f.Records[len(f.Records)-1].Offset
		}

		return kgo.Fetches{{
			Topics: []kgo.FetchTopic{
				{
					Topic:      r.topicName,
					Partitions: []kgo.FetchPartition{f.FetchPartition},
				},
			},
		}}, f.ctx
	}
}

func recordIndexAfterOffset(records []*kgo.Record, offset int64) int {
	for i, r := range records {
		if r.Offset > offset {
			return i
		}
	}
	return len(records)
}

func (r *concurrentFetchers) recordOrderedFetchTelemetry(f fetchResult, firstReturnedRecordIndex int, waitStartTime time.Time) {
	waitDuration := time.Since(waitStartTime)
	level.Debug(r.logger).Log("msg", "received ordered fetch", "num_records", len(f.Records), "wait_duration", waitDuration)
	r.metrics.fetchWaitDuration.Observe(waitDuration.Seconds())

	doubleFetchedBytes := 0
	for i, record := range f.Records {
		if i < firstReturnedRecordIndex {
			doubleFetchedBytes += len(record.Value)
			spanlogger.FromContext(record.Context, r.logger).DebugLog("msg", "skipping record because it has already been returned", "offset", record.Offset)
		}
		r.tracer.OnFetchRecordUnbuffered(record, true)
	}
	r.metrics.fetchedDiscardedRecordBytes.Add(float64(doubleFetchedBytes))
}

// fetchSingle attempts to find out the leader leader Kafka broker for a partition and then sends a fetch request to the leader of the fetchWant request and parses the responses
// fetchSingle returns a fetchResult which may or may not fulfil the entire fetchWant.
// If ctx is cancelled, fetchSingle will return an empty fetchResult without an error.
func (r *concurrentFetchers) fetchSingle(ctx context.Context, fw fetchWant) (fr fetchResult) {
	defer func(fetchStartTime time.Time) {
		fr.logCompletedFetch(fetchStartTime, fw)
	}(time.Now())

	leaderID, leaderEpoch, err := r.client.PartitionLeader(r.topicName, r.partitionID)
	if err != nil || (leaderID == -1 && leaderEpoch == -1) {
		if err != nil {
			return newEmptyFetchResult(ctx, fmt.Errorf("finding leader for partition: %w", err))
		}
		return newEmptyFetchResult(ctx, errUnknownPartitionLeader)
	}

	req := r.buildFetchRequest(fw, leaderEpoch)

	resp, err := req.RequestWith(ctx, r.client.Broker(int(leaderID)))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return newEmptyFetchResult(ctx, nil)
		}
		return newEmptyFetchResult(ctx, fmt.Errorf("fetching from kafka: %w", err))
	}

	return r.parseFetchResponse(ctx, fw.startOffset, resp)
}

func (r *concurrentFetchers) buildFetchRequest(fw fetchWant, leaderEpoch int32) kmsg.FetchRequest {
	req := kmsg.NewFetchRequest()
	req.MinBytes = 1 // Warpstream ignores this field. This means that the WaitTime below is always waited and MaxBytes play a bigger role in how fast Ws responds.
	req.Version = 13
	req.MaxWaitMillis = int32(r.minBytesWaitTime / time.Millisecond)
	req.MaxBytes = fw.MaxBytes()

	reqTopic := kmsg.NewFetchRequestTopic()
	reqTopic.Topic = r.topicName
	reqTopic.TopicID = r.topicID

	reqPartition := kmsg.NewFetchRequestTopicPartition()
	reqPartition.Partition = r.partitionID
	reqPartition.FetchOffset = fw.startOffset
	reqPartition.PartitionMaxBytes = req.MaxBytes
	reqPartition.CurrentLeaderEpoch = leaderEpoch

	reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
	req.Topics = append(req.Topics, reqTopic)
	return req
}

func (r *concurrentFetchers) parseFetchResponse(ctx context.Context, startOffset int64, resp *kmsg.FetchResponse) fetchResult {
	// Here we ignore resp.ErrorCode. That error code was added for support for KIP-227 and is only set if we're using fetch sessions. We don't use fetch sessions.
	// We also ignore rawPartitionResp.PreferredReadReplica to keep the code simpler. We don't provide any rack in the FetchRequest, so the broker _probably_ doesn't have a recommended replica for us.

	// Sanity check for the response we get.
	// If we get something we didn't expect, maybe we're sending the wrong request or there's a bug in the kafka implementation.
	// Even in case of errors we get the topic partition.
	err := assertResponseContainsPartition(resp, r.topicID, r.partitionID)
	if err != nil {
		return newEmptyFetchResult(ctx, err)
	}

	parseOptions := kgo.ProcessFetchPartitionOptions{
		KeepControlRecords: false,
		Offset:             startOffset,
		IsolationLevel:     kgo.ReadUncommitted(), // we don't produce in transactions, but leaving this here so it's explicit.
		Topic:              r.topicName,
		Partition:          r.partitionID,
	}

	observeMetrics := func(m kgo.FetchBatchMetrics) {
		brokerMeta := kgo.BrokerMetadata{} // leave it empty because kprom doesn't use it, and we don't exactly have all the metadata
		r.metrics.kprom.OnFetchBatchRead(brokerMeta, r.topicName, r.partitionID, m)
	}
	rawPartitionResp := resp.Topics[0].Partitions[0]
	partition, _ := kgo.ProcessRespPartition(parseOptions, &rawPartitionResp, observeMetrics)
	partition.EachRecord(r.tracer.OnFetchRecordBuffered)
	partition.EachRecord(func(r *kgo.Record) {
		spanlogger.FromContext(r.Context, log.NewNopLogger()).DebugLog("msg", "received record")
	})

	fetchedBytes := len(rawPartitionResp.RecordBatches)
	if !r.trackCompressedBytes {
		fetchedBytes = sumRecordLengths(partition.Records)
	}

	return fetchResult{
		ctx:            ctx,
		FetchPartition: partition,
		fetchedBytes:   fetchedBytes,
	}
}

func assertResponseContainsPartition(resp *kmsg.FetchResponse, topicID kadm.TopicID, partitionID int32) error {
	if topics := resp.Topics; len(topics) < 1 || topics[0].TopicID != topicID {
		receivedTopicID := kadm.TopicID{}
		if len(topics) > 0 {
			receivedTopicID = topics[0].TopicID
		}
		return fmt.Errorf("didn't find expected topic %s in fetch response; received topic %s", topicID, receivedTopicID)
	}
	if partitions := resp.Topics[0].Partitions; len(partitions) < 1 || partitions[0].Partition != partitionID {
		receivedPartitionID := int32(-1)
		if len(partitions) > 0 {
			receivedPartitionID = partitions[0].Partition
		}
		return fmt.Errorf("didn't find expected partition %d in fetch response; received partition %d", partitionID, receivedPartitionID)
	}
	return nil
}

func sumRecordLengths(records []*kgo.Record) (sum int) {
	for _, r := range records {
		sum += len(r.Value)
	}
	return sum
}

func (r *concurrentFetchers) runFetcher(ctx context.Context, fetchersWg *sync.WaitGroup, wants chan fetchWant, logger log.Logger) {
	defer fetchersWg.Done()
	errBackoff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	for w := range wants {
		// Start new span for each fetchWant. We want to record the lifecycle of a single record from being fetched to being ingested.
		wantSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch")
		wantSpan.SetTag("start_offset", w.startOffset)
		wantSpan.SetTag("end_offset", w.endOffset)

		var previousResult fetchResult
		for attempt := 0; errBackoff.Ongoing() && w.endOffset > w.startOffset; attempt++ {
			attemptSpan, ctx := spanlogger.NewWithLogger(ctx, logger, "concurrentFetcher.fetch.attempt")
			attemptSpan.SetTag("attempt", attempt)

			f := r.fetchSingle(ctx, w)
			f = f.Merge(previousResult)
			previousResult = f
			if f.Err != nil {
				w = handleKafkaFetchErr(f.Err, w, errBackoff, r.startOffsets, r.client, attemptSpan)
			}
			if len(f.Records) == 0 {
				// Typically if we had an error, then there wouldn't be any records.
				// But it's hard to verify this for all errors from the Kafka API docs, so just to be sure, we process any records we might have received.
				attemptSpan.Finish()
				continue
			}
			// Next attempt will be from the last record onwards.
			w.startOffset = f.Records[len(f.Records)-1].Offset + 1

			// We reset the backoff if we received any records whatsoever. A received record means _some_ success.
			// We don't want to slow down until we hit a larger error.
			errBackoff.Reset()

			select {
			case w.result <- f:
				previousResult = fetchResult{}
			case <-ctx.Done():
			default:
				if w.startOffset >= w.endOffset {
					// We've fetched all we were asked for the whole batch is ready, and we definitely have to wait to send on the channel now.
					f.startWaitingForConsumption()
					select {
					case w.result <- f:
						previousResult = fetchResult{}
					case <-ctx.Done():
					}
				}
			}
			attemptSpan.Finish()
		}
		wantSpan.Finish()
		close(w.result)
	}
}

func (r *concurrentFetchers) runFetchers(ctx context.Context, startOffset int64) {
	fetchersWg := &sync.WaitGroup{}
	fetchersWg.Add(r.concurrency)
	defer fetchersWg.Wait()

	wants := make(chan fetchWant)
	defer close(wants)
	for i := 0; i < r.concurrency; i++ {
		logger := log.With(r.logger, "fetcher", i)
		go r.runFetcher(ctx, fetchersWg, wants, logger)
	}

	var (
		nextFetch      = fetchWantFrom(startOffset, r.recordsPerFetch)
		nextResult     chan fetchResult
		pendingResults = list.New()

		bufferedResult       fetchResult
		readyBufferedResults chan fetchResult // this is non-nil when bufferedResult is non-empty
	)
	nextFetch.bytesPerRecord = 10_000 // start with an estimation, we will update it as we consume

	for {
		refillBufferedResult := nextResult
		if readyBufferedResults != nil {
			// We have a single result that's still not consumed.
			// So we don't try to get new results from the fetchers.
			refillBufferedResult = nil
		}
		select {
		case <-ctx.Done():
			return

		case wants <- nextFetch:
			pendingResults.PushBack(nextFetch.result)
			if nextResult == nil {
				// In case we previously exhausted pendingResults, we just created
				nextResult = pendingResults.Front().Value.(chan fetchResult)
				pendingResults.Remove(pendingResults.Front())
			}
			nextFetch = nextFetch.Next(r.recordsPerFetch)

		case result, moreLeft := <-refillBufferedResult:
			if !moreLeft {
				if pendingResults.Len() > 0 {
					nextResult = pendingResults.Front().Value.(chan fetchResult)
					pendingResults.Remove(pendingResults.Front())
				} else {
					nextResult = nil
				}
				continue
			}
			nextFetch = nextFetch.UpdateBytesPerRecord(result.fetchedBytes, len(result.Records))
			bufferedResult = result
			readyBufferedResults = r.orderedFetches

		case readyBufferedResults <- bufferedResult:
			bufferedResult.finishWaitingForConsumption()
			readyBufferedResults = nil
			bufferedResult = fetchResult{}
		}
	}
}

type waiter interface {
	Wait()
}

type metadataRefresher interface {
	ForceMetadataRefresh()
}

// handleKafkaFetchErr handles all the errors listed in the franz-go documentation as possible errors when fetching records.
// For most of them we just apply a backoff. They are listed here so we can be explicit in what we're handling and how.
// It may also return an adjusted fetchWant in case the error indicated, we were consuming not yet produced records or records already deleted due to retention.
func handleKafkaFetchErr(err error, fw fetchWant, longBackoff waiter, partitionStartOffset *genericOffsetReader[int64], refresher metadataRefresher, logger log.Logger) fetchWant {
	// Typically franz-go will update its own metadata when it detects a change in brokers. But it's hard to verify this.
	// So we force a metadata refresh here to be sure.
	// It's ok to call this from multiple fetchers concurrently. franz-go will only be sending one metadata request at a time (whether automatic, periodic, or forced).
	//
	// Metadata refresh is asynchronous. So even after forcing the refresh we might have outdated metadata.
	// Hopefully the backoff that will follow is enough to get the latest metadata.
	// If not, the fetcher will end up here again on the next attempt.
	triggerMetadataRefresh := refresher.ForceMetadataRefresh

	switch {
	case err == nil:
	case errors.Is(err, kerr.OffsetOutOfRange):
		// We're either consuming from before the first offset or after the last offset.
		partitionStart, err := partitionStartOffset.CachedOffset()
		if err != nil {
			level.Error(logger).Log("msg", "failed to find start offset to readjust on OffsetOutOfRange; retrying same records range", "err", err)
			break
		}

		if fw.startOffset < partitionStart {
			// We're too far behind.
			if partitionStart >= fw.endOffset {
				// The next fetch want is responsible for this range. We set startOffset=endOffset to effectively mark this fetch as complete.
				fw.startOffset = fw.endOffset
				level.Debug(logger).Log("msg", "we're too far behind aborting fetch", "log_start_offset", partitionStart, "start_offset", fw.startOffset, "end_offset", fw.endOffset)
				break
			}
			// Only some of the offsets of our want are out of range, so let's fast-forward.
			fw.startOffset = partitionStart
			level.Debug(logger).Log("msg", "part of fetch want is outside of available offsets, adjusted start offset", "log_start_offset", partitionStart, "start_offset", fw.startOffset, "end_offset", fw.endOffset)
		} else {
			// If the broker is behind or if we are requesting offsets which have not yet been produced, we end up here.
			// We set a MaxWaitMillis on fetch requests, but even then there may be no records for some time.
			// Wait for a short time to allow the broker to catch up or for new records to be produced.
			level.Debug(logger).Log("msg", "offset out of range; waiting for new records to be produced")
		}
	case errors.Is(err, kerr.TopicAuthorizationFailed):
		longBackoff.Wait()
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		longBackoff.Wait()
	case errors.Is(err, kerr.UnsupportedCompressionType):
		level.Error(logger).Log("msg", "received UNSUPPORTED_COMPRESSION_TYPE from kafka; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait() // this shouldn't happen - only happens when the request version was under 10, but we always use 13 - log error and backoff - we can't afford to lose records
	case errors.Is(err, kerr.UnsupportedVersion):
		level.Error(logger).Log("msg", "received UNSUPPORTED_VERSION from kafka; the Kafka cluster is probably too old", "err", err)
		longBackoff.Wait() // in this case our client is too old, not much we can do. This will probably continue logging the error until someone upgrades their Kafka cluster.
	case errors.Is(err, kerr.KafkaStorageError):
		longBackoff.Wait() // server-side error, effectively same as HTTP 500
	case errors.Is(err, kerr.UnknownTopicID):
		longBackoff.Wait() // Maybe it wasn't created by the producers yet.
	case errors.Is(err, kerr.OffsetMovedToTieredStorage):
		level.Error(logger).Log("msg", "received OFFSET_MOVED_TO_TIERED_STORAGE from kafka; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait() // This should be only intra-broker error, and we shouldn't get it.
	case errors.Is(err, kerr.NotLeaderForPartition):
		// We're asking a broker which is no longer the leader. For a partition. We should refresh our metadata and try again.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.ReplicaNotAvailable):
		// Maybe the replica hasn't replicated the log yet, or it is no longer a replica for this partition.
		// We should refresh and try again with a leader or replica which is up to date.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.UnknownLeaderEpoch):
		// Maybe there's an ongoing election. We should refresh our metadata and try again with a leader in the current epoch.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.FencedLeaderEpoch):
		// We missed a new epoch (leader election). We should refresh our metadata and try again with a leader in the current epoch.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, kerr.LeaderNotAvailable):
		// This isn't listed in the possible errors in franz-go, but Apache Kafka returns it when the partition has no leader.
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, errUnknownPartitionLeader):
		triggerMetadataRefresh()
		longBackoff.Wait()
	case errors.Is(err, &kgo.ErrFirstReadEOF{}):
		longBackoff.Wait()

	default:
		level.Error(logger).Log("msg", "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug", "err", err)
		longBackoff.Wait()
	}
	return fw
}

type partitionCommitter struct {
	services.Service

	kafkaCfg      KafkaConfig
	partitionID   int32
	consumerGroup string

	toCommit  *atomic.Int64
	admClient *kadm.Client

	logger log.Logger

	// Metrics.
	commitRequestsTotal   prometheus.Counter
	commitFailuresTotal   prometheus.Counter
	commitRequestsLatency prometheus.Histogram
	lastCommittedOffset   prometheus.Gauge
}

func newPartitionCommitter(kafkaCfg KafkaConfig, admClient *kadm.Client, partitionID int32, consumerGroup string, logger log.Logger, reg prometheus.Registerer) *partitionCommitter {
	c := &partitionCommitter{
		logger:        logger,
		kafkaCfg:      kafkaCfg,
		partitionID:   partitionID,
		consumerGroup: consumerGroup,
		toCommit:      atomic.NewInt64(-1),
		admClient:     admClient,

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
	commitTicker := time.NewTicker(r.kafkaCfg.ConsumerGroupOffsetCommitInterval)
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
	receiveDelayWhenStarting         prometheus.Observer
	receiveDelayWhenRunning          prometheus.Observer
	recordsPerFetch                  prometheus.Histogram
	fetchesErrors                    prometheus.Counter
	fetchesTotal                     prometheus.Counter
	fetchWaitDuration                prometheus.Histogram
	fetchedDiscardedRecordBytes      prometheus.Counter
	strongConsistencyInstrumentation *StrongReadConsistencyInstrumentation[struct{}]
	lastConsumedOffset               prometheus.Gauge
	consumeLatency                   prometheus.Histogram
	kprom                            *kprom.Metrics
}

func newReaderMetrics(partitionID int32, reg prometheus.Registerer) readerMetrics {
	const component = "partition-reader"

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
		fetchWaitDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_records_batch_wait_duration_seconds",
			Help:                        "How long a consumer spent waiting for a batch of records from the Kafka client. If fetching is faster than processing, then this will be close to 0.",
			NativeHistogramBucketFactor: 1.1,
		}),
		fetchedDiscardedRecordBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetched_discarded_bytes_total",
			Help: "Total number of uncompressed bytes of records discarded from because they were already consumed. A higher rate means that the concurrent fetching estimations are less accurate.",
		}),
		consumeLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_records_batch_process_duration_seconds",
			Help:                        "How long a consumer spent processing a batch of records from Kafka.",
			NativeHistogramBucketFactor: 1.1,
		}),
		strongConsistencyInstrumentation: NewStrongReadConsistencyInstrumentation[struct{}](component, reg),
		lastConsumedOffset:               lastConsumedOffset,
		kprom:                            NewKafkaReaderClientMetrics(component, reg),
	}
}

type StrongReadConsistencyInstrumentation[T any] struct {
	requests *prometheus.CounterVec
	failures prometheus.Counter
	latency  prometheus.Histogram
}

func NewStrongReadConsistencyInstrumentation[T any](component string, reg prometheus.Registerer) *StrongReadConsistencyInstrumentation[T] {
	i := &StrongReadConsistencyInstrumentation[T]{
		requests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_strong_consistency_requests_total",
			Help:        "Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.",
			ConstLabels: map[string]string{"component": component},
		}, []string{"with_offset"}),
		failures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingest_storage_strong_consistency_failures_total",
			Help:        "Total number of failures while waiting for strong consistency to be enforced.",
			ConstLabels: map[string]string{"component": component},
		}),
		latency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
			Help:                            "How long a request spent waiting for strong consistency to be guaranteed.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
			ConstLabels:                     map[string]string{"component": component},
		}),
	}

	// Init metrics.
	for _, value := range []bool{true, false} {
		i.requests.WithLabelValues(strconv.FormatBool(value))
	}

	return i
}

func (i *StrongReadConsistencyInstrumentation[T]) Observe(withOffset bool, f func() (T, error)) (_ T, returnErr error) {
	startTime := time.Now()
	i.requests.WithLabelValues(strconv.FormatBool(withOffset)).Inc()

	defer func() {
		// Do not track failure or latency if the request was canceled (because the tracking would be incorrect).
		if errors.Is(returnErr, context.Canceled) {
			return
		}

		// Track latency for failures too, so that we have a better measurement of latency if
		// backend latency is high and requests fail because of timeouts.
		i.latency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			i.failures.Inc()
		}
	}()

	return f()
}
