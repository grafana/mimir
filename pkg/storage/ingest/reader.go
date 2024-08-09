// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/klauspost/compress/s2"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kotel"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// kafkaOffsetStart is a special offset value that means the beginning of the partition.
	kafkaOffsetStart = int64(-2)

	// kafkaOffsetEnd is a special offset value that means the end of the partition.
	kafkaOffsetEnd = int64(-1)
)

var (
	errWaitStrongReadConsistencyTimeoutExceeded = errors.Wrap(context.DeadlineExceeded, "wait strong read consistency timeout exceeded")
	errWaitTargetLagDeadlineExceeded            = errors.Wrap(context.DeadlineExceeded, "target lag deadline exceeded")
)

type record struct {
	// Context holds the tracing (and potentially other) info, that the record was enriched with on fetch from Kafka.
	ctx      context.Context
	tenantID string
	content  []byte
}

type recordConsumer interface {
	Close(context.Context) error
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

type noopPusherCloser struct {
	metrics *pusherConsumerMetrics

	Pusher
}

func newNoopPusherCloser(metrics *pusherConsumerMetrics, pusher Pusher) noopPusherCloser {
	return noopPusherCloser{
		metrics: metrics,
		Pusher:  pusher,
	}
}

func (c noopPusherCloser) PushToStorage(ctx context.Context, wr *mimirpb.WriteRequest) error {
	c.metrics.numTimeSeriesPerFlush.Observe(float64(len(wr.Timeseries)))
	return c.Pusher.PushToStorage(ctx, wr)
}

func (noopPusherCloser) Close() []error {
	return nil
}

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, instanceID string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	factory := consumerFactoryFunc(func() recordConsumer {
		return newPusherConsumer(pusher, kafkaCfg, reg, logger)
	})
	return newPartitionReader(kafkaCfg, partitionID, instanceID, factory, logger, reg)
}

type consumerFactory interface {
	consumer() recordConsumer
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, instanceID string, consumer consumerFactory, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:              kafkaCfg,
		partitionID:           partitionID,
		newConsumer:           consumer,
		consumerGroup:         kafkaCfg.GetConsumerGroup(instanceID, partitionID),
		metrics:               newReaderMetrics(partitionID, reg),
		consumedOffsetWatcher: newPartitionOffsetWatcher(),
		logger:                log.With(logger, "partition", partitionID),
		reg:                   reg,
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
	if targetLag, maxLag := r.kafkaCfg.TargetConsumerLagAtStartup, r.kafkaCfg.MaxConsumerLagAtStartup; targetLag > 0 && maxLag > 0 {
		if r.kafkaCfg.ConsumeFromPositionAtStartup != consumeFromEnd {
			if err := r.processNextFetchesUntilTargetOrMaxLagHonored(ctx, startOffset, targetLag, maxLag); err != nil {
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
		fetches := r.pollFetches(ctx)
		err := r.processFetches(ctx, fetches, r.metrics.receiveDelayWhenRunning)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Fail the whole service in case of a non-recoverable error.
			return err
		}
	}

	return nil
}

func (r *PartitionReader) processFetches(ctx context.Context, fetches kgo.Fetches, delayObserver prometheus.Observer) error {
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
func (r *PartitionReader) processNextFetchesUntilTargetOrMaxLagHonored(ctx context.Context, startOffset int64, targetLag, maxLag time.Duration) error {
	logger := log.With(r.logger, "target_lag", targetLag, "max_lag", maxLag)
	level.Info(logger).Log("msg", "partition reader is starting to consume partition until target and max consumer lag is honored")

	attempts := []func(startOffset int64) (currLag time.Duration, _ error){
		// First process fetches until at least the max lag is honored.
		func(startOffset int64) (time.Duration, error) {
			return r.processNextFetchesUntilLagHonored(ctx, startOffset, maxLag, logger)
		},

		// If the target lag hasn't been reached with the first attempt (which stops once at least the max lag
		// is honored) then we try to reach the (lower) target lag within a fixed time (best-effort).
		// The timeout is equal to the max lag. This is done because we expect at least a 2x replay speed
		// from Kafka (which means at most it takes 1s to ingest 2s of data): assuming new data is continuously
		// written to the partition, we give the reader maxLag time to replay the backlog + ingest the new data
		// written in the meanwhile.
		func(startOffset int64) (time.Duration, error) {
			timedCtx, cancel := context.WithTimeoutCause(ctx, maxLag, errWaitTargetLagDeadlineExceeded)
			defer cancel()

			return r.processNextFetchesUntilLagHonored(timedCtx, startOffset, targetLag, logger)
		},

		// If the target lag hasn't been reached with the previous attempt that we'll move on. However,
		// we still need to guarantee that in the meanwhile the lag didn't increase and max lag is still honored.
		func(startOffset int64) (time.Duration, error) {
			return r.processNextFetchesUntilLagHonored(ctx, startOffset, maxLag, logger)
		},
	}

	var currLag time.Duration
	for _, attempt := range attempts {
		var err error

		currLag, err = attempt(startOffset)
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
		startOffset = r.consumedOffsetWatcher.LastConsumedOffset()
	}

	level.Warn(logger).Log(
		"msg", "partition reader consumed partition and current lag is lower than configured max consumer lag but higher than target consumer lag",
		"last_consumed_offset", r.consumedOffsetWatcher.LastConsumedOffset(),
		"current_lag", currLag,
	)
	return nil
}

func (r *PartitionReader) processNextFetchesUntilLagHonored(ctx context.Context, startOffset int64, maxLag time.Duration, logger log.Logger) (currLag time.Duration, _ error) {
	// clean-up resources spun up from this function
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(fmt.Errorf("partition reader stopped consuming partition until max consumer lag is honored"))

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0, // Retry forever (unless context is canceled / deadline exceeded).
	})

	type fetcherI interface {
		pollFetches(ctx2 context.Context) kgo.Fetches
	}

	var fetcher fetcherI
	if r.kafkaCfg.ReplayConcurrency > 1 {
		var err error
		fetcher, err = newConcurrentFetchers(ctx, r.client, r.logger, r.kafkaCfg.Topic, r.partitionID, startOffset, r.kafkaCfg.ReplayConcurrency, r.kafkaCfg.RecordsPerFetch, &r.metrics)
		if err != nil {
			return 0, errors.Wrap(err, "creating fetcher")
		}
	} else {
		fetcher = r
	}

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
			fetches := fetcher.pollFetches(ctx)
			err := r.processFetches(ctx, fetches, r.metrics.receiveDelayWhenStarting)
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

	for boff.Ongoing() {
		// We instantiate the consumer on each iteration because it is stateful, and we can't reuse it after closing.
		consumer := r.newConsumer.consumer()
		// If the PartitionReader is stopping and the ctx was cancelled, we don't want to interrupt the in-flight
		// processing midway. Instead, we let it finish, assuming it'll succeed.
		// If the processing fails while stopping, we log the error and let the backoff stop and bail out.
		// There is an edge-case when the processing gets stuck and doesn't let the stopping process. In such a case,
		// we expect the infrastructure (e.g. k8s) to eventually kill the process.
		consumeCtx := context.WithoutCancel(ctx)
		err := consumer.consume(consumeCtx, records)
		if err == nil {
			_ = consumer.Close(consumeCtx)
			break
		}
		level.Error(r.logger).Log(
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

func (r *PartitionReader) pollFetches(ctx context.Context) (result kgo.Fetches) {
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
		result.EachRecord(func(record *kgo.Record) {
			r.metrics.fetchedBytes.Add(float64(len(record.Value)))
		})
	}(time.Now())

	f := r.client.PollFetches(ctx)
	for fIdx, fetch := range f {
		for tIdx, topic := range fetch.Topics {
			for pIdx, partition := range topic.Partitions {
				afterConsumed := len(partition.Records)
				for i, record := range partition.Records {
					if record.Offset > r.consumedOffsetWatcher.LastConsumedOffset() {
						afterConsumed = i
						break
					}
				}
				f[fIdx].Topics[tIdx].Partitions[pIdx].Records = partition.Records[afterConsumed:]
			}
		}
	}
	return f
}

type fetchWant struct {
	startOffset int64 // inclusive
	endOffset   int64 // exclusive
	// result should be closed when there are no more fetches for this partition. It is ok to send multiple times on the channel.
	result   chan fetchResult
	maxBytes int32
}

type fetchResult struct {
	kgo.FetchPartition
	fetchedBytes int
}

type concurrentFetchers struct {
	client      *kgo.Client
	logger      log.Logger
	partitionID int32
	topicID     [16]byte
	topicName   string
	metrics     *readerMetrics
	tracer      *kotel.Tracer

	concurrency     int
	recordsPerFetch int

	orderedFetches     chan kgo.FetchPartition
	lastReturnedRecord int64
}

// newConcurrentFetchers creates a new concurrentFetchers. startOffset can be kafkaOffsetStart, kafkaOffsetEnd or a specific offset.
func newConcurrentFetchers(ctx context.Context, client *kgo.Client, logger log.Logger, topic string, partition int32, startOffset int64, concurrency int, recordsPerFetch int, metrics *readerMetrics) (*concurrentFetchers, error) {
	f := &concurrentFetchers{
		client:          client,
		logger:          logger,
		concurrency:     concurrency,
		topicName:       topic,
		partitionID:     partition,
		metrics:         metrics,
		recordsPerFetch: recordsPerFetch,
		tracer:          kotel.NewTracer(kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))),
		orderedFetches:  make(chan kgo.FetchPartition),
	}

	var err error
	switch startOffset {
	case kafkaOffsetStart:
		startOffset, err = f.getStartOffset(ctx)
	case kafkaOffsetEnd:
		startOffset, err = f.getEndOffset(ctx)
	}
	if err != nil {
		return nil, err
	}

	topics, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find topic ID: %w", err)
	}
	f.topicID = topics[topic].ID

	go f.runFetchers(ctx, startOffset)

	return f, nil
}

func (r *concurrentFetchers) pollFetches(ctx context.Context) (result kgo.Fetches) {
	waitStartTime := time.Now()
	select {
	case <-ctx.Done():
		return kgo.Fetches{}
	case f := <-r.orderedFetches:
		level.Info(r.logger).Log("msg", "received ordered fetch", "num_records", len(f.Records), "wait_duration", time.Since(waitStartTime))
		r.metrics.fetchWaitDuration.Observe(time.Since(waitStartTime).Seconds())
		trimUntil := 0
		f.EachRecord(func(record *kgo.Record) {
			r.metrics.fetchedBytes.Add(float64(len(record.Value))) // TODO dimitarvdimitrov maybe use the same metric name as franz-go, but make sure we're not conflicting with the actual client; perhaps disable metrics there and just use our own
			if record.Offset <= r.lastReturnedRecord {
				trimUntil++
				return // don't finish the traces multiple times
			}
			r.tracer.OnFetchRecordUnbuffered(record, true)
		})

		r.lastReturnedRecord = f.Records[len(f.Records)-1].Offset
		f.Records = f.Records[trimUntil:]

		r.metrics.fetchedDiscardedRecords.Add(float64(trimUntil))
		return kgo.Fetches{{
			Topics: []kgo.FetchTopic{
				{
					Topic:      r.topicName,
					Partitions: []kgo.FetchPartition{f},
				},
			},
		}}
	}
}

func (r *concurrentFetchers) fetchSingle(ctx context.Context, w fetchWant, _ log.Logger) (_ kgo.FetchPartition, fetchedBytes int) {
	req := kmsg.NewFetchRequest()
	req.Topics = []kmsg.FetchRequestTopic{{
		Topic:   r.topicName,
		TopicID: r.topicID,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:          r.partitionID,
			FetchOffset:        w.startOffset,
			LastFetchedEpoch:   -1,
			CurrentLeaderEpoch: -1,
			LogStartOffset:     -1,
			PartitionMaxBytes:  w.maxBytes,
		}},
	}}
	req.MinBytes = 1
	req.Version = 13
	req.MaxWaitMillis = 10000
	req.MaxBytes = w.maxBytes
	req.SessionEpoch = -1

	resp, err := req.RequestWith(ctx, r.client)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return kgo.FetchPartition{}, 0
		}
		return kgo.FetchPartition{
			Err: fmt.Errorf("fetching from kafka: %w", err),
		}, 0
	}
	rawPartitionResp := resp.Topics[0].Partitions[0]
	fetchedBytes = len(rawPartitionResp.RecordBatches)          // TODO dimitarvdimitrov make this conditional on the kafka backend - for WS we use uncompressed bytes (sumRecordLengths), for kafka we use the size of the response (rawPartitionResp.RecordBatches)
	r.metrics.fetchesCompressedBytes.Add(float64(fetchedBytes)) // This doesn't include overhead in the response, but that should be small.
	partition := processRespPartition(&rawPartitionResp, r.topicName)
	partition.EachRecord(r.tracer.OnFetchRecordBuffered) // TODO dimitarvdimitrov we might end up buffering the same record multiple times - what happens then?
	return partition, sumRecordLengths(partition.Records)
}

func sumRecordLengths(records []*kgo.Record) (sum int) {
	for _, r := range records {
		sum += len(r.Value)
	}
	return sum
}

// getStartOffset does roughly what franz-go does - issues a ListOffsets request to Kafka to get the start offset.
// Check how listOffsetsForBrokerLoad is implemented in franz-go for more details.
func (r *concurrentFetchers) getStartOffset(ctx context.Context) (int64, error) {
	client := kadm.NewClient(r.client)
	offsets, err := client.ListStartOffsets(ctx, r.topicName)
	if err != nil {
		return 0, fmt.Errorf("find topic id list start offset: %w", err)
	}
	return offsets[r.topicName][r.partitionID].Offset, nil
}

// getEndOffset does roughly what franz-go does - issues a ListOffsets request to Kafka to get the end offset.
// Check how listOffsetsForBrokerLoad is implemented in franz-go for more details.
func (r *concurrentFetchers) getEndOffset(ctx context.Context) (int64, error) {
	client := kadm.NewClient(r.client)
	offsets, err := client.ListEndOffsets(ctx, r.topicName)
	if err != nil {
		return 0, fmt.Errorf("find topic id list start offset: %w", err)
	}
	return offsets[r.topicName][r.partitionID].Offset, nil
}

func (r *concurrentFetchers) runFetchers(ctx context.Context, startOffset int64) {
	defer level.Info(r.logger).Log("msg", "done running fetchers")
	wg := sync.WaitGroup{}
	wg.Add(r.concurrency)
	defer wg.Wait()

	wants := make(chan fetchWant)
	defer close(wants)
	for i := 0; i < r.concurrency; i++ {
		logger := log.With(r.logger, "fetcher", i)
		go func() {
			defer wg.Done()
			level.Info(logger).Log("msg", "starting fetcher")
			defer level.Info(logger).Log("msg", "done with fetcher")
			for w := range wants {
				boff := backoff.New(ctx, backoff.Config{
					MinBackoff: 250 * time.Millisecond,
					MaxBackoff: 2 * time.Second,
					MaxRetries: 0, // retry forever
				})
				level.Info(logger).Log("msg", "starting to fetch", "start_offset", w.startOffset, "end_offset", w.endOffset)
				for attempt := 0; boff.Ongoing() && w.endOffset > w.startOffset; attempt++ {
					fetchStartTime := time.Now()
					f, fetchedBytes := r.fetchSingle(ctx, w, logger)
					if f.Err != nil {
						level.Info(logger).Log("msg", "fetcher got en error", "err", f.Err, "num_records", len(f.Records))
					}
					if errors.Is(f.Err, kerr.OffsetOutOfRange) {
						if w.startOffset > f.HighWatermark {
							break
						} else if w.startOffset < f.LogStartOffset {
							w.startOffset = f.LogStartOffset
							continue
						}
					}
					if len(f.Records) == 0 {
						boff.Wait()
						continue
					}
					boff.Reset()
					lastOffset := f.Records[len(f.Records)-1].Offset
					level.Info(logger).Log(
						"msg", "fetched records",
						"duration", time.Since(fetchStartTime),
						"attempt", attempt,
						"start_offset", w.startOffset,
						"end_offset", w.endOffset,
						"asked_records", w.endOffset-w.startOffset,
						"got_records", len(f.Records),
						"diff_records", int(w.endOffset-w.startOffset)-len(f.Records),
						"asked_bytes", w.maxBytes,
						"got_bytes", fetchedBytes,
						"diff_bytes", int(w.maxBytes)-fetchedBytes,
						"remaining_records", w.endOffset-lastOffset,
					)
					w.startOffset = lastOffset + 1
					bytesPerRecord := fetchedBytes / len(f.Records)
					w.maxBytes = max(1_000_000, int32(float64(bytesPerRecord))*int32(w.endOffset-w.startOffset)) // when we have only a few records to fetch we can afford to overfetch in order to not do more requests.

					select {
					case w.result <- fetchResult{FetchPartition: f, fetchedBytes: fetchedBytes}:
					case <-ctx.Done():
					}
				}
				close(w.result)
			}
		}()
	}

	var (
		bytesPerRecord = 10_000 // start with an estimation, we will update it as we consume
		nextFetch      = fetchWantFrom(bytesPerRecord, startOffset, r.recordsPerFetch)
		nextResult     chan fetchResult
		pendingResults = list.New()

		bufferedResult       fetchResult
		readyBufferedResults chan kgo.FetchPartition // this is non-nil when bufferedResult is non-empty
	)
	for {
		refillBufferedResult := nextResult
		if len(bufferedResult.Records) > 0 {
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
			nextFetch = nextFetchWant(bytesPerRecord, nextFetch, r.recordsPerFetch)

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
			bytesPerRecord = estimateBytesPerRecord(bytesPerRecord, result.fetchedBytes, len(result.Records))
			bufferedResult = result
			readyBufferedResults = r.orderedFetches

		case readyBufferedResults <- bufferedResult.FetchPartition:
			readyBufferedResults = nil
			bufferedResult = fetchResult{}
		}
	}
}

func estimateBytesPerRecord(currentBytesPerRecord, recordsSizeBytes, numRecords int) int {
	const currentFetchFactor = 0.8
	return int(
		(1-currentFetchFactor)*float64(currentBytesPerRecord) +
			currentFetchFactor*float64(recordsSizeBytes/numRecords),
	)
}

func nextFetchWant(bytesPerRecord int, fetch fetchWant, recordsPerFetch int) fetchWant {
	return fetchWantFrom(bytesPerRecord, fetch.endOffset, recordsPerFetch)
}

func fetchWantFrom(bytesPerRecord int, offset int64, recordsPerFetch int) fetchWant {
	return fetchWant{
		startOffset: offset,
		endOffset:   offset + int64(recordsPerFetch),
		result:      make(chan fetchResult, 1), // buffer of 1 to reduce impact of refetches
		maxBytes:    int32(recordsPerFetch * bytesPerRecord),
	}
}

type readerFrom interface {
	ReadFrom([]byte) error
}

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table; for consuming/producing

// processRespPartition processes all records in all potentially compressed
// batches (or message sets).
func processRespPartition(rp *kmsg.FetchResponseTopicPartition, topic string) kgo.FetchPartition {
	fp := kgo.FetchPartition{
		Partition:        rp.Partition,
		Err:              kerr.ErrorForCode(rp.ErrorCode),
		HighWatermark:    rp.HighWatermark,
		LastStableOffset: rp.LastStableOffset,
		LogStartOffset:   rp.LogStartOffset,
	}

	// A response could contain any of message v0, message v1, or record
	// batches, and this is solely dictated by the magic byte (not the
	// fetch response version). The magic byte is located at byte 17.
	//
	// 1 thru 8: int64 offset / first offset
	// 9 thru 12: int32 length
	// 13 thru 16: crc (magic 0 or 1), or partition leader epoch (magic 2)
	// 17: magic
	//
	// We decode and validate similarly for messages and record batches, so
	// we "abstract" away the high level stuff into a check function just
	// below, and then switch based on the magic for how to process.
	var (
		in = rp.RecordBatches

		r           readerFrom
		kind        string
		length      int32
		lengthField *int32
		crcField    *int32
		crcTable    *crc32.Table
		crcAt       int

		check = func() bool {
			// If we call into check, we know we have a valid
			// length, so we should be at least able to parse our
			// top level struct and validate the length and CRC.
			if err := r.ReadFrom(in[:length]); err != nil {
				fp.Err = fmt.Errorf("unable to read %s, not enough data", kind)
				return false
			}
			if length := int32(len(in[12:length])); length != *lengthField {
				fp.Err = fmt.Errorf("encoded length %d does not match read length %d", *lengthField, length)
				return false
			}
			// We have already validated that the slice is at least
			// 17 bytes, but our CRC may be later (i.e. RecordBatch
			// starts at byte 21). Ensure there is at least space
			// for a CRC.
			if len(in) < crcAt {
				fp.Err = fmt.Errorf("length %d is too short to allow for a crc", len(in))
				return false
			}
			if crcCalc := int32(crc32.Checksum(in[crcAt:length], crcTable)); crcCalc != *crcField {
				fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", *crcField, crcCalc)
				return false
			}
			return true
		}
	)

	for len(in) > 17 && fp.Err == nil {
		offset := int64(binary.BigEndian.Uint64(in))
		length = int32(binary.BigEndian.Uint32(in[8:]))
		length += 12 // for the int64 offset we skipped and int32 length field itself
		if len(in) < int(length) {
			break
		}

		switch magic := in[16]; magic {
		case 0:
			m := new(kmsg.MessageV0)
			kind = "message v0"
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 1:
			m := new(kmsg.MessageV1)
			kind = "message v1"
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 2:
			rb := new(kmsg.RecordBatch)
			kind = "record batch"
			lengthField = &rb.Length
			crcField = &rb.CRC
			crcTable = crc32c
			crcAt = 21
			r = rb

		default:
			fp.Err = fmt.Errorf("unknown magic %d; message offset is %d and length is %d, skipping and setting to next offset", magic, offset, length)
			return fp
		}

		if !check() {
			break
		}

		in = in[length:]

		switch t := r.(type) {
		case *kmsg.MessageV0:
			panic("unknown message type")
		case *kmsg.MessageV1:
			panic("unknown message type")
		case *kmsg.RecordBatch:
			_, _ = processRecordBatch(topic, &fp, t)
		}

	}

	return fp
}

func processRecordBatch(
	topic string,
	fp *kgo.FetchPartition,
	batch *kmsg.RecordBatch,
) (int, int) {
	if batch.Magic != 2 {
		fp.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return 0, 0
	}

	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		if rawRecords, err = decompress(rawRecords, compression); err != nil {
			return 0, 0 // truncated batch
		}
	}

	uncompressedBytes := len(rawRecords)

	numRecords := int(batch.NumRecords)
	krecords := readRawRecords(numRecords, rawRecords)

	// KAFKA-5443: compacted topics preserve the last offset in a batch,
	// even if the last record is removed, meaning that using offsets from
	// records alone may not get us to the next offset we need to ask for.
	//
	// We only perform this logic if we did not consume a truncated batch.
	// If we consume a truncated batch, then what was truncated could have
	// been an offset we are interested in consuming. Even if our fetch did
	// not advance this partition at all, we will eventually fetch from the
	// partition and not have a truncated response, at which point we will
	// either advance offsets or will set to nextAskOffset.

	for i := range krecords {
		record := recordToRecord(
			topic,
			fp.Partition,
			batch,
			&krecords[i],
		)
		fp.Records = append(fp.Records, record)
	}

	return len(krecords), uncompressedBytes
}

// recordToRecord converts a kmsg.RecordBatch's Record to a kgo Record.
func recordToRecord(
	topic string,
	partition int32,
	batch *kmsg.RecordBatch,
	record *kmsg.Record,
) *kgo.Record {
	h := make([]kgo.RecordHeader, 0, len(record.Headers))
	for _, kv := range record.Headers {
		h = append(h, kgo.RecordHeader{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	r := &kgo.Record{
		Key:       record.Key,
		Value:     record.Value,
		Headers:   h,
		Topic:     topic,
		Partition: partition,
		// Attrs:         kgo.RecordAttrs{uint8(batch.Attributes)},
		ProducerID:    batch.ProducerID,
		ProducerEpoch: batch.ProducerEpoch,
		LeaderEpoch:   batch.PartitionLeaderEpoch,
		Offset:        batch.FirstOffset + int64(record.OffsetDelta),
	}
	if r.Attrs.TimestampType() == 0 {
		r.Timestamp = timeFromMillis(batch.FirstTimestamp + record.TimestampDelta64)
	} else {
		r.Timestamp = timeFromMillis(batch.MaxTimestamp)
	}
	return r
}

func timeFromMillis(millis int64) time.Time {
	return time.Unix(0, millis*1e6)
}

// readRawRecords reads n records from in and returns them, returning early if
// there were partial records.
func readRawRecords(n int, in []byte) []kmsg.Record {
	rs := make([]kmsg.Record, n)
	for i := 0; i < n; i++ {
		length, used := kbin.Varint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return rs[:i]
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			return rs[:i]
		}
		in = in[total:]
	}
	return rs
}

type codecType int8

const (
	codecNone codecType = iota
	codecGzip           // TODO dimitarvdimitrov add support
	codecSnappy
	codecLZ4
	codecZstd // TODO dimitarvdimitrov add support
)

func decompress(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecSnappy:
		if len(src) > 16 && bytes.HasPrefix(src, xerialPfx) {
			return xerialDecode(src)
		}
		return s2.Decode(nil, src)
	case codecLZ4:
		unlz4 := lz4.NewReader(nil) // TODO dimitarvdimitrov this is pooled in franz-go now, consider exposing the funcs there
		unlz4.Reset(bytes.NewReader(src))
		out := new(bytes.Buffer)
		if _, err := io.Copy(out, unlz4); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	default:
		return nil, errors.New("unknown compression codec")
	}
}

var xerialPfx = []byte{130, 83, 78, 65, 80, 80, 89, 0}

var errMalformedXerial = errors.New("malformed xerial framing")

func xerialDecode(src []byte) ([]byte, error) {
	// bytes 0-8: xerial header
	// bytes 8-16: xerial version
	// everything after: uint32 chunk size, snappy chunk
	// we come into this function knowing src is at least 16
	src = src[16:]
	var dst, chunk []byte
	var err error
	for len(src) > 0 {
		if len(src) < 4 {
			return nil, errMalformedXerial
		}
		size := int32(binary.BigEndian.Uint32(src))
		src = src[4:]
		if size < 0 || len(src) < int(size) {
			return nil, errMalformedXerial
		}
		if chunk, err = s2.Decode(chunk[:cap(chunk)], src[:size]); err != nil {
			return nil, err
		}
		src = src[size:]
		dst = append(dst, chunk...)
	}
	return dst, nil
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
	fetchedBytes                     prometheus.Counter
	fetchesCompressedBytes           prometheus.Counter
	fetchWaitDuration                prometheus.Histogram
	fetchedDiscardedRecords          prometheus.Counter
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
		fetchedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetched_bytes_total",
			Help: "Total number of record bytes fetched from Kafka by the consumer.",
		}),
		fetchesCompressedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetches_compressed_bytes_total",
			Help: "Total number of compressed bytes fetched from Kafka by the consumer.",
		}),
		fetchedDiscardedRecords: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetched_discarded_records_total",
			Help: "Total number of records discarded by the consumer because they were already consumed.",
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
