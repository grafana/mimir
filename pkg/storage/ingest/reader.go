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
	"github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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

type consumerFactory interface {
	consumer() recordConsumer
}

type consumerFactoryFunc func() recordConsumer

func (c consumerFactoryFunc) consumer() recordConsumer {
	return c()
}

type PartitionReader struct {
	services.Service
	dependencies *services.Manager

	kafkaCfg                              KafkaConfig
	partitionID                           int32
	consumerGroup                         string
	concurrentFetchersMinBytesMaxWaitTime time.Duration

	// client and fetcher are both start after PartitionReader creation. Fetcher could also be
	// replaced during PartitionReader lifetime. To avoid concurrency issues with functions
	// getting their pointers (e.g. BufferedRecords()) we use atomic to protect.
	client  atomic.Pointer[kgo.Client]
	fetcher atomic.Pointer[fetcher]

	newConsumer consumerFactory
	metrics     readerMetrics

	committer *partitionCommitter

	// consumedOffsetWatcher is used to wait until a given offset has been consumed.
	// This gets initialised with -1 which means nothing has been consumed from the partition yet.
	consumedOffsetWatcher *partitionOffsetWatcher
	offsetReader          *partitionOffsetReader

	logger         log.Logger
	reg            prometheus.Registerer
	lastSeenOffset int64
}

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, instanceID string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	metrics := newPusherConsumerMetrics(reg)
	factory := consumerFactoryFunc(func() recordConsumer {
		return newPusherConsumer(pusher, kafkaCfg, metrics, logger)
	})
	return newPartitionReader(kafkaCfg, partitionID, instanceID, factory, logger, reg)
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, instanceID string, consumer consumerFactory, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:                              kafkaCfg,
		partitionID:                           partitionID,
		newConsumer:                           consumer,
		consumerGroup:                         kafkaCfg.GetConsumerGroup(instanceID, partitionID),
		consumedOffsetWatcher:                 newPartitionOffsetWatcher(),
		concurrentFetchersMinBytesMaxWaitTime: defaultMinBytesMaxWaitTime,
		logger:                                log.With(logger, "partition", partitionID),
		reg:                                   reg,
	}

	r.metrics = newReaderMetrics(partitionID, reg, r)

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

// Stop implements fetcher
func (r *PartitionReader) Stop() {
	// Given the partition reader has no concurrency it doesn't support stopping anything.
}

// Update implements fetcher
func (r *PartitionReader) Update(_ context.Context, _ int) {
	// Given the partition reader has no concurrency it doesn't support updates.
}

func (r *PartitionReader) BufferedRecords() int64 {
	var fcount, ccount int64

	if f := r.getFetcher(); f != nil && f != r {
		fcount = f.BufferedRecords()
	}

	if c := r.client.Load(); c != nil {
		ccount = c.BufferedFetchRecords()
	}

	return fcount + ccount
}

func (r *PartitionReader) BufferedBytes() int64 {
	var fcount, ccount int64

	if f := r.getFetcher(); f != nil && f != r {
		fcount = f.BufferedBytes()
	}

	if c := r.client.Load(); c != nil {
		ccount = c.BufferedFetchBytes()
	}

	return fcount + ccount
}

func (r *PartitionReader) EstimatedBytesPerRecord() int64 {
	if f := r.getFetcher(); f != nil && f != r {
		return f.EstimatedBytesPerRecord()
	}

	return 0
}

func (r *PartitionReader) start(ctx context.Context) (returnErr error) {
	if r.kafkaCfg.AutoCreateTopicEnabled {
		if err := createTopic(r.kafkaCfg, r.logger); err != nil {
			return err
		}
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

	// lastConsumedOffset could be a special negative offset (e.g. partition start, or partition end).
	r.lastSeenOffset = lastConsumedOffset
	// Initialise the last consumed offset only if we've got an actual offset from the consumer group.
	if lastConsumedOffset >= 0 {
		r.consumedOffsetWatcher.Notify(lastConsumedOffset)
	}

	// Create a Kafka client without configuring any partition to consume (it will be done later).
	client, err := NewKafkaReaderClient(r.kafkaCfg, r.metrics.kprom, r.logger)
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	r.client.Store(client)

	r.committer = newPartitionCommitter(r.kafkaCfg, kadm.NewClient(r.client.Load()), r.partitionID, r.consumerGroup, r.logger, r.reg)

	offsetsClient := newPartitionOffsetClient(r.client.Load(), r.kafkaCfg.Topic, r.reg, r.logger)

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

	r.dependencies, err = services.NewManager(r.committer, r.offsetReader, r.consumedOffsetWatcher, startOffsetReader, r.metrics)
	if err != nil {
		return errors.Wrap(err, "creating service manager")
	}
	// Use context.Background() because we want to stop all dependencies when the PartitionReader stops
	// instead of stopping them when ctx is cancelled and while the PartitionReader is still running.
	err = services.StartManagerAndAwaitHealthy(context.Background(), r.dependencies)
	if err != nil {
		return errors.Wrap(err, "starting service manager")
	}

	if r.kafkaCfg.StartupFetchConcurrency > 0 {
		// When concurrent fetch is enabled we manually fetch from the partition so we don't want the Kafka
		// client to buffer any record. However, we still want to configure partition consumption so that
		// the partition metadata is kept updated by the client (our concurrent fetcher requires metadata to
		// get the partition leader).
		//
		// To make it happen, we do pause the fetching first and then we configure consumption. The consumption
		// will be kept paused until the explicit ResumeFetchPartitions() is called.
		r.client.Load().PauseFetchPartitions(map[string][]int32{
			r.kafkaCfg.Topic: {r.partitionID},
		})
		r.client.Load().AddConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaCfg.Topic: {r.partitionID: kgo.NewOffset().At(startOffset)},
		})

		f, err := newConcurrentFetchers(ctx, r.client.Load(), r.logger, r.kafkaCfg.Topic, r.partitionID, startOffset, r.kafkaCfg.StartupFetchConcurrency, int32(r.kafkaCfg.MaxBufferedBytes), r.kafkaCfg.UseCompressedBytesAsFetchMaxBytes, r.concurrentFetchersMinBytesMaxWaitTime, offsetsClient, startOffsetReader, r.kafkaCfg.concurrentFetchersFetchBackoffConfig, &r.metrics)
		if err != nil {
			return errors.Wrap(err, "creating concurrent fetchers during startup")
		}
		r.setFetcher(f)
	} else {
		// When concurrent fetch is disabled we read records directly from the Kafka client, so we want it
		// to consume the partition.
		r.client.Load().AddConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaCfg.Topic: {r.partitionID: kgo.NewOffset().At(startOffset)},
		})

		r.setFetcher(r)
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

	if f := r.getFetcher(); f != nil {
		f.Stop()
	}

	if c := r.client.Load(); c != nil {
		c.Close()
	}

	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	r.switchToOngoingFetcher(ctx)

	for ctx.Err() == nil {
		err := r.processNextFetches(ctx, r.metrics.receiveDelayWhenRunning)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Fail the whole service in case of a non-recoverable error.
			return err
		}
	}

	return nil
}

// switchToOngoingFetcher switches to the configured ongoing fetcher. This function could be
// called multiple times.
func (r *PartitionReader) switchToOngoingFetcher(ctx context.Context) {
	if r.kafkaCfg.StartupFetchConcurrency == r.kafkaCfg.OngoingFetchConcurrency {
		// we're already using the same settings, no need to switch
		return
	}

	if r.kafkaCfg.StartupFetchConcurrency > 0 && r.kafkaCfg.OngoingFetchConcurrency > 0 {
		// No need to switch the fetcher, just update the concurrency.
		r.getFetcher().Update(ctx, r.kafkaCfg.OngoingFetchConcurrency)
		return
	}

	if r.kafkaCfg.StartupFetchConcurrency > 0 && r.kafkaCfg.OngoingFetchConcurrency == 0 {
		if r.getFetcher() == r {
			// This method has been called before, no need to switch the fetcher.
			return
		}

		level.Info(r.logger).Log("msg", "partition reader is switching to non-concurrent fetcher")

		// Stop the current fetcher before replacing it.
		r.getFetcher().Stop()

		// We need to switch to franz-go for ongoing fetches.
		// If we've already fetched some records, we should discard them from franz-go and start from the last consumed offset.
		r.setFetcher(r)

		lastConsumed := r.consumedOffsetWatcher.LastConsumedOffset()
		if lastConsumed == -1 {
			// We haven't consumed any records yet with the other fetcher.
			//
			// The franz-go client is initialized to start consuming from the same place as the other fetcher.
			// We can just use the client, but we have to resume the fetching because it was previously paused.
			r.client.Load().ResumeFetchPartitions(map[string][]int32{
				r.kafkaCfg.Topic: {r.partitionID},
			})
			return
		}

		// The consumption is paused so in theory the client shouldn't have any buffered records. However, to start
		// from a clean setup and have the guarantee that we're not going to read any previously buffered record,
		// we do remove the partition consumption (this clears the buffer), then we resume the fetching and finally
		// we add the consumption back.
		r.client.Load().RemoveConsumePartitions(map[string][]int32{
			r.kafkaCfg.Topic: {r.partitionID},
		})
		r.client.Load().ResumeFetchPartitions(map[string][]int32{
			r.kafkaCfg.Topic: {r.partitionID},
		})
		r.client.Load().AddConsumePartitions(map[string]map[int32]kgo.Offset{
			// Resume from the next unconsumed offset.
			r.kafkaCfg.Topic: {r.partitionID: kgo.NewOffset().At(lastConsumed + 1)},
		})
	}
}

func (r *PartitionReader) processNextFetches(ctx context.Context, delayObserver prometheus.Observer) error {
	fetches, fetchCtx := r.getFetcher().PollFetches(ctx)
	// Propagate the fetching span to consuming the records.
	ctx = opentracing.ContextWithSpan(ctx, opentracing.SpanFromContext(fetchCtx))
	r.recordFetchesMetrics(fetches, delayObserver)
	r.logFetchErrors(fetches)
	fetches = filterOutErrFetches(fetches)

	// instrument only after we've done all pre-processing of records. We don't expect the set of records to change beyond this point.
	instrumentGaps(findGapsInRecords(fetches, r.lastSeenOffset), r.metrics.missedRecords, r.logger)
	r.lastSeenOffset = max(r.lastSeenOffset, lastOffset(fetches))

	err := r.consumeFetches(ctx, fetches)
	if err != nil {
		return fmt.Errorf("consume %d records: %w", fetches.NumRecords(), err)
	}
	r.enqueueCommit(fetches)
	r.notifyLastConsumedOffset(fetches)
	return nil
}

func lastOffset(fetches kgo.Fetches) int64 {
	var o int64
	fetches.EachRecord(func(record *kgo.Record) {
		o = record.Offset
	})
	return o
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
		// The timeout is equal to the max lag x2. This is done because the ongoing fetcher config reduces lag more slowly,
		// but is better at keeping up with the partition and minimizing e2e lag.
		func() (time.Duration, error) {
			timedCtx, cancel := context.WithTimeoutCause(ctx, 2*maxLag, errWaitTargetLagDeadlineExceeded)
			defer cancel()

			// Don't use timedCtx because we want the fetchers to continue running
			// At this point we're close enough to the end of the partition that we should switch to the more responsive fetcher.
			r.switchToOngoingFetcher(ctx)

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
		instrument.ObserveWithExemplar(ctx, r.metrics.consumeLatency, time.Since(consumeStart).Seconds())
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
			// The context might have been cancelled in the meantime, so we return here instead of breaking the loop and returning the context error
			return nil
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

func (r *PartitionReader) setFetcher(f fetcher) {
	r.fetcher.Store(&f)
}

func (r *PartitionReader) getFetcher() fetcher {
	pointer := r.fetcher.Load()
	if pointer == nil {
		return nil
	}

	return *pointer
}

func (r *PartitionReader) PollFetches(ctx context.Context) (result kgo.Fetches, fetchContext context.Context) {
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	return r.client.Load().PollFetches(ctx), ctx
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
	services.Service

	bufferedFetchedRecords           prometheus.GaugeFunc
	bufferedFetchedBytes             prometheus.GaugeFunc
	estimatedBytesPerRecord          prometheus.Histogram
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
	missedRecords                    prometheus.Counter
}

type readerMetricsSource interface {
	BufferedBytes() int64
	BufferedRecords() int64
	EstimatedBytesPerRecord() int64
}

func newReaderMetrics(partitionID int32, reg prometheus.Registerer, metricsSource readerMetricsSource) readerMetrics {
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

	m := readerMetrics{
		bufferedFetchedRecords: promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingest_storage_reader_buffered_fetched_records",
			Help: "The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.",
		}, func() float64 { return float64(metricsSource.BufferedRecords()) }),
		bufferedFetchedBytes: promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingest_storage_reader_buffered_fetched_bytes",
			Help: "The number of bytes fetched or requested from Kafka by both concurrent fetchers and the Kafka client but not yet processed. The value depends on -ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes.",
		}, func() float64 { return float64(metricsSource.BufferedBytes()) }),
		estimatedBytesPerRecord: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_estimated_bytes_per_record",
			Help:                        "Observations with the current size estimation of records fetched from Kafka. Sampled at 10Hz.",
			NativeHistogramBucketFactor: 1.1,
		}),
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
			Help:                        "How long a consumer spent processing a batch of records from Kafka. This includes retries on server errors.",
			NativeHistogramBucketFactor: 1.1,
		}),
		strongConsistencyInstrumentation: NewStrongReadConsistencyInstrumentation[struct{}](component, reg),
		lastConsumedOffset:               lastConsumedOffset,
		kprom:                            NewKafkaReaderClientMetrics(component, reg),
		missedRecords: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_missed_records_total",
			Help: "The number of offsets that were never consumed by the reader because they weren't fetched.",
		}),
	}

	m.Service = services.NewTimerService(100*time.Millisecond, nil, func(context.Context) error {
		m.estimatedBytesPerRecord.Observe(float64(metricsSource.EstimatedBytesPerRecord()))
		return nil
	}, nil)
	return m
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
