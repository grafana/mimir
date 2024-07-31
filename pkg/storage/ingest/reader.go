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

	util_log "github.com/grafana/mimir/pkg/util/log"
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

	committer *partitionCommitter

	// consumedOffsetWatcher is used to wait until a given offset has been consumed.
	// This gets initialised with -1 which means nothing has been consumed from the partition yet.
	consumedOffsetWatcher *partitionOffsetWatcher
	offsetReader          *partitionOffsetReader

	logger log.Logger
	reg    prometheus.Registerer
}

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, instanceID string, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	consumer := newPusherConsumer(pusher, util_log.NewSampler(kafkaCfg.FallbackClientErrorSampleRate), reg, logger)
	return newPartitionReader(kafkaCfg, partitionID, instanceID, consumer, logger, reg)
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, instanceID string, consumer recordConsumer, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:              kafkaCfg,
		partitionID:           partitionID,
		consumer:              consumer,
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
		r.processNextFetches(ctx, r.metrics.receiveDelayWhenRunning)
	}

	return nil
}

func (r *PartitionReader) processNextFetches(ctx context.Context, delayObserver prometheus.Observer) {
	fetches := r.pollFetches(ctx)
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

			r.processNextFetches(ctx, r.metrics.receiveDelayWhenStarting)
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

func (r *PartitionReader) consumeFetches(ctx context.Context, fetches kgo.Fetches) {
	if fetches.NumRecords() == 0 {
		return
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

	for boff.Ongoing() {
		consumeStart := time.Now()
		err := r.consumer.consume(ctx, records)
		r.metrics.consumeLatency.Observe(time.Since(consumeStart).Seconds())
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

func (r *PartitionReader) pollFetches(ctx context.Context) kgo.Fetches {
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	return r.client.PollFetches(ctx)
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
