// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
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
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

type noopConsumer struct{}

func (noopConsumer) consume(ctx context.Context, records []record) error {
	return nil
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
	if maxLag := r.kafkaCfg.MaxConsumerLagAtStartup; maxLag > 0 {
		if r.kafkaCfg.ConsumeFromPositionAtStartup != consumeFromEnd {
			if err := r.processNextFetchesUntilMaxLagHonored(ctx, startOffset, maxLag); err != nil {
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
		r.processFetches(ctx, fetches, r.metrics.receiveDelayWhenRunning)
	}

	return nil
}

func (r *PartitionReader) processFetches(ctx context.Context, fetches kgo.Fetches, delayObserver prometheus.Observer) {
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

func (r *PartitionReader) processNextFetchesUntilMaxLagHonored(ctx context.Context, startOffset int64, maxLag time.Duration) error {
	level.Info(r.logger).Log("msg", "partition reader is starting to consume partition until max consumer lag is honored", "max_lag", maxLag)

	// clean-up resources spun up from this function
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(fmt.Errorf("partition reader stopped consuming partition until max consumer lag is honored"))

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // retry forever
	})

	fetcher, err := newConcurrentFetchers(ctx, r.client, r.logger, r.reg, r.kafkaCfg.Topic, r.partitionID, startOffset, r.kafkaCfg.ReplayConcurrency, &r.metrics)
	if err != nil {
		return errors.Wrap(err, "creating fetcher")
	}

	//defer func() {
	//	r.setPollingStartOffset(r.consumedOffsetWatcher.LastConsumedOffset())
	//}()

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
			fetches := fetcher.pollFetches(ctx)
			r.processFetches(ctx, fetches, r.metrics.receiveDelayWhenStarting)
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

// WaitReadConsistency waits until all data produced up until now has been consumed by the reader.
func (r *PartitionReader) WaitReadConsistency(ctx context.Context) (returnErr error) {
	startTime := time.Now()
	r.metrics.strongConsistencyRequests.Inc()

	spanLog := spanlogger.FromContext(ctx, r.logger)
	spanLog.DebugLog("msg", "waiting for read consistency")

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

	spanLog.DebugLog("msg", "catching up with last produced offset", "offset", lastProducedOffset)

	return r.consumedOffsetWatcher.Wait(ctx, lastProducedOffset)
}

func (r *PartitionReader) pollFetches(ctx context.Context) (result kgo.Fetches) {
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
		result.EachRecord(func(record *kgo.Record) {
			r.metrics.fetchedBytes.Add(float64(len(record.Value))) // TODO dimitarvdimitrov make sure we're not conflicting with the actual client; perhaps disable metrics there and just use our own
		})
		level.Info(r.logger).Log("msg", "PartitionReader.pollFetches done", "num_records", result.NumRecords())
	}(time.Now())

	level.Info(r.logger).Log("msg", "PartitionReader.pollFetches")
	// TODO dimitarvdimitrov consider using franz-go during stable state
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
	//return r.fetcher.pollFetches(ctx)
}

func (r *PartitionReader) setPollingStartOffset(offset int64) {
	r.consumedOffsetWatcher.Notify(offset)
}

type fetchWant struct {
	startOffset int64 // inclusive
	endOffset   int64 // exclusive
	// result should be closed when there are no more fetches for this partition. It is ok to send multiple times on the channel.
	result chan kgo.FetchPartition
	// TODO dimitarvdimitrov consider including expected bytes here so we can tell kafka to not send a ton of bytes back. We can estimate those.
}

type concurrentFetchers struct {
	client      *kgo.Client
	logger      log.Logger
	partitionID int32
	topicID     [16]byte
	topicName   string
	metrics     *readerMetrics

	concurrency            int
	nextFetchOffset        int64
	fetchesCompressedBytes prometheus.Counter

	// TODO dimitarvdimitrov do we need to take care of tracking highwatermark?
	orderedFetches chan kgo.FetchPartition
}

// newConcurrentFetchers creates a new concurrentFetchers. startOffset can be kafkaOffsetStart, kafkaOffsetEnd or a specific offset.
func newConcurrentFetchers(ctx context.Context, client *kgo.Client, logger log.Logger, reg prometheus.Registerer, topic string, partition int32, startOffset int64, concurrency int, metrics *readerMetrics) (*concurrentFetchers, error) {
	f := &concurrentFetchers{
		client:         client,
		logger:         logger,
		concurrency:    concurrency,
		topicName:      topic,
		partitionID:    partition,
		metrics:        metrics,
		orderedFetches: make(chan kgo.FetchPartition, 1),
		fetchesCompressedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetches_compressed_bytes_total",
			Help: "Total number of compressed bytes fetched from Kafka by the consumer.",
		}),
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
	defer func(start time.Time) {
		r.metrics.fetchWaitDuration.Observe(time.Since(start).Seconds())
		result.EachRecord(func(record *kgo.Record) {
			r.metrics.fetchedBytes.Add(float64(len(record.Value))) // TODO dimitarvdimitrov make sure we're not conflicting with the actual client; perhaps disable metrics there and just use our own
		})
	}(time.Now())

	select {
	case <-ctx.Done():
		return kgo.Fetches{}
	case f := <-r.orderedFetches:
		r.logger.Log("msg", "received ordered fetch", "num_records", len(f.Records))
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

func (r *concurrentFetchers) fetchSingle(ctx context.Context, w fetchWant) kgo.FetchPartition {
	level.Debug(r.logger).Log("msg", "fetching", "offset", w.startOffset, "partition", r.partitionID, "topic", r.topicName)
	req := kmsg.NewFetchRequest()
	req.Topics = []kmsg.FetchRequestTopic{{
		Topic:   r.topicName,
		TopicID: r.topicID,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:          r.partitionID,
			FetchOffset:        w.startOffset,
			CurrentLeaderEpoch: 0, // TODO dimitarvdimitrov works for cases with a fresh kafka (like unit tests); needs fixing
			LastFetchedEpoch:   -1,
			LogStartOffset:     -1,
			PartitionMaxBytes:  100_000_000,
		}},
	}}
	req.MinBytes = 1
	req.Version = 13
	req.MaxWaitMillis = 5000
	req.MaxBytes = 100_000_000

	req.SessionEpoch = 0

	resp, err := req.RequestWith(ctx, r.client)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return kgo.FetchPartition{}
		}
		return kgo.FetchPartition{
			Err: fmt.Errorf("fetching from kafka: %w", err),
		}
	}

	r.fetchesCompressedBytes.Add(float64(len(resp.Topics[0].Partitions[0].RecordBatches))) // TODO dimitarvdimitrov this doesn't include overhead in the response - investigate
	partition := processRespPartition(&resp.Topics[0].Partitions[0], r.topicName)
	level.Info(r.logger).Log(
		"msg", "fetched records",
		"error_code", resp.ErrorCode,
		"num_records", len(partition.Records),
		"num_topics", len(resp.Topics),
		"num_partitions", len(resp.Topics[0].Partitions),
	)
	return partition
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
	results := make(chan chan kgo.FetchPartition, r.concurrency+2)
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
				for boff.Ongoing() && w.endOffset > w.startOffset {
					f := r.fetchSingle(ctx, w)
					if f.Err != nil {
						level.Info(logger).Log("msg", "fetcher got en error", "err", f.Err, "num_records", len(f.Records))
					}
					//if errors.Is(f.Err, kerr.OffsetOutOfRange) {
					//	w.startOffset++
					//	continue
					//}
					if len(f.Records) == 0 {
						boff.Wait()
						continue
					}
					boff.Reset()
					lastOffset := f.Records[len(f.Records)-1].Offset
					w.startOffset = lastOffset + 1
					level.Info(logger).Log("msg", "received records", "new_start_offset", w.startOffset, "new_end_offset", w.endOffset)
					select {
					case w.result <- f:
					case <-ctx.Done():
					}
				}
				close(w.result)
			}
		}()
	}

	var (
		nextFetch  = fetchWantFrom(startOffset)
		nextResult chan kgo.FetchPartition
	)
	r.logger.Log("msg", "results", "cap", cap(results), "len", len(results))

	for {
		select {
		case <-ctx.Done():
			return
		case wants <- nextFetch:
			select {
			case results <- nextFetch.result:
				r.logger.Log("msg", "wrote to results")
			default:
				panic("deadlock")
			}

			if nextResult == nil {
				nextResult = <-results
				r.logger.Log("msg", "took from results")
			}
			nextFetch = nextFetchWant(nextFetch)
		case result, moreLeft := <-nextResult:
			if !moreLeft {
				if len(results) > 0 {
					nextResult = <-results
					r.logger.Log("msg", "took from results")
				} else {
					nextResult = nil
				}
				continue
			}
			select {
			case r.orderedFetches <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func nextFetchWant(fetch fetchWant) fetchWant {
	return fetchWantFrom(fetch.endOffset)
}

func fetchWantFrom(offset int64) fetchWant {
	const recordsPerFetch = 128
	return fetchWant{
		startOffset: offset,
		endOffset:   offset + recordsPerFetch,
		result:      make(chan kgo.FetchPartition, 1),
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
			panic("unkown message type")
		case *kmsg.MessageV1:
			panic("unkown message type")
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
		//Attrs:         kgo.RecordAttrs{uint8(batch.Attributes)},
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
	codecGzip
	codecSnappy
	codecLZ4
	codecZstd
)

func decompress(src []byte, codec byte) ([]byte, error) {
	switch codecType(codec) {
	case codecNone:
		return src, nil
	case codecLZ4:
		unlz4 := lz4.NewReader(nil)
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
	receiveDelayWhenStarting  prometheus.Observer
	receiveDelayWhenRunning   prometheus.Observer
	recordsPerFetch           prometheus.Histogram
	fetchesErrors             prometheus.Counter
	fetchesTotal              prometheus.Counter
	fetchedBytes              prometheus.Counter
	fetchWaitDuration         prometheus.Histogram
	strongConsistencyRequests prometheus.Counter
	strongConsistencyFailures prometheus.Counter
	strongConsistencyLatency  prometheus.Histogram
	lastConsumedOffset        prometheus.Gauge
	consumeLatency            prometheus.Histogram
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
		fetchWaitDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_records_batch_wait_duration_seconds",
			Help:                        "How long a consumer spent waiting for a batch of records from the Kafka client. If fetching is faster than processing, then this will be close to 0.",
			NativeHistogramBucketFactor: 1.1,
		}),
		fetchedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetched_bytes_total",
			Help: "Total number of record bytes fetched from Kafka by the consumer.",
		}),
		consumeLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_records_batch_process_duration_seconds",
			Help:                        "How long a consumer spent processing a batch of records from Kafka.",
			NativeHistogramBucketFactor: 1.1,
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
