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

func (r *PartitionReader) start(ctx context.Context) error {
	startFromOffset, err := r.fetchLastCommittedOffsetWithRetries(ctx)
	if err != nil {
		return err
	}
	r.consumedOffsetWatcher.Notify(startFromOffset - 1)
	level.Info(r.logger).Log("msg", "resuming consumption from offset", "offset", startFromOffset)

	r.client, err = r.newKafkaReader(kgo.NewOffset().At(startFromOffset))
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	r.committer = newConsumerCommitter(r.kafkaCfg, kadm.NewClient(r.client), r.partitionID, r.consumerGroup, r.commitInterval, r.logger)

	r.offsetReader = newPartitionOffsetReader(r.client, r.kafkaCfg.Topic, r.partitionID, r.kafkaCfg.LastProducedOffsetPollInterval, r.reg, r.logger)

	r.dependencies, err = services.NewManager(r.committer, r.offsetReader, r.consumedOffsetWatcher)
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

	err := services.StopManagerAndAwaitStopped(context.Background(), r.dependencies)
	if err != nil {
		return errors.Wrap(err, "stopping service manager")
	}
	r.client.Close()
	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := r.client.PollFetches(ctx)
		r.recordFetchesMetrics(fetches)
		r.logFetchErrs(fetches)
		fetches = filterOutErrFetches(fetches)

		// TODO consumeFetches() may get interrupted in the middle because of ctx canceled due to PartitionReader stopped.
		// 		We should improve it, but we shouldn't just pass a context.Background() because if consumption is stuck
		// 		then PartitionReader will never stop.
		r.consumeFetches(ctx, fetches)
		r.enqueueCommit(fetches)
		r.notifyLastConsumedOffset(fetches)
	}

	return nil
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

func (r *PartitionReader) logFetchErrs(fetches kgo.Fetches) {
	mErr := multierror.New()
	fetches.EachError(func(s string, i int32, err error) {
		// kgo advises to "restart" the kafka client if the returned error is a kerr.Error.
		// Recreating the client would cause duplicate metrics registration, so we don't do it for now.
		mErr.Add(fmt.Errorf("topic %q, partition %d: %w", s, i, err))
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
	})
}

func (r *PartitionReader) recordFetchesMetrics(fetches kgo.Fetches) {
	var (
		now        = time.Now()
		numRecords = 0
	)

	fetches.EachRecord(func(record *kgo.Record) {
		numRecords++
		r.metrics.receiveDelay.Observe(now.Sub(record.Timestamp).Seconds())
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

func (r *PartitionReader) fetchLastCommittedOffsetWithRetries(ctx context.Context) (offset int64, err error) {
	var (
		retry = backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 2 * time.Second,
			MaxRetries: 10,
		})
	)

	for retry.Ongoing() {
		offset, err = r.fetchLastCommittedOffset(ctx)
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

func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context) (int64, error) {
	const endOffset = -1 // -1 is a special value for kafka that means "the last offset"

	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.kafkaCfg.Address))
	if err != nil {
		return endOffset, errors.Wrap(err, "unable to create admin client")
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	offsets, err := adm.FetchOffsets(ctx, r.consumerGroup)
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		// In case we are booting up for the first time ever against this topic.
		return endOffset, nil
	}
	if err != nil {
		return endOffset, errors.Wrap(err, "unable to fetch group offsets")
	}
	offset, _ := offsets.Lookup(r.kafkaCfg.Topic, r.partitionID)
	return offset.At, nil
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

	kafkaCfg       KafkaConfig
	commitInterval time.Duration
	partitionID    int32
	consumerGroup  string

	toCommit  *atomic.Int64
	admClient *kadm.Client

	logger log.Logger
}

func newConsumerCommitter(kafkaCfg KafkaConfig, admClient *kadm.Client, partitionID int32, consumerGroup string, commitInterval time.Duration, logger log.Logger) *partitionCommitter {
	c := &partitionCommitter{
		logger:         logger,
		kafkaCfg:       kafkaCfg,
		partitionID:    partitionID,
		consumerGroup:  consumerGroup,
		toCommit:       atomic.NewInt64(-1),
		admClient:      admClient,
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
	toCommit := kadm.Offsets{}
	// Commit the offset after the last record.
	// The reason for this is that we resume consumption at this offset.
	// Leader epoch is -1 because we don't know it. This lets Kafka figure it out.
	toCommit.AddOffset(r.kafkaCfg.Topic, r.partitionID, offset+1, -1)

	committed, err := r.admClient.CommitOffsets(ctx, r.consumerGroup, toCommit)
	if err != nil || !committed.Ok() {
		level.Error(r.logger).Log("msg", "encountered error while committing offsets", "err", err, "commit_err", committed.Error(), "offset", offset)
	} else {
		committedOffset, _ := committed.Lookup(r.kafkaCfg.Topic, r.partitionID)
		level.Debug(r.logger).Log("msg", "committed offset", "offset", committedOffset.Offset.At)
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
	recordsPerFetch           prometheus.Histogram
	fetchesErrors             prometheus.Counter
	fetchesTotal              prometheus.Counter
	strongConsistencyRequests prometheus.Counter
	strongConsistencyFailures prometheus.Counter
	strongConsistencyLatency  prometheus.Summary
	kprom                     *kprom.Metrics
}

func newReaderMetrics(partitionID int32, reg prometheus.Registerer) readerMetrics {
	factory := promauto.With(reg)

	return readerMetrics{
		receiveDelay: factory.NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_reader_receive_delay_seconds",
			Help:       "Delay between producing a record and receiving it in the consumer.",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		recordsPerFetch: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_reader_records_per_fetch",
			Help:    "The number of records received by the consumer in a single fetch operation.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		}),
		fetchesErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetch_errors_total",
			Help: "The number of fetch errors encountered by the consumer.",
		}),
		fetchesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_fetches_total",
			Help: "Total number of Kafka fetches received by the consumer.",
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
		kprom: kprom.NewMetrics("cortex_ingest_storage_reader",
			kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}, reg)),
			// Do not export the client ID, because we use it to specify options to the backend.
			kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes)),
	}
}
