// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
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

// consumerGroup is only used to store commit offsets, not for actual consuming.
const consumerGroup = "mimir"

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

	kafkaCfg    KafkaConfig
	partitionID int32

	client *kgo.Client

	consumer recordConsumer
	metrics  readerMetrics

	committer      *partitionCommitter
	commitInterval time.Duration

	logger log.Logger
}

func NewPartitionReaderForPusher(kafkaCfg KafkaConfig, partitionID int32, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	metrics := newReaderMetrics(partitionID, reg)
	consumer := newPusherConsumer(pusher, reg, logger)
	return newPartitionReader(kafkaCfg, partitionID, consumer, logger, metrics)
}

func newPartitionReader(kafkaCfg KafkaConfig, partitionID int32, consumer recordConsumer, logger log.Logger, metrics readerMetrics) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaCfg:       kafkaCfg,
		partitionID:    partitionID,
		consumer:       consumer,
		metrics:        metrics,
		commitInterval: time.Second,
		logger:         log.With(logger, "partition", partitionID),
	}

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) error {
	offset, err := r.fetchLastCommittedOffsetWithRetries(ctx)
	if err != nil {
		return err
	}
	level.Info(r.logger).Log("msg", "resuming consumption from offset", "offset", offset)

	r.client, err = r.newKafkaReader(offset)
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	r.committer = newConsumerCommitter(r.kafkaCfg, kadm.NewClient(r.client), r.partitionID, r.commitInterval, r.logger)

	r.dependencies, err = services.NewManager(r.committer)
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
	consumeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for ctx.Err() == nil {
		fetches := r.client.PollFetches(ctx)
		if fetches.Err() != nil {
			if errors.Is(fetches.Err(), context.Canceled) {
				return nil
			}
			err := collectFetchErrs(fetches)
			level.Error(r.logger).Log("msg", "encountered error while fetching", "err", err)
			continue
		}

		r.recordFetchesMetrics(fetches)
		r.consumeFetches(consumeCtx, fetches)
		r.enqueueCommit(fetches)
	}

	return nil
}

func collectFetchErrs(fetches kgo.Fetches) (_ error) {
	mErr := multierror.New()
	fetches.EachError(func(s string, i int32, err error) {
		// kgo advises to "restart" the kafka client if the returned error is a kerr.Error.
		// Recreating the client would cause duplicate metrics registration, so we don't do it for now.
		mErr.Add(err)
	})
	return mErr.Err()
}

func (r *PartitionReader) enqueueCommit(fetches kgo.Fetches) {
	if fetches.NumRecords() == 0 {
		return
	}
	lastOffset := int64(0)
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		lastOffset = partition.Records[len(partition.Records)-1].Offset
	})
	r.committer.enqueueOffset(lastOffset)
}

func (r *PartitionReader) consumeFetches(ctx context.Context, fetches kgo.Fetches) {
	records := make([]record, 0, len(fetches.Records()))

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
			"msg", "encountered error while consuming; will retry",
			"err", err,
			"record_min_offset", minOffset,
			"record_max_offset", maxOffset,
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}

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

	r.metrics.recordsPerFetch.Observe(float64(numRecords))
}

func (r *PartitionReader) newKafkaReader(at kgo.Offset) (*kgo.Client, error) {
	opts := append(
		commonKafkaClientOptions(r.kafkaCfg, r.metrics.kprom, r.logger),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaCfg.Topic: {r.partitionID: at},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(100_000_000),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating kafka client")
	}

	return client, nil
}

func (r *PartitionReader) fetchLastCommittedOffsetWithRetries(ctx context.Context) (offset kgo.Offset, err error) {
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

func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context) (kgo.Offset, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.kafkaCfg.Address))
	if err != nil {
		return kgo.NewOffset(), errors.Wrap(err, "unable to create admin client")
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	offsets, err := adm.FetchOffsets(ctx, consumerGroup)
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		// In case we are booting up for the first time ever against this topic.
		return kgo.NewOffset().AtEnd(), nil
	}
	if err != nil {
		return kgo.NewOffset(), errors.Wrap(err, "unable to fetch group offsets")
	}
	offset, _ := offsets.Lookup(r.kafkaCfg.Topic, r.partitionID)
	return kgo.NewOffset().At(offset.At), nil
}

type partitionCommitter struct {
	services.Service

	kafkaCfg       KafkaConfig
	commitInterval time.Duration
	partitionID    int32

	toCommit  *atomic.Int64
	admClient *kadm.Client

	logger log.Logger
}

func newConsumerCommitter(kafkaCfg KafkaConfig, admClient *kadm.Client, partitionID int32, commitInterval time.Duration, logger log.Logger) *partitionCommitter {
	c := &partitionCommitter{
		logger:         logger,
		kafkaCfg:       kafkaCfg,
		partitionID:    partitionID,
		toCommit:       atomic.NewInt64(0),
		admClient:      admClient,
		commitInterval: commitInterval,
	}
	c.Service = services.NewBasicService(nil, c.run, nil)
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

			toCommit := kadm.Offsets{}
			// Commit the offset after the last record.
			// The reason for this is that we resume consumption at this offset.
			// Leader epoch is -1 because we don't know it. This lets Kafka figure it out.
			toCommit.AddOffset(r.kafkaCfg.Topic, r.partitionID, currOffset+1, -1)

			committed, err := r.admClient.CommitOffsets(ctx, consumerGroup, toCommit)
			if err != nil || !committed.Ok() {
				level.Error(r.logger).Log("msg", "encountered error while committing offsets", "err", err, "commit_err", committed.Error(), "offset", currOffset)
			} else {
				committedOffset, _ := committed.Lookup(r.kafkaCfg.Topic, r.partitionID)
				level.Debug(r.logger).Log("msg", "committed offset", "offset", committedOffset.Offset.At)
			}
		}
	}
}

type readerMetrics struct {
	receiveDelay    prometheus.Summary
	recordsPerFetch prometheus.Histogram
	kprom           *kprom.Metrics
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
		kprom: kprom.NewMetrics("cortex_ingest_storage_reader",
			kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}, reg)),
			// Do not export the client ID, because we use it to specify options to the backend.
			kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes)),
	}
}
