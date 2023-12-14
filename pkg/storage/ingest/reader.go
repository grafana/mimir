// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"strconv"
	"sync"
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
)

// consumerGroup is only used to store commit offsets, not for actual consuming.
const consumerGroup = "mimir"

type record struct {
	tenantID string
	content  []byte
}

type recordConsumer interface {
	consume(context.Context, []record) error
}

type PartitionReader struct {
	services.Service

	kafkaAddress  string
	kafkaTopic    string
	kafkaClientID string
	partitionID   int32

	client    *kgo.Client
	admClient *kadm.Client

	consumer recordConsumer
	metrics  readerMetrics

	commitInterval time.Duration
	commitLoopWg   *sync.WaitGroup
	commitFetches  chan kgo.Fetches

	logger log.Logger
}

func NewReaderForPusher(kafkaAddress, kafkaTopic, kafkaClientID string, partitionID int32, pusher Pusher, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	metrics := newReaderMetrics(partitionID, reg)
	consumer := newPusherConsumer(pusher, metrics, logger)
	return newReader(kafkaAddress, kafkaTopic, kafkaClientID, partitionID, consumer, logger, metrics)
}

func newReader(kafkaAddress, kafkaTopic, kafkaClientID string, partitionID int32, consumer recordConsumer, logger log.Logger, metrics readerMetrics) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaAddress:   kafkaAddress,
		kafkaTopic:     kafkaTopic,
		kafkaClientID:  kafkaClientID,
		partitionID:    partitionID,
		consumer:       consumer, // TODO consume records in parallel
		commitInterval: time.Second,
		commitLoopWg:   &sync.WaitGroup{},
		commitFetches:  make(chan kgo.Fetches, 1),
		metrics:        metrics,
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
	r.admClient = kadm.NewClient(r.client)

	return nil
}

func (r *PartitionReader) stop(error) error {
	r.client.Close()
	// r.admClient needs no closing since it's using r.client
	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	defer r.commitLoopWg.Wait()

	consumeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.commitLoopWg.Add(1)
	go r.commitLoop(consumeCtx)

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
	r.commitFetches <- fetches
}

func (r *PartitionReader) consumeFetches(ctx context.Context, fetches kgo.Fetches) {
	records := make([]record, 0, len(fetches.Records()))

	var minOffset, maxOffset int
	fetches.EachRecord(func(record *kgo.Record) {
		minOffset = min(minOffset, int(record.Offset))
		maxOffset = max(maxOffset, int(record.Offset))
		records = append(records, mapRecord(record))
	})

	err := r.consumer.consume(ctx, records)
	if err != nil {
		level.Error(r.logger).Log("msg", "encountered error processing records; skipping", "min_offset", minOffset, "max_offset", maxOffset, "err", err)
		// TODO abort ingesting & back off if it's a server error, ignore error if it's a client error
	}
}

func mapRecord(r *kgo.Record) record {
	return record{
		content:  r.Value,
		tenantID: string(r.Key),
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
	client, err := kgo.NewClient(
		kgo.ClientID(r.kafkaClientID),
		kgo.SeedBrokers(r.kafkaAddress),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaTopic: {r.partitionID: at},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(100_000_000),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
		// Frequently refresh the cluster metadata. This allows us to quickly react in case some brokers are unhealthy,
		// because the client updates the cluster info only at this frequency (and not after errors too).
		//
		// The side effect of frequently refreshing the cluster metadata is that if the backend returns each time a
		// different authoritative owner for a partition, then each time cluster metadata is updated the Kafka client
		// will client a new connection for each partition, leading to an high connections churn rate.
		kgo.MetadataMaxAge(10*time.Second),
		kgo.WithHooks(r.metrics.kprom),
		kgo.WithLogger(newKafkaLogger(r.logger)),
	)

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

		level.Warn(r.logger).Log("msg", "failed to fetch last committed offset", "partition", r.partitionID, "err", err)
		retry.Wait()
	}

	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = retry.Err()
	}

	return offset, err
}

func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context) (kgo.Offset, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.kafkaAddress))
	if err != nil {
		return kgo.NewOffset(), errors.Wrap(err, "unable to create admin client")
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	offsets, err := adm.FetchOffsets(ctx, consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		// In case we are booting up for the first time ever against this topic.
		return kgo.NewOffset().AtEnd(), nil
	}
	if err != nil {
		return kgo.NewOffset(), errors.Wrap(err, "unable to fetch group offsets")
	}
	offset, _ := offsets.Lookup(r.kafkaTopic, r.partitionID)
	return kgo.NewOffset().At(offset.At), nil
}

func (r *PartitionReader) commitLoop(ctx context.Context) {
	defer r.commitLoopWg.Done()

	commitTicker := time.NewTicker(r.commitInterval)
	defer commitTicker.Stop()

	toCommit := kadm.Offsets{}

	for {
		select {
		case <-ctx.Done():
			return
		case fetches := <-r.commitFetches:
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}
				r := p.Records[len(p.Records)-1]
				toCommit.AddOffset(r.Topic, r.Partition, r.Offset+1, r.LeaderEpoch)
			})
		case <-commitTicker.C:
			if len(toCommit) == 0 {
				continue
			}
			committed, err := r.admClient.CommitOffsets(ctx, consumerGroup, toCommit)
			if err != nil || !committed.Ok() {
				level.Error(r.logger).Log("msg", "encountered error while committing offsets", "err", err, "commit_err", committed.Error())
			} else {
				committedOffset, _ := committed.Lookup(r.kafkaTopic, r.partitionID)
				level.Debug(r.logger).Log("msg", "committed offset", "offset", committedOffset.Offset.At)
				clear(toCommit)
			}
		}
	}
}

type readerMetrics struct {
	processingTime  prometheus.Summary
	receiveDelay    prometheus.Summary
	recordsPerFetch prometheus.Histogram
	kprom           *kprom.Metrics
}

func newReaderMetrics(partitionID int32, reg prometheus.Registerer) readerMetrics {
	factory := promauto.With(reg)

	return readerMetrics{
		processingTime: factory.NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_reader_processing_time_seconds",
			Help:       "Time taken to process a single record (write request).",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
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
