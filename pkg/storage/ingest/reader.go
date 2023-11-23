package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

// consumerGroup is only used to store commit offsets, not for actual consuming.
const consumerGroup = "mimir"

type Record struct {
	Content []byte
}

type RecordConsumer interface {
	Consume(Record) error
}

type PartitionReader struct {
	services.Service

	kafkaAddress string
	kafkaTopic   string
	partition    int32

	client *kgo.Client

	consumer RecordConsumer

	logger log.Logger
	reg    prometheus.Registerer
}

type noopConsumer struct {
}

func (n noopConsumer) Consume(record Record) error { return nil }

func NewReader(kafkaAddress, kafkaTopic string, partitionID int32, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaAddress: kafkaAddress,
		kafkaTopic:   kafkaTopic,
		partition:    partitionID,
		reg:          reg,
		consumer:     noopConsumer{},
		logger:       log.With(logger, "partition", partitionID),
	}

	r.Service = services.NewBasicService(r.start, r.run, r.stop)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) error {
	offset, err := r.fetchLastCommittedOffset(ctx)
	if err != nil {
		return err
	}
	level.Info(r.logger).Log("msg", "resuming consumption from offset", "offset", offset)

	r.client, err = r.newKafkaReader(offset, r.reg)
	if err != nil {
		return errors.Wrap(err, "creating kafka reader client")
	}
	return nil
}

func (r *PartitionReader) run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := r.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			level.Error(r.logger).Log("msg", "encountered error while fetching", errs)
			continue
		}
		r.consumeFetches(fetches)
		r.commitFetches(ctx, fetches)
	}

	return nil
}

func (r *PartitionReader) commitFetches(ctx context.Context, fetches kgo.Fetches) {
	committed, err := kadm.NewClient(r.client).CommitOffsets(ctx, consumerGroup, kadm.OffsetsFromFetches(fetches))
	if err != nil {
		level.Error(r.logger).Log("msg", "encountered error while committing offsets", err)
	} else {
		committedOffset, _ := committed.Lookup(r.kafkaTopic, r.partition)
		level.Debug(r.logger).Log("msg", "committed offset", "offset", committedOffset.Offset.At)
	}
}

func (r *PartitionReader) consumeFetches(fetches kgo.Fetches) {
	fetches.EachRecord(func(record *kgo.Record) {
		level.Debug(r.logger).Log("msg", "fetched record", "offset", record.Offset)

		err := r.consumer.Consume(Record{Content: record.Value})
		if err != nil {
			level.Error(r.logger).Log("msg", "encountered error processing record; skipping", "offset", record.Offset, "err", err)
			// TODO abort ingesting & back off if it's a server error, ignore error if it's a client error
		}
	})
}

func (r *PartitionReader) newKafkaReader(offset int64, reg prometheus.Registerer) (*kgo.Client, error) {
	metrics := kprom.NewMetrics("cortex_ingest_storage_reader",
		kprom.Registerer(reg),
		kprom.WithClientLabel(),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	client, err := kgo.NewClient(
		kgo.ClientID(fmt.Sprintf("partition-%d", r.partition)),
		kgo.SeedBrokers(r.kafkaAddress),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaTopic: {r.partition: kgo.NewOffset().At(offset)},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(100_000_000),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
		kgo.MetadataMaxAge(time.Minute),
		kgo.WithHooks(metrics),
		kgo.WithLogger(newKafkaLogger(r.logger, kgo.LogLevelInfo)), // TODO pass the log level configured in Mimir
	)

	if err != nil {
		return nil, errors.Wrap(err, "creating kafka client")
	}

	return client, nil
}

func (r *PartitionReader) fetchLastCommittedOffset(ctx context.Context) (int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.kafkaAddress))
	if err != nil {
		return 0, errors.Wrap(err, "unable to create admin client")
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	offsets, err := adm.ListCommittedOffsets(ctx, r.kafkaTopic)
	if err != nil {
		return 0, errors.Wrap(err, "unable to fetch group offsets")
	}
	offset, _ := offsets.Lookup(r.kafkaTopic, r.partition)
	return offset.Offset, nil
}

func (r *PartitionReader) stop(error) error {
	r.client.Close()
	return nil
}
