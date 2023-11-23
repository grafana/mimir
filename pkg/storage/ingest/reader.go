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

type PartitionReader struct {
	services.Service

	kafkaAddress string
	kafkaTopic   string
	partition    int32

	client *kgo.Client

	logger log.Logger
	reg    prometheus.Registerer
}

func NewReader(kafkaAddress, kafkaTopic string, partitionID int32, logger log.Logger, reg prometheus.Registerer) (*PartitionReader, error) {
	r := &PartitionReader{
		kafkaAddress: kafkaAddress,
		kafkaTopic:   kafkaTopic,
		partition:    partitionID,
		reg:          reg,
		logger:       log.With(logger, "partition", partitionID),
	}

	r.Service = services.NewBasicService(r.start, r.run, nil)
	return r, nil
}

func (r *PartitionReader) start(ctx context.Context) error {
	var err error
	r.client, err = r.newKafkaReader(ctx, r.partition, r.reg)
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
		fetches.EachRecord(func(record *kgo.Record) {
			level.Debug(r.logger).Log("msg", "fetched record", "offset", record.Offset)
			// TODO dimitarvdimitrov
		})

		committed, err := kadm.NewClient(r.client).CommitOffsets(ctx, consumerGroup, kadm.OffsetsFromFetches(fetches))
		if err != nil {
			level.Error(r.logger).Log("msg", "encountered error while committing offsets", err)
		} else {
			committedOffset, _ := committed.Lookup(r.kafkaTopic, r.partition)
			level.Debug(r.logger).Log("msg", "committed offset", "offset", committedOffset.Offset.At)
		}
	}

	return nil
}

func (r *PartitionReader) newKafkaReader(ctx context.Context, partition int32, reg prometheus.Registerer) (*kgo.Client, error) {
	metrics := kprom.NewMetrics("cortex_ingest_storage_reader",
		kprom.Registerer(reg),
		kprom.WithClientLabel(),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	var adm *kadm.Client
	{
		cl, err := kgo.NewClient(kgo.SeedBrokers(r.kafkaAddress))
		if err != nil {
			return nil, errors.Wrap(err, "unable to create admin client")
		}
		adm = kadm.NewClient(cl)
		defer adm.Close()
	}

	offsets, err := adm.ListCommittedOffsets(ctx, r.kafkaTopic)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch group offsets")
	}
	offset, _ := offsets.Lookup(r.kafkaTopic, partition)
	level.Info(r.logger).Log("msg", "resuming consumption from offset", "offset", offset.Offset)

	client, err := kgo.NewClient(
		kgo.ClientID(fmt.Sprintf("partition-%d", partition)),
		kgo.SeedBrokers(r.kafkaAddress),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			r.kafkaTopic: {partition: kgo.NewOffset().At(offset.Offset)},
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
