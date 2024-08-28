// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/validation"
)

type BlockBuilder struct {
	services.Service

	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	limits      *validation.Overrides
	kafkaClient *kgo.Client
	bucket      objstore.Bucket

	assignedPartitionIDs []int32
	// fallbackOffsetMillis is the milliseconds timestamp after which a partition that doesn't have a commit will be consumed from.
	fallbackOffsetMillis int64

	metrics blockBuilderMetrics
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (*BlockBuilder, error) {
	b := &BlockBuilder{
		cfg:      cfg,
		logger:   logger,
		register: reg,
		limits:   limits,
		metrics:  newBlockBuilderMetrics(reg),
	}

	b.assignedPartitionIDs = b.cfg.PartitionAssignment[b.cfg.InstanceID]
	if len(b.assignedPartitionIDs) == 0 {
		// This is just an assertion check. The config validation prevents this from happening.
		return nil, fmt.Errorf("no partitions assigned to instance %s", b.cfg.InstanceID)
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "block-builder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			if b.kafkaClient != nil {
				b.kafkaClient.Close()
			}
		}
	}()

	// Empty any previous artifacts.
	if err := os.RemoveAll(b.cfg.BlocksStorageConfig.TSDB.Dir); err != nil {
		return fmt.Errorf("removing tsdb dir: %w", err)
	}
	if err := os.MkdirAll(b.cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm); err != nil {
		return fmt.Errorf("creating tsdb dir: %w", err)
	}

	// TODO: add a test to test the case where the consumption on startup happens
	// after the LookbackOnNoCommit with records before the after the consumption
	// start point.
	startAtOffsets, fallbackOffsetMillis, err := b.findOffsetsToStartAt(ctx)
	if err != nil {
		return fmt.Errorf("find offsets to start at: %w", err)
	}
	b.fallbackOffsetMillis = fallbackOffsetMillis

	const fetchMaxBytes = 100_000_000

	opts := []kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			b.cfg.Kafka.Topic: startAtOffsets,
		}),
		kgo.FetchMinBytes(128),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2 * fetchMaxBytes),
	}

	metrics := ingest.NewKafkaReaderClientMetrics("block-builder", b.register)
	opts = append(
		commonKafkaClientOptions(b.cfg.Kafka, b.logger, metrics),
		opts...,
	)
	b.kafkaClient, err = kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	// Immediately pause fetching all assigned partitions. We control the order and the pace of partition fetching within the cycles.
	b.kafkaClient.PauseFetchPartitions(map[string][]int32{b.cfg.Kafka.Topic: b.assignedPartitionIDs})

	return nil
}

func (b *BlockBuilder) findOffsetsToStartAt(ctx context.Context) (map[int32]kgo.Offset, int64, error) {
	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(commonKafkaClientOptions(b.cfg.Kafka, b.logger, nil)...)
	if err != nil {
		return nil, -1, fmt.Errorf("unable to create bootstrap client: %w", err)
	}
	defer cl.Close()

	admClient := kadm.NewClient(cl)

	// Fallback offset is the millisecond timestamp used to look up a real offset if partition doesn't have a commit.
	ts := time.Now().Add(-b.cfg.LookbackOnNoCommit)
	fallbackOffsetMillis := ts.UnixMilli()
	defaultKOffset := kgo.NewOffset().AfterMilli(fallbackOffsetMillis)

	fetchOffsets := func(ctx context.Context) (offsets map[int32]kgo.Offset, err error) {
		resp, err := admClient.FetchOffsets(ctx, b.cfg.Kafka.ConsumerGroup)
		if err == nil {
			err = resp.Error()
		}
		// Either success or the requested group does not exist.
		if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
			offsets = make(map[int32]kgo.Offset)
		} else if err != nil {
			return nil, fmt.Errorf("unable to fetch group offsets: %w", err)
		} else {
			offsets = resp.KOffsets()[b.cfg.Kafka.Topic]
		}

		// Look over the assigned partitions and fallback to the ConsumerResetOffset if a partition doesn't have any commit for the configured group.
		for _, partition := range b.assignedPartitionIDs {
			if _, ok := offsets[partition]; ok {
				level.Info(b.logger).Log("msg", "consuming from last consumed offset", "consumer_group", b.cfg.Kafka.ConsumerGroup, "partition", partition, "offset", offsets[partition].String())
			} else {
				offsets[partition] = defaultKOffset
				level.Info(b.logger).Log("msg", "consuming from partition lookback because no offset has been found", "consumer_group", b.cfg.Kafka.ConsumerGroup, "partition", partition, "offset", offsets[partition].String())
			}
		}
		return offsets, nil
	}

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 10,
	})
	for boff.Ongoing() {
		var offsets map[int32]kgo.Offset
		offsets, err = fetchOffsets(ctx)
		if err == nil {
			return offsets, fallbackOffsetMillis, nil
		}
		level.Warn(b.logger).Log("msg", "failed to fetch startup offsets; will retry", "err", err)
		boff.Wait()
	}
	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = boff.Err()
	}
	return nil, -1, err
}

// TODO(v): consider exposing storage/ingest.commonKafkaClientOptions
func commonKafkaClientOptions(cfg KafkaConfig, logger log.Logger, metrics *kprom.Metrics) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),
		kgo.WithLogger(ingest.NewKafkaLogger(logger)),
	}
	if metrics != nil {
		opts = append(opts, kgo.WithHooks(metrics))
	}
	return opts
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the last hour boundary + buffer.
	cycleEnd := cycleEndAtStartup(b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.NextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	nextCycleTime := time.Now().Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
	waitTime := time.Until(nextCycleTime)
	for waitTime > b.cfg.ConsumeInterval {
		// NB: at now=14:12, next cycle starts at 14:15 (startup cycle ended at 13:15)
		//     at now=14:17, next cycle starts at 15:15 (startup cycle ended at 14:15)
		nextCycleTime = nextCycleTime.Add(-b.cfg.ConsumeInterval)
		waitTime -= b.cfg.ConsumeInterval
	}

	for {
		select {
		case <-time.After(waitTime):
			cycleEnd := nextCycleTime
			level.Info(b.logger).Log("msg", "triggering next consume cycle", "cycle_end", cycleEnd)
			err := b.NextConsumeCycle(ctx, cycleEnd)
			if err != nil && !errors.Is(err, context.Canceled) {
				// Fail the whole service in case of a non-recoverable error.
				return fmt.Errorf("consume next cycle until cycle_end %s: %w", cycleEnd, err)
			}

			// If we took more than ConsumeInterval to consume the records, this will immediately start the next consumption.
			// TODO(codesome): track waitTime < 0, which is the time we ran over. Should have an alert on this.
			nextCycleTime = nextCycleTime.Add(b.cfg.ConsumeInterval)
			waitTime = time.Until(nextCycleTime)
		case <-ctx.Done():
			level.Info(b.logger).Log("msg", "context cancelled, stopping")
			return nil
		}
	}
}

func cycleEndAtStartup(interval, buffer time.Duration) time.Time {
	cycleEnd := time.Now().Truncate(interval).Add(buffer)
	if cycleEnd.After(time.Now()) {
		cycleEnd = cycleEnd.Add(-interval)
	}
	return cycleEnd
}

// NextConsumeCycle manages consumption of currently assigned partitions.
// The cycleEnd argument indicates the timestamp (relative to Kafka records) up until which to consume from partitions
// in this cycle. That is, Kafka records produced after the cycleEnd mark will be consumed in the next cycle.
func (b *BlockBuilder) NextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
	defer func(t time.Time) {
		b.metrics.consumeCycleDuration.Observe(time.Since(t).Seconds())
	}(time.Now())

	for _, partition := range b.assignedPartitionIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// TODO(v): calculating lag for each individual partition requests data for the whole group every time. This is redundant.
		// As, currently, we don't expect rebalance (re-assignment) happening in the middle of the cycle, we could calculate the lag once for all assigned partitions
		// in the beginning of the cycle.
		// Lag is the upperbound number of records we'll have to consume from Kafka to build the blocks.
		// It's the "upperbound" because the consumption may be stopped earlier if we get records containing
		// samples with timestamp greater than the cycleEnd timestamp.
		lag, err := b.getLagForPartition(ctx, partition)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", partition, "cycle_end", cycleEnd)
			continue
		}

		b.metrics.consumerLagRecords.WithLabelValues(lag.Topic, fmt.Sprintf("%d", lag.Partition)).Set(float64(lag.Lag))

		if lag.Lag <= 0 {
			if err := lag.Err; err != nil {
				level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", partition, "cycle_end", cycleEnd)
			} else {
				level.Info(b.logger).Log(
					"msg", "nothing to consume in partition",
					"partition", partition,
					"offset", lag.Commit.At,
					"end_offset", lag.End.Offset,
					"lag", lag.Lag,
				)
			}
		}

		// TODO(v): backport the code for consuming partition up to its lag.Lag
	}
	return nil
}

func (b *BlockBuilder) getLagForPartition(ctx context.Context, partition int32) (kadm.GroupMemberLag, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
	})
	var lastErr error
	for boff.Ongoing() {
		groupLag, err := getGroupLag(ctx, kadm.NewClient(b.kafkaClient), b.cfg.Kafka.Topic, b.cfg.Kafka.ConsumerGroup, b.fallbackOffsetMillis)
		if err != nil {
			lastErr = fmt.Errorf("get consumer group lag: %w", err)
		} else {
			lag, ok := groupLag.Lookup(b.cfg.Kafka.Topic, partition)
			if ok {
				return lag, nil
			}
			// This should not happen with the recent implementation of getGroupLag, that handles a case when the group doesn't have live participants;
			// leaving the check here for completeness.
			lastErr = fmt.Errorf("partition %d not found in lag response", partition)
		}

		level.Warn(b.logger).Log("msg", "failed to get consumer group lag; will retry", "err", lastErr, "partition", partition)
		boff.Wait()
	}

	return kadm.GroupMemberLag{}, lastErr
}
