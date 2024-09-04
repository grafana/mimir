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
	"github.com/twmb/franz-go/pkg/kgo"

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

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorage.Bucket, "block-builder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func (b *BlockBuilder) starting(context.Context) (err error) {
	// Empty any previous artifacts.
	if err := os.RemoveAll(b.cfg.DataDir); err != nil {
		return fmt.Errorf("removing data dir: %w", err)
	}
	if err := os.MkdirAll(b.cfg.DataDir, os.ModePerm); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	// Fallback offset is a millisecond timestamp used to look up a real offset if partition doesn't have a commit.
	b.fallbackOffsetMillis = time.Now().Add(-b.cfg.LookbackOnNoCommit).UnixMilli()

	opts := []kgo.Opt{
		kgo.ConsumeTopics(b.cfg.Kafka.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(b.fallbackOffsetMillis)),
	}
	b.kafkaClient, err = ingest.NewKafkaReaderClient(
		b.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics("block-builder", b.register),
		b.logger,
		opts...,
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	// Immediately pause fetching all assigned partitions. We control the order and the pace of partition fetching within the cycles.
	b.kafkaClient.PauseFetchPartitions(map[string][]int32{b.cfg.Kafka.Topic: b.assignedPartitionIDs})

	return nil
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the <consume interval> boundary + buffer.
	cycleEnd := cycleEndAtStartup(time.Now, b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.nextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	cycleEnd, waitDur := nextCycleEnd(time.Now, b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	for {
		select {
		case <-time.After(waitDur):
			level.Info(b.logger).Log("msg", "triggering next consume cycle", "cycle_end", cycleEnd)
			err := b.nextConsumeCycle(ctx, cycleEnd)
			if err != nil && !errors.Is(err, context.Canceled) {
				// Fail the whole service in case of a non-recoverable error.
				return fmt.Errorf("consume next cycle until cycle_end %s: %w", cycleEnd, err)
			}

			// If we took more than ConsumeInterval to consume the records, this will immediately start the next consumption.
			// TODO(codesome): track waitDur < 0, which is the time we ran over. Should have an alert on this.
			cycleEnd = cycleEnd.Add(b.cfg.ConsumeInterval)
			waitDur = time.Until(cycleEnd)
		case <-ctx.Done():
			level.Info(b.logger).Log("msg", "context cancelled, stopping")
			return nil
		}
	}
}

func cycleEndAtStartup(now func() time.Time, interval, buffer time.Duration) time.Time {
	t := now()
	cycleEnd := t.Truncate(interval).Add(buffer)
	if cycleEnd.After(t) {
		cycleEnd = cycleEnd.Add(-interval)
	}
	return cycleEnd
}

func nextCycleEnd(now func() time.Time, interval, buffer time.Duration) (time.Time, time.Duration) {
	t := now()
	cycleEnd := t.Truncate(interval).Add(interval + buffer)
	waitTime := cycleEnd.Sub(t)
	for waitTime > interval {
		// Example - with interval=1h and buffer=15m:
		// - at now=14:12, next cycle starts at 14:15 (startup cycle ended at 13:15)
		// - at now=14:17, next cycle starts at 15:15 (startup cycle ended at 14:15)
		cycleEnd = cycleEnd.Add(-interval)
		waitTime -= interval
	}
	return cycleEnd, waitTime
}

// nextConsumeCycle manages consumption of currently assigned partitions.
// The cycleEnd argument indicates the timestamp (relative to Kafka records) up until which to consume from partitions
// in this cycle. That is, Kafka records produced after the cycleEnd mark will be consumed in the next cycle.
func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
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
		groupLag, err := getGroupLag(ctx, kadm.NewClient(b.kafkaClient), b.cfg.Kafka.Topic, b.cfg.ConsumerGroup, b.fallbackOffsetMillis)
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
