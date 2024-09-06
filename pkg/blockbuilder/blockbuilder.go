// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
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
	cycleEnd := cycleEndAtStartup(time.Now(), b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.nextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	cycleEnd, waitDur := nextCycleEnd(time.Now(), b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
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

func cycleEndAtStartup(t time.Time, interval, buffer time.Duration) time.Time {
	cycleEnd := t.Truncate(interval).Add(buffer)
	if cycleEnd.After(t) {
		cycleEnd = cycleEnd.Add(-interval)
	}
	return cycleEnd
}

func nextCycleEnd(t time.Time, interval, buffer time.Duration) (time.Time, time.Duration) {
	cycleEnd := t.Truncate(interval).Add(interval + buffer)
	waitTime := cycleEnd.Sub(t)
	for waitTime > interval {
		// Example - with interval=1h and buffer=15m:
		// - at t=14:12, next cycle starts at 14:15 (startup cycle ended at 13:15)
		// - at t=14:17, next cycle starts at 15:15 (startup cycle ended at 14:15)
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
				level.Info(b.logger).Log("msg", "nothing to consume in partition", "partition", partition, "offset", lag.Commit.At, "end_offset", lag.End.Offset, "lag", lag.Lag)
			}
			continue
		}

		state := partitionStateFromLag(b.logger, lag, b.fallbackOffsetMillis)
		if err := b.consumePartition(ctx, state, cycleEnd); err != nil {
			level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "partition", partition)
		}
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

type partitionState struct {
	// Partition is the partition the state belongs to.
	Partition int32
	// Lag is the upperbound number of records the cycle will need to consume.
	Lag int64
	// Commit is the current offset commit for the partition.
	Commit kadm.Offset
	// CommitRecTimestamp is the timestamp of the record whose offset was committed (and not the time of commit).
	CommitRecTimestamp time.Time
	// OldestSeenOffset is the offset of the last record consumed in the commiter-cycle. It can be greater than Commit.Offset if previous cycle overconumed.
	OldestSeenOffset int64
	// LastBlockEnd is the timestamp of the block end in the commiter-cycle.
	LastBlockEnd time.Time
}

func partitionStateFromLag(logger log.Logger, lag kadm.GroupMemberLag, fallbackMillis int64) *partitionState {
	commitRecTs, oldestSeenOffset, lastBlockEndTs, err := unmarshallCommitMeta(lag.Commit.Metadata)
	if err != nil {
		// If there is an error in unmarshalling the metadata, treat it as if
		// we have no commit. There is no reason to stop the cycle for this.
		level.Warn(logger).Log("msg", "error unmarshalling commit metadata", "err", err, "partition", lag.Partition, "offset", lag.Commit.At, "metadata", lag.Commit.Metadata)
	}

	if commitRecTs == 0 {
		// If there was no commit metadata, we use the fallback to replay a set amount of
		// records because it is non-trivial to peek at the first record in a partition to determine
		// the range of replay required. Without knowing the range, we might end up trying to consume
		// a lot of records in a single partition consumption call and end up in an OOM loop.
		// TODO(codesome): add a test for this.
		commitRecTs = fallbackMillis
	}

	return &partitionState{
		Partition:          lag.Partition,
		Lag:                lag.Lag,
		Commit:             lag.Commit,
		CommitRecTimestamp: time.UnixMilli(commitRecTs),
		OldestSeenOffset:   oldestSeenOffset,
		LastBlockEnd:       time.UnixMilli(lastBlockEndTs),
	}
}

func (b *BlockBuilder) consumePartition(ctx context.Context, state *partitionState, cycleEnd time.Time) error {
	sectionCycleEnd := cycleEnd
	if sectionCycleEnd.Sub(state.CommitRecTimestamp) > 3*b.cfg.ConsumeInterval/2 {
		// We are lagging behind by more thatn 1.5*interval or there is no commit. We need to consume the partition in sections.
		// We iterate through all the ConsumeInterval intervals, starting from the first one after the last commit until the cycleEnd,
		// i.e. [T, T+interval), [T+interval, T+2*interval), ... [T+S*interval, cycleEnd)
		// where T is the CommitRecTimestamp, the timestamp of the record, whose offset we committed previously.
		// When there is no kafka commit, we play safe and assume oldestSeenOffset and lastBlockEnd was 0 to not discard any samples unnecessarily.
		sectionCycleEnd, _ = nextCycleEnd(
			state.CommitRecTimestamp,
			b.cfg.ConsumeInterval,
			b.cfg.ConsumeIntervalBuffer,
		)
		level.Info(b.logger).Log("msg", "partition is lagging behind the cycle", "partition", state.Partition, "lag", state.Lag, "section_cycle_end", sectionCycleEnd, "cycle_end", cycleEnd)
	}
	for !sectionCycleEnd.After(cycleEnd) {
		if err := b.consumePartitionCycle(ctx, state, sectionCycleEnd); err != nil {
			return fmt.Errorf("consume partition %d: %w", state.Partition, err)
		}
		sectionCycleEnd = sectionCycleEnd.Add(b.cfg.ConsumeInterval)
	}

	return nil
}

// consumePartition consumes records from the given partition until the cycleEnd timestamp.
// It modifies state, tracking the consumption progress. If the partition is lagging behind, the caller needs to take care
// of calling consumePartition in sections.
func (b *BlockBuilder) consumePartitionCycle(ctx context.Context, state *partitionState, cycleEnd time.Time) (retErr error) {
	blockEnd := cycleEnd.Truncate(b.cfg.ConsumeInterval)

	var (
		numBlocks     int
		compactionDur time.Duration
	)
	defer func(t time.Time, startState partitionState) {
		dur := time.Since(t)

		// Don't propagate any errors derived from the canceled context.
		if errors.Is(retErr, context.Canceled) {
			retErr = nil
		}

		if retErr != nil {
			level.Error(b.logger).Log("msg", "partition consumption failed", "partition", state.Partition, "dur", dur, "start_lag", startState.Lag, "end_lag", state.Lag, "err", retErr)
			return
		}

		b.metrics.processPartitionDuration.WithLabelValues(fmt.Sprintf("%d", state.Partition)).Observe(dur.Seconds())
		level.Info(b.logger).Log("msg", "done consuming", "partition", state.Partition, "dur", dur,
			"start_lag", startState.Lag, "end_lag", state.Lag, "cycle_end", cycleEnd,
			"last_block_end", startState.LastBlockEnd, "curr_block_end", blockEnd,
			"last_oldest_seen_offset", startState.OldestSeenOffset, "curr_seen_offset", state.OldestSeenOffset,
			"num_blocks", numBlocks, "compact_and_upload_dur", compactionDur)
	}(time.Now(), *state)

	builder := NewTSDBBuilder(b.logger, b.cfg.DataDir, b.cfg.BlocksStorage, b.limits)
	defer runutil.CloseWithErrCapture(&retErr, builder, "closing tsdb builder")

	level.Info(b.logger).Log("msg", "start consuming", "partition", state.Partition, "offset", state.Commit.At, "lag", state.Lag, "cycle_end", cycleEnd)

	// TopicPartition to resume consuming on this iteration.
	tp := map[string][]int32{b.cfg.Kafka.Topic: {state.Partition}}
	b.kafkaClient.ResumeFetchPartitions(tp)
	defer b.kafkaClient.PauseFetchPartitions(tp)

	// We always rewind the partition's offset to the commit offset. This is so the cycle started exactly at the commit point,
	// and not at what was (potentially over-) consumed previously.
	b.seekPartition(state.Partition, state.Commit.At)

	var (
		firstRec  *kgo.Record
		lastRec   *kgo.Record
		commitRec *kgo.Record
	)

consumerLoop:
	for remaining := state.Lag; remaining > 0; {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		fetches := b.kafkaClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			level.Warn(b.logger).Log("msg", "client closed when fetching records")
			return nil
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.Canceled) {
				level.Error(b.logger).Log("msg", "failed to fetch records", "partition", state.Partition, "err", err)
				b.metrics.fetchErrors.Inc()
			}
		})

		numRecs := fetches.NumRecords()
		remaining -= int64(numRecs)
		b.metrics.fetchRecordsTotal.Add(float64(numRecs))

		recIter := fetches.RecordIter()
		for !recIter.Done() {
			rec := recIter.Next()

			if firstRec == nil {
				firstRec = rec
			}

			level.Debug(b.logger).Log("msg", "consumed record", "offset", rec.Offset, "rec_ts", rec.Timestamp, "last_block_end", state.LastBlockEnd, "block_end", blockEnd)

			// Stop consuming after we reached the cycleEnd marker.
			// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
			if rec.Timestamp.After(cycleEnd) {
				break consumerLoop
			}

			recProcessedBefore := rec.Offset <= state.OldestSeenOffset
			allSamplesProcessed, err := builder.Process(ctx, rec, state.LastBlockEnd.UnixMilli(), blockEnd.UnixMilli(), recProcessedBefore)
			if err != nil {
				// All "non-terminal" errors are handled by the TSDBBuilder.
				return fmt.Errorf("process record in partition %d at offset %d: %w", rec.Partition, rec.Offset, err)
			}
			if !allSamplesProcessed {
				if lastRec == nil {
					// The first record was not fully processed, meaning the record before this is the commit point.
					// We have to re-commit this one to update the oldestSeenOffset; this is so the next cycle handled partially processed record properly.
					commitRec = &kgo.Record{
						Topic:       state.Commit.Topic,
						Partition:   state.Commit.Partition,
						Offset:      state.Commit.At - 1, // Previous commit's offset-1 means we expect the next cycle to start from the commit's offset.
						Timestamp:   state.CommitRecTimestamp,
						LeaderEpoch: state.Commit.LeaderEpoch,
					}
				} else if commitRec == nil {
					// The commit offset should be the last record that was fully processed and not the first record that was not fully processed.
					commitRec = lastRec
				}
			}
			lastRec = rec
		}
	}

	// Nothing was consumed from Kafka at all.
	if firstRec == nil {
		level.Info(b.logger).Log("msg", "no records were consumed", "partition", state.Partition)
		return nil
	}

	// No records were processed for this cycle.
	if lastRec == nil {
		level.Info(b.logger).Log("msg", "nothing to commit due to first record fetched is from next cycle", "partition", state.Partition, "first_rec_offset", firstRec.Offset)
		return nil
	}

	// All samples in all records were processed. We can commit the last record's offset.
	if commitRec == nil {
		commitRec = lastRec
	}

	compactStart := time.Now()
	var err error
	numBlocks, err = builder.CompactAndUpload(ctx, b.uploadBlocks)
	if err != nil {
		return err
	}
	compactionDur = time.Since(compactStart)
	b.metrics.compactAndUploadDuration.WithLabelValues(fmt.Sprintf("%d", state.Partition)).Observe(compactionDur.Seconds())

	commitRecOffset := commitRec.Offset + 1 // offset+1 means everything up to (including) the offset was processed

	// We should take the max of "seen till" offset. If the partition was lagging
	// due to some record not being processed because of a future sample, we might be
	// coming back to the same consume cycle again.
	oldestSeenOffset := max(lastRec.Offset, state.OldestSeenOffset)
	// Take the max of block max times because of the same reasons above.
	lastBlockEnd := blockEnd
	if lastBlockEnd.Before(state.LastBlockEnd) {
		lastBlockEnd = state.LastBlockEnd
	}

	commit := kadm.Offset{
		Topic:       commitRec.Topic,
		Partition:   commitRec.Partition,
		At:          commitRecOffset,
		LeaderEpoch: commitRec.LeaderEpoch,
		Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), oldestSeenOffset, lastBlockEnd.UnixMilli()),
	}
	if err := b.commitOffset(ctx, b.cfg.Kafka.ConsumerGroup, commit); err != nil {
		return err
	}

	// The new lag is the distance between fully processed offsets. If the partition's cycle was chopped into sections due to lagging,
	// the cycle will continue consuming the next portion of the lag.
	state.Lag -= commitRecOffset - firstRec.Offset

	state.OldestSeenOffset = oldestSeenOffset
	state.LastBlockEnd = lastBlockEnd
	state.Commit = commit

	return nil
}

func (b *BlockBuilder) seekPartition(partition int32, offset int64) {
	offsets := map[string]map[int32]kgo.EpochOffset{
		b.cfg.Kafka.Topic: {
			partition: {
				Epoch:  -1, // Epoch doesn't play any role outside the group consumption.
				Offset: offset,
			},
		},
	}
	b.kafkaClient.SetOffsets(offsets)
}

func (b *BlockBuilder) commitOffset(ctx context.Context, group string, offset kadm.Offset) error {
	offsets := make(kadm.Offsets)
	offsets.Add(offset)
	if err := kadm.NewClient(b.kafkaClient).CommitAllOffsets(ctx, group, offsets); err != nil {
		return fmt.Errorf("commit with partition %d, offset %d: %w", offset.Partition, offset.At, err)
	}

	level.Debug(b.logger).Log("msg", "successfully committed to Kafka", "partition", offset.Partition, "offset", offset.At)

	return nil
}

func (b *BlockBuilder) uploadBlocks(ctx context.Context, tenantID, dbDir string, blockIDs []string) error {
	buc := bucket.NewUserBucketClient(tenantID, b.bucket, b.limits)
	for _, bid := range blockIDs {
		blockDir := path.Join(dbDir, bid)
		meta, err := block.ReadMetaFromDir(blockDir)
		if err != nil {
			return err
		}

		if meta.Stats.NumSamples == 0 {
			// No need to upload empty block.
			return nil
		}

		meta.Thanos.Source = block.BlockBuilderSource
		meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

		if meta.Compaction.FromOutOfOrder() && b.limits.OutOfOrderBlocksExternalLabelEnabled(tenantID) {
			// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
			meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
		}

		// Upload block with custom metadata.
		if err := block.Upload(ctx, b.logger, buc, blockDir, meta); err != nil {
			return err
		}
	}
	return nil
}
