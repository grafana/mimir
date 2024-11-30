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
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/spanlogger"
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

	scheduler schedulerpb.SchedulerClient

	assignedPartitionIDs []int32
	// fallbackOffsetMillis is the milliseconds timestamp after which a partition that doesn't have a commit will be consumed from.
	fallbackOffsetMillis int64

	blockBuilderMetrics blockBuilderMetrics
	tsdbBuilderMetrics  tsdbBuilderMetrics
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (*BlockBuilder, error) {
	b := &BlockBuilder{
		cfg:                 cfg,
		logger:              logger,
		register:            reg,
		limits:              limits,
		blockBuilderMetrics: newBlockBuilderMetrics(reg),
		tsdbBuilderMetrics:  newTSDBBBuilderMetrics(reg),
	}

	b.assignedPartitionIDs = b.cfg.PartitionAssignment[b.cfg.InstanceID]

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorage.Bucket, "block-builder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	if cfg.SchedulerConfig.Address != "" {
		sched, err := b.makeSchedulerClient()
		if err != nil {
			return nil, fmt.Errorf("failed to create scheduler client: %w", err)
		}
		b.scheduler = sched
	}

	return b, nil
}

func (b *BlockBuilder) makeSchedulerClient() (schedulerpb.SchedulerClient, error) {
	dialOpts, err := b.cfg.SchedulerConfig.GRPCClientConfig.DialOption(
		[]grpc.UnaryClientInterceptor{otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer())},
		nil)
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(b.cfg.SchedulerConfig.Address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return schedulerpb.NewSchedulerClient(
		b.cfg.InstanceID,
		schedulerpb.NewBlockBuilderSchedulerClient(conn),
		b.logger,
		b.cfg.SchedulerConfig.UpdateInterval,
		b.cfg.SchedulerConfig.MaxUpdateAge,
	), nil
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

	b.kafkaClient, err = ingest.NewKafkaReaderClient(
		b.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics("block-builder", b.register),
		b.logger,
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	return nil
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go b.scheduler.Run(sctx)

	for {
		key, _, err := b.scheduler.GetJob(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			level.Warn(b.logger).Log("msg", "failed to get job", "err", err)
			continue
		}

		time.Sleep(5 * time.Second)

		if err := b.scheduler.CompleteJob(key); err != nil {
			level.Warn(b.logger).Log("msg", "failed to complete job", "job_id", key.Id, "epoch", key.Epoch)
		}
	}
}

func (b *BlockBuilder) running_old(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the <consume interval> boundary + buffer.
	cycleEndTime := cycleEndAtStartup(time.Now(), b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.nextConsumeCycle(ctx, cycleEndTime)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	cycleEndTime, waitDur := nextCycleEnd(time.Now(), b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	for {
		select {
		case <-time.After(waitDur):
			level.Info(b.logger).Log("msg", "triggering next consume cycle", "cycle_end", cycleEndTime)
			err := b.nextConsumeCycle(ctx, cycleEndTime)
			if err != nil && !errors.Is(err, context.Canceled) {
				// Fail the whole service in case of a non-recoverable error.
				return fmt.Errorf("consume next cycle until cycle_end %s: %w", cycleEndTime, err)
			}

			// If we took more than ConsumeInterval to consume the records, this will immediately start the next consumption.
			// TODO(codesome): track waitDur < 0, which is the time we ran over. Should have an alert on this.
			cycleEndTime = cycleEndTime.Add(b.cfg.ConsumeInterval)
			waitDur = time.Until(cycleEndTime)
		case <-ctx.Done():
			level.Info(b.logger).Log("msg", "context cancelled, stopping")
			return nil
		}
	}
}

// cycleEndAtStartup returns the timestamp of the first cycleEnd relative to the start time t.
// One cycle is a duration of one interval plus extra time buffer.
func cycleEndAtStartup(t time.Time, interval, buffer time.Duration) time.Time {
	cycleEnd := t.Truncate(interval).Add(buffer)
	if cycleEnd.After(t) {
		cycleEnd = cycleEnd.Add(-interval)
	}
	return cycleEnd
}

// nextCycleEnd returns the timestamp of the next cycleEnd relative to the time t.
// One cycle is a duration of one interval plus extra time buffer.
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
func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, cycleEndTime time.Time) error {
	defer func(t time.Time) {
		b.blockBuilderMetrics.consumeCycleDuration.Observe(time.Since(t).Seconds())
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
		// samples with timestamp greater than the cycleEndTime timestamp.
		lag, err := b.getLagForPartition(ctx, partition)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", partition, "cycle_end", cycleEndTime)
			continue
		}
		if err := lag.Err; err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", partition, "cycle_end", cycleEndTime)
			continue
		}

		b.blockBuilderMetrics.consumerLagRecords.WithLabelValues(fmt.Sprintf("%d", lag.Partition)).Set(float64(lag.Lag))

		if lag.Lag <= 0 {
			level.Info(b.logger).Log("msg", "nothing to consume in partition", "partition", partition, "commit_offset", lag.Commit.At, "start_offset", lag.Start.Offset, "end_offset", lag.End.Offset, "lag", lag.Lag)
			continue
		}

		state := PartitionStateFromLag(b.logger, lag, b.fallbackOffsetMillis)
		if err := b.consumePartition(ctx, partition, state, cycleEndTime, lag.End.Offset); err != nil {
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
		groupLag, err := GetGroupLag(ctx, kadm.NewClient(b.kafkaClient), b.cfg.Kafka.Topic, b.cfg.ConsumerGroup, b.fallbackOffsetMillis)
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

type PartitionState struct {
	// Commit is the offset of the next record we'll start consuming.
	Commit kadm.Offset
	// CommitRecordTimestamp is the timestamp of the record whose offset was committed (and not the time of commit).
	CommitRecordTimestamp time.Time
	// LastSeenOffset is the offset of the last record consumed in the commiter-cycle. It can be greater than Commit.Offset if previous cycle overconsumed.
	LastSeenOffset int64
	// LastBlockEnd is the timestamp of the block end in the commiter-cycle.
	LastBlockEnd time.Time
}

func PartitionStateFromLag(logger log.Logger, lag kadm.GroupMemberLag, fallbackMillis int64) PartitionState {
	commitRecTs, lastSeenOffset, lastBlockEndTs, err := unmarshallCommitMeta(lag.Commit.Metadata)
	if err != nil {
		// If there is an error in unmarshalling the metadata, treat it as if
		// we have no commit. There is no reason to stop the cycle for this.
		level.Error(logger).Log("msg", "error unmarshalling commit metadata", "err", err, "partition", lag.Partition, "commit_offset", lag.Commit.At, "metadata", lag.Commit.Metadata)
	}

	if commitRecTs == 0 {
		// If there was no commit metadata, we use the fallback to replay a set amount of
		// records because it is non-trivial to peek at the first record in a partition to determine
		// the range of replay required. Without knowing the range, we might end up trying to consume
		// a lot of records in a single partition consumption call and end up in an OOM loop.
		level.Info(logger).Log("msg", "no commit record timestamp in commit metadata; needs to fall back", "partition", lag.Partition, "commit_offset", lag.Commit.At, "metadata", lag.Commit.Metadata, "fallback_millis", fallbackMillis)
		commitRecTs = fallbackMillis
	}

	level.Info(logger).Log(
		"msg", "creating partition state",
		"partition", lag.Partition,
		"start_offset", lag.Start.Offset,
		"end_offset", lag.End.Offset,
		"lag", lag.Lag,
		"commit_rec_ts", commitRecTs,
		"commit_offset", lag.Commit.At,
		"last_seen_offset", lastSeenOffset,
		"last_block_end_ts", lastBlockEndTs,
	)

	return PartitionState{
		Commit:                lag.Commit,
		CommitRecordTimestamp: time.UnixMilli(commitRecTs),
		LastSeenOffset:        lastSeenOffset,
		LastBlockEnd:          time.UnixMilli(lastBlockEndTs),
	}
}

// consumePartition consumes records from the given partition until the cycleEnd timestamp.
// If the partition is lagging behind, it takes care of consuming it in sections.
func (b *BlockBuilder) consumePartition(ctx context.Context, partition int32, state PartitionState, cycleEndTime time.Time, cycleEndOffset int64) (err error) {
	sp, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BlockBuilder.consumePartition")
	defer sp.Finish()

	logger := log.With(sp, "partition", partition, "cycle_end", cycleEndTime, "cycle_end_offset", cycleEndOffset)

	builder := NewTSDBBuilder(b.logger, b.cfg.DataDir, b.cfg.BlocksStorage, b.limits, b.tsdbBuilderMetrics, b.cfg.ApplyMaxGlobalSeriesPerUserBelow)
	defer runutil.CloseWithErrCapture(&err, builder, "closing tsdb builder")

	// Section is a portion of the partition to process in a single pass. One cycle may process multiple sections if the partition is lagging.
	sectionEndTime := cycleEndTime
	if sectionEndTime.Sub(state.CommitRecordTimestamp) > time.Duration(1.5*float64(b.cfg.ConsumeInterval)) {
		// We are lagging behind by more than 1.5*interval or there is no commit. We need to consume the partition in sections.
		// We iterate through all the ConsumeInterval intervals, starting from the first one after the last commit until the cycleEndTime,
		// i.e. [T, T+interval), [T+interval, T+2*interval), ... [T+S*interval, cycleEndTime)
		// where T is the CommitRecordTimestamp, the timestamp of the record, whose offset we committed previously.
		// When there is no kafka commit, we play safe and assume LastSeenOffset, and LastBlockEnd were 0 to not discard any samples unnecessarily.
		sectionEndTime, _ = nextCycleEnd(
			state.CommitRecordTimestamp,
			b.cfg.ConsumeInterval,
			b.cfg.ConsumeIntervalBuffer,
		)
		level.Info(logger).Log("msg", "partition is lagging behind the cycle", "section_end", sectionEndTime, "commit_rec_ts", state.CommitRecordTimestamp)
	}
	for !sectionEndTime.After(cycleEndTime) {
		logger := log.With(logger, "section_end", sectionEndTime, "offset", state.Commit.At)
		state, err = b.consumePartitionSection(ctx, logger, builder, partition, state, sectionEndTime, cycleEndOffset)
		if err != nil {
			return fmt.Errorf("consume partition %d: %w", partition, err)
		}
		sectionEndTime = sectionEndTime.Add(b.cfg.ConsumeInterval)
	}

	return nil
}

func (b *BlockBuilder) consumePartitionSection(
	ctx context.Context,
	logger log.Logger,
	builder *TSDBBuilder,
	partition int32,
	state PartitionState,
	sectionEndTime time.Time,
	cycleEndOffset int64,
) (retState PartitionState, retErr error) {
	// Oppose to the section's range (and cycle's range), that include the ConsumeIntervalBuffer, the block's range doesn't.
	// Thus, truncate the timestamp with ConsumptionInterval here to round the block's range.
	blockEnd := sectionEndTime.Truncate(b.cfg.ConsumeInterval)

	// If the last commit offset has already reached the offset, that marks the end of the cycle, bail. This can happen when the partition
	// was scaled down (it's inactive now), but block-builder was lagging, and it chopped this cycle into sections. After a cycle section
	// reaches the cycle end offset, the whole cycle must notice that there is nothing more to consume.
	if state.Commit.At == cycleEndOffset {
		level.Info(logger).Log("msg", "nothing to consume")
		return state, nil
	}

	var numBlocks int
	defer func(t time.Time, startState PartitionState) {
		// No need to log or track time of the unfinished section. Just bail out.
		if errors.Is(retErr, context.Canceled) {
			return
		}

		dur := time.Since(t)

		if retErr != nil {
			level.Error(logger).Log("msg", "partition consumption failed", "err", retErr, "duration", dur)
			return
		}

		b.blockBuilderMetrics.processPartitionDuration.WithLabelValues(fmt.Sprintf("%d", partition)).Observe(dur.Seconds())
		level.Info(logger).Log("msg", "done consuming", "duration", dur,
			"last_block_end", startState.LastBlockEnd, "curr_block_end", blockEnd,
			"last_seen_offset", startState.LastSeenOffset, "curr_seen_offset", retState.LastSeenOffset,
			"num_blocks", numBlocks)
	}(time.Now(), state)

	// We always rewind the partition's offset to the commit offset by reassigning the partition to the client (this triggers partition assignment).
	// This is so the consumption started exactly at the commit offset, and not at what was (potentially over-) consumed in the previous iteration.
	// In the end, we remove the partition from the client (refer to the defer below) to guarantee the client always consumes
	// from one partition at a time. I.e. when this partition is consumed, we start consuming the next one.
	b.kafkaClient.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		b.cfg.Kafka.Topic: {
			partition: kgo.NewOffset().At(state.Commit.At),
		},
	})
	defer b.kafkaClient.RemoveConsumePartitions(map[string][]int32{b.cfg.Kafka.Topic: {partition}})

	level.Info(logger).Log("msg", "start consuming")

	var (
		firstRec  *kgo.Record
		lastRec   *kgo.Record
		commitRec *kgo.Record
	)

consumerLoop:
	for recOffset := int64(-1); recOffset < cycleEndOffset-1; {
		if err := context.Cause(ctx); err != nil {
			return PartitionState{}, err
		}

		// PollFetches can return a non-failed fetch with zero records. In such a case, with only the fetches at hands,
		// we cannot tell if the consumer has already reached the latest end of the partition, i.e. no more records to consume,
		// or there is more data in the backlog, and we must retry the poll. That's why the consumer loop above has to guard
		// the iterations against the cycleEndOffset, so it retried the polling up until the expected end of the partition is reached.
		fetches := b.kafkaClient.PollFetches(ctx)
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.Canceled) {
				level.Error(logger).Log("msg", "failed to fetch records", "err", err)
				b.blockBuilderMetrics.fetchErrors.WithLabelValues(fmt.Sprintf("%d", partition)).Inc()
			}
		})

		for recIter := fetches.RecordIter(); !recIter.Done(); {
			rec := recIter.Next()
			recOffset = rec.Offset

			if firstRec == nil {
				firstRec = rec
			}

			// Stop consuming after we reached the sectionEndTime marker.
			// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
			if rec.Timestamp.After(sectionEndTime) {
				break consumerLoop
			}

			recordAlreadyProcessed := rec.Offset <= state.LastSeenOffset
			allSamplesProcessed, err := builder.Process(ctx, rec, state.LastBlockEnd.UnixMilli(), blockEnd.UnixMilli(), recordAlreadyProcessed)
			if err != nil {
				// All "non-terminal" errors are handled by the TSDBBuilder.
				return state, fmt.Errorf("process record in partition %d at offset %d: %w", rec.Partition, rec.Offset, err)
			}
			if !allSamplesProcessed {
				if lastRec == nil {
					// The first record was not fully processed, meaning the record before this is the commit point.
					// We hand-craft the commitRec from the data in the state to re-commit it. On commit the commit's meta is updated
					// with the new value of LastSeenOffset. This is so the next cycle handled partially processed record properly.
					commitRec = &kgo.Record{
						Topic:     state.Commit.Topic,
						Partition: state.Commit.Partition,
						// This offset points at the previous commit's offset-1, meaning on commit, we will store the offset-1+1 (minus-one-plus-one),
						// which is the offset of the previous commit itself (details https://github.com/grafana/mimir/pull/9199#discussion_r1772979364).
						Offset:      state.Commit.At - 1,
						Timestamp:   state.CommitRecordTimestamp,
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
		level.Info(logger).Log("msg", "no records were consumed")
		return state, nil
	}

	// No records were processed for this cycle.
	if lastRec == nil {
		level.Info(logger).Log("msg", "nothing to commit due to first record has a timestamp greater than this section end", "first_rec_offset", firstRec.Offset, "first_rec_ts", firstRec.Timestamp)
		return state, nil
	}

	// All samples in all records were processed. We can commit the last record's offset.
	if commitRec == nil {
		commitRec = lastRec
	}

	var err error
	numBlocks, err = builder.CompactAndUpload(ctx, b.uploadBlocks)
	if err != nil {
		return state, err
	}

	// We should take the max of last seen offsets. If the partition was lagging due to some record not being processed
	// because of a future sample, we might be coming back to the same consume cycle again.
	lastSeenOffset := max(lastRec.Offset, state.LastSeenOffset)
	// Take the max of block max times because of the same reasons above.
	lastBlockEnd := blockEnd
	if lastBlockEnd.Before(state.LastBlockEnd) {
		lastBlockEnd = state.LastBlockEnd
	}
	commit := kadm.Offset{
		Topic:       commitRec.Topic,
		Partition:   commitRec.Partition,
		At:          commitRec.Offset + 1, // offset+1 means everything up to (including) the offset was processed
		LeaderEpoch: commitRec.LeaderEpoch,
		Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastSeenOffset, lastBlockEnd.UnixMilli()),
	}
	newState := PartitionState{
		Commit:                commit,
		CommitRecordTimestamp: commitRec.Timestamp,
		LastSeenOffset:        lastSeenOffset,
		LastBlockEnd:          lastBlockEnd,
	}
	if err := b.commitState(ctx, logger, b.cfg.ConsumerGroup, newState); err != nil {
		return state, err
	}

	return newState, nil
}

func (b *BlockBuilder) commitState(ctx context.Context, logger log.Logger, group string, state PartitionState) error {
	offsets := make(kadm.Offsets)
	offsets.Add(state.Commit)

	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Minute, // If there is a network hiccup, we prefer to wait longer retrying, than fail the whole section.
		MaxRetries: 10,
	})
	for boff.Ongoing() {
		err := kadm.NewClient(b.kafkaClient).CommitAllOffsets(ctx, group, offsets)
		if err == nil {
			break
		}
		level.Warn(logger).Log("msg", "failed to commit offsets; will retry", "err", err, "offset", state.Commit.At)
		boff.Wait()
	}
	if err := boff.ErrCause(); err != nil {
		return fmt.Errorf("commit with partition %d, offset %d: %w", state.Commit.Partition, state.Commit.At, err)
	}

	level.Info(logger).Log("msg", "successfully committed offset to kafka", "offset", state.Commit.At)

	return nil
}

func (b *BlockBuilder) uploadBlocks(ctx context.Context, tenantID, dbDir string, blockIDs []string) error {
	buc := bucket.NewUserBucketClient(tenantID, b.bucket, b.limits)
	for _, bid := range blockIDs {
		blockDir := path.Join(dbDir, bid)
		meta, err := block.ReadMetaFromDir(blockDir)
		if err != nil {
			return fmt.Errorf("read block meta for block %s (tenant %s): %w", bid, tenantID, err)
		}

		if meta.Stats.NumSamples == 0 {
			// No need to upload empty block.
			level.Info(b.logger).Log("msg", "skip uploading empty block", "tenant", tenantID, "block", bid)
			return nil
		}

		meta.Thanos.Source = block.BlockBuilderSource
		meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

		if meta.Compaction.FromOutOfOrder() && b.limits.OutOfOrderBlocksExternalLabelEnabled(tenantID) {
			// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
			meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
		}

		boff := backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: time.Minute, // If there is a network hiccup, we prefer to wait longer retrying, than fail the whole section.
			MaxRetries: 10,
		})
		for boff.Ongoing() {
			err := block.Upload(ctx, b.logger, buc, blockDir, meta)
			if err == nil {
				break
			}
			level.Warn(b.logger).Log("msg", "failed to upload block; will retry", "err", err, "block", bid, "tenant", tenantID)
			boff.Wait()
		}
		if err := boff.ErrCause(); err != nil {
			return fmt.Errorf("upload block %s (tenant %s): %w", bid, tenantID, err)
		}
	}
	return nil
}
