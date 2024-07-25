// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

type BlockBuilder struct {
	services.Service

	id int

	cfg          Config
	logger       log.Logger
	register     prometheus.Registerer
	limits       *validation.Overrides
	kafkaClients map[int32]*kgo.Client
	groupClient  *kgo.Client // Used only to commit. TODO: see why kadm client on kafkaClients is not able to commit.
	bucket       objstore.Bucket

	metrics blockBuilderMetrics

	// for testing
	tsdbBuilder func() builder

	// mu protects the following fields
	//mu               sync.Mutex
	//parts            partitions
	//cancelActivePart context.CancelCauseFunc
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (_ *BlockBuilder, err error) {
	b := &BlockBuilder{
		cfg:          cfg,
		logger:       logger,
		register:     reg,
		limits:       limits,
		metrics:      newBlockBuilderMetrics(reg),
		kafkaClients: make(map[int32]*kgo.Client),
	}

	if cfg.TotalBlockBuilders <= 0 || cfg.TotalPartitions <= 0 {
		return nil, fmt.Errorf("total block builders and total partitions must be greater than 0")
	}

	podName := os.Getenv("POD_NAME")
	splits := strings.Split(podName, "-")
	if len(splits) == 0 {
		return nil, fmt.Errorf("pod id not found in the name %s", podName)
	}
	id, err := strconv.Atoi(splits[len(splits)-1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse pod id from %s: %w", podName, err)
	}

	b.id = id
	level.Info(b.logger).Log("msg", "initialising block builder", "id", id)

	b.tsdbBuilder = func() builder {
		return newTSDBBuilder(b.logger, b.limits, b.cfg.BlocksStorageConfig)
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "blockbuilder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	const fetchMaxBytes = 100_000_000

	defer func() {
		if err != nil {
			if b.groupClient != nil {
				b.groupClient.Close()
			}
			for _, cl := range b.kafkaClients {
				cl.Close()
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

	// TODO: metrics without clashes
	//metrics := kprom.NewMetrics(
	//	"cortex_blockbuilder_kafka",
	//	kprom.Registerer(b.register),
	//	kprom.FetchAndProduceDetail(kprom.ByNode, kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes),
	//)

	opts := []kgo.Opt{
		kgo.ConsumeTopics(b.cfg.Kafka.Topic),
		kgo.ConsumerGroup(b.cfg.Kafka.ConsumerGroup),
		kgo.Balancers(kgo.RoundRobinBalancer()),
		kgo.DisableAutoCommit(),
		kgo.FetchMinBytes(128),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),
		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2 * fetchMaxBytes),
	}
	opts = append(
		commonKafkaClientOptions(b.cfg.Kafka, b.logger, nil),
		opts...,
	)
	groupClient, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	b.groupClient = groupClient

	var myParts []int
	for part := 0; part < b.cfg.TotalPartitions; part++ {
		if part%b.cfg.TotalBlockBuilders != b.id {
			continue
		}

		myParts = append(myParts, part)

		startAt, err := b.findCommitToStartAt(ctx, int32(part), nil)
		if err != nil {
			return fmt.Errorf("finding commit to start at: %w", err)
		}

		opts := []kgo.Opt{
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				b.cfg.Kafka.Topic: {int32(part): kgo.NewOffset().At(startAt)},
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

		opts = append(
			commonKafkaClientOptions(b.cfg.Kafka, b.logger, nil),
			opts...,
		)
		kafkaClient, err := kgo.NewClient(opts...)
		if err != nil {
			return fmt.Errorf("creating kafka client: %w", err)
		}

		b.kafkaClients[int32(part)] = kafkaClient

	}

	level.Info(b.logger).Log("msg", "waiting until kafka brokers are reachable", "parts", myParts)
	return b.waitKafka(ctx)
}

const (
	// kafkaOffsetStart is a special offset value that means the beginning of the partition.
	//kafkaOffsetStart = int64(-2)

	// kafkaOffsetEnd is a special offset value that means the end of the partition.
	kafkaOffsetEnd = int64(-1)
)

func (b *BlockBuilder) findCommitToStartAt(ctx context.Context, part int32, metrics *kprom.Metrics) (int64, error) {
	// We use an ephemeral client to fetch the offset and then create a new client with this offset.
	// The reason for this is that changing the offset of an existing client requires to have used this client for fetching at least once.
	// We don't want to do noop fetches just to warm up the client, so we create a new client instead.
	cl, err := kgo.NewClient(commonKafkaClientOptions(b.cfg.Kafka, b.logger, metrics)...)
	if err != nil {
		return kafkaOffsetEnd, fmt.Errorf("unable to create bootstrap client: %w", err)
	}
	defer cl.Close()

	fetchOffset := func(ctx context.Context) (offset int64, err error) {
		offset, exists, err := b.fetchLastCommittedOffset(ctx, cl, part)
		if err != nil {
			return -1, err
		}
		if exists {
			level.Info(b.logger).Log("msg", "starting consumption from last consumed offset", "start_offset", offset, "consumer_group", b.cfg.Kafka.ConsumerGroup)
			return offset, nil
		}

		offset = kafkaOffsetEnd
		level.Info(b.logger).Log("msg", "starting consumption from partition start because no offset has been found", "start_offset", offset)

		return offset, err
	}

	retry := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 10,
	})
	var (
		startOffset int64
	)
	for retry.Ongoing() {
		startOffset, err = fetchOffset(ctx)
		if err == nil {
			return startOffset, nil
		}

		level.Warn(b.logger).Log("msg", "failed to fetch offset", "err", err)
		retry.Wait()
	}

	// Handle the case the context was canceled before the first attempt.
	if err == nil {
		err = retry.Err()
	}

	return kafkaOffsetEnd, err
}

// fetchLastCommittedOffset returns the last consumed offset which has been committed by the PartitionReader
// to the consumer group.
func (b *BlockBuilder) fetchLastCommittedOffset(ctx context.Context, cl *kgo.Client, part int32) (offset int64, exists bool, _ error) {
	offsets, err := kadm.NewClient(cl).FetchOffsets(ctx, b.cfg.Kafka.ConsumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("unable to fetch group offsets: %w", err)
	}

	offsetRes, exists := offsets.Lookup(b.cfg.Kafka.Topic, part)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, offsetRes.Err
	}

	return offsetRes.At, true, nil
}

func commonKafkaClientOptions(cfg KafkaConfig, logger log.Logger, metrics *kprom.Metrics) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),
		kgo.WithLogger(newKafkaLogger(logger)),
	}
	if metrics != nil {
		opts = append(opts, kgo.WithHooks(metrics))
	}
	return opts
}

func (b *BlockBuilder) waitKafka(ctx context.Context) error {
Outer:
	for part, kafkaClient := range b.kafkaClients {
		boff := backoff.New(ctx, backoff.Config{
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: time.Second,
			MaxRetries: 0,
		})
		for boff.Ongoing() {
			err := kafkaClient.Ping(ctx)
			if err == nil {
				kafkaClient.PauseFetchPartitions(map[string][]int32{b.cfg.Kafka.Topic: {part}})
				continue Outer
			}

			level.Error(b.logger).Log(
				"msg", "waiting for kafka ping to succeed",
				"part", part,
				"num_retries", boff.NumRetries(),
			)
			boff.Wait()
		}
		return boff.Err()
	}

	return nil
}

func (b *BlockBuilder) stopping(_ error) error {
	b.groupClient.Close()
	for _, cl := range b.kafkaClients {
		cl.Close()
	}

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the last hour boundary + buffer.
	cycleEnd := cycleEndAtStartup(b.cfg.ConsumeInterval, b.cfg.ConsumeIntervalBuffer)
	err := b.NextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		return err
	}

	nextCycleTime := time.Now().Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
	waitTime := time.Until(nextCycleTime)

	for {
		select {
		case cycleEnd := <-time.After(waitTime):
			level.Info(b.logger).Log("msg", "triggering next consume from running", "cycle_end", cycleEnd, "cycle_time", nextCycleTime)
			err := b.NextConsumeCycle(ctx, cycleEnd.Add(-time.Second))
			if err != nil {
				b.metrics.consumeCycleFailures.Inc()
				level.Error(b.logger).Log("msg", "consume cycle failed", "cycle_end", cycleEnd, "err", err)
			}

			// If we took more than consumptionItvl time to consume the records, this
			// will immediately start the next consumption.
			nextCycleTime = nextCycleTime.Add(b.cfg.ConsumeInterval)
			waitTime = time.Until(nextCycleTime)
		// TODO(codesome): track "-waitTime" (when waitTime < 0), which is the time we ran over. Should have an alert on this.
		case <-ctx.Done():
			level.Info(b.logger).Log("msg", "context cancelled, stopping block builder")
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
// in this cycle. That is, Kafka records produced after the mark will be consumed in the next cycle.
func (b *BlockBuilder) NextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
	defer func(t time.Time) {
		b.metrics.consumeCycleDuration.Observe(time.Since(t).Seconds())
	}(time.Now())

	for part, kafkaClient := range b.kafkaClients {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if part < 0 {
			return nil
		}

		level.Debug(b.logger).Log("msg", "next consume cycle", "part", part)

		lag, err := b.getLagForPartition(ctx, kadm.NewClient(kafkaClient), part)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "part", part)
			continue
		}

		b.metrics.consumerLag.WithLabelValues(lag.Topic, fmt.Sprintf("%d", lag.Partition)).Set(float64(lag.Lag))

		if lag.Lag <= 0 {
			if err := lag.Err; err != nil {
				level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "part", part)
			} else {
				level.Info(b.logger).Log(
					"msg", "nothing to consume in partition",
					"part", part,
					"offset", lag.Commit.At,
					"end_offset", lag.End.Offset,
					"lag", lag.Lag,
				)
			}
			continue
		}

		// We look at the commit offset timestamp to determine how far behind we are lagging
		// relative to the cycleEnd. We will consume the partition in parts accordingly.
		offset := lag.Commit
		commitRecTs, seenTillTs, lastBlockEnd, err := unmarshallCommitMeta(offset.Metadata)
		if err != nil {
			// If there is an error in unmarshalling the metadata, treat it as if
			// we have no commit. There is no reason to stop the cycle for this.
			level.Warn(b.logger).Log("msg", "error unmarshalling commit metadata", "err", err, "part", part, "offset", offset.At, "metadata", offset.Metadata)
		}

		pl := partitionInfo{
			Partition:    part,
			Lag:          lag.Lag,
			Commit:       offset,
			CommitRecTs:  commitRecTs,
			SeenTillTs:   seenTillTs,
			LastBlockEnd: lastBlockEnd,
		}
		if err := b.nextConsumeCycle(ctx, kafkaClient, pl, cycleEnd); err != nil {
			level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "part", part)
		}
	}
	return nil
}

func (b *BlockBuilder) getLagForPartition(ctx context.Context, admClient *kadm.Client, part int32) (kadm.GroupMemberLag, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
	})
	var lastErr error
	for boff.Ongoing() {
		lags, err := admClient.Lag(ctx, b.cfg.Kafka.ConsumerGroup)
		if err != nil {
			lastErr = fmt.Errorf("get consumer group lag: %w", err)
			continue
		} else if err := lags.Error(); err != nil {
			lastErr = fmt.Errorf("get consumer group lag: %w", err)
			continue
		}

		groupLag := lags[b.cfg.Kafka.ConsumerGroup].Lag
		lag, ok := groupLag.Lookup(b.cfg.Kafka.Topic, part)
		if ok {
			return lag, nil
		}
		lastErr = fmt.Errorf("partition %d not found in lag response", part)
		boff.Wait()
	}

	return kadm.GroupMemberLag{}, lastErr
}

type partitionInfo struct {
	Partition int32
	Lag       int64

	Commit       kadm.Offset
	CommitRecTs  int64
	SeenTillTs   int64
	LastBlockEnd int64
}

func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, client *kgo.Client, pl partitionInfo, cycleEnd time.Time) error {
	// TODO(codesome): Add an option to block builder to start consuming from the end for the first rollout.
	var commitRecTime time.Time
	var skipRecordsBefore time.Time
	if pl.CommitRecTs > 0 {
		commitRecTime = time.UnixMilli(pl.CommitRecTs)
	}
	if commitRecTime.IsZero() {
		// If there is no commit metadata, we use the lookback config to replay a set amount of
		// records because it is non-trivial to peek at the first record in a partition to determine
		// the range of replay required. Without knowing the range, we might end up trying to consume
		// a lot of records in a single partition consumption call and end up in an OOM loop.
		// TODO(codesome): add a test for this.
		commitRecTime = time.Now().Add(-b.cfg.LookbackOnNoCommit).Truncate(b.cfg.ConsumeInterval)
		skipRecordsBefore = commitRecTime
	}

	lagging := cycleEnd.Sub(commitRecTime) > 3*b.cfg.ConsumeInterval/2
	if !lagging {
		// Either we did not find a commit offset or we are not lagging behind by
		// more than 1.5 times the consume interval.
		// When there is no kafka commit, we play safe and assume seenTillTs and
		// lastBlockEnd was 0 to not discard any samples unnecessarily.
		_, err := b.consumePartition(ctx, client, pl, cycleEnd, skipRecordsBefore)
		if err != nil {
			return fmt.Errorf("consume partition %d: %w", pl.Partition, err)
		}

		return nil
	}

	// We are lagging behind. We need to consume the partition in parts.
	// We iterate through all the cycleEnds starting from the first one after commit until the cycleEnd.
	cycleEndStartAt := commitRecTime.Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
	for ce := cycleEndStartAt; cycleEnd.Sub(ce) >= 0; ce = ce.Add(b.cfg.ConsumeInterval) {
		// Instead of looking for the commit metadata for each iteration, we use the data returned by consumePartition
		// in the next iteration.
		var err error
		pl, err = b.consumePartition(ctx, client, pl, ce, skipRecordsBefore)
		if err != nil {
			return fmt.Errorf("consume partition %d: %w", pl.Partition, err)
		}

		// If adding the ConsumeInterval takes it beyond the cycleEnd, we set it to the cycleEnd to not
		// exit the loop without consuming until cycleEnd.
		if ce.Compare(cycleEnd) != 0 && ce.Add(b.cfg.ConsumeInterval).After(cycleEnd) {
			ce = cycleEnd
		}
	}

	return nil
}

// consumePartition consumes records from the given partition until the cycleEnd timestamp.
// If the partition is lagging behind, the caller of consumePartition needs to take care of
// calling consumePartition in parts.
// consumePartition returns
// * retLag: updated lag after consuming the partition.
// * retSeenTillTs: timestamp of the last record processed (part of commit metadata).
// * retBlockMax: timestamp of the block end in this cycle (part of commit metadata).
func (b *BlockBuilder) consumePartition(
	ctx context.Context,
	client *kgo.Client,
	pl partitionInfo,
	cycleEnd time.Time,
	skipRecordsBefore time.Time,
) (retPl partitionInfo, retErr error) {
	var (
		part         = pl.Partition
		seenTillTs   = pl.SeenTillTs
		lastBlockEnd = pl.LastBlockEnd
		lastCommit   = pl.Commit
		lag          = pl.Lag

		blockEndAt = cycleEnd.Truncate(b.cfg.ConsumeInterval)
		blockEnd   = blockEndAt.UnixMilli()
	)

	// TopicPartition to resume consuming on this iteration.
	// Note: pause/resume is a client-local state. On restart or a crash, the client will be assigned its share of partitions,
	// during consumer group's rebalancing, and it will continue consuming as usual.
	tp := map[string][]int32{b.cfg.Kafka.Topic: {part}}
	client.ResumeFetchPartitions(tp)
	defer client.PauseFetchPartitions(tp)

	var (
		numBlocks     int
		compactionDur time.Duration
	)

	defer func(t time.Time, startingLag int64) {
		dur := time.Since(t)
		if retErr != nil {
			level.Error(b.logger).Log("msg", "partition consumption failed", "part", part, "dur", dur, "lag", lag, "err", retErr)
			return
		}
		b.metrics.processPartitionDuration.WithLabelValues(fmt.Sprintf("%d", part)).Observe(dur.Seconds())
		level.Info(b.logger).Log("msg", "done consuming partition", "part", part, "dur", dur,
			"start_lag", startingLag, "cycle_end", cycleEnd,
			"last_block_end", time.UnixMilli(lastBlockEnd), "curr_block_end", time.UnixMilli(retPl.LastBlockEnd),
			"last_seen_till", time.UnixMilli(seenTillTs), "curr_seen_till", time.UnixMilli(retPl.SeenTillTs),
			"num_blocks", numBlocks, "compact_and_upload_dur", compactionDur)
	}(time.Now(), lag)

	builder := b.tsdbBuilder()
	defer builder.close() // TODO: handle error

	level.Info(b.logger).Log(
		"msg", "consuming partition", "part", part, "lag", lag,
		"cycle_end", cycleEnd, "last_block_end", time.UnixMilli(lastBlockEnd), "curr_block_end", blockEndAt,
		"last_seen_till", time.UnixMilli(seenTillTs))

	var (
		done                         bool
		commitRec, firstRec, lastRec *kgo.Record
		preFirstRec                  *kgo.Record // Record that is discarded to be before the lookback, and right before firstRec.
	)
	for !done {
		if err := context.Cause(ctx); err != nil {
			return pl, err
		}
		// Limit the time the consumer blocks waiting for a new batch. If not set, the consumer will hang
		// when it lands on an inactive partition.
		ctx1, cancel := context.WithTimeout(ctx, b.cfg.Kafka.PollTimeout)
		fetches := client.PollFetches(ctx1)
		cancel()

		if fetches.IsClientClosed() {
			level.Warn(b.logger).Log("msg", "client closed when fetching records")
			return pl, nil
		}

		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				level.Error(b.logger).Log("msg", "failed to fetch records", "part", part, "err", err)
				b.metrics.fetchErrors.Inc()
			}
		})

		numRecs := fetches.NumRecords()
		if numRecs == 0 && lag <= 0 {
			level.Warn(b.logger).Log("msg", "got empty fetches from broker", "part", part)
			break
		}

		b.metrics.fetchRecordsTotal.Add(float64(numRecs))

		recIter := fetches.RecordIter()
		for !recIter.Done() {
			rec := recIter.Next()
			if rec.Timestamp.Before(skipRecordsBefore) {
				lag--
				if firstRec == nil {
					preFirstRec = rec
				}
				continue
			}

			if firstRec == nil {
				firstRec = rec
			}

			level.Debug(b.logger).Log("msg", "process record", "offset", rec.Offset, "rec", rec.Timestamp, "last_bmax", lastBlockEnd, "bmax", blockEnd)
			// Stop consuming after we reached the cycleEnd marker.
			// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
			if rec.Timestamp.After(cycleEnd) {
				done = true
				break
			}

			lag--

			recProcessedBefore := rec.Timestamp.UnixMilli() <= seenTillTs
			allSamplesProcessed, err := builder.process(ctx, rec, lastBlockEnd, blockEnd, recProcessedBefore)
			if err != nil {
				// TODO(codesome): do we just ignore this? What if it was Mimir's issue and this leading to data loss?
				level.Error(b.logger).Log("msg", "failed to process record", "part", part, "key", string(rec.Key), "err", err)
				continue
			}
			if !allSamplesProcessed && commitRec == nil {
				// If block builder restarts, it will start consuming from the record after this from kafka (from the commit record).
				// So the commit record should be the last record that was fully processed and not the
				// first record that was not fully processed.
				commitRec = lastRec

				// The first record itself was not fully processed, meaning the record before
				// this is the commit point.
				if commitRec == nil {
					commitRec = &kgo.Record{
						Timestamp:   time.UnixMilli(pl.CommitRecTs),
						Topic:       lastCommit.Topic,
						Partition:   lastCommit.Partition,
						LeaderEpoch: lastCommit.LeaderEpoch,
						Offset:      lastCommit.At - 1, // TODO(v): lastCommit.At can be zero if there wasn't any commit yet
					}
				}
			}
			lastRec = rec
		}
	}

	compactStart := time.Now()
	var err error
	numBlocks, err = builder.compactAndUpload(ctx, b.blockUploaderForUser)
	if err != nil {
		return pl, err
	}
	compactionDur = time.Since(compactStart)
	b.metrics.compactAndUploadDuration.WithLabelValues(fmt.Sprintf("%d", part)).Observe(compactionDur.Seconds())

	// Nothing was processed.
	if lastRec == nil && firstRec == nil {
		return pl, nil
	}

	// No records were for this cycle, which can be the case if the very first record fetched
	// was from the next cycle and/or there were records that were beyond the look back before this.
	// We must rewind partition's offset and re-consume this record again on the next cycle, but we
	// skip the records that didn't make the cut in this cycle.
	if lastRec == nil && preFirstRec != nil {
		// We have a record that was beyond the lookback. We must commit the record before this.
		commitRec = preFirstRec
	} else if lastRec == nil {
		rec := kgo.EpochOffset{
			Epoch:  firstRec.LeaderEpoch,
			Offset: firstRec.Offset,
		}
		b.seekPartition(client, part, rec)
		return pl, nil
	}

	// All samples in all records were processed. We can commit the last record's offset.
	if commitRec == nil {
		commitRec = lastRec
	}

	// If there were records that we consumed but didn't process, we must rewind the partition's offset
	// to the commit record. This is so on the next cycle, when the partition is read again, the consumer
	// starts at the commit point.
	defer func() {
		rec := kgo.EpochOffset{
			Epoch:  commitRec.LeaderEpoch,
			Offset: commitRec.Offset + 1, // offset+1 means everything up (including) to commitRec was processed
		}
		b.seekPartition(client, part, rec)
	}()

	// We should take the max of "seen till" timestamp. If the partition was lagging
	// due to some record not being processed because of a future sample, we might be
	// coming back to the same consume cycle again.
	commitSeenTillTs := seenTillTs
	if lastRec != nil && commitSeenTillTs < lastRec.Timestamp.UnixMilli() {
		commitSeenTillTs = lastRec.Timestamp.UnixMilli()
	}
	// Take the max of block max times because of same reasons above.
	commitBlockEnd := blockEnd
	if lastBlockEnd > blockEnd {
		commitBlockEnd = lastBlockEnd
	}

	pl = partitionInfo{
		Partition: pl.Partition,
		Lag:       lag,
		Commit: kadm.Offset{
			Topic:       commitRec.Topic,
			Partition:   commitRec.Partition,
			At:          commitRec.Offset + 1,
			LeaderEpoch: commitRec.LeaderEpoch,
			Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), commitSeenTillTs, commitBlockEnd),
		},
		CommitRecTs:  commitRec.Timestamp.UnixMilli(),
		SeenTillTs:   commitSeenTillTs,
		LastBlockEnd: commitBlockEnd,
	}

	err = commitRecord(ctx, b.logger, b.groupClient, b.cfg.Kafka.ConsumerGroup, commitRec, pl.Commit.Metadata)
	return pl, err
}

func (b *BlockBuilder) seekPartition(client *kgo.Client, part int32, rec kgo.EpochOffset) {
	offsets := map[string]map[int32]kgo.EpochOffset{
		b.cfg.Kafka.Topic: {
			part: rec,
		},
	}
	client.SetOffsets(offsets)
}

func (b *BlockBuilder) blockUploaderForUser(ctx context.Context, userID string) blockUploader {
	buc := bucket.NewUserBucketClient(userID, b.bucket, b.limits)
	return func(blockDir string) error {
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

		if meta.Compaction.FromOutOfOrder() && b.limits.OutOfOrderBlocksExternalLabelEnabled(userID) {
			// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
			meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
		}

		// Upload block with custom metadata.
		return block.Upload(ctx, b.logger, buc, blockDir, meta)
	}
}

func commitRecord(ctx context.Context, l log.Logger, kc *kgo.Client, group string, commitRec *kgo.Record, meta string) error {
	if commitRec == nil {
		return nil
	}

	ctx = kgo.PreCommitFnContext(ctx, func(req *kmsg.OffsetCommitRequest) error {
		for ti := range req.Topics {
			if req.Topics[ti].Topic != commitRec.Topic {
				continue
			}
			for pi := range req.Topics[ti].Partitions {
				if req.Topics[ti].Partitions[pi].Partition == commitRec.Partition {
					req.Topics[ti].Partitions[pi].Metadata = &meta
				}
			}
		}
		level.Info(l).Log("commit request", fmt.Sprintf("%+v", req))
		return nil
	})

	if err := kc.CommitRecords(ctx, commitRec); err != nil {
		return fmt.Errorf("commit record with part %d, offset %d: %w", commitRec.Partition, commitRec.Offset, err)
	}

	level.Debug(l).Log("msg", "successfully committed to Kafka", "part", commitRec.Partition, "offset", commitRec.Offset)

	_ = group // For lint.

	return nil
}

const (
	kafkaCommitMetaV1 = 1
)

// commitRecTs: timestamp of the record which was committed (and not the commit time).
// lastRecTs: timestamp of the last record processed (which will be >= commitRecTs).
// blockEnd: timestamp of the block end in this cycle.
func marshallCommitMeta(commitRecTs, lastRecTs, blockEnd int64) string {
	return fmt.Sprintf("%d,%d,%d,%d", kafkaCommitMetaV1, commitRecTs, lastRecTs, blockEnd)
}

// commitRecTs: timestamp of the record which was committed (and not the commit time).
// lastRecTs: timestamp of the last record processed (which will be >= commitRecTs).
// blockEnd: timestamp of the block end in this cycle.
func unmarshallCommitMeta(meta string) (commitRecTs, lastRecTs, blockEnd int64, err error) {
	if meta == "" {
		return
	}
	var (
		version int
		s       string
	)
	_, err = fmt.Sscanf(meta, "%d,%s", &version, &s)
	if err != nil {
		return
	}

	switch version {
	case kafkaCommitMetaV1:
		_, err = fmt.Sscanf(s, "%d,%d,%d", &commitRecTs, &lastRecTs, &blockEnd)
	default:
		err = fmt.Errorf("unsupported commit meta version %d", version)
	}
	return
}

type Config struct {
	ConsumeInterval       time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer time.Duration `yaml:"consume_interval_buffer"`
	LookbackOnNoCommit    time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`
	TotalBlockBuilders    int           `yaml:"total_block_builders"`
	TotalPartitions       int           `yaml:"total_partitions"`

	Kafka               KafkaConfig                    `yaml:"kafka"`
	BlocksStorageConfig mimir_tsdb.BlocksStorageConfig `yaml:"-"` // TODO(codesome): check how this is passed. Copied over form ingester.
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Kafka.RegisterFlagsWithPrefix("block-builder.", f)

	f.DurationVar(&cfg.ConsumeInterval, "block-builder.consume-interval", time.Hour, "Interval between block consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent block consumption cycles to avoid small blocks.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder.lookback-on-no-commit", 12*time.Hour, "How much of the historical records to look back when there is no kafka commit for a partition.")
	f.IntVar(&cfg.TotalPartitions, "block-builder.total-partitions", 0, "How many total kafka partitions exist.")
	f.IntVar(&cfg.TotalBlockBuilders, "block-builder.total-block-builders", 0, "Total block builders deployed.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}
	// TODO(codesome): validate the consumption interval. Must be <=2h and can divide 2h into an integer.
	if cfg.ConsumeInterval < 0 {
		return fmt.Errorf("-consume-interval must be non-negative")
	}

	return nil
}

// KafkaConfig holds the generic config for the Kafka backend.
type KafkaConfig struct {
	Address       string        `yaml:"address"`
	Topic         string        `yaml:"topic"`
	ClientID      string        `yaml:"client_id"`
	DialTimeout   time.Duration `yaml:"dial_timeout"`
	PollTimeout   time.Duration `yaml:"poll_timeout"`
	ConsumerGroup string        `yaml:"consumer_group"`
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+"kafka.address", "", "The Kafka seed broker address.")
	f.StringVar(&cfg.Topic, prefix+"kafka.topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+"kafka.client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+"kafka.dial-timeout", 5*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.PollTimeout, prefix+"kafka.poll-timeout", 5*time.Second, "The maximum time allowed to block if data is not available in the broker to consume.")
	f.StringVar(&cfg.ConsumerGroup, prefix+"kafka.consumer-group", "", "The consumer group used by the consumer to track the last consumed offset.")
}

func (cfg *KafkaConfig) Validate() error {
	// TODO(v): validate kafka config
	return nil
}

type kafkaLogger struct {
	logger log.Logger
}

func newKafkaLogger(logger log.Logger) *kafkaLogger {
	return &kafkaLogger{
		logger: log.With(logger, "component", "kafka_client"),
	}
}

func (l *kafkaLogger) Level() kgo.LogLevel {
	// The Kafka client calls Level() to check whether debug level is enabled or not.
	// To keep it simple, we always return Info, so the Kafka client will never try
	// to log expensive debug messages.
	return kgo.LogLevelInfo
}

func (l *kafkaLogger) Log(lev kgo.LogLevel, msg string, keyvals ...any) {
	if lev == kgo.LogLevelNone {
		return
	}
	keyvals = append([]any{"msg", msg}, keyvals...)
	switch lev {
	case kgo.LogLevelDebug:
		level.Debug(l.logger).Log(keyvals...)
	case kgo.LogLevelInfo:
		level.Info(l.logger).Log(keyvals...)
	case kgo.LogLevelWarn:
		level.Warn(l.logger).Log(keyvals...)
	case kgo.LogLevelError:
		level.Error(l.logger).Log(keyvals...)
	}
}
