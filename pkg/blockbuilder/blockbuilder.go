package blockbuilder

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
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

	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	limits      *validation.Overrides
	kafkaClient *kgo.Client
	bucket      objstore.Bucket

	// for testing
	tsdbBuilder func() builder

	assignmentMu sync.Mutex
	assignment   map[string][]int32
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
	limits *validation.Overrides,
) (_ *BlockBuilder, err error) {
	b := &BlockBuilder{
		cfg:      cfg,
		logger:   logger,
		register: reg,
		limits:   limits,
	}

	b.tsdbBuilder = func() builder {
		return newTSDBBuilder(b.logger, b.limits, b.cfg.BlocksStorageConfig)
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "ingester", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	// TODO(codesome): add a shipping subservice responsible for shipping the blocks to the storage.

	return b, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	const fetchMaxBytes = 100_000_000
	// Empty any previous artifacts.
	if err := os.RemoveAll(b.cfg.BlocksStorageConfig.TSDB.Dir); err != nil {
		return fmt.Errorf("removing tsdb dir: %w", err)
	}
	if err := os.MkdirAll(b.cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm); err != nil {
		return fmt.Errorf("creating tsdb dir: %w", err)
	}

	opts := []kgo.Opt{
		kgo.ClientID(b.cfg.Kafka.ClientID),
		kgo.SeedBrokers(b.cfg.Kafka.Address),
		kgo.DialTimeout(b.cfg.Kafka.DialTimeout),

		kgo.ConsumeTopics(b.cfg.Kafka.Topic),
		kgo.ConsumerGroup(b.cfg.Kafka.ConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.RoundRobinBalancer()), // TODO(v): figure out the best strategy to assign partitions and handle rebalancing
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(b.handlePartitionsAssigned),
		kgo.OnPartitionsRevoked(b.handlePartitionsLost),
		kgo.OnPartitionsLost(b.handlePartitionsLost),

		kgo.FetchMinBytes(128),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5 * time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),

		kgo.MetadataMinAge(10 * time.Second),
		kgo.MetadataMaxAge(10 * time.Second),

		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2 * fetchMaxBytes),
	}

	metrics := kprom.NewMetrics(
		"cortex_blockbuilder_kafka",
		kprom.Registerer(b.register),
		kprom.FetchAndProduceDetail(kprom.ByNode, kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes),
	)
	opts = append(opts,
		kgo.WithLogger(newKafkaLogger(b.logger)),
		kgo.WithHooks(metrics),
	)
	b.kafkaClient, err = kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	level.Info(b.logger).Log("msg", "waiting until kafka brokers are reachable")

	return b.waitKafka(ctx)
}

func (b *BlockBuilder) handlePartitionsAssigned(_ context.Context, _ *kgo.Client, assignment map[string][]int32) {
	level.Info(b.logger).Log("msg", "partition assigned", "assignment", fmt.Sprintf("%+v", assignment))

	// Pause fetching for all assigned partitions. We manage the order and the pace of the consumption ourself.
	// TODO(codesome): how does this behave when there is a block building cycle in progress? Should we not pause the
	// ones being consumed at the moment by this BB?
	assignment = b.kafkaClient.PauseFetchPartitions(assignment)

	b.assignmentMu.Lock()
	b.assignment = assignment
	b.assignmentMu.Unlock()
}

// TODO(codesome): question: is handlePartitionsAssigned also called by kafka client when partitions are revoked?
// TODO(codesome): how does this behave when there is a block building cycle in progress?
func (b *BlockBuilder) handlePartitionsLost(_ context.Context, _ *kgo.Client, lostAssignment map[string][]int32) {
	level.Info(b.logger).Log("msg", "partition lost", "lost", fmt.Sprintf("%+v", lostAssignment))

	// Unpause fetching of all previously assigned partitions. After rebalance gets completed,
	// the instance will receive a new assignment, and will start managing the set's consumption.
	b.kafkaClient.ResumeFetchPartitions(lostAssignment)
}

func (b *BlockBuilder) waitKafka(ctx context.Context) error {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})
	var pinged bool
	for boff.Ongoing() {
		if !pinged {
			err := b.kafkaClient.Ping(ctx)
			if err == nil {
				pinged = true
				boff.Reset()
			}
		}

		b.assignmentMu.Lock()
		assignment := b.assignment
		b.assignmentMu.Unlock()
		if assignment != nil {
			return nil
		}

		level.Error(b.logger).Log(
			"msg", "waiting for group assignment",
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}
	return boff.Err()
}

func (b *BlockBuilder) stopping(_ error) error {
	if b.kafkaClient != nil {
		b.kafkaClient.Close()
	}

	// TODO(codesome): wait for any active consume cycle or abort it.

	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the last hour boundary + buffer.
	cycleEnd := time.Now().Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeIntervalBuffer)
	if cycleEnd.After(time.Now()) {
		cycleEnd = cycleEnd.Add(-b.cfg.ConsumeInterval)
	}
	err := b.nextConsumeCycle(ctx, cycleEnd)
	if err != nil {
		return err
	}

	nextBlockTime := time.Now().Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval)
	waitTime := time.Until(nextBlockTime)

	for {
		select {
		case cycleEnd := <-time.After(waitTime):
			err := b.nextConsumeCycle(ctx, cycleEnd.Add(-time.Second))
			if err != nil {
				level.Error(b.logger).Log("msg", "consume cycle failed", "cycle_end", cycleEnd, "err", err)
			}

			// If we took more than consumptionItvl time to consume the records, this
			// will immediately start the next consumption.
			nextBlockTime = nextBlockTime.Add(b.cfg.ConsumeInterval)
			waitTime = time.Until(nextBlockTime)
			if waitTime < 0 {
				// TODO(codesome): track "-waitTime", which is the time we ran over. Or something better that lets us alert
				// if it goes beyond a certain point consistently.
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// nextConsumeCycle manages consumption of currently assigned partitions.
// The cycleEnd argument indicates the timestamp (relative to Kafka records) up until which to consume from partitions
// in this cycle. That is, Kafka records produced after the mark will be consumed in the next cycle.
func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
	b.assignmentMu.Lock()
	assignment := b.assignment
	b.assignmentMu.Unlock()

	assignmentParts, ok := assignment[b.cfg.Kafka.Topic]
	if !ok || len(assignmentParts) == 0 {
		return fmt.Errorf("no partitions assigned in %+v, topic %s", assignment, b.cfg.Kafka.Topic)
	}

	lags, err := kadm.NewClient(b.kafkaClient).Lag(ctx, b.cfg.Kafka.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get consumer group lag: %w", err)
	} else if err := lags.Error(); err != nil {
		return fmt.Errorf("get consumer group lag: %w", err)
	}

	// parts maps partition to this partition's current lag
	parts := make(map[int32]int64, len(assignmentParts))
	groupLag := lags[b.cfg.Kafka.ConsumerGroup].Lag
	for _, part := range assignmentParts {
		pl, ok := groupLag.Lookup(b.cfg.Kafka.Topic, part)
		if !ok {
			continue
		}
		if pl.Lag > 0 {
			parts[part] = pl.Lag
		} else {
			level.Info(b.logger).Log(
				"msg", "nothing to consume in partition",
				"part", part,
				"offset", pl.Commit.At,
				"end_offset", pl.End.Offset,
				"lag", pl.Lag,
			)
		}
	}

	if len(parts) == 0 {
		level.Warn(b.logger).Log("msg", "nothing to consume in this cycle", "assignment", fmt.Sprintf("%+v", assignment))
		return nil
	}

	// TODO(v): rebalancing can happen between the calls to consumePartition; if that happens, the instance may loose
	// the ownership of some of its partitions
	// TODO(v): test for this case

	offsets, err := kadm.NewClient(b.kafkaClient).FetchOffsetsForTopics(ctx, b.cfg.Kafka.ConsumerGroup, b.cfg.Kafka.Topic)
	if err != nil {
		return fmt.Errorf("fetch offsets for topics: %w", err)
	}
	for part, lag := range parts {
		// We look at the commit offset timestamp to determine how far behind we are lagging
		// relative to the cycleEnd. We consume the partition in parts accordingly.
		offset, ok := offsets.Lookup(b.cfg.Kafka.Topic, part)
		var commitRecTs, seenTillTs, lastBlockEnd int64
		var commitRecTime time.Time
		if ok {
			level.Debug(b.logger).Log("part", offset.Partition, "offset", offset.At, "meta", offset.Metadata)
			commitRecTs, seenTillTs, lastBlockEnd, err = unmarshallCommitMeta(offset.Metadata)
			if err != nil {
				return err
			}
			commitRecTime = time.UnixMilli(commitRecTs)
		} else {
			level.Warn(b.logger).Log("msg", "didn't find partition", "part", part, "offsets", fmt.Sprintf("%+v", offsets))
		}

		lagging := ok && cycleEnd.Sub(commitRecTime) > 3*b.cfg.ConsumeInterval/2
		if !lagging {
			// Either we did not find a commit offset or we are not lagging behind by
			// more than 1.5 times the consume interval.
			// When there is no kafka commit, we play safe and assume seenTillTs and
			// lastBlockEnd was 0 to not discard any samples unnecessarily.
			_, _, _, err = b.consumePartition(ctx, part, lag, seenTillTs, lastBlockEnd, cycleEnd)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "part", part)
			}

			// Make sure to unblock rebalance of the group after the partition was consumed AND after we (potentially) committed
			// this partition's offset to the group.
			// TODO(v): test for this case
			b.kafkaClient.AllowRebalance()
			continue
		}

		// We are lagging behind. We need to consume the partition in parts.

		// We iterate through all the cycleEnds starting from the first one after commit until the cycleEnd.
		cycleEndStartAt := commitRecTime.Truncate(b.cfg.ConsumeInterval).Add(b.cfg.ConsumeInterval + b.cfg.ConsumeIntervalBuffer)
		for ce := cycleEndStartAt; cycleEnd.Sub(ce) >= 0; ce = ce.Add(b.cfg.ConsumeInterval) {
			// Instead of looking for the commit metadata for each iteration, we use the data returned by consumePartition
			// in the next iteration.
			lag, seenTillTs, lastBlockEnd, err = b.consumePartition(ctx, part, lag, seenTillTs, lastBlockEnd, ce)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "part", part)
				return err
			}
			// If adding the ConsumeInterval takes it beyond the cycleEnd, we set it to the cycleEnd to not
			// exit the loop without consuming until cycleEnd.
			if ce.Compare(cycleEnd) != 0 && ce.Add(b.cfg.ConsumeInterval).After(cycleEnd) {
				ce = cycleEnd
			}
		}

		// Make sure to unblock rebalance of the group after the partition was consumed AND after we (potentially) committed
		// this partition's offset to the group.
		// TODO(v): test for this case
		b.kafkaClient.AllowRebalance()
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
	part int32,
	lag,
	seenTillTs, // Kafka record timestamp till which records were processed before.
	lastBlockMax int64, // blockEndAt associated with the previous commit
	cycleEnd time.Time,
) (retLag, retSeenTillTs, retBlockMax int64, err error) {
	// TopicPartition to resume consuming on this iteration.
	// Note: pause/resume is a client-local state. On restart or a crash, the client will be assigned its share of partitions,
	// during consumer group's rebalancing, and it will continue consuming as usual.
	tp := map[string][]int32{b.cfg.Kafka.Topic: {part}}
	b.kafkaClient.ResumeFetchPartitions(tp)
	defer b.kafkaClient.PauseFetchPartitions(tp)

	level.Info(b.logger).Log("msg", "consume partition", "part", part, "lag", lag, "cycle_end", cycleEnd)

	defer func(t time.Time) {
		level.Info(b.logger).Log("msg", "done consuming partition", "part", part, "dur", time.Since(t))
	}(time.Now())

	builder := b.tsdbBuilder()
	defer builder.close() // TODO: handle error

	var (
		done       bool
		commitRec  *kgo.Record
		lastRec    *kgo.Record
		blockEndAt = cycleEnd.Truncate(b.cfg.ConsumeInterval)
		blockMax   = blockEndAt.UnixMilli()
	)
	for !done {
		// Limit time client waits for a new batch. Otherwise, the client will hang if it lands on an inactive partition.
		ctx1, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := b.kafkaClient.PollFetches(ctx1)
		cancel()
		if fetches.IsClientClosed() {
			level.Warn(b.logger).Log("msg", "client closed when fetching records")
			return lag, seenTillTs, lastBlockMax, nil
		}

		fetches.EachError(func(_ string, part int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				level.Error(b.logger).Log("msg", "failed to fetch records", "part", part, "err", err)
			}
		})

		if fetches.Empty() && lag <= 0 {
			level.Warn(b.logger).Log("msg", "got empty fetches from broker", "part", part)
			break
		}

		recIter := fetches.RecordIter()
		for !recIter.Done() {
			rec := recIter.Next()

			lag--

			// Stop consuming after we reached the cycleEnd marker.
			// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
			if rec.Timestamp.After(cycleEnd) {
				done = true
				break
			}

			level.Debug(b.logger).Log("msg", "process record", "offset", rec.Offset, "rec", rec.Timestamp, "last_bmax", lastBlockMax, "bmax", blockMax)

			recProcessedBefore := rec.Timestamp.UnixMilli() <= seenTillTs
			allSamplesProcessed, err := builder.process(ctx, rec, lastBlockMax, blockMax, recProcessedBefore)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to process record", "part", part, "key", string(rec.Key), "err", err)
				continue
				// TODO(codesome): do we just ignore this? What if it was Mimir's issue and this leading to data loss?
				// TODO(codesome): add metric
			}
			if !allSamplesProcessed && commitRec == nil {
				// If block builder restarts, it will start consuming from the record after this from kafka.
				// So the commit record should be the last record that was fully processed and not the
				// first record that was not fully processed.
				commitRec = lastRec
			}
			lastRec = rec
		}
	}

	if err := builder.compactAndUpload(ctx, b.blockUploaderForUser); err != nil {
		// TODO(codesome): add metric
		return lag, seenTillTs, lastBlockMax, err
	}

	if commitRec == nil {
		// All samples in all records were processed. We can commit the last record's offset.
		commitRec = lastRec
	}

	if lastRec != nil {
		retSeenTillTs = lastRec.Timestamp.UnixMilli()
	}
	return lag, retSeenTillTs, blockMax, b.finalizePartition(ctx, commitRec, lastRec, blockMax)
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

func (b *BlockBuilder) finalizePartition(ctx context.Context, commitRec, lastRec *kgo.Record, blockEnd int64) error {
	if commitRec == nil {
		return nil
	}
	// Rewind the offset to the commit record so that when the partition is read again by this
	// block builder, it starts at the commit point.
	b.kafkaClient.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		b.cfg.Kafka.Topic: {
			commitRec.Partition: {
				Epoch:  commitRec.LeaderEpoch,
				Offset: commitRec.Offset,
			},
		},
	})

	ctx = kgo.PreCommitFnContext(ctx, func(req *kmsg.OffsetCommitRequest) error {
		meta := marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastRec.Timestamp.UnixMilli(), blockEnd)
		for _, topic := range req.Topics {
			if topic.Topic != b.cfg.Kafka.Topic {
				continue
			}
			for _, part := range req.Topics[0].Partitions {
				if part.Partition == commitRec.Partition {
					part.Metadata = &meta
				}
			}
		}
		level.Info(b.logger).Log("commit request", fmt.Sprintf("%+v", req))
		return nil
	})
	err := b.kafkaClient.CommitRecords(ctx, commitRec)
	if err != nil {
		return fmt.Errorf("commit record with part %d, offset %d: %w", commitRec.Partition, commitRec.Offset, err)
	}

	level.Debug(b.logger).Log("msg", "successfully committed to Kafka", "part", commitRec.Partition, "offset", commitRec.Offset)

	return nil
}

const (
	kafkaCommitMetaV1 = 1
)

// commitRecTs: timestamp of the record which was comitted (and not the commit time).
// lastRecTs: timestamp of the last record processed (which will be >= commitRecTs).
// blockEnd: timestamp of the block end in this cycle.
func marshallCommitMeta(commitRecTs, lastRecTs, blockEnd int64) string {
	return fmt.Sprintf("%d,%d,%d,%d", kafkaCommitMetaV1, commitRecTs, lastRecTs, blockEnd)
}

// commitRecTs: timestamp of the record which was comitted (and not the commit time).
// lastRecTs: timestamp of the last record processed (which will be >= commitRecTs).
// blockEnd: timestamp of the block end in this cycle.
func unmarshallCommitMeta(meta string) (commitRecTs, lastRecTs, blockEnd int64, err error) {
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

	Kafka               KafkaConfig                    `yaml:"kafka"`
	BlocksStorageConfig mimir_tsdb.BlocksStorageConfig `yaml:"-"` // TODO(codesome): check how this is passed. Copied over form ingester.
}

// RegisterFlags registers the MultitenantCompactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Kafka.RegisterFlags(f, logger)

	f.DurationVar(&cfg.ConsumeInterval, "consume-internal", time.Hour, "Interval between block consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "consume-internal-buffer", 5*time.Minute, "Extra buffer between subsequent block consumption cycles to avoid small blocks.")
}

func (cfg *Config) Validate(logger log.Logger) error {
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
	Address     string        `yaml:"address"`
	Topic       string        `yaml:"topic"`
	ClientID    string        `yaml:"client_id"`
	DialTimeout time.Duration `yaml:"dial_timeout"`

	ConsumerGroup                     string        `yaml:"consumer_group"`
	ConsumerGroupOffsetCommitInterval time.Duration `yaml:"consumer_group_offset_commit_interval"`

	ConsumeFromPositionAtStartup string `yaml:"consume_from_position_at_startup"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.RegisterFlagsWithPrefix("block-builder.kafka", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka seed broker address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.StringVar(&cfg.ConsumerGroup, prefix+".consumer-group", "", "The consumer group used by the consumer to track the last consumed offset.")
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
