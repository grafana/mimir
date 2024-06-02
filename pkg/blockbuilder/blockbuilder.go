package blockbuilder

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type BlockBuilder struct {
	services.Service

	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	limits      *validation.Overrides
	kafkaClient *kgo.Client

	assignmentMu sync.Mutex
	assignment   map[string][]int32
}

func NewBlockBuilder(
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

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	const fetchMaxBytes = 100_000_000

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

		kgo.FetchMinBytes(1),
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
	if err := b.waitKafka(ctx); err != nil {
		return err
	}

	return nil
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
	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// Do initial consumption on start using current time as the point up to which we are consuming.
	// To avoid small blocks at startup, we consume until the last hour boundary + buffer.
	consumptionItvl := time.Hour
	timeBuffer := 15 * time.Minute
	mark := time.Now().Truncate(consumptionItvl).Add(timeBuffer)
	if mark.After(time.Now()) {
		mark = mark.Add(-consumptionItvl)
	}
	err := b.nextConsumeCycle(ctx, mark)
	if err != nil {
		return err
	}

	// TODO(v): configure consumption interval
	// TODO(codesome): validate the consumption interval. Must be <=2h and can divide 2h into an integer.

	nextBlockTime := time.Now().Truncate(consumptionItvl).Add(consumptionItvl + timeBuffer)
	waitTime := time.Until(nextBlockTime)

	for {
		select {
		case mark := <-time.After(waitTime):
			_ = b.nextConsumeCycle(ctx, mark.Add(-time.Second))
			// If we took more than consumptionItvl time to consume the records, this
			// will immediately start the next consumption.
			nextBlockTime = nextBlockTime.Add(consumptionItvl)
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
// The mark argument indicates the timestamp (relative to Kafka records) up until which to consume from partitions
// in this cycle. That is, Kafka records produced after the mark will be consumed in the next cycle.
func (b *BlockBuilder) nextConsumeCycle(ctx context.Context, mark time.Time) error {
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

	lag := lags[b.cfg.Kafka.ConsumerGroup].Lag
	parts := make([]int32, 0, len(assignmentParts))
	for _, part := range assignmentParts {
		pl, ok := lag.Lookup(b.cfg.Kafka.Topic, part)
		if !ok {
			continue
		}
		if pl.Lag > 0 {
			parts = append(parts, part)
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
		level.Warn(b.logger).Log("msg", "nothing to consume in this cycle", "assignment", fmt.Sprintf("%+v", assignmentParts))
		return nil
	}

	slices.Sort(parts)

	// TODO(v): rebalancing can happen between the calls to consumePartitions; if that happens, the instace may loose
	// the ownership of some of its partitions
	// TODO(v): test for this case
	for _, part := range parts {
		err := b.consumePartitions(ctx, part, mark)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to consume partition", "err", err, "part", part)
		}
	}

	return nil
}

func (b *BlockBuilder) consumePartitions(ctx context.Context, part int32, mark time.Time) error {
	// TopicPartition to resume consuming on this iteration.
	tp := map[string][]int32{b.cfg.Kafka.Topic: {part}}

	// Note: pause/resume is a client-local state. On restart or a crash, the client will be assigned its share of partitions,
	// during consumer group's rebalancing, and it will continue consuming as usual.
	b.kafkaClient.ResumeFetchPartitions(tp)
	defer b.kafkaClient.PauseFetchPartitions(tp)

	// Make sure to unblock rebalance of the group after the very last poll AND after we (potentially) commited
	// this partition's offset to the group.
	// TODO(v): test for this case
	defer b.kafkaClient.AllowRebalance()

	level.Info(b.logger).Log(
		"msg", "consume partition",
		"part", part,
	)

	defer func(t time.Time) {
		level.Info(b.logger).Log(
			"msg", "done consuming partition",
			"part", part,
			"dur", time.Since(t),
		)
	}(time.Now())

	var lastOffset int64

	// TODO(v): signal to bail out from the consume loop, otherwise a busy partition will starve the consumer
	var (
		consumptionItvl     = time.Hour        // TODO(codesome): get this from config
		timeBuffer          = 15 * time.Minute // TODO(codesome): get this from config
		checkpointOffset    = int64(-1)
		resetBlockBuilderAt time.Time
		builder             *tsdbBuilder
		currEnd, lastEnd    int64
		done                bool
	)
	for !done {
		// Limit the time the client waits for new batch of records, otherwise, it will hang when landed to a inactive partition.
		// TODO(v): configure fetch timeout
		ctx1, cancel := context.WithTimeout(ctx, 10*time.Second)
		fetches := b.kafkaClient.PollFetches(ctx1)
		cancel()

		if fetches.IsClientClosed() {
			return nil
		}

		fetches.EachError(func(_ string, part int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				level.Error(b.logger).Log("msg", "failed to fetch records", "part", part, "err", err)
			}
		})

		if fetches.Empty() {
			done = true
		}

		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			level.Info(b.logger).Log("msg", "consumed", "part", ftp.Partition, "hi", ftp.HighWatermark, "lo", ftp.LogStartOffset, "batch_size", len(ftp.Records))

			for _, rec := range ftp.Records {
				// When BB is first deployed or if it is lagging behind, then it might consuming data from too much
				// in the past. In which case if we try to consume all at once, it can overwhelm the system.
				// So we break this into multiple block building cycles by resetting the block builder at intervals.
				// TODO(codesome): the logic for this below is broken. Fix it. How can we determine the block ends when we are catching up?
				if builder != nil && rec.Timestamp.After(resetBlockBuilderAt) {
					if err := builder.compactAndRemoveDBs(ctx); err != nil {
						level.Error(b.logger).Log("msg", "failed to compact and remove dbs", "part", part, "err", err)
						// TODO(codesome): return err?
						// TODO(codesome): add metric
					}
					builder = nil
				}
				if builder == nil {
					builder = newTSDBBuilder(b.logger, b.limits, b.cfg.BlocksStorageConfig)
					resetBlockBuilderAt = rec.Timestamp.Truncate(consumptionItvl).Add(consumptionItvl + timeBuffer)

					// TODO(codesome): verify. this can be wrong.
					currEnd = rec.Timestamp.Truncate(consumptionItvl).UnixMilli()
					lastEnd = currEnd - consumptionItvl.Milliseconds()
				}

				// TODO(v): the allSamplesProcessed logic isn't seem right.
				// I.e. kafka client consumes the log in one direction, thus skipped records will be skipped until either
				// 1. we manually set the offset of the kafka client on every cycle (ref. kgo#Client.SetOffsets)
				// 2. process was restarted, the client fetched last commited offset for the group+partition and started consuming from this (old) offset
				recordProcessedBefore := false // TODO(codesome): get this from checkpoint
				allSamplesProcessed, err := builder.process(ctx, rec, lastEnd, currEnd, recordProcessedBefore)
				if !allSamplesProcessed && checkpointOffset < 0 {
					checkpointOffset = rec.Offset
				}
				if err != nil {
					level.Error(b.logger).Log("msg", "failed to process record", "part", part, "key", string(rec.Key), "err", err)
					// TODO(codesome): do we just ignore this? What if it was Mimir's issue and this leading to data loss?
					// TODO(codesome): add metric
				}

				lastOffset = rec.Offset

				// Stop consuming after we reached the marker.
				// NOTE: the timestamp of the record is when the record was produced relative to distributor's time.
				if rec.Timestamp.After(mark) {
					done = true
				}
			}
		})

		// After the rebalance, if another instance took over the partition, the next poll
		// will return empty fetches, and the loop will bail out.
		// TODO(v): test for this case
		b.kafkaClient.AllowRebalance()
	}

	if err := builder.compactAndRemoveDBs(ctx); err != nil {
		// TODO(codesome): add metric
		return err
	}

	// TODO(codesome): store the lastOffset and currEnd as a metadata in the checkpoint
	_ = lastOffset // to avoid unused error. TODO: remove this once used
	return b.commitOffset(ctx, part, checkpointOffset)
}

func (b *BlockBuilder) commitOffset(ctx context.Context, part int32, offset int64) (returnErr error) {
	defer func() {
		if returnErr != nil {
			level.Error(b.logger).Log("msg", "failed to commit offset to Kafka", "err", returnErr, "partition", part, "offset", offset)
			// TODO(codesome): add metric
		}
	}()

	toCommit := kadm.Offsets{}
	toCommit.AddOffset(b.cfg.Kafka.Topic, part, offset, -1)

	committed, err := kadm.NewClient(b.kafkaClient).CommitOffsets(ctx, b.cfg.Kafka.ConsumerGroup, toCommit)
	if err != nil {
		return err
	} else if !committed.Ok() {
		return committed.Error()
	}

	committedOffset, _ := committed.Lookup(b.cfg.Kafka.Topic, part)
	level.Debug(b.logger).Log("msg", "offset successfully committed to Kafka", "offset", committedOffset.At)
	// TODO(codesome): add this metric
	// r.lastCommittedOffset.Set(float64(committedOffset.At))

	return nil
}

type Config struct {
	Kafka               KafkaConfig                    `yaml:"kafka"`
	BlocksStorageConfig mimir_tsdb.BlocksStorageConfig `yaml:"-"` // TODO(codesome): check how this is passed. Copied over form ingester.
}

// RegisterFlags registers the MultitenantCompactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Kafka.RegisterFlags(f, logger)
}

func (cfg *Config) Validate(logger log.Logger) error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
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
