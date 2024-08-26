// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"encoding/json"
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
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	storagetsdb "github.com/grafana/mimir/pkg/storage/tsdb"
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

	parts []int32
	// fallbackOffset is the low watermark from where a partition which doesn't have a commit will be consumed.
	// It can be either a timestamp or the kafkaStartOffset (the latter isn't supported still).
	// TODO(v): to support setting it to kafkaStartOffset we need to rework how a lagging cycle is chopped into time frames.
	fallbackOffset int64

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

	instanceID, err := parseIDFromInstance(b.cfg.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse instance id: %w", err)
	}

	var ok bool
	b.parts, ok = b.cfg.PartitionAssignment[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance id %d must be present in partition assignment", instanceID)
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "block-builder", logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create the bucket client: %w", err)
	}
	b.bucket = bucketClient

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func parseIDFromInstance(instanceID string) (int, error) {
	splits := strings.Split(instanceID, "-")
	if len(splits) == 0 {
		return 0, fmt.Errorf("malformed instance id %s", instanceID)
	}

	partID, err := strconv.Atoi(splits[len(splits)-1])
	if err != nil {
		return 0, fmt.Errorf("parse instance id %s, expect sequence number: %w", instanceID, err)
	}

	return partID, nil
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
	startAtOffsets, fallbackOffset, err := b.findOffsetsToStartAt(ctx)
	if err != nil {
		return fmt.Errorf("find offsets to start at: %w", err)
	}
	b.fallbackOffset = fallbackOffset

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
	b.kafkaClient.PauseFetchPartitions(map[string][]int32{b.cfg.Kafka.Topic: b.parts})

	return nil
}

// kafkaOffsetStart is a special offset value that means the beginning of the partition.
const kafkaOffsetStart = int64(-2)

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

	var fallbackOffset int64
	defaultKOffset := kgo.NewOffset()

	// When LookbackOnNoCommit is positive, its value is used to calculate the timestamp after which the consumption starts,
	// otherwise it is a special offset for the "start" of the partition.
	if b.cfg.LookbackOnNoCommit > 0 {
		ts := time.Now().Add(-b.cfg.LookbackOnNoCommit)
		fallbackOffset = ts.UnixMilli()
		defaultKOffset = defaultKOffset.AfterMilli(fallbackOffset)
	} else {
		fallbackOffset = int64(b.cfg.LookbackOnNoCommit)
		switch fallbackOffset {
		case kafkaOffsetStart:
			defaultKOffset = defaultKOffset.AtStart()
		default:
			// We don't support consuming from the "end" of the partition because starting a cycle from the end always results to a zero lag.
			// This may be done later.
			return nil, -1, fmt.Errorf("unexpected fallback offset value %v", b.cfg.LookbackOnNoCommit)
		}
	}

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
		for _, part := range b.parts {
			if _, ok := offsets[part]; ok {
				level.Info(b.logger).Log("msg", "consuming from last consumed offset", "consumer_group", b.cfg.Kafka.ConsumerGroup, "partition", part, "offset", offsets[part].String())
			} else {
				offsets[part] = defaultKOffset
				level.Info(b.logger).Log("msg", "consuming from partition lookback because no offset has been found", "consumer_group", b.cfg.Kafka.ConsumerGroup, "partition", part, "offset", offsets[part].String())
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
			return offsets, fallbackOffset, nil
		}
		level.Warn(b.logger).Log("msg", "failed to fetch startup offsets", "err", err)
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
		kgo.WithLogger(newKafkaLogger(logger)),
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
			level.Info(b.logger).Log("msg", "triggering next consume from running", "cycle_end", cycleEnd, "cycle_time", nextCycleTime)
			err := b.NextConsumeCycle(ctx, cycleEnd)
			if err != nil {
				b.metrics.consumeCycleFailures.Inc()
				level.Error(b.logger).Log("msg", "consume cycle failed", "cycle_end", cycleEnd, "err", err)
			}

			// If we took more than consumptionItvl time to consume the records, this will immediately start the next consumption.
			// TODO(codesome): track waitTime < 0, which is the time we ran over. Should have an alert on this.
			nextCycleTime = nextCycleTime.Add(b.cfg.ConsumeInterval)
			waitTime = time.Until(nextCycleTime)
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
// in this cycle. That is, Kafka records produced after the cycleEnd mark will be consumed in the next cycle.
func (b *BlockBuilder) NextConsumeCycle(ctx context.Context, cycleEnd time.Time) error {
	defer func(t time.Time) {
		b.metrics.consumeCycleDuration.Observe(time.Since(t).Seconds())
	}(time.Now())

	for _, part := range b.parts {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if part < 0 {
			return nil
		}

		// TODO(v): calculating lag for each individual partition requests data for the whole group every time. This is redundant.
		// Since we no longer expect a rebalance in the middle of the cycle, we may calculate the lag once for all assigned partitions
		// in the beginning of the cycle.
		lag, err := b.getLagForPartition(ctx, part)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", part, "cycle_end", cycleEnd)
			continue
		}

		b.metrics.consumerLag.WithLabelValues(lag.Topic, fmt.Sprintf("%d", lag.Partition)).Set(float64(lag.Lag))

		if lag.Lag <= 0 {
			if err := lag.Err; err != nil {
				level.Error(b.logger).Log("msg", "failed to get partition lag", "err", err, "partition", part, "cycle_end", cycleEnd)
			} else {
				level.Info(b.logger).Log(
					"msg", "nothing to consume in partition",
					"partition", part,
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

func (b *BlockBuilder) getLagForPartition(ctx context.Context, part int32) (kadm.GroupMemberLag, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
	})
	var lastErr error
	for boff.Ongoing() {
		if lastErr != nil {
			level.Error(b.logger).Log("msg", "failed to get consumer group lag", "err", lastErr, "partition", part)
		}
		groupLag, err := getGroupLag(ctx, kadm.NewClient(b.kafkaClient), b.cfg.Kafka.Topic, b.cfg.Kafka.ConsumerGroup, b.fallbackOffset)
		if err != nil {
			lastErr = fmt.Errorf("get consumer group lag: %w", err)
			continue
		}

		lag, ok := groupLag.Lookup(b.cfg.Kafka.Topic, part)
		if ok {
			return lag, nil
		}
		lastErr = fmt.Errorf("partition %d not found in lag response", part)
		boff.Wait()
	}

	return kadm.GroupMemberLag{}, lastErr
}

type Config struct {
	InstanceID          string          `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	PartitionAssignment map[int][]int32 `yaml:"partition_assignment" category:"experimental"`

	ConsumeInterval       time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer time.Duration `yaml:"consume_interval_buffer"`
	LookbackOnNoCommit    time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`

	Kafka               KafkaConfig                     `yaml:"kafka"`
	BlocksStorageConfig storagetsdb.BlocksStorageConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.Kafka.RegisterFlagsWithPrefix("block-builder.", f)

	f.StringVar(&cfg.InstanceID, "block-builder.instance-id", hostname, "Instance id.")
	f.Var(newPartitionAssignmentVar(&cfg.PartitionAssignment), "block-builder.partition-assignment", "Static partition assignment. Format map[instance-id][]partitions).")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder.consume-interval", time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent consumption cycles. To avoid small blocks the block-builder consumes until the last hour boundary of the consumption interval, plus the buffer.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder.lookback-on-no-commit", 12*time.Hour, "How much of the historical records to look back when there is no kafka commit for a partition.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}

	if len(cfg.PartitionAssignment) == 0 {
		return fmt.Errorf("partition assignment is required")
	}
	// TODO(codesome): validate the consumption interval. Must be <=2h and can divide 2h into an integer.
	if cfg.ConsumeInterval < 0 {
		return fmt.Errorf("-consume-interval must be non-negative")
	}

	return nil
}

type KafkaConfig struct {
	Address       string        `yaml:"address"`
	Topic         string        `yaml:"topic"`
	ClientID      string        `yaml:"client_id"`
	DialTimeout   time.Duration `yaml:"dial_timeout"`
	ConsumerGroup string        `yaml:"consumer_group"`
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+"kafka.address", "", "The Kafka seed broker address.")
	f.StringVar(&cfg.Topic, prefix+"kafka.topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+"kafka.client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+"kafka.dial-timeout", 5*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.StringVar(&cfg.ConsumerGroup, prefix+"kafka.consumer-group", "block-builder", "The consumer group used by the consumer.")
}

func (cfg *KafkaConfig) Validate() error {
	return nil
}

type partitionAssignmentVar map[int][]int32

func newPartitionAssignmentVar(p *map[int][]int32) *partitionAssignmentVar {
	return (*partitionAssignmentVar)(p)
}

func (v *partitionAssignmentVar) Set(s string) error {
	if s == "" {
		return nil
	}
	val := make(map[int][]int32)
	err := json.Unmarshal([]byte(s), &val)
	if err != nil {
		return fmt.Errorf("unmarshal partition assignment: %w", err)
	}
	*v = val
	return nil
}

func (v partitionAssignmentVar) String() string {
	return fmt.Sprintf("%v", map[int][]int32(v))
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
