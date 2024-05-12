package blockbuilder

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/grafana/mimir/pkg/util"
)

type partitionRingReader interface {
	PartitionRing() *ring.PartitionRing
}

type BlockBuilder struct {
	services.Service

	cfg    Config
	logger log.Logger

	ringl          *ring.BasicLifecycler
	ring           *ring.Ring
	kafkaClient    *kgo.Client
	partRingReader partitionRingReader

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	builder *tsdbBuilder
}

func NewBlockBuilder(
	cfg Config,
	logger log.Logger,
	partitionRing partitionRingReader,
	reg prometheus.Registerer,
) (_ *BlockBuilder, err error) {
	b := &BlockBuilder{
		cfg:            cfg,
		logger:         logger,
		partRingReader: partitionRing,
		builder:        newTSDBBuilder(logger),
	}

	b.ring, b.ringl, err = newRingAndLifecycler(b.cfg.Ring, b.logger, reg)
	if err != nil {
		return nil, err
	}

	b.kafkaClient, err = newKafkaClient(b.cfg.Kafka, b.logger, reg)
	if err != nil {
		return nil, err
	}

	var subservices []services.Service
	subservices = append(subservices, b.ring, b.ringl)

	b.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}
	b.subservicesWatcher = services.NewFailureWatcher()
	b.subservicesWatcher.WatchManager(b.subservices)

	b.Service = services.NewBasicService(b.starting, b.running, b.stopping)

	return b, nil
}

func (b *BlockBuilder) starting(ctx context.Context) (err error) {
	if err := services.StartManagerAndAwaitHealthy(ctx, b.subservices); err != nil {
		return fmt.Errorf("starting subservices: %w", err)
	}

	level.Info(b.logger).Log("msg", "waiting until instance is ACTIVE in its ring")
	if err := ring.WaitInstanceState(ctx, b.ring, b.ringl.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}

	level.Info(b.logger).Log("msg", "waiting until kafka brokers are reachable")
	if err := waitKafka(ctx, b.kafkaClient, b.logger); err != nil {
		return err
	}

	return nil
}

func (b *BlockBuilder) stopping(_ error) error {
	b.kafkaClient.Close()

	err := services.StopManagerAndAwaitStopped(context.Background(), b.subservices)
	if err != nil {
		return err
	}
	return nil
}

func (b *BlockBuilder) running(ctx context.Context) error {
	// do initial consumption on start
	err := b.consumeRecords(ctx)
	if err != nil {
		return err
	}

	// TODO(v): configure consumption interval
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()

	go func() {
		// TODO(v): can remove after debugging the ring
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			_ = b.watchPartitionRing(ctx)
		}
	}()

	for {
		select {
		case <-tick.C:
			_ = b.consumeRecords(ctx)
		case <-ctx.Done():
			return nil
		case err := <-b.subservicesWatcher.Chan():
			return err
		}
	}
}

func (b *BlockBuilder) watchPartitionRing(_ context.Context) error {
	r := b.partRingReader.PartitionRing()

	partitions := r.ActivePartitionIDs()
	level.Info(b.logger).Log("num_partitions", len(partitions), "partitions", fmt.Sprintf("%+v", partitions))

	mid, gen := b.kafkaClient.GroupMetadata()
	level.Info(b.logger).Log("msg", "kafka group metadata", "member-id", mid, "generation", gen)

	return nil
}

func (b *BlockBuilder) consumeRecords(ctx context.Context) error {
	// TODO(v): advance the per-partition offset
	offset := kgo.NewOffset().AtStart()
	return b.consumePartition(ctx, 1, offset)
}

func (b *BlockBuilder) consumePartition(ctx context.Context, part int32, offset kgo.Offset) error {
	// Copy the base client, but manually specify the consumed partition.
	opts := b.kafkaClient.Opts()
	opts = append(
		opts,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			b.cfg.Kafka.Topic: {
				part: offset,
			},
		}),
	)
	kafkaClient, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer kafkaClient.Close()

	level.Info(b.logger).Log("msg", "consuming partition", "part", part, "offset", offset.String())

	for ctx.Err() == nil {
		ctx1, cancel := context.WithTimeout(ctx, time.Second)
		fetches := kafkaClient.PollFetches(ctx1)
		fetches.EachError(func(_ string, _ int32, err error) {
			level.Error(b.logger).Log("msg", "failed to fetch records", "err", err)
		})
		cancel()

		if fetches.Empty() {
			return nil
		}

		level.Info(b.logger).Log("msg", "consumed", "part", part, "num_records", fetches.NumRecords())

		recIter := fetches.RecordIter()
		for !recIter.Done() {
			rec := recIter.Next()

			err := b.builder.process(ctx, rec)
			if err != nil {
				level.Error(b.logger).Log("msg", "failed to process record", "err", err)
			}
		}
	}

	return nil
}

func (b *BlockBuilder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if b.ring != nil {
		b.ring.ServeHTTP(w, req)
	} else {
		ringNotEnabledPage := `
			<!DOCTYPE html>
			<html>
				<head>
					<meta charset="UTF-8">
					<title>Distributor Status</title>
				</head>
				<body>
					<h1>Distributor Status</h1>
					<p>Distributor is not running with global limits enabled</p>
				</body>
			</html>`
		util.WriteHTMLResponse(w, ringNotEnabledPage)
	}
}

type Config struct {
	Ring  RingConfig  `yaml:"ring"`
	Kafka KafkaConfig `yaml:"kafka"`
}

// RegisterFlags registers the MultitenantCompactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Ring.RegisterFlags(f, logger)
	cfg.Kafka.RegisterFlags(f, logger)
}

func (cfg *Config) Validate(logger log.Logger) error {
	if err := cfg.Ring.Validate(); err != nil {
		return err
	}
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}
	return nil
}

const (
	// ringName is the name of the ring.
	ringName = "blockbuilder"
	// ringKey is the key under which we store the component's ring in the KVStore.
	ringKey = "blockbuilder"
	// ringNumTokens is how many tokens each component should have in the ring. Safe default is used
	// instead of exposing it as a config option in order to simplify the config for the users.
	ringNumTokens = 512
	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 10
)

// RingConfig masks the ring lifecycler config which contains many options not really required here.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Common.RegisterFlags("block-builder.ring.", "collectors/", "blockbuilders", f, logger)
}

func (cfg *RingConfig) Validate() error {
	// TODO(v): validate ring config
	return nil
}

func (cfg *RingConfig) ToBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Common.InstanceAddr, cfg.Common.InstanceInterfaceNames, logger, cfg.Common.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Common.InstancePort, cfg.Common.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              cfg.Common.InstanceID,
		Addr:                            net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		HeartbeatPeriod:                 cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.Common.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       ringNumTokens,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = 1

	return rc
}

func newRingAndLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "blockbuilder-lifecycler"), logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize KV store: %w", err)
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build lifecycler config: %w", err)
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	forgetPeriod := ringAutoForgetUnhealthyPeriods * lifecyclerCfg.HeartbeatTimeout
	delegate = ring.NewAutoForgetDelegate(forgetPeriod, delegate, logger)

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, ringName, ringKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize lifecycler: %w", err)
	}

	ringClient, err := ring.New(cfg.toRingConfig(), ringName, ringKey, logger, reg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize ring client: %w", err)
	}

	return ringClient, lifecycler, nil
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
	f.DurationVar(&cfg.ConsumerGroupOffsetCommitInterval, prefix+".consumer-group-offset-commit-interval", time.Second, "How frequently a consumer should commit the consumed offset to Kafka. The last committed offset is used at startup to continue the consumption from where it was left.")

	f.StringVar(&cfg.ConsumeFromPositionAtStartup, prefix+".consume-from-position-at-startup", "last-offset", "")
}

func (cfg *KafkaConfig) Validate() error {
	// TODO(v): validate ring config
	return nil
}

func newKafkaClient(cfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) (*kgo.Client, error) {
	const fetchMaxBytes = 100_000_000

	opts := []kgo.Opt{
		kgo.ClientID(cfg.ClientID),
		kgo.SeedBrokers(cfg.Address),
		kgo.DialTimeout(cfg.DialTimeout),

		// TODO(v): figure out the best strategy to assign partitions and handle rebalancing
		//kgo.ConsumerGroup(cfg.ConsumerGroup),
		//kgo.DisableAutoCommit(),
		//kgo.BlockRebalanceOnPoll(),
		//kgo.Balancers(NewPartitionRingBalancer()),

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

	//metrics := kprom.NewMetrics(
	//	"cortex_block_builder_kafka",
	//	kprom.Registerer(reg),
	//	kprom.FetchAndProduceDetail(kprom.ByNode, kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes),
	//)
	tracer := kotel.NewTracer(
		kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
	)
	opts = append(opts,
		kgo.WithLogger(newKafkaLogger(logger)),
		//kgo.WithHooks(metrics),
		kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()...),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}

	return client, nil
}

func waitKafka(ctx context.Context, kafkaClient *kgo.Client, logger log.Logger) error {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})
	for boff.Ongoing() {
		err := kafkaClient.Ping(ctx)
		if err == nil {
			return nil
		}
		level.Error(logger).Log(
			"msg", "ping kafka brokers",
			"err", err,
			"num_retries", boff.NumRetries(),
		)
		boff.Wait()
	}
	return boff.Err()
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
