// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
)

const (
	consumeFromLastOffset = "last-offset"
	consumeFromStart      = "start"
	consumeFromEnd        = "end"
	consumeFromTimestamp  = "timestamp"

	kafkaConfigFlagPrefix          = "ingest-storage.kafka"
	targetConsumerLagAtStartupFlag = kafkaConfigFlagPrefix + ".target-consumer-lag-at-startup"
	maxConsumerLagAtStartupFlag    = kafkaConfigFlagPrefix + ".max-consumer-lag-at-startup"
)

var (
	ErrMissingKafkaAddress               = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic                 = errors.New("the Kafka topic has not been configured")
	ErrInvalidWriteClients               = errors.New("the configured number of write clients is invalid (must be greater than 0)")
	ErrInvalidConsumePosition            = errors.New("the configured consume position is invalid")
	ErrInvalidProducerMaxRecordSizeBytes = fmt.Errorf("the configured producer max record size bytes must be a value between %d and %d", minProducerRecordDataBytesLimit, maxProducerRecordDataBytesLimit)
	ErrInconsistentConsumerLagAtStartup  = fmt.Errorf("the target and max consumer lag at startup must be either both set to 0 or to a value greater than 0")
	ErrInvalidMaxConsumerLagAtStartup    = fmt.Errorf("the configured max consumer lag at startup must greater or equal than the configured target consumer lag")
	ErrInconsistentSASLCredentials       = fmt.Errorf("the SASL username and password must be both configured to enable SASL authentication")
	ErrInvalidIngestionConcurrencyMax    = errors.New("ingest-storage.kafka.ingestion-concurrency-max must either be set to 0 or to a value greater than 0")
	ErrInvalidIngestionConcurrencyParams = errors.New("ingest-storage.kafka.ingestion-concurrency-queue-capacity, ingest-storage.kafka.ingestion-concurrency-estimated-bytes-per-sample, ingest-storage.kafka.ingestion-concurrency-batch-size and ingest-storage.kafka.ingestion-concurrency-target-flushes-per-shard must be greater than 0")

	consumeFromPositionOptions = []string{consumeFromLastOffset, consumeFromStart, consumeFromEnd, consumeFromTimestamp}

	defaultFetchBackoffConfig = backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
		MaxRetries: 0, // Retry forever. Do NOT change!
	}
)

type Config struct {
	Enabled     bool            `yaml:"enabled"`
	KafkaConfig KafkaConfig     `yaml:"kafka"`
	Migration   MigrationConfig `yaml:"migration"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	cfg.KafkaConfig.RegisterFlagsWithPrefix(kafkaConfigFlagPrefix, f)
	cfg.Migration.RegisterFlagsWithPrefix("ingest-storage.migration", f)
}

// Validate the config.
func (cfg *Config) Validate() error {
	// Skip validation if disabled.
	if !cfg.Enabled {
		return nil
	}

	if err := cfg.KafkaConfig.Validate(); err != nil {
		return err
	}

	return nil
}

// KafkaConfig holds the generic config for the Kafka backend.
type KafkaConfig struct {
	Address      string        `yaml:"address"`
	Topic        string        `yaml:"topic"`
	ClientID     string        `yaml:"client_id"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	WriteClients int           `yaml:"write_clients"`

	SASLUsername string         `yaml:"sasl_username"`
	SASLPassword flagext.Secret `yaml:"sasl_password"`

	ConsumerGroup                     string        `yaml:"consumer_group"`
	ConsumerGroupOffsetCommitInterval time.Duration `yaml:"consumer_group_offset_commit_interval"`

	LastProducedOffsetPollInterval time.Duration `yaml:"last_produced_offset_poll_interval"`
	LastProducedOffsetRetryTimeout time.Duration `yaml:"last_produced_offset_retry_timeout"`

	ConsumeFromPositionAtStartup  string        `yaml:"consume_from_position_at_startup"`
	ConsumeFromTimestampAtStartup int64         `yaml:"consume_from_timestamp_at_startup"`
	TargetConsumerLagAtStartup    time.Duration `yaml:"target_consumer_lag_at_startup"`
	MaxConsumerLagAtStartup       time.Duration `yaml:"max_consumer_lag_at_startup"`

	AutoCreateTopicEnabled           bool `yaml:"auto_create_topic_enabled"`
	AutoCreateTopicDefaultPartitions int  `yaml:"auto_create_topic_default_partitions"`

	ProducerMaxRecordSizeBytes int   `yaml:"producer_max_record_size_bytes"`
	ProducerMaxBufferedBytes   int64 `yaml:"producer_max_buffered_bytes"`

	WaitStrongReadConsistencyTimeout time.Duration `yaml:"wait_strong_read_consistency_timeout"`

	// Used when logging unsampled client errors. Set from ingester's ErrorSampleRate.
	FallbackClientErrorSampleRate int64 `yaml:"-"`

	StartupFetchConcurrency           int  `yaml:"startup_fetch_concurrency"`
	OngoingFetchConcurrency           int  `yaml:"ongoing_fetch_concurrency"`
	UseCompressedBytesAsFetchMaxBytes bool `yaml:"use_compressed_bytes_as_fetch_max_bytes"`
	MaxBufferedBytes                  int  `yaml:"max_buffered_bytes"`

	IngestionConcurrencyMax       int `yaml:"ingestion_concurrency_max"`
	IngestionConcurrencyBatchSize int `yaml:"ingestion_concurrency_batch_size"`

	// IngestionConcurrencyQueueCapacity controls how many batches can be enqueued for flushing series to the TSDB HEAD.
	// We don't want to push any batches in parallel and instead want to prepare the next ones while the current one finishes, hence the buffer of 5.
	// For example, if we flush 1 batch/sec, then batching 2 batches/sec doesn't make us faster.
	// This is our initial assumption, and there's potential in testing with higher numbers if there's a high variability in flush times - assuming we can preserve the order of the batches. For now, we'll stick to 5.
	// If there's high variability in the time to flush or in the time to batch, then this buffer might need to be increased.
	IngestionConcurrencyQueueCapacity int `yaml:"ingestion_concurrency_queue_capacity"`

	// IngestionConcurrencyTargetFlushesPerShard is the number of flushes we want to target per shard.
	// There is some overhead in the parallelization. With fewer flushes, the overhead of splitting up the work is higher than the benefit of parallelization.
	// the default of 80 was devised experimentally to keep the memory and CPU usage low ongoing consumption, while keeping replay speed high during cold replay.
	IngestionConcurrencyTargetFlushesPerShard int `yaml:"ingestion_concurrency_target_flushes_per_shard"`

	// IngestionConcurrencyEstimatedBytesPerSample is the estimated number of bytes per sample.
	// Our data indicates that the average sample size is somewhere between ~250 and ~500 bytes. We'll use 500 bytes as a conservative estimate.
	IngestionConcurrencyEstimatedBytesPerSample int `yaml:"ingestion_concurrency_estimated_bytes_per_sample"`

	// The fetch backoff config to use in the concurrent fetchers (when enabled). This setting
	// is just used to change the default backoff in tests.
	concurrentFetchersFetchBackoffConfig backoff.Config `yaml:"-"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.concurrentFetchersFetchBackoffConfig = defaultFetchBackoffConfig

	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")
	f.IntVar(&cfg.WriteClients, prefix+".write-clients", 1, "The number of Kafka clients used by producers. When the configured number of clients is greater than 1, partitions are sharded among Kafka clients. A higher number of clients may provide higher write throughput at the cost of additional Metadata requests pressure to Kafka.")

	f.StringVar(&cfg.SASLUsername, prefix+".sasl-username", "", "The username used to authenticate to Kafka using the SASL plain mechanism. To enable SASL, configure both the username and password.")
	f.Var(&cfg.SASLPassword, prefix+".sasl-password", "The password used to authenticate to Kafka using the SASL plain mechanism. To enable SASL, configure both the username and password.")

	f.StringVar(&cfg.ConsumerGroup, prefix+".consumer-group", "", "The consumer group used by the consumer to track the last consumed offset. The consumer group must be different for each ingester. If the configured consumer group contains the '<partition>' placeholder, it is replaced with the actual partition ID owned by the ingester. When empty (recommended), Mimir uses the ingester instance ID to guarantee uniqueness.")
	f.DurationVar(&cfg.ConsumerGroupOffsetCommitInterval, prefix+".consumer-group-offset-commit-interval", time.Second, "How frequently a consumer should commit the consumed offset to Kafka. The last committed offset is used at startup to continue the consumption from where it was left.")

	f.DurationVar(&cfg.LastProducedOffsetPollInterval, prefix+".last-produced-offset-poll-interval", time.Second, "How frequently to poll the last produced offset, used to enforce strong read consistency.")
	f.DurationVar(&cfg.LastProducedOffsetRetryTimeout, prefix+".last-produced-offset-retry-timeout", 10*time.Second, "How long to retry a failed request to get the last produced offset.")

	f.StringVar(&cfg.ConsumeFromPositionAtStartup, prefix+".consume-from-position-at-startup", consumeFromLastOffset, fmt.Sprintf("From which position to start consuming the partition at startup. Supported options: %s.", strings.Join(consumeFromPositionOptions, ", ")))
	f.Int64Var(&cfg.ConsumeFromTimestampAtStartup, prefix+".consume-from-timestamp-at-startup", 0, fmt.Sprintf("Milliseconds timestamp after which the consumption of the partition starts at startup. Only applies when consume-from-position-at-startup is %s", consumeFromTimestamp))

	howToDisableConsumerLagAtStartup := fmt.Sprintf("Set both -%s and -%s to 0 to disable waiting for maximum consumer lag being honored at startup.", targetConsumerLagAtStartupFlag, maxConsumerLagAtStartupFlag)
	f.DurationVar(&cfg.TargetConsumerLagAtStartup, targetConsumerLagAtStartupFlag, 2*time.Second, "The best-effort maximum lag a consumer tries to achieve at startup. "+howToDisableConsumerLagAtStartup)
	f.DurationVar(&cfg.MaxConsumerLagAtStartup, maxConsumerLagAtStartupFlag, 15*time.Second, "The guaranteed maximum lag before a consumer is considered to have caught up reading from a partition at startup, becomes ACTIVE in the hash ring and passes the readiness check. "+howToDisableConsumerLagAtStartup)

	f.BoolVar(&cfg.AutoCreateTopicEnabled, prefix+".auto-create-topic-enabled", true, "Enable auto-creation of Kafka topic if it doesn't exist.")
	f.IntVar(&cfg.AutoCreateTopicDefaultPartitions, prefix+".auto-create-topic-default-partitions", 0, "When auto-creation of Kafka topic is enabled and this value is positive, Kafka's num.partitions configuration option is set on Kafka brokers with this value when Mimir component that uses Kafka starts. This configuration option specifies the default number of partitions that the Kafka broker uses for auto-created topics. Note that this is a Kafka-cluster wide setting, and applies to any auto-created topic. If the setting of num.partitions fails, Mimir proceeds anyways, but auto-created topics could have an incorrect number of partitions.")

	f.IntVar(&cfg.ProducerMaxRecordSizeBytes, prefix+".producer-max-record-size-bytes", maxProducerRecordDataBytesLimit, "The maximum size of a Kafka record data that should be generated by the producer. An incoming write request larger than this size is split into multiple Kafka records. We strongly recommend to not change this setting unless for testing purposes.")
	f.Int64Var(&cfg.ProducerMaxBufferedBytes, prefix+".producer-max-buffered-bytes", 1024*1024*1024, "The maximum size of (uncompressed) buffered and unacknowledged produced records sent to Kafka. The produce request fails once this limit is reached. This limit is per Kafka client. 0 to disable the limit.")

	f.DurationVar(&cfg.WaitStrongReadConsistencyTimeout, prefix+".wait-strong-read-consistency-timeout", 20*time.Second, "The maximum allowed for a read requests processed by an ingester to wait until strong read consistency is enforced. 0 to disable the timeout.")

	f.IntVar(&cfg.StartupFetchConcurrency, prefix+".startup-fetch-concurrency", 0, "The number of concurrent fetch requests that the ingester makes when reading data from Kafka during startup. 0 to disable.")
	f.IntVar(&cfg.OngoingFetchConcurrency, prefix+".ongoing-fetch-concurrency", 0, "The number of concurrent fetch requests that the ingester makes when reading data continuously from Kafka after startup. Is disabled unless "+prefix+".startup-fetch-concurrency is greater than 0. 0 to disable.")
	f.BoolVar(&cfg.UseCompressedBytesAsFetchMaxBytes, prefix+".use-compressed-bytes-as-fetch-max-bytes", true, "When enabled, the fetch request MaxBytes field is computed using the compressed size of previous records. When disabled, MaxBytes is computed using uncompressed bytes. Different Kafka implementations interpret MaxBytes differently.")
	f.IntVar(&cfg.MaxBufferedBytes, prefix+".max-buffered-bytes", 100_000_000, "The maximum number of buffered records ready to be processed. This limit applies to the sum of all inflight requests. Set to 0 to disable the limit.")

	f.IntVar(&cfg.IngestionConcurrencyMax, prefix+".ingestion-concurrency-max", 0, "The maximum number of concurrent ingestion streams to the TSDB head. Every tenant has their own set of streams. 0 to disable.")
	f.IntVar(&cfg.IngestionConcurrencyBatchSize, prefix+".ingestion-concurrency-batch-size", 150, "The number of timeseries to batch together before ingesting to the TSDB head. Only use this setting when -ingest-storage.kafka.ingestion-concurrency-max is greater than 0.")
	f.IntVar(&cfg.IngestionConcurrencyQueueCapacity, prefix+".ingestion-concurrency-queue-capacity", 5, "The number of batches to prepare and queue to ingest to the TSDB head. Only use this setting when -ingest-storage.kafka.ingestion-concurrency-max is greater than 0.")
	f.IntVar(&cfg.IngestionConcurrencyTargetFlushesPerShard, prefix+".ingestion-concurrency-target-flushes-per-shard", 80, "The expected number of times to ingest timeseries to the TSDB head after batching. With fewer flushes, the overhead of splitting up the work is higher than the benefit of parallelization. Only use this setting when -ingest-storage.kafka.ingestion-concurrency-max is greater than 0.")
	f.IntVar(&cfg.IngestionConcurrencyEstimatedBytesPerSample, prefix+".ingestion-concurrency-estimated-bytes-per-sample", 500, "The estimated number of bytes a sample has at time of ingestion. This value is used to estimate the timeseries without decompressing them. Only use this setting when -ingest-storage.kafka.ingestion-concurrency-max is greater than 0.")
}

func (cfg *KafkaConfig) Validate() error {
	if cfg.Address == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.Topic == "" {
		return ErrMissingKafkaTopic
	}
	if cfg.WriteClients < 1 {
		return ErrInvalidWriteClients
	}
	if !slices.Contains(consumeFromPositionOptions, cfg.ConsumeFromPositionAtStartup) {
		return ErrInvalidConsumePosition
	}
	if cfg.ConsumeFromPositionAtStartup == consumeFromTimestamp {
		// We only do a simple soundness check for the value be a millisecond precision timestamp.
		if cfg.ConsumeFromTimestampAtStartup < 1e12 {
			return fmt.Errorf("%w: configured timestamp must be a millisecond timestamp", ErrInvalidConsumePosition)
		}
	} else {
		if cfg.ConsumeFromTimestampAtStartup > 0 {
			return fmt.Errorf("%w: configured consume position must be set to %q", ErrInvalidConsumePosition, consumeFromTimestamp)
		}
	}
	if cfg.ProducerMaxRecordSizeBytes < minProducerRecordDataBytesLimit || cfg.ProducerMaxRecordSizeBytes > maxProducerRecordDataBytesLimit {
		return ErrInvalidProducerMaxRecordSizeBytes
	}
	if (cfg.TargetConsumerLagAtStartup != 0) != (cfg.MaxConsumerLagAtStartup != 0) {
		return ErrInconsistentConsumerLagAtStartup
	}
	if cfg.MaxConsumerLagAtStartup < cfg.TargetConsumerLagAtStartup {
		return ErrInvalidMaxConsumerLagAtStartup
	}

	if cfg.StartupFetchConcurrency < 0 {
		return fmt.Errorf("ingest-storage.kafka.startup-fetch-concurrency must be greater or equal to 0")
	}

	if cfg.OngoingFetchConcurrency > 0 && cfg.StartupFetchConcurrency <= 0 {
		return fmt.Errorf("ingest-storage.kafka.startup-fetch-concurrency must be greater than 0 when ingest-storage.kafka.ongoing-fetch-concurrency is greater than 0")
	}

	if cfg.MaxBufferedBytes >= math.MaxInt32 {
		return fmt.Errorf("ingest-storage.kafka.max-buffered-bytes must be less than %d", math.MaxInt32)
	}

	if (cfg.SASLUsername == "") != (cfg.SASLPassword.String() == "") {
		return ErrInconsistentSASLCredentials
	}

	if cfg.IngestionConcurrencyMax < 0 {
		return ErrInvalidIngestionConcurrencyMax
	}

	if cfg.IngestionConcurrencyMax >= 1 {
		if cfg.IngestionConcurrencyBatchSize <= 0 || cfg.IngestionConcurrencyQueueCapacity <= 0 || cfg.IngestionConcurrencyEstimatedBytesPerSample <= 0 || cfg.IngestionConcurrencyTargetFlushesPerShard <= 0 {
			return ErrInvalidIngestionConcurrencyParams
		}
	}

	return nil
}

// GetConsumerGroup returns the consumer group to use for the given instanceID and partitionID.
func (cfg *KafkaConfig) GetConsumerGroup(instanceID string, partitionID int32) string {
	if cfg.ConsumerGroup == "" {
		return instanceID
	}

	return strings.ReplaceAll(cfg.ConsumerGroup, "<partition>", strconv.Itoa(int(partitionID)))
}

// MigrationConfig holds the configuration used to migrate Mimir to ingest storage. This config shouldn't be
// set for any other reason.
type MigrationConfig struct {
	DistributorSendToIngestersEnabled bool `yaml:"distributor_send_to_ingesters_enabled"`
}

func (cfg *MigrationConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *MigrationConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.DistributorSendToIngestersEnabled, prefix+".distributor-send-to-ingesters-enabled", false, "When both this option and ingest storage are enabled, distributors write to both Kafka and ingesters. A write request is considered successful only when written to both backends.")
}
