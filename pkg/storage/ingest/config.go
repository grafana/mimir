// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/dskit/grpcclient"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	DNSBalancingStrategyJumpHash = "jump-hash"
	DNSBalancingStrategyMod      = "mod"
)

var (
	DNSBalancingStrategies = []string{DNSBalancingStrategyJumpHash, DNSBalancingStrategyMod}
)

type Config struct {
	Enabled                        bool          `yaml:"enabled"`
	KafkaConfig                    KafkaConfig   `yaml:"kafka"` // TODO remove
	BufferSize                     int           `yaml:"buffer_size"`
	LastProducedOffsetPollInterval time.Duration `yaml:"last_produced_offset_poll_interval"`

	PostgresConfig         PostgresqlConfig       `yaml:"postgresql"`
	WriteAgent             WriteAgentConfig       `yaml:"write_agent"`
	GarbageCollectorConfig GarbageCollectorConfig `yaml:"garbage_collector"`
	Bucket                 bucket.Config          `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	cfg.KafkaConfig.RegisterFlagsWithPrefix("ingest-storage.kafka", f)
	f.IntVar(&cfg.BufferSize, "ingest-storage.buffer-size", 10, "The segment reader's buffer size")
	f.DurationVar(&cfg.LastProducedOffsetPollInterval, "ingest-storage.last-produced-offset-poll-interval", time.Second, "How frequently to poll the last produced offset, used to enforce strong read consistency.")

	cfg.Bucket.RegisterFlagsWithPrefixAndDefaultDirectory("ingest-storage.", "ingest", f)
	cfg.PostgresConfig.RegisterFlagsWithPrefix("ingest-storage.postgresql", f)
	cfg.WriteAgent.RegisterFlagsWithPrefix("ingest-storage.write-agent", f)
	cfg.GarbageCollectorConfig.RegisterFlagsWithPrefix("ingest-storage.garbage-collector", f)
}

// Validate the config.
func (cfg *Config) Validate() error {
	// Skip validation if disabled.
	if !cfg.Enabled {
		return nil
	}

	if err := cfg.Bucket.Validate(); err != nil {
		return err
	}

	return nil
}

// KafkaConfig holds the generic config for the Kafka backend.
// TODO remove
type KafkaConfig struct {
	Address      string        `yaml:"address"`
	Topic        string        `yaml:"topic"`
	ClientID     string        `yaml:"client_id"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`

	LastProducedOffsetPollInterval time.Duration `yaml:"last_produced_offset_poll_interval"`
	LastProducedOffsetRetryTimeout time.Duration `yaml:"last_produced_offset_retry_timeout"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")

	f.DurationVar(&cfg.LastProducedOffsetPollInterval, prefix+".last-produced-offset-poll-interval", time.Second, "How frequently to poll the last produced offset, used to enforce strong read consistency.")
	f.DurationVar(&cfg.LastProducedOffsetRetryTimeout, prefix+".last-produced-offset-retry-timeout", 10*time.Second, "How long to retry a failed request to get the last produced offset.")
}

func (cfg *KafkaConfig) Validate() error {
	return nil
}

type PostgresqlConfig struct {
	Address string `yaml:"address"`
}

func (cfg *PostgresqlConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *PostgresqlConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The PostgreSQL backend address.")
}

type WriteAgentConfig struct {
	Address                    string            `yaml:"address"`
	DNSLookupPeriod            time.Duration     `yaml:"dns_lookup_duration" category:"advanced"`
	DNSBalancingStrategy       string            `yaml:"dns_balancing_strategy"`
	WriteAgentGRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the distributor and the write-agent."`
}

func (cfg *WriteAgentConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *WriteAgentConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The write-agent address.")
	f.DurationVar(&cfg.DNSLookupPeriod, prefix+".dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")
	f.StringVar(&cfg.DNSBalancingStrategy, prefix+".dns-balancing-strategy", DNSBalancingStrategyMod, fmt.Sprintf("The balancing strategy to use. Values: %s.", strings.Join(DNSBalancingStrategies, ", ")))
	cfg.WriteAgentGRPCClientConfig.RegisterFlagsWithPrefix(prefix+".write-agent.grpc-client-config", f)
}

type GarbageCollectorConfig struct {
	CleanupInterval   time.Duration `yaml:"cleanup_interval"`
	RetentionPeriod   time.Duration `yaml:"retention_period"`
	DeleteConcurrency int           `yaml:"delete_concurrency"`
}

func (cfg *GarbageCollectorConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *GarbageCollectorConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.CleanupInterval, prefix+".cleanup-interval", time.Minute, "How frequently to check for segments to delete.")
	f.DurationVar(&cfg.RetentionPeriod, prefix+".retention-period", time.Hour, "The segments retention period.")
	f.IntVar(&cfg.DeleteConcurrency, prefix+".delete-concurrency", 10, "How many concurrent delete operations to run.")
}
