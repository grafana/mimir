package wgo

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds all parameters for WarpstreamClient.
type Config struct {
	// Connection settings.
	Address      []string
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	TLSEnabled   bool
	TLSConfig    *tls.Config
	ClientID     string
	Topic        string

	// SASLOptions are pre-built kgo options for SASL authentication.
	SASLOptions []kgo.Opt

	// Producer settings.

	// Linger is how long the buffer waits to coalesce more records into a
	// produce before flushing it; zero flushes as soon as possible.
	Linger time.Duration

	// MaxBatchBytes caps the uncompressed wire size of a single per-partition
	// RecordBatch. It must be set at or below the WarpStream agents'
	// message.max.bytes: the client guarantees no per-partition batch exceeds
	// this value (splitting oversized partition groups across batches), so as
	// long as the cap is <= the agent limit the broker never rejects a flush
	// with MessageTooLarge.
	MaxBatchBytes int32

	// DirectProducer holds the per-attempt timing enforced at the kgo
	// boundary by KafkaDirectProducer.
	DirectProducer KafkaDirectProducerConfig

	// HealthCheck holds the shared "is this agent unhealthy?" thresholds
	// consumed by both the Hedger and the Demoter.
	HealthCheck HealthCheckConfig

	// Hedger holds the Hedger-specific timing knobs. Health thresholds live on
	// HealthCheck.
	Hedger HedgerConfig

	// Demoter holds the Demoter-specific knobs. Health thresholds live on
	// HealthCheck.
	Demoter DemoterConfig

	// ClusterStatsTTL is how long a cluster-wide stats snapshot is reused
	// before being recomputed. Per-agent stats are not cached.
	ClusterStatsTTL time.Duration

	// MetadataRefreshInterval is how often the AgentPool is refreshed in the
	// background. Each refresh updates the partition assignment strategy and
	// purges agent-stats entries for agents that have left the cluster.
	MetadataRefreshInterval time.Duration
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if len(c.Address) == 0 {
		return errors.New("at least one broker address must be configured")
	}
	if c.Topic == "" {
		return errors.New("topic must not be empty")
	}
	if c.DialTimeout < 0 {
		return errors.New("dial timeout must be non-negative")
	}
	if c.WriteTimeout <= 0 {
		return errors.New("write timeout must be positive")
	}
	if c.MaxBatchBytes <= 0 {
		return errors.New("max batch bytes must be positive")
	}
	if c.MaxBatchBytes > maxBatchBytesCeiling {
		return fmt.Errorf("max batch bytes must not exceed %d", maxBatchBytesCeiling)
	}
	if c.Linger < 0 {
		return errors.New("linger must be non-negative")
	}
	if c.TLSEnabled && c.TLSConfig == nil {
		return errors.New("TLS config must be set when TLS is enabled")
	}
	if err := c.HealthCheck.Validate(); err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	if c.Hedger.MinHedgeDelay < 0 {
		return errors.New("hedge min delay must be non-negative")
	}
	if c.Hedger.MaxHedgeAgents < 1 {
		return errors.New("hedge max agents must be >= 1")
	}
	if err := c.Demoter.Validate(); err != nil {
		return fmt.Errorf("demoter: %w", err)
	}
	if c.ClusterStatsTTL <= 0 {
		return errors.New("cluster stats TTL must be positive")
	}
	if c.MetadataRefreshInterval <= 0 {
		return errors.New("metadata refresh interval must be positive")
	}
	if err := c.DirectProducer.Validate(); err != nil {
		return fmt.Errorf("direct producer: %w", err)
	}
	// If WriteTimeout is below a single produce attempt's deadline, the flush ctx
	// cancels before the first attempt completes and hedging never runs.
	if attempt := c.DirectProducer.ProduceRequestTimeout + c.DirectProducer.ProduceRequestTimeoutOverhead; c.WriteTimeout < attempt {
		return fmt.Errorf("write timeout (%s) must be at least the produce request timeout plus overhead (%s)", c.WriteTimeout, attempt)
	}
	return nil
}
