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

	// BatchMaxBytes caps the uncompressed wire size of a single per-partition
	// RecordBatch. It must be set at or below the WarpStream agents'
	// message.max.bytes: the client guarantees no per-partition batch exceeds
	// this value (splitting oversized partition groups across batches), so as
	// long as the cap is <= the agent limit the broker never rejects a flush
	// with MessageTooLarge.
	BatchMaxBytes int32

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

// Default values applied by DefaultConfig and the functional options. They are
// exported so callers (e.g. a CLI) can reuse them as their own flag defaults.
const (
	DefaultDialTimeout                         = 500 * time.Millisecond
	DefaultWriteTimeout                        = 4 * time.Second
	DefaultLinger                              = 50 * time.Millisecond
	DefaultBatchMaxBytes                 int32 = 16_000_000
	DefaultProduceRequestTimeoutOverhead       = 2 * time.Second
	DefaultProduceRequestTimeout               = DefaultWriteTimeout - DefaultProduceRequestTimeoutOverhead
	DefaultHealthCheckSlowMultiplier           = 2.0
	DefaultHealthCheckMaxSlowFraction          = 0.3
	DefaultHealthCheckFaultyThreshold          = 0.2
	DefaultHealthCheckMaxFaultyFraction        = 0.3
	DefaultHedgerMinHedgeDelay                 = 500 * time.Millisecond
	DefaultHedgerMaxHedgeAgents                = 3
	DefaultDemoterProbeInterval                = time.Second
	DefaultClusterStatsTTL                     = time.Second
	DefaultMetadataRefreshInterval             = 10 * time.Second
)

// DefaultConfig returns a Config populated with the default values. Address and
// Topic have no default: they must be set (via options or directly) before the
// config passes Validate.
func DefaultConfig() Config {
	return Config{
		DialTimeout:             DefaultDialTimeout,
		WriteTimeout:            DefaultWriteTimeout,
		Linger:                  DefaultLinger,
		BatchMaxBytes:           DefaultBatchMaxBytes,
		ClusterStatsTTL:         DefaultClusterStatsTTL,
		MetadataRefreshInterval: DefaultMetadataRefreshInterval,
		DirectProducer: KafkaDirectProducerConfig{
			ProduceRequestTimeout:         DefaultProduceRequestTimeout,
			ProduceRequestTimeoutOverhead: DefaultProduceRequestTimeoutOverhead,
		},
		HealthCheck: HealthCheckConfig{
			SlowMultiplier:    DefaultHealthCheckSlowMultiplier,
			MaxSlowFraction:   DefaultHealthCheckMaxSlowFraction,
			FaultyThreshold:   DefaultHealthCheckFaultyThreshold,
			MaxFaultyFraction: DefaultHealthCheckMaxFaultyFraction,
		},
		Hedger: HedgerConfig{
			MinHedgeDelay:  DefaultHedgerMinHedgeDelay,
			MaxHedgeAgents: DefaultHedgerMaxHedgeAgents,
		},
		Demoter: DemoterConfig{
			ProbeInterval: DefaultDemoterProbeInterval,
		},
	}
}

// NewConfig returns DefaultConfig with opts applied. It does not validate the
// result.
func NewConfig(opts ...Opt) Config {
	cfg := DefaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	return cfg
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
	if c.BatchMaxBytes <= 0 {
		return errors.New("batch max bytes must be positive")
	}
	if c.BatchMaxBytes > batchMaxBytesCeiling {
		return fmt.Errorf("batch max bytes must not exceed %d", batchMaxBytesCeiling)
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

// Opt configures a WarpstreamClient. Pass options to NewWarpstreamClient; each
// overrides one field of the default Config.
type Opt interface {
	apply(*Config)
}

type opt struct{ fn func(*Config) }

func (o opt) apply(c *Config) { o.fn(c) }

// WithAddress sets the seed broker addresses to connect to. Required.
func WithAddress(addrs ...string) Opt {
	return opt{func(c *Config) { c.Address = addrs }}
}

// WithTopic sets the topic records are produced to. Required.
func WithTopic(topic string) Opt {
	return opt{func(c *Config) { c.Topic = topic }}
}

// WithClientID sets the Kafka client ID.
func WithClientID(id string) Opt {
	return opt{func(c *Config) { c.ClientID = id }}
}

// WithDialTimeout sets the timeout for establishing a connection to a broker.
func WithDialTimeout(d time.Duration) Opt {
	return opt{func(c *Config) { c.DialTimeout = d }}
}

// WithWriteTimeout sets the deadline bounding a full produce flush, including
// the whole hedge cascade.
func WithWriteTimeout(d time.Duration) Opt {
	return opt{func(c *Config) { c.WriteTimeout = d }}
}

// WithTLSConfig enables TLS and dials brokers with tlsCfg.
func WithTLSConfig(tlsCfg *tls.Config) Opt {
	return opt{func(c *Config) {
		c.TLSEnabled = true
		c.TLSConfig = tlsCfg
	}}
}

// WithSASL sets pre-built kgo options for SASL authentication.
func WithSASL(opts ...kgo.Opt) Opt {
	return opt{func(c *Config) { c.SASLOptions = opts }}
}

// WithLinger sets how long the buffer waits to coalesce more records into a
// produce before flushing; zero flushes as soon as possible.
func WithLinger(d time.Duration) Opt {
	return opt{func(c *Config) { c.Linger = d }}
}

// WithBatchMaxBytes caps the uncompressed wire size of a single per-partition
// RecordBatch. Set it at or below the agents' message.max.bytes.
func WithBatchMaxBytes(n int32) Opt {
	return opt{func(c *Config) { c.BatchMaxBytes = n }}
}

// WithProduceRequestTimeout sets the agent-side deadline written into each
// ProduceRequest.
func WithProduceRequestTimeout(d time.Duration) Opt {
	return opt{func(c *Config) { c.DirectProducer.ProduceRequestTimeout = d }}
}

// WithProduceRequestTimeoutOverhead sets the slack added on top of the produce
// request timeout to compute the client-side deadline.
func WithProduceRequestTimeoutOverhead(d time.Duration) Opt {
	return opt{func(c *Config) { c.DirectProducer.ProduceRequestTimeoutOverhead = d }}
}

// WithHealthCheckSlowMultiplier sets the factor over the cluster baseline
// latency above which an agent is marked slow. Must be >= 1.
func WithHealthCheckSlowMultiplier(v float64) Opt {
	return opt{func(c *Config) { c.HealthCheck.SlowMultiplier = v }}
}

// WithHealthCheckMaxSlowFraction sets the fraction of slow agents at or above
// which slow-based action is suppressed.
func WithHealthCheckMaxSlowFraction(v float64) Opt {
	return opt{func(c *Config) { c.HealthCheck.MaxSlowFraction = v }}
}

// WithHealthCheckFaultyThreshold sets the window error rate above which an agent
// is marked faulty.
func WithHealthCheckFaultyThreshold(v float64) Opt {
	return opt{func(c *Config) { c.HealthCheck.FaultyThreshold = v }}
}

// WithHealthCheckMaxFaultyFraction sets the fraction of faulty agents at or
// above which faulty-based action is suppressed.
func WithHealthCheckMaxFaultyFraction(v float64) Opt {
	return opt{func(c *Config) { c.HealthCheck.MaxFaultyFraction = v }}
}

// WithHedgerMinHedgeDelay sets the floor on the dynamically-computed hedge delay.
func WithHedgerMinHedgeDelay(d time.Duration) Opt {
	return opt{func(c *Config) { c.Hedger.MinHedgeDelay = d }}
}

// WithHedgerMaxHedgeAgents sets the maximum distinct agents tried per partition per
// produce, counting the primary.
func WithHedgerMaxHedgeAgents(n int) Opt {
	return opt{func(c *Config) { c.Hedger.MaxHedgeAgents = n }}
}

// WithDemoterProbeInterval sets the minimum wall-clock gap between probes to the
// same demoted agent.
func WithDemoterProbeInterval(d time.Duration) Opt {
	return opt{func(c *Config) { c.Demoter.ProbeInterval = d }}
}

// WithClusterStatsTTL sets how long a cluster-wide stats snapshot is reused
// before being recomputed.
func WithClusterStatsTTL(d time.Duration) Opt {
	return opt{func(c *Config) { c.ClusterStatsTTL = d }}
}

// WithMetadataRefreshInterval sets how often the agent pool is refreshed in the
// background.
func WithMetadataRefreshInterval(d time.Duration) Opt {
	return opt{func(c *Config) { c.MetadataRefreshInterval = d }}
}
