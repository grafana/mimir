// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds all parameters for WarpstreamClient.
// It has no CLI flags; the ingest package constructs it from KafkaConfig.
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
	Linger           time.Duration
	MaxBatchBytes    int32
	MaxBufferedBytes int64

	// HedgeSlowMultiplier triggers hedging when an agent's window-average
	// latency exceeds the cluster baseline multiplied by this value. Must be >= 1.
	HedgeSlowMultiplier float64

	// HedgeMaxSlowFraction suppresses hedging when the fraction of slow agents
	// exceeds this value, indicating a cluster-wide issue rather than a single
	// slow agent.
	HedgeMaxSlowFraction float64

	// HedgeFaultyThreshold marks an agent as "faulty" when its observed error
	// rate over the window exceeds this value (in [0, 1]).
	HedgeFaultyThreshold float64

	// HedgeMaxFaultyFraction suppresses hedging when the fraction of faulty
	// agents exceeds this value, indicating a cluster-wide issue rather than a
	// single faulty agent.
	HedgeMaxFaultyFraction float64

	// HedgeMinDelay is the floor on the dynamically computed hedge delay (the
	// hedge fires after max(cluster baseline latency, HedgeMinDelay)).
	HedgeMinDelay time.Duration

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
	if c.MaxBufferedBytes <= 0 {
		return errors.New("max buffered bytes must be positive")
	}
	if c.Linger < 0 {
		return errors.New("linger must be non-negative")
	}
	if c.TLSEnabled && c.TLSConfig == nil {
		return errors.New("TLS config must be set when TLS is enabled")
	}
	if c.HedgeSlowMultiplier < 1 {
		return errors.New("hedge slow multiplier must be >= 1")
	}
	if c.HedgeMaxSlowFraction < 0 || c.HedgeMaxSlowFraction > 1 {
		return errors.New("hedge max slow fraction must be between 0 and 1")
	}
	if c.HedgeFaultyThreshold < 0 || c.HedgeFaultyThreshold > 1 {
		return errors.New("hedge faulty threshold must be between 0 and 1")
	}
	if c.HedgeMaxFaultyFraction < 0 || c.HedgeMaxFaultyFraction > 1 {
		return errors.New("hedge max faulty fraction must be between 0 and 1")
	}
	if c.HedgeMinDelay < 0 {
		return errors.New("hedge min delay must be non-negative")
	}
	if c.ClusterStatsTTL <= 0 {
		return errors.New("cluster stats TTL must be positive")
	}
	if c.MetadataRefreshInterval <= 0 {
		return errors.New("metadata refresh interval must be positive")
	}
	return nil
}
