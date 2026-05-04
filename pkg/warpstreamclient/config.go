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
	return nil
}
