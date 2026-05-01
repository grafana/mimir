// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	validBase := Config{
		Address:       []string{"localhost:9092"},
		Topic:         "mimir-ingest",
		DialTimeout:   2 * time.Second,
		WriteTimeout:  10 * time.Second,
		MaxBatchBytes: 16_000_000,
		HealthCheck: HealthCheckConfig{
			SlowMultiplier:    2.0,
			MaxSlowFraction:   0.3,
			FaultyThreshold:   0.05,
			MaxFaultyFraction: 0.3,
		},
		Hedger: HedgerConfig{
			MinHedgeDelay:  10 * time.Millisecond,
			MaxHedgeAgents: 3,
		},
		Demoter: DemoterConfig{
			ProbeInterval: time.Second,
		},
		ClusterStatsTTL:         time.Second,
		MetadataRefreshInterval: 10 * time.Second,
		DirectProducer: KafkaDirectProducerConfig{
			ProduceRequestTimeout:         2 * time.Second,
			ProduceRequestTimeoutOverhead: time.Second,
		},
	}

	tests := map[string]struct {
		mutate     func(*Config)
		wantErrMsg string
	}{
		"valid config": {
			mutate: func(_ *Config) {},
		},
		"empty address list": {
			mutate:     func(c *Config) { c.Address = nil },
			wantErrMsg: "at least one broker address must be configured",
		},
		"empty topic": {
			mutate:     func(c *Config) { c.Topic = "" },
			wantErrMsg: "topic must not be empty",
		},
		"negative dial timeout": {
			mutate:     func(c *Config) { c.DialTimeout = -1 },
			wantErrMsg: "dial timeout must be non-negative",
		},
		"zero write timeout": {
			mutate:     func(c *Config) { c.WriteTimeout = 0 },
			wantErrMsg: "write timeout must be positive",
		},
		"negative write timeout": {
			mutate:     func(c *Config) { c.WriteTimeout = -1 },
			wantErrMsg: "write timeout must be positive",
		},
		"zero max batch bytes": {
			mutate:     func(c *Config) { c.MaxBatchBytes = 0 },
			wantErrMsg: "max batch bytes must be positive",
		},
		"negative max batch bytes": {
			mutate:     func(c *Config) { c.MaxBatchBytes = -1 },
			wantErrMsg: "max batch bytes must be positive",
		},
		"negative linger": {
			mutate:     func(c *Config) { c.Linger = -1 },
			wantErrMsg: "linger must be non-negative",
		},
		"TLS enabled without TLS config": {
			mutate:     func(c *Config) { c.TLSEnabled = true; c.TLSConfig = nil },
			wantErrMsg: "TLS config must be set when TLS is enabled",
		},
		"health check slow multiplier below 1": {
			mutate:     func(c *Config) { c.HealthCheck.SlowMultiplier = 0.5 },
			wantErrMsg: "health check: health check slow multiplier must be >= 1",
		},
		"zero health check slow multiplier": {
			mutate:     func(c *Config) { c.HealthCheck.SlowMultiplier = 0 },
			wantErrMsg: "health check: health check slow multiplier must be >= 1",
		},
		"health check max slow fraction below 0": {
			mutate:     func(c *Config) { c.HealthCheck.MaxSlowFraction = -0.1 },
			wantErrMsg: "health check: health check max slow fraction must be between 0 and 1",
		},
		"health check max slow fraction above 1": {
			mutate:     func(c *Config) { c.HealthCheck.MaxSlowFraction = 1.1 },
			wantErrMsg: "health check: health check max slow fraction must be between 0 and 1",
		},
		"zero dial timeout is valid": {
			mutate: func(c *Config) { c.DialTimeout = 0 },
		},
		"zero linger is valid": {
			mutate: func(c *Config) { c.Linger = 0 },
		},
		"TLS enabled with TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = true; c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12} },
		},
		"TLS disabled without TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = false; c.TLSConfig = nil },
		},
		"health check max slow fraction of exactly 1 is valid": {
			mutate: func(c *Config) { c.HealthCheck.MaxSlowFraction = 1.0 },
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := validBase
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErrMsg != "" {
				require.EqualError(t, err, tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
