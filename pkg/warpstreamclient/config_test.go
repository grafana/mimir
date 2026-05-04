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
		Address:              []string{"localhost:9092"},
		Topic:                "mimir-ingest",
		DialTimeout:          2 * time.Second,
		WriteTimeout:         10 * time.Second,
		MaxBatchBytes:        16_000_000,
		MaxBufferedBytes:     1 << 30,
		HedgeSlowMultiplier:  2.0,
		HedgeMaxSlowFraction: 0.3,
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
		"zero max buffered bytes": {
			mutate:     func(c *Config) { c.MaxBufferedBytes = 0 },
			wantErrMsg: "max buffered bytes must be positive",
		},
		"negative linger": {
			mutate:     func(c *Config) { c.Linger = -1 },
			wantErrMsg: "linger must be non-negative",
		},
		"TLS enabled without TLS config": {
			mutate:     func(c *Config) { c.TLSEnabled = true; c.TLSConfig = nil },
			wantErrMsg: "TLS config must be set when TLS is enabled",
		},
		"hedge slow multiplier below 1": {
			mutate:     func(c *Config) { c.HedgeSlowMultiplier = 0.5 },
			wantErrMsg: "hedge slow multiplier must be >= 1",
		},
		"zero hedge slow multiplier": {
			mutate:     func(c *Config) { c.HedgeSlowMultiplier = 0 },
			wantErrMsg: "hedge slow multiplier must be >= 1",
		},
		"hedge max slow fraction below 0": {
			mutate:     func(c *Config) { c.HedgeMaxSlowFraction = -0.1 },
			wantErrMsg: "hedge max slow fraction must be between 0 and 1",
		},
		"hedge max slow fraction above 1": {
			mutate:     func(c *Config) { c.HedgeMaxSlowFraction = 1.1 },
			wantErrMsg: "hedge max slow fraction must be between 0 and 1",
		},
		"zero dial timeout is valid": {
			mutate: func(c *Config) { c.DialTimeout = 0 },
		},
		"zero linger is valid": {
			mutate: func(c *Config) { c.Linger = 0 },
		},
		"TLS enabled with TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = true; c.TLSConfig = &tls.Config{} },
		},
		"TLS disabled without TLS config is valid": {
			mutate: func(c *Config) { c.TLSEnabled = false; c.TLSConfig = nil },
		},
		"hedge max slow fraction of exactly 1 is valid": {
			mutate: func(c *Config) { c.HedgeMaxSlowFraction = 1.0 },
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
