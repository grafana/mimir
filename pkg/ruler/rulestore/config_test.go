// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulestore

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
)

func TestIsDefaults(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config)
		expected bool
	}{
		"should return true if the config only contains default values": {
			setup: func(cfg *Config) {
				flagext.DefaultValues(cfg)
			},
			expected: true,
		},
		"should return false if the config contains zero values": {
			setup:    func(*Config) {},
			expected: false,
		},
		"should return false if the config contains default values and some overrides": {
			setup: func(cfg *Config) {
				flagext.DefaultValues(cfg)
				cfg.Backend = "local"
			},
			expected: false,
		},
		"should return true if only a non-config field has changed": {
			setup: func(cfg *Config) {
				flagext.DefaultValues(cfg)
				cfg.Middlewares = append(cfg.Middlewares, nil)
			},
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			testData.setup(&cfg)

			assert.Equal(t, testData.expected, cfg.IsDefaults())
		})
	}
}
