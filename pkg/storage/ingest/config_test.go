// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup       func(*Config)
		expectedErr error
	}{
		"should pass with the default config": {
			setup: func(_ *Config) {},
		},
		"should fail if ingest storage is enabled and Kafka address is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Topic = "test"
			},
			expectedErr: ErrMissingKafkaAddress,
		},
		"should fail if ingest storage is enabled and Kafka topic is not configured": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
			},
			expectedErr: ErrMissingKafkaTopic,
		},
		"should pass if ingest storage is enabled and required config is set": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			assert.Equal(t, testData.expectedErr, cfg.Validate())
		})
	}
}
