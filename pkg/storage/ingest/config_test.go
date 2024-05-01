// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"
	"time"

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
		"should fail if ingest storage is enabled and consume position is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = "middle"
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume timestamp is set and consume position is not expected": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromEnd
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = time.Now().UnixMilli()
			},
			expectedErr: ErrInvalidConsumePosition,
		},
		"should fail if ingest storage is enabled and consume position is expected but consume timestamp is invalid": {
			setup: func(cfg *Config) {
				cfg.Enabled = true
				cfg.KafkaConfig.Address = "localhost"
				cfg.KafkaConfig.Topic = "test"
				cfg.KafkaConfig.ConsumeFromPositionAtStartup = consumeFromTimestamp
				cfg.KafkaConfig.ConsumeFromTimestampAtStartup = 0
			},
			expectedErr: ErrInvalidConsumePosition,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			assert.ErrorIs(t, cfg.Validate(), testData.expectedErr)
		})
	}
}
