// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
)

func TestRetryConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		cfg         RetryConfig
		expectedErr error
	}{
		"should pass with default config": {
			cfg: func() RetryConfig {
				cfg := RetryConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should fail if retry base is 0": {
			cfg: RetryConfig{
				Base:               0,
				MaxAllowedAttempts: 5,
			},
			expectedErr: errNonPositiveRetryBase,
		},
		"should fail if retry base is negative": {
			cfg: RetryConfig{
				Base:               -1,
				MaxAllowedAttempts: 5,
			},
			expectedErr: errNonPositiveRetryBase,
		},
		"should fail if max allowed attempts is 0": {
			cfg: RetryConfig{
				Base:               3 * time.Second,
				MaxAllowedAttempts: 0,
			},
			expectedErr: errNonPositiveMaxAllowedAttempts,
		},
		"should fail if max allowed attempts is negative": {
			cfg: RetryConfig{
				Base:               3 * time.Second,
				MaxAllowedAttempts: -1,
			},
			expectedErr: errNonPositiveMaxAllowedAttempts,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErr, testData.cfg.Validate())
		})
	}
}
