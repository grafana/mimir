// SPDX-License-Identifier: AGPL-3.0-only

package retryafter

import (
	"flag"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		expectedErr error
	}{
		"should pass with default config": {
			cfg: func() Config {
				cfg := Config{}
				cfg.RegisterFlagsWithPrefix("", flag.NewFlagSet("test", flag.PanicOnError), true, "429/5xx errors")
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass with min and max backoff equal to 1s": {
			cfg: Config{
				MinBackoff: 1 * time.Second,
				MaxBackoff: 1 * time.Second,
			},
			expectedErr: nil,
		},
		"should fail if min backoff is 0": {
			cfg: Config{
				MinBackoff: 0,
				MaxBackoff: 3 * time.Second,
			},
			expectedErr: ErrNonPositiveMinBackoffDuration,
		},
		"should fail if min backoff is negative": {
			cfg: Config{
				MinBackoff: -1 * time.Second,
				MaxBackoff: 5 * time.Second,
			},
			expectedErr: ErrNonPositiveMinBackoffDuration,
		},
		"should fail if max backoff is 0": {
			cfg: Config{
				MinBackoff: 3 * time.Second,
				MaxBackoff: 0,
			},
			expectedErr: ErrNonPositiveMaxBackoffDuration,
		},
		"should fail if max backoff is negative": {
			cfg: Config{
				MinBackoff: 3 * time.Second,
				MaxBackoff: -1,
			},
			expectedErr: ErrNonPositiveMaxBackoffDuration,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErr, testData.cfg.Validate())
		})
	}
}

func TestConfig_Seconds(t *testing.T) {
	testCases := []struct {
		name          string
		retryAttempt  string
		cfg           Config
		minRetryAfter int
		maxRetryAfter int
	}{
		{
			name:          "no Retry-Attempt set, default Retry-Attempt to 1",
			cfg:           Config{MinBackoff: 5 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 5,
			maxRetryAfter: 7,
		},
		{
			name:          "Retry-Attempt is not an integer, default Retry-Attempt to 1",
			retryAttempt:  "not-an-integer",
			cfg:           Config{MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 3,
			maxRetryAfter: 4,
		},
		{
			name:          "Retry-Attempt is float, default Retry-Attempt to 1",
			retryAttempt:  "3.50",
			cfg:           Config{MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 2,
			maxRetryAfter: 3,
		},
		{
			name:          "Retry-Attempt a list of integers, default Retry-Attempt to 1",
			retryAttempt:  "[1, 2, 3]",
			cfg:           Config{MinBackoff: 1 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 1,
			maxRetryAfter: 3,
		},
		{
			name:          "Retry-Attempt is negative, default Retry-Attempt to 1",
			retryAttempt:  "-1",
			cfg:           Config{MinBackoff: 4 * time.Second, MaxBackoff: 16 * time.Second},
			minRetryAfter: 4,
			maxRetryAfter: 6,
		},
		{
			name:          "valid Retry-Attempt set to 2",
			retryAttempt:  "2",
			cfg:           Config{MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 4 - 0.5*4,
			maxRetryAfter: 4 + 0.5*4,
		},
		{
			name:          "valid Retry-Attempt set to 3",
			retryAttempt:  "3",
			cfg:           Config{MinBackoff: 2 * time.Second, MaxBackoff: 64 * time.Second},
			minRetryAfter: 8 - 0.5*8,
			maxRetryAfter: 8 + 0.5*8,
		},
		{
			name:          "Retry-Attempt set higher than MaxBackoff",
			retryAttempt:  "8",
			cfg:           Config{MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 8 * 0.5,
			maxRetryAfter: 8,
		},
		{
			name:          "Retry-Attempt set to a very high value (MaxInt64)",
			retryAttempt:  "9223372036854775807",
			cfg:           Config{MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 4,
			maxRetryAfter: 8,
		},
		{
			name:          "Retry-Attempt set to a too high value fails to parse the value (MaxInt64+1)",
			retryAttempt:  "9223372036854775808",
			cfg:           Config{MinBackoff: 3 * time.Second, MaxBackoff: 8 * time.Second},
			minRetryAfter: 2,
			maxRetryAfter: 4,
		},
		{
			name:          "MinBackoff and MaxBackoff set to <1s",
			retryAttempt:  "3",
			cfg:           Config{MinBackoff: 3 * time.Millisecond, MaxBackoff: 8 * time.Millisecond},
			minRetryAfter: 0,
			maxRetryAfter: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			retryAfter := tc.cfg.Seconds(tc.retryAttempt)

			retryAfterInt, err := strconv.Atoi(retryAfter)
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, retryAfterInt, tc.minRetryAfter)
			assert.LessOrEqual(t, retryAfterInt, tc.maxRetryAfter)
		})
	}
}

func TestStatusCodeSet(t *testing.T) {
	shouldRetry := StatusCodeSet(429, 503)
	assert.False(t, shouldRetry(200))
	assert.False(t, shouldRetry(400))
	assert.False(t, shouldRetry(500))
	assert.True(t, shouldRetry(429))
	assert.True(t, shouldRetry(503))
}

func TestDefaultShouldRetry(t *testing.T) {
	assert.False(t, DefaultShouldRetry(200))
	assert.False(t, DefaultShouldRetry(400))
	assert.False(t, DefaultShouldRetry(408))
	assert.False(t, DefaultShouldRetry(500))
	assert.True(t, DefaultShouldRetry(429))
	assert.True(t, DefaultShouldRetry(503))
}
