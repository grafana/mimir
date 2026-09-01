// SPDX-License-Identifier: AGPL-3.0-only

// Package retryafter computes and writes the value of the Retry-After HTTP header for 429 and 5xx responses.
package retryafter

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand" // #nosec G404 -- nosemgrep: used only for retry backoff jitter, not security-sensitive
	"net/http"
	"strconv"
	"time"
)

var (
	ErrNonPositiveMinBackoffDuration = errors.New("min-backoff should be greater than or equal to 1s")
	ErrNonPositiveMaxBackoffDuration = errors.New("max-backoff should be greater than or equal to 1s")
)

// Config configures the Retry-After header.
type Config struct {
	Enabled    bool          `yaml:"enabled" category:"advanced"`
	MinBackoff time.Duration `yaml:"min_backoff" category:"advanced"`
	MaxBackoff time.Duration `yaml:"max_backoff" category:"advanced"`
}

// RegisterFlagsWithPrefix registers the Retry-After header flags with the given prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet, enabledByDefault bool, retryableResponsesDesc string) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", enabledByDefault, "Enables inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it.")
	f.DurationVar(&cfg.MinBackoff, prefix+"min-backoff", 6*time.Second, fmt.Sprintf("Minimum duration of the Retry-After HTTP header in responses to %s. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50%% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header.", retryableResponsesDesc))
	f.DurationVar(&cfg.MaxBackoff, prefix+"max-backoff", 96*time.Second, fmt.Sprintf("Maximum duration of the Retry-After HTTP header in responses to %s. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50%% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header.", retryableResponsesDesc))
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.MinBackoff < time.Second {
		return ErrNonPositiveMinBackoffDuration
	}
	if cfg.MaxBackoff < time.Second {
		return ErrNonPositiveMaxBackoffDuration
	}
	return nil
}

// ShouldRetryFunc reports whether a response with the given HTTP status code should carry a Retry-After header.
type ShouldRetryFunc func(statusCode int) bool

// StatusCodeSet returns a ShouldRetryFunc that matches exactly the given status codes.
func StatusCodeSet(codes ...int) ShouldRetryFunc {
	set := make(map[int]struct{}, len(codes))
	for _, code := range codes {
		set[code] = struct{}{}
	}
	return func(statusCode int) bool {
		_, ok := set[statusCode]
		return ok
	}
}

// DefaultShouldRetry is the default retry policy: 429 (Too Many Requests) and 503 (Service Unavailable).
// Callers with different needs (e.g. treating all 5xx as retryable) should build their own ShouldRetryFunc.
var DefaultShouldRetry = StatusCodeSet(http.StatusTooManyRequests, http.StatusServiceUnavailable)

// Seconds calculates the Retry-After value, in whole seconds, given the value of the request's
// Retry-Attempt header. The result is MinBackoff*2^(RetryAttempt-1), clamped to MaxBackoff, with
// random jitter of 50% in either direction.
func (cfg Config) Seconds(retryAttemptHeader string) string {
	const jitterFactor = 0.5

	retryAttempt, err := strconv.Atoi(retryAttemptHeader)
	// If retry-attempt is not valid, set it to default 1
	if err != nil || retryAttempt < 1 {
		retryAttempt = 1
	}

	delaySeconds := cfg.MinBackoff.Seconds() * math.Pow(2.0, float64(retryAttempt-1))
	delaySeconds = min(cfg.MaxBackoff.Seconds(), delaySeconds)
	if jitterAmount := int64(delaySeconds * jitterFactor); jitterAmount > 0 {
		// The random jitter can be negative too, so we generate a 2x greater the random number and subtract the jitter.
		randomJitter := float64(rand.Int63n(jitterAmount*2+1) - jitterAmount)
		delaySeconds += randomJitter
	}
	// Jitter might have pushed the delaySeconds over maxBackoff or minBackoff, so we need to clamp it again.
	delaySeconds = min(cfg.MaxBackoff.Seconds(), delaySeconds)
	delaySeconds = max(cfg.MinBackoff.Seconds(), delaySeconds)

	return strconv.FormatInt(int64(delaySeconds), 10)
}
