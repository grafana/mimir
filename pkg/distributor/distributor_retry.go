// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"flag"
	"time"

	"github.com/pkg/errors"
)

var (
	errNonPositiveRetryBase          = errors.New("retry base duration should not be less than 1 second")
	errNonPositiveMaxAllowedAttempts = errors.New("maxAllowedAttempts should be a positive value")
)

type RetryConfig struct {
	Enabled            bool          `yaml:"enabled" category:"experimental"`
	Base               time.Duration `yaml:"base" category:"experimental"`
	MaxAllowedAttempts int           `yaml:"max_allowed_attempts" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *RetryConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "distributor.retry-after-header.enabled", false, "Enabled controls inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it.")
	f.DurationVar(&cfg.Base, "distributor.retry-after-header.base", 3*time.Second, "Base duration for calculating the Retry-After header in responses to 429/5xx errors.")
	f.IntVar(&cfg.MaxAllowedAttempts, "distributor.retry-after-header.max-allowed-attempts", 5, "Sets the upper limit on the number of retry attempts considered for calculation. It caps the calculated attempts without rejecting additional attempts, controlling exponential backoff calculations. For example, when the base is set to 3 and max-allowed-attempts to 5, the maximum retry duration would be 3 * 2^5 = 96 seconds.")
}

func (cfg *RetryConfig) Validate() error {
	if cfg.Base < 1 {
		return errNonPositiveRetryBase
	}
	if cfg.MaxAllowedAttempts < 1 {
		return errNonPositiveMaxAllowedAttempts
	}
	return nil
}
