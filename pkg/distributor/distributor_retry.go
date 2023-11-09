// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"flag"
	"time"

	"github.com/pkg/errors"
)

var (
	errNonPositiveRetryBase          = errors.New("Retry base duration should be a positive value")
	errNonPositiveMaxAllowedAttempts = errors.New("MaxAllowedAttempts should be a positive value")
)

type RetryConfig struct {
	Base    time.Duration `yaml:"base" category:"experimental"`
	Enabled bool          `yaml:"enabled" category:"experimental"`
	// MaxAllowedAttempts limits the number of retry attempts considered for calculation,
	// capping it at the specified value. Additional attempts beyond this limit are not rejected.
	// Used to control exponential backoff calculation with an upper limit.
	MaxAllowedAttempts int `yaml:"max_allowed_attempts" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *RetryConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.Base, "distributor.retry.base", 3*time.Second, "Base time to wait before retrying a 429/5xx request.")
	f.IntVar(&cfg.MaxAllowedAttempts, "distributor.retry.max-allowed-attempts", 5, "Sets the upper limit on the number of retry attempts considered for calculation. It caps the calculated attempts without rejecting additional attempts. Used for controlling exponential backoff calculations.")
	f.BoolVar(&cfg.Enabled, "distributor.retry.enabled", false, "Enabled controls inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it.")
}

func (cfg *RetryConfig) Validate() error {
	if cfg.Base <= 0 {
		return errNonPositiveRetryBase
	}
	if cfg.MaxAllowedAttempts < 1 {
		return errNonPositiveMaxAllowedAttempts
	}
	return nil
}
