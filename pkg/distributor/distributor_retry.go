package distributor

import (
	"flag"
	"time"
)

type RetryConfig struct {
	Base time.Duration `yaml:"retry_base" category:"experimental"`
	// Retry strategies for handling 429/5xx requests.
	// Strategy 0 involves retrying after a duration within the range [1, retry.Base].
	// Strategy 1 entails retrying after a duration within the range [1, retry.Base * retry-Attempts].
	// Strategy 2, on the other hand, prescribes retrying after a duration within the range [1, retry.Base * 2^(retryAttempts-1)].
	Strategy int           `yaml:"retry_strategy" category:"experimental"`
	MaxDelay time.Duration `yaml:"retry_max_delay" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *RetryConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.Base, "distributor.retry.base", 5*time.Second, "Base time to wait before retrying a 429/5xx request.")
	f.DurationVar(&cfg.MaxDelay, "distributor.retry.max-delay", time.Minute, "Maximum time to wait before retrying a retryable request.")
	f.IntVar(&cfg.Strategy, "distributor.retry.strategy", 0, "Retry strategies for handling 429/5xx requests. Strategy 0 is not activated, Strategy 1 involves retrying after a duration within the range [1, retry.Base]. Strategy 2 entails retrying after a duration within the range [1, retry.Base * retry-Attempts]. Strategy 3, on the other hand, prescribes retrying after a duration within the range [1, retry.Base * 2^(retryAttempts-1)].")
}
