// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"flag"
	"fmt"
	"math"
	"time"
)

type Config struct {
	PrimaryAddress       string
	ShadowAddress        string
	ShadowTimeout        time.Duration
	SampleRate           float64
	MaxConcurrent        int
	MaxResponseBytes     int
	MaxSamples           int
	SkipRecentSamples    time.Duration
	LogMismatchedQueries bool
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.PrimaryAddress, "primary-address", "", "gRPC address of the primary ingester for this partition.")
	f.StringVar(&c.ShadowAddress, "shadow-address", "", "gRPC address of the shadow ingester owning the same partition.")
	f.DurationVar(&c.ShadowTimeout, "shadow-timeout", 30*time.Second, "Maximum duration of a shadow request.")
	f.Float64Var(&c.SampleRate, "sample-rate", 1, "Fraction of mirrored read requests selected for comparison (0 to 1); all reads are mirrored.")
	f.IntVar(&c.MaxConcurrent, "max-concurrent-comparisons", 8, "Maximum concurrent comparisons; excess requests still mirror and discard shadow responses.")
	f.IntVar(&c.MaxResponseBytes, "max-response-bytes", 16<<20, "Maximum captured response bytes per backend; larger responses skip comparison.")
	f.IntVar(&c.MaxSamples, "max-samples", 100000, "Maximum decoded samples per backend; larger responses skip comparison.")
	f.BoolVar(&c.LogMismatchedQueries, "log-mismatched-queries", false, "Include QueryStream requests in mismatch logs; may contain sensitive query matchers.")
	f.DurationVar(&c.SkipRecentSamples, "skip-recent-samples", 0, "Exclude QueryStream samples newer than request start time minus this duration from comparisons.")
}

func (c Config) Validate() error {
	if c.PrimaryAddress == "" || c.ShadowAddress == "" || c.PrimaryAddress == c.ShadowAddress {
		return fmt.Errorf("primary-address and shadow-address must be nonempty and different")
	}
	if c.ShadowTimeout <= 0 || c.MaxConcurrent <= 0 || c.MaxResponseBytes <= 0 || c.MaxSamples <= 0 {
		return fmt.Errorf("shadow-timeout and comparison limits must be positive")
	}
	if math.IsNaN(c.SampleRate) || c.SampleRate < 0 || c.SampleRate > 1 {
		return fmt.Errorf("sample-rate must be between 0 and 1")
	}
	if c.SkipRecentSamples < 0 {
		return fmt.Errorf("skip-recent-samples must be nonnegative")
	}
	return nil
}
