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
	LogMismatchedSamples bool
	MismatchRetries      int
	MismatchRetryDelay   time.Duration

	Amplification       float64
	AmplificationDelay  time.Duration
	MaxConcurrentCopies int

	PodIndex       int
	ShadowPodCount int

	LogSlowResponseThreshold time.Duration
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.PrimaryAddress, "primary-address", "", "gRPC address of the primary ingester for this partition.")
	f.StringVar(&c.ShadowAddress, "shadow-address", "", "gRPC address of the shadow ingester owning the same partition.")
	f.DurationVar(&c.ShadowTimeout, "shadow-timeout", 30*time.Second, "Maximum duration of a shadow request.")
	f.Float64Var(&c.SampleRate, "sample-rate", 1, "Fraction of mirrored read requests selected for comparison (0 to 1); all reads are mirrored.")
	f.IntVar(&c.MaxConcurrent, "max-concurrent-comparisons", 8, "Maximum concurrent comparisons; excess requests still mirror and discard shadow responses.")
	f.IntVar(&c.MaxResponseBytes, "max-response-bytes", 16<<20, "Maximum captured response bytes per backend; larger responses skip comparison.")
	f.IntVar(&c.MaxSamples, "max-samples", 100000, "Maximum decoded samples per backend; larger responses skip comparison.")
	f.BoolVar(&c.LogMismatchedQueries, "log-mismatched-queries", false, "Include QueryStream requests, tenant and replay metadata in mismatch and comparison-error logs; may contain sensitive data.")
	f.BoolVar(&c.LogMismatchedSamples, "log-mismatched-samples", false, "Include one bounded differing sample in mismatch logs, with labels and values; may contain sensitive data.")
	f.IntVar(&c.MismatchRetries, "mismatch-retries", 0, "How many times to ask both backends again, after -mismatch-retry-delay each, when a comparison's items or samples differ; it's a mismatch only if some of the same still differ every time, as ingesters that agree can differ for a while, such as on active series between their updates, or on samples written late that one consumed before the other.")
	f.DurationVar(&c.MismatchRetryDelay, "mismatch-retry-delay", 70*time.Second, "How long to wait before each of -mismatch-retries.")
	f.Float64Var(&c.Amplification, "amplification", 1, "How many times each read is sent to both ingesters, on average, to load them above the read traffic: 1 sends it once, and 2.5 adds one or two copies. Each copy goes to the primary and the shadow at the same time, so both see the same load. Copies are timed but never compared, and the caller's response doesn't wait for them. A copy of a read hits what the ingesters cached from it, so copies load them less than as many other reads would.")
	f.DurationVar(&c.AmplificationDelay, "amplification-delay", 0, "Copies of a read are sent after a random delay between 0 and this, so they spread over time instead of arriving with the read.")
	f.DurationVar(&c.LogSlowResponseThreshold, "log-slow-response-threshold", 0, "The minimum difference in response time between the slower and the faster backend over which to log a mirrored read, compared or not, at most 10 a second. With -log-mismatched-queries, logs include the tenant and the request. 0 to disable.")
	f.IntVar(&c.PodIndex, "pod-index", -1, "Index of the pod the tee runs in, such as a statefulset's pod index, for -shadow-pod-count.")
	f.IntVar(&c.ShadowPodCount, "shadow-pod-count", 0, "Only the tees of pods whose -pod-index is below this mirror, amplify and compare reads, so a shadow can cover only the first partitions; the others pass reads to the primary alone. 0 means every pod.")
	f.IntVar(&c.MaxConcurrentCopies, "max-concurrent-copies", 64, "Copies in flight at most, each to both ingesters, and 16 times as many waiting for their delay; copies beyond are dropped for both and counted, so an ingester falling behind doesn't grow the tee.")
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
	if c.MismatchRetries < 0 || (c.MismatchRetries > 0 && c.MismatchRetryDelay <= 0) {
		return fmt.Errorf("mismatch-retries must be nonnegative, with a positive mismatch-retry-delay")
	}
	if c.LogSlowResponseThreshold < 0 {
		return fmt.Errorf("log-slow-response-threshold must be nonnegative")
	}
	if c.ShadowPodCount < 0 || (c.ShadowPodCount > 0 && c.PodIndex < 0) {
		return fmt.Errorf("shadow-pod-count must be nonnegative, with a pod-index when it's set")
	}
	if math.IsNaN(c.Amplification) || c.Amplification < 1 {
		return fmt.Errorf("amplification must be at least 1")
	}
	if c.AmplificationDelay < 0 || (c.Amplification > 1 && c.MaxConcurrentCopies <= 0) {
		return fmt.Errorf("amplification-delay must be nonnegative, with a positive max-concurrent-copies")
	}
	return nil
}

// shadowed reports whether the tee's pod mirrors to the shadow, by -shadow-pod-count.
func (c Config) shadowed() bool {
	return c.ShadowPodCount == 0 || c.PodIndex < c.ShadowPodCount
}
