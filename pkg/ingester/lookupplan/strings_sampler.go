// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"math/rand/v2"
	"strings"
)

// StringsSampler samples strings probabilistically with configurable limits.
// It is designed to sample label values for selectivity estimation.
type StringsSampler struct {
	rng         *rand.Rand
	probability float64
	maxCount    int
	maxBytes    int

	sampled      []string
	sampledBytes int
}

// NewStringsSampler creates a sampler for a label with the given number of values.
// The seed is derived from numValues for deterministic sampling.
func NewStringsSampler(numValues int, cfg CostConfig) *StringsSampler {
	expectedNumSamples := min(cfg.SampleValuesMaxCount, int(cfg.SampleValuesProbability*float64(numValues)))
	return &StringsSampler{
		rng:         rand.New(rand.NewPCG(uint64(numValues), 0)),
		probability: cfg.SampleValuesProbability,
		maxCount:    cfg.SampleValuesMaxCount,
		maxBytes:    cfg.SampleValuesMaxBytes,
		sampled:     make([]string, 0, max(1, expectedNumSamples)),
	}
}

// MaybeSample considers the given string for sampling.
// It samples with the configured probability, respecting max count and size limits.
func (s *StringsSampler) MaybeSample(value string) {
	if s.rng.Float64() < s.probability &&
		len(s.sampled) < s.maxCount &&
		s.sampledBytes < s.maxBytes {
		s.sampled = append(s.sampled, strings.Clone(value))
		s.sampledBytes += len(value)
	}
}

// Sampled returns all sampled strings.
func (s *StringsSampler) Sampled() []string {
	return s.sampled
}
