// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStringsSamplerCollection verifies that sample values are collected correctly
// during statistics generation, respecting the configured limits.
func TestStringsSamplerCollection(t *testing.T) {
	numValues := 10000

	cfg := CostConfig{
		SampleValuesProbability: 0.01,      // 1% sample rate
		SampleValuesMaxCount:    1024,      // High enough to not be a factor
		SampleValuesMaxBytes:    64 * 1024, // High enough to not be a factor
	}
	sampler := NewStringsSampler(numValues, cfg)

	for i := 0; i < numValues; i++ {
		value := strconv.Itoa(i) // 1-5 bytes each
		sampler.MaybeSample(value)
	}

	samples := sampler.Sampled()

	// With 1% probability and 10000 values, we expect ~100 samples (with some variance)
	// Allow for statistical variance: expect between 50 and 150 samples
	require.GreaterOrEqual(t, len(samples), 50, "expected at least 50 samples, got %d", len(samples))
	require.LessOrEqual(t, len(samples), 150, "expected at most 150 samples, got %d", len(samples))

	// Verify all sampled values are valid (exist in the original set)
	for _, s := range samples {
		val, err := strconv.Atoi(s)
		require.NoError(t, err, "sample %q should be a valid integer", s)
		require.GreaterOrEqual(t, val, 0)
		require.Less(t, val, numValues)
	}
}

// TestStringsSamplerAreDeterministic verifies that sampling is deterministic
// (same input produces same samples) for reproducibility.
func TestStringsSamplerAreDeterministic(t *testing.T) {
	numValues := 1000

	cfg := CostConfig{
		SampleValuesProbability: 0.01,
		SampleValuesMaxCount:    1024,
		SampleValuesMaxBytes:    64 * 1024,
	}
	sampler1 := NewStringsSampler(numValues, cfg)
	sampler2 := NewStringsSampler(numValues, cfg)

	for i := 0; i < numValues; i++ {
		value := strconv.Itoa(i)
		sampler1.MaybeSample(value)
		sampler2.MaybeSample(value)
	}

	samples1 := sampler1.Sampled()
	samples2 := sampler2.Sampled()

	require.Equal(t, samples1, samples2, "samples should be deterministic")
}

// TestStringsSamplerSmallDataset verifies behavior with few values (less than expected samples).
func TestStringsSamplerSmallDataset(t *testing.T) {
	numValues := 5 // Very small, less than 1% sample would give

	cfg := CostConfig{
		SampleValuesProbability: 0.01,
		SampleValuesMaxCount:    1024,
		SampleValuesMaxBytes:    64 * 1024,
	}
	sampler := NewStringsSampler(numValues, cfg)
	for i := 0; i < numValues; i++ {
		sampler.MaybeSample(strconv.Itoa(i))
	}

	samples := sampler.Sampled()

	// With only 5 values and 1% probability, we might get 0 samples
	// but the capacity is pre-allocated to at least 1
	require.LessOrEqual(t, len(samples), numValues)
}

// TestStringsSamplerMaxCount verifies that the sampler respects the max count limit.
func TestStringsSamplerMaxCount(t *testing.T) {
	numValues := 10000
	maxCount := 50

	cfg := CostConfig{
		SampleValuesProbability: 1.0, // Always sample
		SampleValuesMaxCount:    maxCount,
		SampleValuesMaxBytes:    1024 * 1024, // Large enough to not be a factor
	}

	sampler := NewStringsSampler(numValues, cfg)
	for i := 0; i < numValues; i++ {
		sampler.MaybeSample(strconv.Itoa(i))
	}

	samples := sampler.Sampled()
	require.Equal(t, maxCount, len(samples))
}

// TestStringsSamplerMaxBytes verifies that the sampler respects the max bytes limit.
func TestStringsSamplerMaxBytes(t *testing.T) {
	numValues := 1000
	maxBytes := 100

	cfg := CostConfig{
		SampleValuesProbability: 1.0,    // Always sample
		SampleValuesMaxCount:    100000, // Large enough to not be a factor
		SampleValuesMaxBytes:    maxBytes,
	}

	sampler := NewStringsSampler(numValues, cfg)
	for i := 0; i < numValues; i++ {
		// Each value is ~4 bytes ("0000" to "0999")
		sampler.MaybeSample(strconv.Itoa(i))
	}

	samples := sampler.Sampled()

	// Count actual bytes
	totalBytes := 0
	for _, s := range samples {
		totalBytes += len(s)
	}

	// Should have stopped around maxBytes
	require.Less(t, totalBytes, maxBytes+10, // allow small overshoot for the last sample
		"total bytes %d should be close to maxBytes %d", totalBytes, maxBytes)
}

// TestStringsSamplerDisabled verifies that zero probability results in no samples.
func TestStringsSamplerDisabled(t *testing.T) {
	numValues := 1000

	cfg := CostConfig{
		SampleValuesProbability: 0, // Disabled
		SampleValuesMaxCount:    1000,
		SampleValuesMaxBytes:    10000,
	}

	sampler := NewStringsSampler(numValues, cfg)
	for i := 0; i < numValues; i++ {
		sampler.MaybeSample(strconv.Itoa(i))
	}

	samples := sampler.Sampled()
	require.Empty(t, samples)
}
