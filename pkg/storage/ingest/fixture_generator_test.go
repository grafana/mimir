// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixtureConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		assert.NoError(t, cfg.Validate())
	})

	t.Run("invalid TotalUniqueTimeseries", func(t *testing.T) {
		cfg := createTestFixtureConfig(0, 200)
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid TotalSamples less than TotalUniqueTimeseries", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 99)
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid tenant percentages", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		cfg.SmallTenants.TenantPercent = 50
		cfg.MediumTenants.TenantPercent = 30
		cfg.LargeTenants.TenantPercent = 10 // Sum is 90, not 100
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid timeseries percentages", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		cfg.SmallTenants.TimeseriesPercent = 10
		cfg.MediumTenants.TimeseriesPercent = 20
		cfg.LargeTenants.TimeseriesPercent = 60 // Sum is 90, not 100
		assert.Error(t, cfg.Validate())
	})

	t.Run("negative percentage", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		cfg.SmallTenants.TenantPercent = -10
		cfg.MediumTenants.TenantPercent = 60
		cfg.LargeTenants.TenantPercent = 50 // Sum is 100 but negative value
		assert.Error(t, cfg.Validate())
	})

	t.Run("zero AvgTimeseriesPerReq", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		cfg.SmallTenants.AvgTimeseriesPerReq = 0
		assert.Error(t, cfg.Validate())
	})

	t.Run("zero AvgLabelsPerTimeseries", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		cfg.AvgLabelsPerTimeseries = 0
		assert.Error(t, cfg.Validate())
	})
}

func TestNewFixtureGenerator(t *testing.T) {
	t.Run("should return error on invalid config", func(t *testing.T) {
		cfg := createTestFixtureConfig(0, 200) // Invalid: TotalUniqueTimeseries = 0
		gen, err := NewFixtureGenerator(cfg, 10, 12345)
		assert.Error(t, err)
		assert.Nil(t, gen)
	})

	t.Run("should return error when all tenants have zero timeseries", func(t *testing.T) {
		// Config where 100% of tenants are small but 0% of timeseries go to small tenants.
		cfg := createTestFixtureConfig(100, 200)
		cfg.SmallTenants.TenantPercent = 100
		cfg.SmallTenants.TimeseriesPercent = 0
		cfg.MediumTenants.TenantPercent = 0
		cfg.MediumTenants.TimeseriesPercent = 50
		cfg.LargeTenants.TenantPercent = 0
		cfg.LargeTenants.TimeseriesPercent = 50

		gen, err := NewFixtureGenerator(cfg, 10, 12345)
		assert.ErrorContains(t, err, "zero total timeseries")
		assert.Nil(t, gen)
	})
}

func TestFixtureGenerator_GenerateNextWriteRequest(t *testing.T) {
	t.Run("should be deterministic", func(t *testing.T) {
		cfg := createTestFixtureConfig(100, 200)
		numTenants := 5
		seed := int64(42)

		// Create two generators with the same seed.
		gen1, err := NewFixtureGenerator(cfg, numTenants, seed)
		require.NoError(t, err)
		gen2, err := NewFixtureGenerator(cfg, numTenants, seed)
		require.NoError(t, err)

		// Generate requests and verify they are identical.
		for i := 0; i < 20; i++ {
			tenantID1, req1, err1 := gen1.GenerateNextWriteRequest(int64(1000 + i))
			require.NoError(t, err1)
			tenantID2, req2, err2 := gen2.GenerateNextWriteRequest(int64(1000 + i))
			require.NoError(t, err2)

			assert.Equal(t, tenantID1, tenantID2, "Same seed should produce same tenant selection at iteration %d", i)
			require.NotNil(t, req1)
			require.NotNil(t, req2)
			require.Equal(t, len(req1.Timeseries), len(req2.Timeseries), "Same seed should produce same number of timeseries at iteration %d", i)

			// Verify each timeseries has identical content.
			for j := range req1.Timeseries {
				assert.Equal(t, req1.Timeseries[j].Labels, req2.Timeseries[j].Labels, "Same seed should produce identical labels at iteration %d, timeseries %d", i, j)
				assert.Equal(t, req1.Timeseries[j].Samples, req2.Timeseries[j].Samples, "Same seed should produce identical samples at iteration %d, timeseries %d", i, j)
			}
		}
	})

	t.Run("should honor the configured number of unique series", func(t *testing.T) {
		// This test verifies that consecutive requests for the same tenant
		// contain different series (rotating through all unique series).
		const numUniqueSeries = 100

		// Use only large tenants to simplify the test
		cfg := createTestFixtureConfig(numUniqueSeries, 200)
		cfg.SmallTenants.TenantPercent = 0
		cfg.SmallTenants.TimeseriesPercent = 0
		cfg.SmallTenants.AvgTimeseriesPerReq = 0
		cfg.MediumTenants.TenantPercent = 0
		cfg.MediumTenants.TimeseriesPercent = 0
		cfg.MediumTenants.AvgTimeseriesPerReq = 0
		cfg.LargeTenants.TenantPercent = 100
		cfg.LargeTenants.TimeseriesPercent = 100
		cfg.LargeTenants.AvgTimeseriesPerReq = 5

		gen, err := NewFixtureGenerator(cfg, 1, 12345)
		require.NoError(t, err)

		// Generate enough requests to cycle through all series.
		// With ~5 series per request, 30 requests gives ~150 series,
		// which should cover all 100 unique series.
		allMetricNames := make(map[string]bool)
		for i := 0; i < 30; i++ {
			_, req, err := gen.GenerateNextWriteRequest(int64(1000 + i))
			require.NoError(t, err)
			require.NotNil(t, req)

			for _, ts := range req.Timeseries {
				for _, lbl := range ts.Labels {
					if lbl.Name == model.MetricNameLabel {
						allMetricNames[lbl.Value] = true
					}
				}
			}
		}

		// Verify we saw all unique metric names due to rotation.
		assert.Equal(t, numUniqueSeries, len(allMetricNames), "Expected to see all %d unique metric names due to rotation", numUniqueSeries)
	})

	t.Run("should interleave tenants", func(t *testing.T) {
		// Create a fixture generator with multiple tenants.
		cfg := createTestFixtureConfig(1000, 2000)
		numTenants := 10
		gen, err := NewFixtureGenerator(cfg, numTenants, 12345)
		require.NoError(t, err)

		// Generate a sequence of records and verify they are interleaved by tenant.
		numRecords := 1000
		tenantSequence := make([]string, numRecords)

		for i := 0; i < numRecords; i++ {
			tenantID, _, err := gen.GenerateNextWriteRequest(int64(i))
			require.NoError(t, err)
			tenantSequence[i] = tenantID
		}

		// Verify that records are interleaved - no single tenant should have an
		// excessively long consecutive run. With weighted selection, large tenants
		// appear more often but should still be interleaved.
		// Allow up to 20 consecutive records from the same tenant (reasonable for weighted selection).
		maxAllowedConsecutive := 20

		currentTenant := ""
		consecutiveCount := 0
		maxObservedConsecutive := 0
		for _, tenant := range tenantSequence {
			if tenant == currentTenant {
				consecutiveCount++
				if consecutiveCount > maxObservedConsecutive {
					maxObservedConsecutive = consecutiveCount
				}
			} else {
				currentTenant = tenant
				consecutiveCount = 1
			}
		}
		assert.LessOrEqual(t, maxObservedConsecutive, maxAllowedConsecutive,
			"Max consecutive records from same tenant should be <= %d, got %d", maxAllowedConsecutive, maxObservedConsecutive)

		// Verify that multiple tenants are represented in the sequence.
		uniqueTenants := make(map[string]bool)
		for _, tenant := range tenantSequence {
			uniqueTenants[tenant] = true
		}
		assert.GreaterOrEqual(t, len(uniqueTenants), numTenants/2, "At least half of tenants should appear in %d records", numRecords)
	})
}

func TestFixtureGenerator_TenantDistribution(t *testing.T) {
	cfg := createTestFixtureConfig(10000, 20000)

	// With createTestFixtureConfig:
	// - Tenant distribution: 40% small, 44% medium, 16% large
	// - Timeseries distribution: 5% small, 35% medium, 60% large
	// Tenant selection is weighted by timeseries count, so large tenants
	// should be selected more often despite having fewer tenants.
	numTenants := 100
	gen, err := NewFixtureGenerator(cfg, numTenants, 12345)
	require.NoError(t, err)

	// Count records by tenant size category.
	// Tenants are created in order: small first, then medium, then large.
	// With 100 tenants: 40 small, 44 medium, 16 large
	numSmall := 40
	numMedium := 44

	smallRecords := 0
	mediumRecords := 0
	largeRecords := 0
	totalRecords := 10000

	for i := 0; i < totalRecords; i++ {
		tenantIdx := gen.selectNextTenant()
		if tenantIdx < numSmall {
			smallRecords++
		} else if tenantIdx < numSmall+numMedium {
			mediumRecords++
		} else {
			largeRecords++
		}
	}

	// Verify weighted distribution matches configured timeseries percentages.
	// Small tenants have 5% of timeseries -> ~5% of records
	// Medium tenants have 35% of timeseries -> ~35% of records
	// Large tenants have 60% of timeseries -> ~60% of records
	smallPct := float64(smallRecords) * 100 / float64(totalRecords)
	mediumPct := float64(mediumRecords) * 100 / float64(totalRecords)
	largePct := float64(largeRecords) * 100 / float64(totalRecords)

	assert.InDelta(t, float64(cfg.SmallTenants.TimeseriesPercent), smallPct, 3.0,
		"Small tenants should get ~%d%% of records, got %.1f%%", cfg.SmallTenants.TimeseriesPercent, smallPct)
	assert.InDelta(t, float64(cfg.MediumTenants.TimeseriesPercent), mediumPct, 3.0,
		"Medium tenants should get ~%d%% of records, got %.1f%%", cfg.MediumTenants.TimeseriesPercent, mediumPct)
	assert.InDelta(t, float64(cfg.LargeTenants.TimeseriesPercent), largePct, 3.0,
		"Large tenants should get ~%d%% of records, got %.1f%%", cfg.LargeTenants.TimeseriesPercent, largePct)
}

func TestFixtureGenerator_generateSeriesLabels(t *testing.T) {
	cfg := createTestFixtureConfig(100, 200)
	cfg.AvgLabelsPerTimeseries = 10
	cfg.NumUniqueLabelNames = 50
	cfg.NumUniqueLabelValues = 100

	gen, err := NewFixtureGenerator(cfg, 1, 12345)
	require.NoError(t, err)

	t.Run("deterministic output", func(t *testing.T) {
		// For the same input idx, generateSeriesLabels should always return identical labels.
		builder := labels.NewBuilder(labels.EmptyLabels())

		for idx := 0; idx < 20; idx++ {
			labels1 := gen.generateSeriesLabels(idx, builder)
			labels2 := gen.generateSeriesLabels(idx, builder)
			assert.True(t, labels.Equal(labels1, labels2), "Same idx should produce identical labels")
		}
	})

	t.Run("average number of labels honored", func(t *testing.T) {
		builder := labels.NewBuilder(labels.EmptyLabels())
		lbls := gen.generateSeriesLabels(0, builder)

		// Labels include __name__ plus AvgLabelsPerTimeseries other labels
		expectedCount := cfg.AvgLabelsPerTimeseries + 1
		assert.Equal(t, expectedCount, lbls.Len(),
			"Expected %d labels (1 __name__ + %d configured), got %d",
			expectedCount, cfg.AvgLabelsPerTimeseries, lbls.Len())
	})

	t.Run("unique label names and values match config", func(t *testing.T) {
		builder := labels.NewBuilder(labels.EmptyLabels())
		uniqueLabelNames := make(map[string]bool)
		uniqueLabelValues := make(map[string]bool)

		// Generate enough series to cycle through all unique names and values.
		// We need at least NumUniqueLabelNames * NumUniqueLabelValues iterations
		// to have a chance to see all combinations.
		numIterations := cfg.NumUniqueLabelNames * cfg.NumUniqueLabelValues

		for idx := 0; idx < numIterations; idx++ {
			lbls := gen.generateSeriesLabels(idx, builder)
			lbls.Range(func(l labels.Label) {
				if l.Name != "__name__" {
					uniqueLabelNames[l.Name] = true
					uniqueLabelValues[l.Value] = true
				}
			})
		}

		assert.Equal(t, cfg.NumUniqueLabelNames, len(uniqueLabelNames),
			"Expected %d unique label names, got %d", cfg.NumUniqueLabelNames, len(uniqueLabelNames))
		assert.Equal(t, cfg.NumUniqueLabelValues, len(uniqueLabelValues),
			"Expected %d unique label values, got %d", cfg.NumUniqueLabelValues, len(uniqueLabelValues))
	})
}

// createTestFixtureConfig returns a valid FixtureConfig for testing.
func createTestFixtureConfig(totalUniqueTimeseries, totalSamples int) FixtureConfig {
	return FixtureConfig{
		TotalUniqueTimeseries: totalUniqueTimeseries,
		TotalSamples:          totalSamples,

		SmallTenants: FixtureTenantClassConfig{
			TenantPercent:       40,
			TimeseriesPercent:   5,
			AvgTimeseriesPerReq: 5.0,
		},
		MediumTenants: FixtureTenantClassConfig{
			TenantPercent:       44,
			TimeseriesPercent:   35,
			AvgTimeseriesPerReq: 5.0,
		},
		LargeTenants: FixtureTenantClassConfig{
			TenantPercent:       16,
			TimeseriesPercent:   60,
			AvgTimeseriesPerReq: 5.5,
		},

		AvgLabelsPerTimeseries: 14,
		AvgLabelNameLength:     10,
		AvgLabelValueLength:    21,
		AvgMetricNameLength:    36,
		NumUniqueLabelNames:    100,
		NumUniqueLabelValues:   1000,
	}
}
