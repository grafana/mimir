// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// FixtureConfig holds all configurable parameters for generating realistic test and benchmark fixtures.
type FixtureConfig struct {
	// Total unique timeseries across all tenants.
	TotalUniqueTimeseries int

	// Total samples to generate (must be >= TotalUniqueTimeseries).
	TotalSamples int

	// Tenant class configurations.
	SmallTenants  FixtureTenantClassConfig
	MediumTenants FixtureTenantClassConfig
	LargeTenants  FixtureTenantClassConfig

	// Label configuration.
	AvgLabelsPerTimeseries int
	AvgLabelNameLength     int
	AvgLabelValueLength    int
	AvgMetricNameLength    int
	NumUniqueLabelNames    int
	NumUniqueLabelValues   int
}

// Validate checks that the configuration is valid and returns an error if not.
func (c FixtureConfig) Validate() error {
	var errs []error

	// Validate total volumes.
	if c.TotalUniqueTimeseries <= 0 {
		errs = append(errs, fmt.Errorf("TotalUniqueTimeseries must be positive, got %d", c.TotalUniqueTimeseries))
	}
	if c.TotalSamples <= 0 {
		errs = append(errs, fmt.Errorf("TotalSamples must be positive, got %d", c.TotalSamples))
	}
	if c.TotalSamples < c.TotalUniqueTimeseries {
		errs = append(errs, fmt.Errorf("TotalSamples (%d) must be >= TotalUniqueTimeseries (%d)", c.TotalSamples, c.TotalUniqueTimeseries))
	}

	// Validate each tenant class.
	if err := c.SmallTenants.Validate("SmallTenants"); err != nil {
		errs = append(errs, err)
	}
	if err := c.MediumTenants.Validate("MediumTenants"); err != nil {
		errs = append(errs, err)
	}
	if err := c.LargeTenants.Validate("LargeTenants"); err != nil {
		errs = append(errs, err)
	}

	// Validate tenant distribution percentages sum to 100.
	tenantPctSum := c.SmallTenants.TenantPercent + c.MediumTenants.TenantPercent + c.LargeTenants.TenantPercent
	if tenantPctSum != 100 {
		errs = append(errs, fmt.Errorf("TenantPercent values must sum to 100, got %d (small=%d, medium=%d, large=%d)",
			tenantPctSum, c.SmallTenants.TenantPercent, c.MediumTenants.TenantPercent, c.LargeTenants.TenantPercent))
	}

	// Validate timeseries distribution percentages sum to 100.
	tsPctSum := c.SmallTenants.TimeseriesPercent + c.MediumTenants.TimeseriesPercent + c.LargeTenants.TimeseriesPercent
	if tsPctSum != 100 {
		errs = append(errs, fmt.Errorf("TimeseriesPercent values must sum to 100, got %d (small=%d, medium=%d, large=%d)",
			tsPctSum, c.SmallTenants.TimeseriesPercent, c.MediumTenants.TimeseriesPercent, c.LargeTenants.TimeseriesPercent))
	}

	// Validate label configuration.
	if c.AvgLabelsPerTimeseries <= 0 {
		errs = append(errs, fmt.Errorf("AvgLabelsPerTimeseries must be positive, got %d", c.AvgLabelsPerTimeseries))
	}
	if c.AvgLabelNameLength <= 0 {
		errs = append(errs, fmt.Errorf("AvgLabelNameLength must be positive, got %d", c.AvgLabelNameLength))
	}
	if c.AvgLabelValueLength <= 0 {
		errs = append(errs, fmt.Errorf("AvgLabelValueLength must be positive, got %d", c.AvgLabelValueLength))
	}
	if c.AvgMetricNameLength <= 0 {
		errs = append(errs, fmt.Errorf("AvgMetricNameLength must be positive, got %d", c.AvgMetricNameLength))
	}
	if c.NumUniqueLabelNames <= 0 {
		errs = append(errs, fmt.Errorf("NumUniqueLabelNames must be positive, got %d", c.NumUniqueLabelNames))
	}
	if c.NumUniqueLabelValues <= 0 {
		errs = append(errs, fmt.Errorf("NumUniqueLabelValues must be positive, got %d", c.NumUniqueLabelValues))
	}
	if c.NumUniqueLabelNames < c.AvgLabelsPerTimeseries {
		errs = append(errs, fmt.Errorf("NumUniqueLabelNames (%d) must be >= AvgLabelsPerTimeseries (%d) to avoid duplicate label names", c.NumUniqueLabelNames, c.AvgLabelsPerTimeseries))
	}

	return errors.Join(errs...)
}

// FixtureTenantClassConfig holds configuration for a tenant size class (small, medium, or large).
type FixtureTenantClassConfig struct {
	// TenantPercent is the percentage of tenants that belong to this class.
	// The sum of TenantPercent across all classes must equal 100.
	TenantPercent int

	// TimeseriesPercent is the percentage of total unique timeseries owned by this class.
	// The sum of TimeseriesPercent across all classes must equal 100.
	TimeseriesPercent int

	// AvgTimeseriesPerReq is the average number of timeseries per WriteRequest for this class.
	AvgTimeseriesPerReq float64
}

// Validate checks that the tenant class configuration is valid and returns an error if not.
func (c FixtureTenantClassConfig) Validate(className string) error {
	var errs []error

	if c.TenantPercent < 0 {
		errs = append(errs, fmt.Errorf("%s.TenantPercent must be non-negative, got %d", className, c.TenantPercent))
	}
	if c.TimeseriesPercent < 0 {
		errs = append(errs, fmt.Errorf("%s.TimeseriesPercent must be non-negative, got %d", className, c.TimeseriesPercent))
	}
	// AvgTimeseriesPerReq must be positive only if there are tenants in this class.
	if c.TenantPercent > 0 && c.AvgTimeseriesPerReq <= 0 {
		errs = append(errs, fmt.Errorf("%s.AvgTimeseriesPerReq must be positive, got %f", className, c.AvgTimeseriesPerReq))
	}

	return errors.Join(errs...)
}

type tenantSize int

const (
	tenantSizeSmall tenantSize = iota
	tenantSizeMedium
	tenantSizeLarge
)

type tenantProfile struct {
	id                    string
	size                  tenantSize
	uniqueSeries          int     // Number of unique timeseries for this tenant
	avgSeriesPerReq       float64 // Avg timeseries per WriteRequest
	globalSeriesIdxOffset int     // Global offset for this tenant's series indices
	nextSeriesIdx         int     // Rotation position within tenant's series (0 to uniqueSeries-1)
}

// FixtureGenerator generates realistic WriteRequest fixtures for benchmarks.
type FixtureGenerator struct {
	cfg     FixtureConfig
	rng     *rand.Rand
	tenants []tenantProfile

	// Cumulative weights for weighted tenant selection.
	// tenantWeights[i] = sum of uniqueTS for tenants 0..i
	// Used to select tenants proportionally to their timeseries count.
	tenantWeights []int
	totalWeight   int
}

// NewFixtureGenerator creates a generator with pre-allocated pools and tenant assignments.
// It returns an error if the configuration is invalid.
func NewFixtureGenerator(cfg FixtureConfig, numTenants int, seed int64) (*FixtureGenerator, error) {
	if numTenants <= 0 {
		return nil, fmt.Errorf("numTenants must be positive, got %d", numTenants)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid fixture config: %w", err)
	}

	rng := rand.New(rand.NewSource(seed))

	g := &FixtureGenerator{
		cfg: cfg,
		rng: rng,
	}

	// Calculate tenant counts per size category
	numSmall := numTenants * cfg.SmallTenants.TenantPercent / 100
	numMedium := numTenants * cfg.MediumTenants.TenantPercent / 100
	numLarge := numTenants - numSmall - numMedium // Remainder goes to large
	if numLarge < 0 {
		numLarge = 0
	}
	// Ensure at least 1 tenant if percentages round down to 0
	if numSmall == 0 && cfg.SmallTenants.TenantPercent > 0 {
		numSmall = 1
	}
	if numMedium == 0 && cfg.MediumTenants.TenantPercent > 0 {
		numMedium = 1
	}
	if numLarge == 0 && cfg.LargeTenants.TenantPercent > 0 {
		numLarge = 1
	}

	// Calculate timeseries per size category
	tsSmall := cfg.TotalUniqueTimeseries * cfg.SmallTenants.TimeseriesPercent / 100
	tsMedium := cfg.TotalUniqueTimeseries * cfg.MediumTenants.TimeseriesPercent / 100
	tsLarge := cfg.TotalUniqueTimeseries - tsSmall - tsMedium // Remainder goes to large

	// Create tenant profiles
	g.tenants = make([]tenantProfile, 0, numTenants)
	seriesIdx := 0

	// Helper to add tenants of a given size
	addTenants := func(numTenants, numSeries int, size tenantSize, classCfg FixtureTenantClassConfig) {
		if numTenants == 0 {
			return
		}
		seriesPerTenant := numSeries / numTenants
		seriesPerTenantRemainder := numSeries % numTenants

		for i := 0; i < numTenants; i++ {
			ts := seriesPerTenant
			if i < seriesPerTenantRemainder {
				ts++ // Distribute remainder across first tenants
			}

			g.tenants = append(g.tenants, tenantProfile{
				id:                    fmt.Sprintf("tenant-%d", len(g.tenants)),
				size:                  size,
				uniqueSeries:          ts,
				avgSeriesPerReq:       classCfg.AvgTimeseriesPerReq,
				globalSeriesIdxOffset: seriesIdx,
			})
			seriesIdx += ts
		}
	}

	addTenants(numSmall, tsSmall, tenantSizeSmall, cfg.SmallTenants)
	addTenants(numMedium, tsMedium, tenantSizeMedium, cfg.MediumTenants)
	addTenants(numLarge, tsLarge, tenantSizeLarge, cfg.LargeTenants)

	// Compute cumulative weights for weighted tenant selection.
	// Tenants with more timeseries should be selected more often.
	g.tenantWeights = make([]int, len(g.tenants))
	cumulative := 0
	for i := range g.tenants {
		cumulative += g.tenants[i].uniqueSeries
		g.tenantWeights[i] = cumulative
	}
	g.totalWeight = cumulative

	if g.totalWeight == 0 {
		return nil, fmt.Errorf("configuration results in zero total timeseries across all tenants")
	}

	return g, nil
}

func (g *FixtureGenerator) generatePaddedString(prefix string, idx, targetLen int) string {
	base := fmt.Sprintf("%s%d", prefix, idx)
	if len(base) >= targetLen {
		// Don't truncate to maintain uniqueness. Accept slightly longer strings
		// when the index requires more digits than targetLen allows.
		return base
	}
	return base + strings.Repeat("_", targetLen-len(base))
}

func (g *FixtureGenerator) generateSeriesLabels(idx int, builder *labels.Builder) labels.Labels {
	numLabels := g.cfg.AvgLabelsPerTimeseries
	metricName := g.generatePaddedString("metric_", idx, g.cfg.AvgMetricNameLength)

	builder.Reset(labels.EmptyLabels())
	builder.Set(model.MetricNameLabel, metricName)
	for i := 0; i < numLabels; i++ {
		// Generate label names and values deterministically
		labelNameIdx := (idx + i) % g.cfg.NumUniqueLabelNames
		labelValueIdx := (idx*numLabels + i) % g.cfg.NumUniqueLabelValues
		labelName := g.generatePaddedString("label_", labelNameIdx, g.cfg.AvgLabelNameLength)
		labelValue := g.generatePaddedString("value_", labelValueIdx, g.cfg.AvgLabelValueLength)
		builder.Set(labelName, labelValue)
	}

	return builder.Labels()
}

// GetNumTenants returns the total number of tenants.
func (g *FixtureGenerator) GetNumTenants() int {
	return len(g.tenants)
}

// GenerateNextWriteRequest selects the next tenant using weighted random selection
// and returns a WriteRequest for that tenant along with the tenant ID.
func (g *FixtureGenerator) GenerateNextWriteRequest(timestampMs int64) (string, *mimirpb.WriteRequest, error) {
	tenantIdx := g.selectNextTenant()
	tenantID := g.tenants[tenantIdx].id
	req, err := g.generateWriteRequest(tenantIdx, timestampMs)
	return tenantID, req, err
}

// selectNextTenant returns the next tenant index using weighted random selection.
// Tenants with more timeseries are selected more frequently, which produces
// the realistic pattern where large tenants generate more WriteRequests.
func (g *FixtureGenerator) selectNextTenant() int {
	r := g.rng.Intn(g.totalWeight)
	return sort.Search(len(g.tenantWeights), func(i int) bool {
		return g.tenantWeights[i] > r
	})
}

// generateWriteRequest returns a WriteRequest for the given tenant.
func (g *FixtureGenerator) generateWriteRequest(tenantIdx int, timestampMs int64) (*mimirpb.WriteRequest, error) {
	tenant := &g.tenants[tenantIdx]
	if tenant.uniqueSeries == 0 {
		return nil, nil
	}

	// Calculate number of timeseries for this request with +/- 20% variance.
	numSeries := int(math.Round(tenant.avgSeriesPerReq * (0.8 + 0.4*g.rng.Float64())))
	numSeries = max(1, min(numSeries, tenant.uniqueSeries))

	// Select series from tenant's pool, rotating through all series over time.
	timeseries := make([]mimirpb.PreallocTimeseries, numSeries)
	builder := labels.NewBuilder(labels.EmptyLabels())
	for i := 0; i < numSeries; i++ {
		// Get global series index using tenant's offset + rotation position
		globalSeriesIdx := tenant.globalSeriesIdxOffset + tenant.nextSeriesIdx

		timeseries[i] = mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels:  mimirpb.FromLabelsToLabelAdapters(g.generateSeriesLabels(globalSeriesIdx, builder)),
				Samples: []mimirpb.Sample{{TimestampMs: timestampMs, Value: float64(timestampMs)}},
			},
		}

		// Advance rotation for next series
		tenant.nextSeriesIdx = (tenant.nextSeriesIdx + 1) % tenant.uniqueSeries
	}

	req := &mimirpb.WriteRequest{
		Timeseries: timeseries,
		Source:     mimirpb.API,
	}

	// Marshal and then unmarshal the write request to get the buffer holder properly initialized.
	// This way we can also assert on use-after-free bugs in tests using the fixture generator.
	marshalled, err := req.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal WriteRequest: %w", err)
	}

	unmarshalled := &mimirpb.WriteRequest{}
	if err := unmarshalled.Unmarshal(marshalled); err != nil {
		return nil, fmt.Errorf("failed to unmarshal WriteRequest: %w", err)
	}

	return unmarshalled, nil
}

// ProduceWriteRequests generates WriteRequests and produces them to Kafka.
// It creates a Kafka client, produces all requests based on TotalSamples, and flushes.
// Returns the total number of records produced, or an error if any step fails.
func (g *FixtureGenerator) ProduceWriteRequests(ctx context.Context, kafkaAddress, topic string, partitionID int32) (int, error) {
	// Create a Kafka client for async batch production.
	cfg := KafkaConfig{}
	flagext.DefaultValues(&cfg)
	cfg.SetAddress(kafkaAddress)
	cfg.Topic = topic
	cfg.DisableLinger = true

	client, err := NewKafkaWriterClient(cfg, 20, log.NewNopLogger(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	defer client.Close()

	// Generate and produce all WriteRequests.
	var totalSamplesGenerated int
	var numRecordsProduced int
	serializer := versionOneRecordSerializer{}

	for totalSamplesGenerated < g.cfg.TotalSamples {
		tenantID, req, err := g.GenerateNextWriteRequest(int64(totalSamplesGenerated))
		if err != nil {
			return numRecordsProduced, fmt.Errorf("failed to generate WriteRequest: %w", err)
		}
		if req == nil {
			continue
		}

		// Count samples in this request.
		for _, ts := range req.Timeseries {
			totalSamplesGenerated += len(ts.Samples)
		}

		// Serialize the WriteRequest to Kafka records.
		records, err := serializer.ToRecords(partitionID, tenantID, req, math.MaxInt)
		if err != nil {
			return numRecordsProduced, fmt.Errorf("failed to serialize WriteRequest: %w", err)
		}

		// Produce asynchronously.
		for _, record := range records {
			client.Produce(ctx, record, nil)
			numRecordsProduced++
		}
	}

	// Wait for all records to be sent.
	if err := client.Flush(ctx); err != nil {
		return numRecordsProduced, fmt.Errorf("failed to flush Kafka client: %w", err)
	}

	return numRecordsProduced, nil
}
