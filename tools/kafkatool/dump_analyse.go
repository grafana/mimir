// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/common/model"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Tenant class boundaries based on unique timeseries count.
const (
	smallTenantMaxTimeseries  = 10_000
	mediumTenantMaxTimeseries = 100_000
)

// tenantAnalysis holds per-tenant statistics.
type tenantAnalysis struct {
	// Overall counters.
	totalRequests int
	totalSamples  int
	totalSeries   int

	// Unique timeseries (hash-based tracking).
	uniqueSeries map[uint64]struct{}

	// Label metrics.
	totalLabels          int
	totalLabelNameChars  int
	totalLabelValueChars int
	totalMetricNameChars int
	uniqueLabelNames     map[string]struct{}
	uniqueLabelValues    map[string]struct{}
}

func newTenantAnalysis() *tenantAnalysis {
	return &tenantAnalysis{
		uniqueSeries:      make(map[uint64]struct{}),
		uniqueLabelNames:  make(map[string]struct{}),
		uniqueLabelValues: make(map[string]struct{}),
	}
}

// dumpAnalysis holds the overall analysis results.
type dumpAnalysis struct {
	// Per-tenant statistics (keyed by tenant ID).
	tenants map[string]*tenantAnalysis
}

func newDumpAnalysis() *dumpAnalysis {
	return &dumpAnalysis{
		tenants: make(map[string]*tenantAnalysis),
	}
}

// tenantClassAnalysis holds aggregated statistics for a tenant size class.
type tenantClassAnalysis struct {
	name string

	// Number of tenants in this class.
	uniqueTenants int

	// Number of unique timeseries across all tenants in this class.
	uniqueSeries int

	// Total number of timeseries in requests across all tenants in this class.
	totalSeries int

	// Total number of requests across all tenants in this class.
	totalRequests int
}

func (c *DumpCommand) doAnalyse(_ *kingpin.ParseContext) error {
	analysis := newDumpAnalysis()

	// Buffer for label hashing (reused across records).
	hashBuf := make([]byte, 0, 1024)

	err := c.parseWriteRequestsFromDumpFile(
		func(_ int, record *kgo.Record, req *mimirpb.WriteRequest) {
			tenantID := string(record.Key)

			// Get or create tenant analysis.
			tenant, ok := analysis.tenants[tenantID]
			if !ok {
				tenant = newTenantAnalysis()
				analysis.tenants[tenantID] = tenant
			}

			// Update counters.
			tenant.totalRequests++
			tenant.totalSeries += len(req.Timeseries)

			for _, ts := range req.Timeseries {
				// Count samples.
				sampleCount := len(ts.Samples) + len(ts.Histograms)
				tenant.totalSamples += sampleCount

				// Track unique timeseries using label hash.
				var hash uint64
				hashBuf, hash = ingest.LabelAdaptersHash(hashBuf, ts.Labels)
				tenant.uniqueSeries[hash] = struct{}{}

				// Accumulate label metrics (excluding __name__ which is tracked separately).
				for _, label := range ts.Labels {
					if label.Name == model.MetricNameLabel {
						tenant.totalMetricNameChars += len(label.Value)
						continue
					}

					tenant.totalLabels++
					tenant.totalLabelNameChars += len(label.Name)
					tenant.totalLabelValueChars += len(label.Value)
					tenant.uniqueLabelNames[label.Name] = struct{}{}
					tenant.uniqueLabelValues[label.Value] = struct{}{}
				}
			}
		},
		func(recordIdx int, err error) {
			c.printer.PrintLine(fmt.Sprintf("corrupted record %d: %v", recordIdx, err))
		},
	)
	if err != nil {
		return err
	}

	// Print the analysis results.
	c.printAnalysis(analysis)

	return nil
}

func (c *DumpCommand) printAnalysis(analysis *dumpAnalysis) {
	// Calculate global metrics.
	var (
		totalRequests        int
		totalSamples         int
		uniqueSeries         int
		totalSeries          int
		totalLabels          int
		totalLabelNameChars  int
		totalLabelValueChars int
		totalMetricNameChars int
		uniqueLabelNames     = make(map[string]struct{})
		uniqueLabelValues    = make(map[string]struct{})
	)

	for _, tenantStats := range analysis.tenants {
		totalRequests += tenantStats.totalRequests
		totalSamples += tenantStats.totalSamples
		uniqueSeries += len(tenantStats.uniqueSeries)
		totalSeries += tenantStats.totalSeries
		totalLabels += tenantStats.totalLabels
		totalLabelNameChars += tenantStats.totalLabelNameChars
		totalLabelValueChars += tenantStats.totalLabelValueChars
		totalMetricNameChars += tenantStats.totalMetricNameChars

		for name := range tenantStats.uniqueLabelNames {
			uniqueLabelNames[name] = struct{}{}
		}
		for value := range tenantStats.uniqueLabelValues {
			uniqueLabelValues[value] = struct{}{}
		}
	}

	// Classify tenants and aggregate by class.
	small := tenantClassAnalysis{name: fmt.Sprintf("Small (<%dK)", smallTenantMaxTimeseries/1000)}
	medium := tenantClassAnalysis{name: fmt.Sprintf("Medium (<%dK)", mediumTenantMaxTimeseries/1000)}
	large := tenantClassAnalysis{name: fmt.Sprintf("Large (>=%dK)", mediumTenantMaxTimeseries/1000)}

	for _, tenantStats := range analysis.tenants {
		tenantUniqueSeries := len(tenantStats.uniqueSeries)
		var class *tenantClassAnalysis
		switch {
		case tenantUniqueSeries < smallTenantMaxTimeseries:
			class = &small
		case tenantUniqueSeries < mediumTenantMaxTimeseries:
			class = &medium
		default:
			class = &large
		}

		class.uniqueTenants++
		class.uniqueSeries += tenantUniqueSeries
		class.totalRequests += tenantStats.totalRequests
		class.totalSeries += tenantStats.totalSeries
	}

	// Print global metrics.
	c.printer.PrintLine("=== DUMP ANALYSIS ===")
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Total requests:", totalRequests))
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Total unique tenants:", len(analysis.tenants)))
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Total unique series:", uniqueSeries))
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Total samples:", totalSamples))
	c.printer.PrintLine(fmt.Sprintf("%-25s %.2f", "Avg samples/series:", safeDiv(float64(totalSamples), float64(uniqueSeries))))
	c.printer.PrintLine(fmt.Sprintf("%-25s %.2f", "Avg series/req:", safeDiv(float64(totalSeries), float64(totalRequests))))
	c.printer.PrintLine("")

	// Print tenant class distribution.
	c.printer.PrintLine("=== TENANT CLASS DISTRIBUTION (by unique timeseries) ===")
	c.printer.PrintLine(fmt.Sprintf("%-20s %10s %12s %15s", "Class", "Tenants", "Series", "Avg series/req"))

	for _, class := range []tenantClassAnalysis{small, medium, large} {
		tenantPct := safeDiv(float64(class.uniqueTenants)*100, float64(len(analysis.tenants)))
		seriesPct := safeDiv(float64(class.uniqueSeries)*100, float64(uniqueSeries))
		avgSeriesPerReq := safeDiv(float64(class.totalSeries), float64(class.totalRequests))
		c.printer.PrintLine(fmt.Sprintf("%-20s %9.1f%% %11.1f%% %15.2f", class.name, tenantPct, seriesPct, avgSeriesPerReq))
	}
	c.printer.PrintLine("")

	// Print label metrics (averages are per timeseries appearance in requests).
	c.printer.PrintLine("=== LABEL METRICS ===")
	c.printer.PrintLine(fmt.Sprintf("%-25s %.0f", "Avg labels/series:", safeDiv(float64(totalLabels), float64(totalSeries))))
	c.printer.PrintLine(fmt.Sprintf("%-25s %.0f", "Avg label name length:", safeDiv(float64(totalLabelNameChars), float64(totalLabels))))
	c.printer.PrintLine(fmt.Sprintf("%-25s %.0f", "Avg label value length:", safeDiv(float64(totalLabelValueChars), float64(totalLabels))))
	c.printer.PrintLine(fmt.Sprintf("%-25s %.0f", "Avg metric name length:", safeDiv(float64(totalMetricNameChars), float64(totalSeries))))
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Unique label names:", len(uniqueLabelNames)))
	c.printer.PrintLine(fmt.Sprintf("%-25s %d", "Unique label values:", len(uniqueLabelValues)))
}
