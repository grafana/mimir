// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package exporter

import (
	"context"
	"flag"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Config holds the configuration for an overrides-exporter
type Config struct {
	Ring RingConfig `yaml:"ring"`
}

// RegisterFlags configs this instance to the given FlagSet
func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.Ring.RegisterFlags(f, logger)
}

// Validate validates the configuration for an overrides-exporter.
func (c *Config) Validate() error {
	return c.Ring.Validate()
}

// OverridesExporter exposes per-tenant resource limit overrides as Prometheus metrics
type OverridesExporter struct {
	services.Service

	defaultLimits       *validation.Limits
	tenantLimits        validation.TenantLimits
	overrideDescription *prometheus.Desc
	defaultsDescription *prometheus.Desc
	logger              log.Logger

	// OverridesExporter can optionally use a ring to uniquely shard tenants to
	// instances and avoid export of duplicate metrics.
	ring *overridesExporterRing
}

// NewOverridesExporter creates an OverridesExporter that reads updates to per-tenant
// limits using the provided function.
func NewOverridesExporter(
	config Config,
	defaultLimits *validation.Limits,
	tenantLimits validation.TenantLimits,
	log log.Logger,
	registerer prometheus.Registerer,
) (*OverridesExporter, error) {
	exporter := &OverridesExporter{
		defaultLimits: defaultLimits,
		tenantLimits:  tenantLimits,
		overrideDescription: prometheus.NewDesc(
			"cortex_limits_overrides",
			"Resource limit overrides applied to tenants",
			[]string{"limit_name", "user"},
			nil,
		),
		defaultsDescription: prometheus.NewDesc(
			"cortex_limits_defaults",
			"Resource limit defaults for tenants without overrides",
			[]string{"limit_name"},
			nil,
		),
		logger: log,
	}

	if config.Ring.Enabled {
		var err error

		exporter.ring, err = newRing(config.Ring, log, registerer)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create ring/lifecycler")
		}
	}

	exporter.Service = services.NewBasicService(exporter.starting, exporter.running, exporter.stopping)
	return exporter, nil
}

func (oe *OverridesExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- oe.defaultsDescription
	ch <- oe.overrideDescription
}

func (oe *OverridesExporter) Collect(ch chan<- prometheus.Metric) {
	if !oe.isLeader() {
		// If another replica is the leader, don't expose any metrics from this one.
		return
	}

	// Write path limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.IngestionRate, "ingestion_rate")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.IngestionBurstSize), "ingestion_burst_size")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerUser), "max_global_series_per_user")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalMetricsWithMetadataPerUser), "max_global_metadata_per_user")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalMetadataPerMetric), "max_global_metadata_per_metric")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.RequestRate, "request_rate")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RequestBurstSize), "request_burst_size")

	// Read path limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxChunksPerQuery), "max_fetched_chunks_per_query")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedSeriesPerQuery), "max_fetched_series_per_query")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedChunkBytesPerQuery), "max_fetched_chunk_bytes_per_query")

	// Ruler limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRulesPerRuleGroup), "ruler_max_rules_per_rule_group")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRuleGroupsPerTenant), "ruler_max_rule_groups_per_tenant")

	// Do not export per-tenant limits if they've not been configured at all.
	if oe.tenantLimits == nil {
		return
	}

	allLimits := oe.tenantLimits.AllByUserID()
	for tenant, limits := range allLimits {
		// Write path limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.IngestionRate, "ingestion_rate", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.IngestionBurstSize), "ingestion_burst_size", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerUser), "max_global_series_per_user", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalMetricsWithMetadataPerUser), "max_global_metadata_per_user", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalMetadataPerMetric), "max_global_metadata_per_metric", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.RequestRate, "request_rate", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RequestBurstSize), "request_burst_size", tenant)

		// Read path limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxChunksPerQuery), "max_fetched_chunks_per_query", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedSeriesPerQuery), "max_fetched_series_per_query", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedChunkBytesPerQuery), "max_fetched_chunk_bytes_per_query", tenant)

		// Ruler limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRulesPerRuleGroup), "ruler_max_rules_per_rule_group", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRuleGroupsPerTenant), "ruler_max_rule_groups_per_tenant", tenant)
	}
}

// RingHandler is an http.Handler that serves requests for the overrides-exporter ring status page
func (oe *OverridesExporter) RingHandler(w http.ResponseWriter, req *http.Request) {
	if oe.ring != nil {
		oe.ring.lifecycler.ServeHTTP(w, req)
		return
	}

	ringDisabledPage := `
		<!DOCTYPE html>
		<html>
			<head>
				<meta charset="UTF-8">
				<title>Overrides-exporter Status</title>
			</head>
			<body>
				<h1>Overrides-exporter Status</h1>
				<p>Overrides-exporter hash ring is disabled.</p>
			</body>
		</html>`
	util.WriteHTMLResponse(w, ringDisabledPage)
}

// isLeader determines whether this overrides-exporter instance is the leader
// replica that exports all limit metrics. If the ring is disabled, leadership is
// assumed. If the ring is enabled, it is used to determine which ring member is
// the leader replica.
func (oe *OverridesExporter) isLeader() bool {
	if oe.ring == nil {
		// If the ring is not enabled, export all metrics
		return true
	}
	if oe.Service.State() != services.Running {
		// We haven't finished startup yet, likely waiting for ring stability.
		return false
	}
	isLeaderNow, err := oe.ring.isLeader()
	if err != nil {
		// If there was an error establishing ownership using the ring, log a warning and
		// default to not exporting metrics to keep series churn low for transient ring
		// issues.
		level.Warn(oe.logger).Log("msg", "overrides-exporter failed to determine ring leader", "err", err.Error())
		return false
	}
	return isLeaderNow
}

func (oe *OverridesExporter) starting(ctx context.Context) error {
	if oe.ring == nil {
		return nil
	}
	return oe.ring.starting(ctx)
}

func (oe *OverridesExporter) running(ctx context.Context) error {
	if oe.ring == nil {
		<-ctx.Done()
		return nil
	}
	return oe.ring.running(ctx)
}

func (oe *OverridesExporter) stopping(err error) error {
	if oe.ring == nil {
		return nil
	}
	return oe.ring.stopping(err)
}
