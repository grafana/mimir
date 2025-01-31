// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package exporter

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	allowedMetricNames = []string{
		ingestionRate,
		ingestionBurstSize,
		ingestionArtificialDelay,
		maxGlobalSeriesPerUser,
		maxGlobalSeriesPerMetric,
		maxGlobalExemplarsPerUser,
		maxChunksPerQuery,
		maxFetchedSeriesPerQuery,
		maxFetchedChunkBytesPerQuery,
		rulerMaxRulesPerRuleGroup,
		rulerMaxRuleGroupsPerTenant,
		maxGlobalMetricsWithMetadataPerUser,
		maxGlobalMetadataPerMetric,
		requestRate,
		requestBurstSize,
		notificationRateLimit,
		alertmanagerMaxDispatcherAggregationGroups,
		alertmanagerMaxAlertsCount,
		alertmanagerMaxAlertsSizeBytes,
	}
	defaultEnabledMetricNames = []string{
		ingestionRate,
		ingestionBurstSize,
		maxGlobalSeriesPerUser,
		maxGlobalSeriesPerMetric,
		maxGlobalExemplarsPerUser,
		maxChunksPerQuery,
		maxFetchedSeriesPerQuery,
		maxFetchedChunkBytesPerQuery,
		rulerMaxRulesPerRuleGroup,
		rulerMaxRuleGroupsPerTenant,
	}
)

const (
	ingestionRate                              = "ingestion_rate"
	ingestionBurstSize                         = "ingestion_burst_size"
	ingestionArtificialDelay                   = "ingestion_artificial_delay_msec"
	maxGlobalSeriesPerUser                     = "max_global_series_per_user"
	maxGlobalSeriesPerMetric                   = "max_global_series_per_metric"
	maxGlobalExemplarsPerUser                  = "max_global_exemplars_per_user"
	maxChunksPerQuery                          = "max_fetched_chunks_per_query"
	maxFetchedSeriesPerQuery                   = "max_fetched_series_per_query"
	maxFetchedChunkBytesPerQuery               = "max_fetched_chunk_bytes_per_query"
	maxEstimatedChunksPerQueryMultiplier       = "max_estimated_fetched_chunks_per_query_multiplier"
	rulerMaxRulesPerRuleGroup                  = "ruler_max_rules_per_rule_group"
	rulerMaxRuleGroupsPerTenant                = "ruler_max_rule_groups_per_tenant"
	maxGlobalMetricsWithMetadataPerUser        = "max_global_metadata_per_user"
	maxGlobalMetadataPerMetric                 = "max_global_metadata_per_metric"
	requestRate                                = "request_rate"
	requestBurstSize                           = "request_burst_size"
	notificationRateLimit                      = "alertmanager_notification_rate_limit"
	alertmanagerMaxDispatcherAggregationGroups = "alertmanager_max_dispatcher_aggregation_groups"
	alertmanagerMaxAlertsCount                 = "alertmanager_max_alerts_count"
	alertmanagerMaxAlertsSizeBytes             = "alertmanager_max_alerts_size_bytes"
)

// Config holds the configuration for an overrides-exporter
type Config struct {
	Ring           RingConfig             `yaml:"ring"`
	EnabledMetrics flagext.StringSliceCSV `yaml:"enabled_metrics"`

	// This allows downstream projects to define their own metrics and expose them via the exporter.
	// Donwstream projects should be responsible for enabling/disabling their own metrics,
	// so these won't be checked against EnabledMetrics.
	ExtraMetrics []ExportedMetric `yaml:"-"`
}

type ExportedMetric struct {
	Name string
	Get  func(limits *validation.Limits) float64
}

// RegisterFlags configs this instance to the given FlagSet
func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.Ring.RegisterFlags(f, logger)

	// Keep existing default metrics
	c.EnabledMetrics = defaultEnabledMetricNames
	f.Var(&c.EnabledMetrics, "overrides-exporter.enabled-metrics", "Comma-separated list of metrics to include in the exporter. Allowed metric names: "+strings.Join(allowedMetricNames, ", ")+".")
}

// Validate validates the configuration for an overrides-exporter.
func (c *Config) Validate() error {
	if err := c.Ring.Validate(); err != nil {
		return errors.Wrap(err, "invalid overrides-exporter.ring config")
	}
	for _, metricName := range c.EnabledMetrics {
		if !util.StringsContain(allowedMetricNames, metricName) {
			return fmt.Errorf("enabled-metrics: unknown metric name '%s'", metricName)
		}
	}
	return nil
}

// OverridesExporter exposes per-tenant resource limit overrides as Prometheus metrics
type OverridesExporter struct {
	services.Service

	defaultLimits       *validation.Limits
	tenantLimits        validation.TenantLimits
	exportedMetrics     []ExportedMetric
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
	enabledMetrics := util.NewAllowedTenants(config.EnabledMetrics, nil)

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
		exportedMetrics: setupExportedMetrics(enabledMetrics, config.ExtraMetrics),
		logger:          log,
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

func setupExportedMetrics(enabledMetrics *util.AllowedTenants, extraMetrics []ExportedMetric) []ExportedMetric {
	var exportedMetrics []ExportedMetric

	// Write path limits
	if enabledMetrics.IsAllowed(ingestionRate) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{ingestionRate, func(limits *validation.Limits) float64 { return limits.IngestionRate }})
	}
	if enabledMetrics.IsAllowed(ingestionBurstSize) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{ingestionBurstSize, func(limits *validation.Limits) float64 { return float64(limits.IngestionBurstSize) }})
	}
	if enabledMetrics.IsAllowed(ingestionArtificialDelay) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{ingestionArtificialDelay, func(limits *validation.Limits) float64 {
			return float64(time.Duration(limits.IngestionArtificialDelay) / time.Millisecond)
		}})
	}
	if enabledMetrics.IsAllowed(maxGlobalSeriesPerUser) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxGlobalSeriesPerUser, func(limits *validation.Limits) float64 { return float64(limits.MaxGlobalSeriesPerUser) }})
	}
	if enabledMetrics.IsAllowed(maxGlobalSeriesPerMetric) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxGlobalSeriesPerMetric, func(limits *validation.Limits) float64 { return float64(limits.MaxGlobalSeriesPerMetric) }})
	}
	if enabledMetrics.IsAllowed(maxGlobalExemplarsPerUser) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxGlobalExemplarsPerUser, func(limits *validation.Limits) float64 { return float64(limits.MaxGlobalExemplarsPerUser) }})
	}
	if enabledMetrics.IsAllowed(maxGlobalMetricsWithMetadataPerUser) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxGlobalMetricsWithMetadataPerUser, func(limits *validation.Limits) float64 { return float64(limits.MaxGlobalMetricsWithMetadataPerUser) }})
	}
	if enabledMetrics.IsAllowed(maxGlobalMetadataPerMetric) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxGlobalMetadataPerMetric, func(limits *validation.Limits) float64 { return float64(limits.MaxGlobalMetadataPerMetric) }})
	}
	if enabledMetrics.IsAllowed(requestRate) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{requestRate, func(limits *validation.Limits) float64 { return limits.RequestRate }})
	}
	if enabledMetrics.IsAllowed(requestBurstSize) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{requestBurstSize, func(limits *validation.Limits) float64 { return float64(limits.RequestBurstSize) }})
	}

	// Read path limits
	if enabledMetrics.IsAllowed(maxChunksPerQuery) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxChunksPerQuery, func(limits *validation.Limits) float64 { return float64(limits.MaxChunksPerQuery) }})
	}
	if enabledMetrics.IsAllowed(maxFetchedSeriesPerQuery) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxFetchedSeriesPerQuery, func(limits *validation.Limits) float64 { return float64(limits.MaxFetchedSeriesPerQuery) }})
	}
	if enabledMetrics.IsAllowed(maxFetchedChunkBytesPerQuery) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxFetchedChunkBytesPerQuery, func(limits *validation.Limits) float64 { return float64(limits.MaxFetchedChunkBytesPerQuery) }})
	}
	if enabledMetrics.IsAllowed(maxEstimatedChunksPerQueryMultiplier) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{maxEstimatedChunksPerQueryMultiplier, func(limits *validation.Limits) float64 { return limits.MaxEstimatedChunksPerQueryMultiplier }})
	}

	// Ruler limits
	if enabledMetrics.IsAllowed(rulerMaxRulesPerRuleGroup) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{rulerMaxRulesPerRuleGroup, func(limits *validation.Limits) float64 { return float64(limits.RulerMaxRulesPerRuleGroup) }})
	}
	if enabledMetrics.IsAllowed(rulerMaxRuleGroupsPerTenant) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{rulerMaxRuleGroupsPerTenant, func(limits *validation.Limits) float64 { return float64(limits.RulerMaxRuleGroupsPerTenant) }})
	}

	// Alertmanager limits
	if enabledMetrics.IsAllowed(notificationRateLimit) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{notificationRateLimit, func(limits *validation.Limits) float64 { return limits.NotificationRateLimit }})
	}
	if enabledMetrics.IsAllowed(alertmanagerMaxDispatcherAggregationGroups) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{alertmanagerMaxDispatcherAggregationGroups, func(limits *validation.Limits) float64 {
			return float64(limits.AlertmanagerMaxDispatcherAggregationGroups)
		}})
	}
	if enabledMetrics.IsAllowed(alertmanagerMaxAlertsCount) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{alertmanagerMaxAlertsCount, func(limits *validation.Limits) float64 { return float64(limits.AlertmanagerMaxAlertsCount) }})
	}
	if enabledMetrics.IsAllowed(alertmanagerMaxAlertsSizeBytes) {
		exportedMetrics = append(exportedMetrics, ExportedMetric{alertmanagerMaxAlertsSizeBytes, func(limits *validation.Limits) float64 { return float64(limits.AlertmanagerMaxAlertsSizeBytes) }})
	}

	// Add extra exported metrics
	exportedMetrics = append(exportedMetrics, extraMetrics...)

	return exportedMetrics
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

	// default limits
	for _, em := range oe.exportedMetrics {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, em.Get(oe.defaultLimits), em.Name)
	}

	// Do not export per-tenant limits if they've not been configured at all.
	if oe.tenantLimits == nil {
		return
	}

	allLimits := oe.tenantLimits.AllByUserID()
	for tenant, limits := range allLimits {
		for _, em := range oe.exportedMetrics {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, em.Get(limits), em.Name, tenant)
		}
	}
}

// RingHandler is a http.Handler that serves requests for the overrides-exporter ring status page
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
