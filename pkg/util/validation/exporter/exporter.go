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
	AllowedMetricNames = []string{
		IngestionRate,
		IngestionBurstSize,
		MaxGlobalSeriesPerUser,
		MaxGlobalSeriesPerMetric,
		MaxGlobalExemplarsPerUser,
		MaxChunksPerQuery,
		MaxFetchedSeriesPerQuery,
		MaxFetchedChunkBytesPerQuery,
		RulerMaxRulesPerRuleGroup,
		RulerMaxRuleGroupsPerTenant,
		MaxGlobalMetricsWithMetadataPerUser,
		MaxGlobalMetadataPerMetric,
		RequestRate,
		RequestBurstSize,
		NotificationRateLimit,
		AlertmanagerMaxDispatcherAggregationGroups,
		AlertmanagerMaxAlertsCount,
		AlertmanagerMaxAlertsSizeBytes,
	}
	DefaultEnabledMetricNames = []string{
		IngestionRate,
		IngestionBurstSize,
		MaxGlobalSeriesPerUser,
		MaxGlobalSeriesPerMetric,
		MaxGlobalExemplarsPerUser,
		MaxChunksPerQuery,
		MaxFetchedSeriesPerQuery,
		MaxFetchedChunkBytesPerQuery,
		RulerMaxRulesPerRuleGroup,
		RulerMaxRuleGroupsPerTenant,
	}
)

const (
	IngestionRate                              = "ingestion_rate"
	IngestionBurstSize                         = "ingestion_burst_size"
	MaxGlobalSeriesPerUser                     = "max_global_series_per_user"
	MaxGlobalSeriesPerMetric                   = "max_global_series_per_metric"
	MaxGlobalExemplarsPerUser                  = "max_global_exemplars_per_user"
	MaxChunksPerQuery                          = "max_fetched_chunks_per_query"
	MaxFetchedSeriesPerQuery                   = "max_fetched_series_per_query"
	MaxFetchedChunkBytesPerQuery               = "max_fetched_chunk_bytes_per_query"
	RulerMaxRulesPerRuleGroup                  = "ruler_max_rules_per_rule_group"
	RulerMaxRuleGroupsPerTenant                = "ruler_max_rule_groups_per_tenant"
	MaxGlobalMetricsWithMetadataPerUser        = "max_global_metadata_per_user"
	MaxGlobalMetadataPerMetric                 = "max_global_metadata_per_metric"
	RequestRate                                = "request_rate"
	RequestBurstSize                           = "request_burst_size"
	NotificationRateLimit                      = "alertmanager_notification_rate_limit"
	AlertmanagerMaxDispatcherAggregationGroups = "alertmanager_max_dispatcher_aggregation_groups"
	AlertmanagerMaxAlertsCount                 = "alertmanager_max_alerts_count"
	AlertmanagerMaxAlertsSizeBytes             = "alertmanager_max_alerts_size_bytes"
)

// Config holds the configuration for an overrides-exporter
type Config struct {
	Ring           RingConfig             `yaml:"ring"`
	EnabledMetrics flagext.StringSliceCSV `yaml:"enabled_metrics"`
}

// RegisterFlags configs this instance to the given FlagSet
func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.Ring.RegisterFlags(f, logger)

	// Keep existing default metrics
	c.EnabledMetrics = DefaultEnabledMetricNames
	f.Var(&c.EnabledMetrics, "overrides-exporter.enabled-metrics", "Comma-separated list of metrics to include in the exporter. Allowed metric names: "+strings.Join(AllowedMetricNames, ", ")+".")
}

// Validate validates the configuration for an overrides-exporter.
func (c *Config) Validate() error {
	if err := c.Ring.Validate(); err != nil {
		return errors.Wrap(err, "invalid overrides-exporter.ring config")
	}
	for _, metricName := range c.EnabledMetrics {
		if !util.StringsContain(AllowedMetricNames, metricName) {
			return fmt.Errorf("enabled-metrics: metric name '%s' not allowed", metricName)
		}
	}
	return nil
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

	enabledMetrics *util.AllowedTenants
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

	exporter.enabledMetrics = util.NewAllowedTenants(config.EnabledMetrics, nil)

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
	if oe.enabledMetrics.IsAllowed(IngestionRate) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.IngestionRate, IngestionRate)
	}
	if oe.enabledMetrics.IsAllowed(IngestionBurstSize) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.IngestionBurstSize), IngestionBurstSize)
	}
	if oe.enabledMetrics.IsAllowed(MaxGlobalSeriesPerUser) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerUser), MaxGlobalSeriesPerUser)
	}
	if oe.enabledMetrics.IsAllowed(MaxGlobalSeriesPerMetric) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerMetric), MaxGlobalSeriesPerMetric)
	}
	if oe.enabledMetrics.IsAllowed(MaxGlobalExemplarsPerUser) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalExemplarsPerUser), MaxGlobalExemplarsPerUser)
	}
	if oe.enabledMetrics.IsAllowed(MaxGlobalMetricsWithMetadataPerUser) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalMetricsWithMetadataPerUser), MaxGlobalMetricsWithMetadataPerUser)
	}
	if oe.enabledMetrics.IsAllowed(MaxGlobalMetadataPerMetric) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalMetadataPerMetric), MaxGlobalMetadataPerMetric)
	}
	if oe.enabledMetrics.IsAllowed(RequestRate) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.RequestRate, RequestRate)
	}
	if oe.enabledMetrics.IsAllowed(RequestBurstSize) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RequestBurstSize), RequestBurstSize)
	}

	// Read path limits
	if oe.enabledMetrics.IsAllowed(MaxChunksPerQuery) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxChunksPerQuery), MaxChunksPerQuery)
	}
	if oe.enabledMetrics.IsAllowed(MaxFetchedSeriesPerQuery) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedSeriesPerQuery), MaxFetchedSeriesPerQuery)
	}
	if oe.enabledMetrics.IsAllowed(MaxFetchedChunkBytesPerQuery) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedChunkBytesPerQuery), MaxFetchedChunkBytesPerQuery)
	}

	// Ruler limits
	if oe.enabledMetrics.IsAllowed(RulerMaxRulesPerRuleGroup) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRulesPerRuleGroup), RulerMaxRulesPerRuleGroup)
	}
	if oe.enabledMetrics.IsAllowed(RulerMaxRuleGroupsPerTenant) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRuleGroupsPerTenant), RulerMaxRuleGroupsPerTenant)
	}

	// Alertmanager limits
	if oe.enabledMetrics.IsAllowed(NotificationRateLimit) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.NotificationRateLimit, NotificationRateLimit)
	}
	if oe.enabledMetrics.IsAllowed(AlertmanagerMaxDispatcherAggregationGroups) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.AlertmanagerMaxDispatcherAggregationGroups), AlertmanagerMaxDispatcherAggregationGroups)
	}
	if oe.enabledMetrics.IsAllowed(AlertmanagerMaxAlertsCount) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.AlertmanagerMaxAlertsCount), AlertmanagerMaxAlertsCount)
	}
	if oe.enabledMetrics.IsAllowed(AlertmanagerMaxAlertsSizeBytes) {
		ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.AlertmanagerMaxAlertsSizeBytes), AlertmanagerMaxAlertsSizeBytes)
	}

	// Do not export per-tenant limits if they've not been configured at all.
	if oe.tenantLimits == nil {
		return
	}

	allLimits := oe.tenantLimits.AllByUserID()
	for tenant, limits := range allLimits {
		// Write path limits
		if oe.enabledMetrics.IsAllowed(IngestionRate) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.IngestionRate, IngestionRate, tenant)
		}
		if oe.enabledMetrics.IsAllowed(IngestionBurstSize) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.IngestionBurstSize), IngestionBurstSize, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxGlobalSeriesPerUser) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerUser), MaxGlobalSeriesPerUser, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxGlobalSeriesPerMetric) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerMetric), MaxGlobalSeriesPerMetric, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxGlobalExemplarsPerUser) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalExemplarsPerUser), MaxGlobalExemplarsPerUser, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxGlobalMetricsWithMetadataPerUser) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalMetricsWithMetadataPerUser), MaxGlobalMetricsWithMetadataPerUser, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxGlobalMetadataPerMetric) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalMetadataPerMetric), MaxGlobalMetadataPerMetric, tenant)
		}
		if oe.enabledMetrics.IsAllowed(RequestRate) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.RequestRate, RequestRate, tenant)
		}
		if oe.enabledMetrics.IsAllowed(RequestBurstSize) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RequestBurstSize), RequestBurstSize, tenant)
		}

		// Read path limits
		if oe.enabledMetrics.IsAllowed(MaxChunksPerQuery) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxChunksPerQuery), MaxChunksPerQuery, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxFetchedSeriesPerQuery) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedSeriesPerQuery), MaxFetchedSeriesPerQuery, tenant)
		}
		if oe.enabledMetrics.IsAllowed(MaxFetchedChunkBytesPerQuery) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedChunkBytesPerQuery), MaxFetchedChunkBytesPerQuery, tenant)
		}

		// Ruler limits
		if oe.enabledMetrics.IsAllowed(RulerMaxRulesPerRuleGroup) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRulesPerRuleGroup), RulerMaxRulesPerRuleGroup, tenant)
		}
		if oe.enabledMetrics.IsAllowed(RulerMaxRuleGroupsPerTenant) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRuleGroupsPerTenant), RulerMaxRuleGroupsPerTenant, tenant)
		}

		// Alertmanager limits
		if oe.enabledMetrics.IsAllowed(NotificationRateLimit) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.NotificationRateLimit, NotificationRateLimit, tenant)
		}
		if oe.enabledMetrics.IsAllowed(AlertmanagerMaxDispatcherAggregationGroups) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.AlertmanagerMaxDispatcherAggregationGroups), AlertmanagerMaxDispatcherAggregationGroups, tenant)
		}
		if oe.enabledMetrics.IsAllowed(AlertmanagerMaxAlertsCount) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.AlertmanagerMaxAlertsCount), AlertmanagerMaxAlertsCount, tenant)
		}
		if oe.enabledMetrics.IsAllowed(AlertmanagerMaxAlertsSizeBytes) {
			ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.AlertmanagerMaxAlertsSizeBytes), AlertmanagerMaxAlertsSizeBytes, tenant)
		}
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
