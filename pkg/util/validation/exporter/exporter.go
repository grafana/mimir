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
	"reflect"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
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
	ingestionArtificialDelay                   = "ingestion_artificial_delay"
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
	f.Var(&c.EnabledMetrics, "overrides-exporter.enabled-metrics", "Comma-separated list of metrics to include in the exporter. Metric names must match yaml tags from the limits section of the configuration.")
}

// Validate validates the configuration for an overrides-exporter.
func (c *Config) Validate() error {
	if err := c.Ring.Validate(); err != nil {
		return fmt.Errorf("invalid overrides-exporter.ring config: %w", err)
	}
	fieldRegistry := newLimitsFieldRegistry()
	for _, metricName := range c.EnabledMetrics {
		if err := fieldRegistry.ValidateMetricName(metricName); err != nil {
			return err
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
	enabledMetrics := util.NewAllowList(config.EnabledMetrics, nil)

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
			return nil, fmt.Errorf("failed to create ring/lifecycler: %w", err)
		}
	}

	exporter.Service = services.NewBasicService(exporter.starting, exporter.running, exporter.stopping)
	return exporter, nil
}

// convertToFloat64 converts various types to float64 for metric export
func convertToFloat64(value reflect.Value) (float64, error) {
	float64Type := reflect.TypeOf(float64(0))

	// Handle special case: model.Duration should be converted to seconds
	if value.Type().String() == "model.Duration" {
		duration := value.Interface().(model.Duration)
		return time.Duration(duration).Seconds(), nil
	}

	// Handle boolean: convert to 1.0 or 0.0
	if value.Kind() == reflect.Bool {
		if value.Bool() {
			return 1.0, nil
		}
		return 0.0, nil
	}

	// Try to convert to float64 directly
	if value.CanConvert(float64Type) {
		converted := value.Convert(float64Type)
		return converted.Float(), nil
	}

	// Return error if conversion is not possible
	return 0.0, fmt.Errorf("cannot convert type %s to float64", value.Type())
}

// LimitsFieldRegistry provides efficient field lookup and validation for validation.Limits struct
type LimitsFieldRegistry struct {
	limitsType         reflect.Type
	fieldsByYamlTag    map[string]reflect.StructField
	allowedMetricNames []string
	defaultLimitsValue reflect.Value
}

// newLimitsFieldRegistry creates a new registry for validation.Limits struct field operations
func newLimitsFieldRegistry() *LimitsFieldRegistry {
	limitsType := reflect.TypeOf(validation.Limits{})
	defaultLimits := &validation.Limits{}
	defaultLimitsValue := reflect.ValueOf(defaultLimits).Elem()

	fieldsByTag := make(map[string]reflect.StructField)
	var metricNames []string

	for i := 0; i < limitsType.NumField(); i++ {
		field := limitsType.Field(i)
		tag := field.Tag.Get("yaml")
		if tag != "" && tag != "-" {
			// Remove any options like ",omitempty"
			tagValue := strings.Split(tag, ",")[0]
			if tagValue != "" {
				fieldsByTag[tagValue] = field
				metricNames = append(metricNames, tagValue)
			}
		}
	}

	return &LimitsFieldRegistry{
		limitsType:         limitsType,
		fieldsByYamlTag:    fieldsByTag,
		allowedMetricNames: metricNames,
		defaultLimitsValue: defaultLimitsValue,
	}
}

// GetField returns the struct field for a given yaml tag name
func (r *LimitsFieldRegistry) GetField(yamlTag string) (reflect.StructField, bool) {
	field, found := r.fieldsByYamlTag[yamlTag]
	return field, found
}

// ValidateMetricName validates that a metric name exists and can be converted to float64
func (r *LimitsFieldRegistry) ValidateMetricName(metricName string) error {
	field, found := r.fieldsByYamlTag[metricName]
	if !found {
		return fmt.Errorf("enabled-metrics: unknown metric name '%s' - must match a yaml tag in the Limits struct", metricName)
	}

	// Attempt to convert the field type to float64 to ensure it's supported
	fieldValue := r.defaultLimitsValue.FieldByName(field.Name)
	if _, err := convertToFloat64(fieldValue); err != nil {
		return fmt.Errorf("enabled-metrics: metric '%s' has unsupported type %s - %v", metricName, fieldValue.Type(), err)
	}

	return nil
}

// GetAllowedMetricNames returns all available metric names
func (r *LimitsFieldRegistry) GetAllowedMetricNames() []string {
	return r.allowedMetricNames
}

func setupExportedMetrics(enabledMetrics *util.AllowList, extraMetrics []ExportedMetric) []ExportedMetric {
	var (
		exportedMetrics []ExportedMetric
		fieldRegistry   = newLimitsFieldRegistry()
	)

	// Get all possible metric names from global registry and check if each is enabled
	for _, metricName := range fieldRegistry.GetAllowedMetricNames() {
		if enabledMetrics.IsAllowed(metricName) {
			field, found := fieldRegistry.GetField(metricName)
			if !found {
				panic(fmt.Sprintf("couldn't find fields that the fields registry returned, this shouldn't happen (metric name: %s)", metricName))
			}
			fieldName := field.Name

			// Create a getter function for this field using a closure that captures fieldName
			exportedMetrics = append(exportedMetrics, ExportedMetric{
				Name: metricName,
				Get: func(fName string) func(*validation.Limits) float64 {
					return func(limits *validation.Limits) float64 {
						limitsValue := reflect.ValueOf(limits).Elem()
						fieldValue := limitsValue.FieldByName(fName)
						value, err := convertToFloat64(fieldValue)
						if err != nil {
							// Return 0.0 as fallback to avoid breaking metrics collection
							// This should not happen in practice if the reflection setup is correct
							return 0.0
						}
						return value
					}
				}(fieldName),
			})
		}
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
			if em.Get(limits) == em.Get(oe.defaultLimits) {
				// Skip exporting tenant limits that are the same as the default limits.
				// Note: this comes with an expected tradeoff where metrics, passed via Config.ExtraMetrics, and whose getter
				// doesn't depend on the passed limit cannot be exposed in the per-tenant limit overrides.
				continue
			}
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
	if oe.State() != services.Running {
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
