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
	"github.com/grafana/dskit/ring"
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
	ring              *overridesExporterRing
	subserviceManager *services.Manager
	subserviceWatcher *services.FailureWatcher
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
		logger:            log,
		subserviceWatcher: services.NewFailureWatcher(),
	}

	if config.Ring.Enabled {
		var err error

		exporter.ring, err = newRing(config.Ring, log, registerer)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create ring/lifecycler")
		}

		exporter.subserviceManager, err = services.NewManager(exporter.ring.lifecycler, exporter.ring.client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create service manager")
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
	// Write path limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.IngestionRate, "ingestion_rate")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.IngestionBurstSize), "ingestion_burst_size")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerUser), "max_global_series_per_user")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user")

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
		if !oe.ownsTenant(tenant) {
			continue
		}

		// Write path limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.IngestionRate, "ingestion_rate", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.IngestionBurstSize), "ingestion_burst_size", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerUser), "max_global_series_per_user", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user", tenant)

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

// ownsTenant determines whether this overrides-exporter instance owns the given
// tenant. If the ring is disabled, it assumes this instance has ownership of all
// tenants. If the ring is enabled, it uses it to check ownership of the given
// tenant.
func (oe *OverridesExporter) ownsTenant(tenantID string) bool {
	if oe.ring == nil {
		// If sharding is not enabled, every instance exports metrics for every tenant
		return true
	}
	owned, err := oe.ring.Owns(tenantID)
	if err != nil {
		level.Warn(oe.logger).Log("msg", "overrides-exporter failed to determine tenant ownership", "err", err.Error())
		// if there was an error establishing ownership using the ring, err on the safe
		// side and assume this instance owns the tenant
		return true
	}
	return owned
}

func (oe *OverridesExporter) starting(ctx context.Context) error {
	if oe.subserviceManager == nil {
		return nil
	}

	oe.subserviceWatcher.WatchManager(oe.subserviceManager)
	if err := services.StartManagerAndAwaitHealthy(ctx, oe.subserviceManager); err != nil {
		return errors.Wrap(err, "unable to start overrides-exporter subservice manager")
	}

	_ = level.Info(oe.logger).Log("msg", "waiting until overrides-exporter is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, oe.ring.client, oe.ring.lifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return errors.Wrap(err, "overrides-exporter failed to become ACTIVE in the ring")
	}
	_ = level.Info(oe.logger).Log("msg", "overrides-exporter is ACTIVE in the ring")

	return nil
}

func (oe *OverridesExporter) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-oe.subserviceWatcher.Chan():
		return errors.Wrap(err, "a subservice of overrides-exporter has failed")
	}
}

func (oe *OverridesExporter) stopping(error) error {
	if oe.subserviceManager == nil {
		return nil
	}

	return errors.Wrap(
		services.StopManagerAndAwaitStopped(context.Background(), oe.subserviceManager),
		"failed to stop overrides-exporter's subservice manager",
	)
}
