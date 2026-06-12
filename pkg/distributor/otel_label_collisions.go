// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"slices"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// otelServiceNameAttr, otelServiceNamespaceAttr and otelServiceInstanceIDAttr are the identifying
// resource attributes that the OTLP translator converts to job and instance labels and, by default,
// drops from target_info. See addResourceTargetInfo in
// vendor/github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite/helper.go.
const (
	otelServiceNameAttr       = "service.name"
	otelServiceNamespaceAttr  = "service.namespace"
	otelServiceInstanceIDAttr = "service.instance.id"
)

// maxLoggedTargetInfoLabelCollisions caps detail log lines per request; the
// remainder is reported in a single summary line. The cap bounds both log and
// trace span volume (spanlogger records events on sampled spans regardless of
// the log-level filter).
const maxLoggedTargetInfoLabelCollisions = 10

// maxMetricNamesPerCollisionLog caps the number of metric names included in a
// collision detail line; the full count is always included via metrics_total.
const maxMetricNamesPerCollisionLog = 3

// logTargetInfoLabelCollisions logs, at debug level, resource attributes that
// sanitize to the same Prometheus label name. The OTLP translator may
// concatenate such values with ';' in target_info labels (empty values are
// overwritten or dropped instead).
//
// Each detail line includes up to maxMetricNamesPerCollisionLog metric names
// plus a total count to help locate the offending resource; names come from
// the first resource that produced the (deduplicated) collision.
//
// When opts.allowUTF8 is true the function logs nothing, because LabelNamer.Build
// returns names unchanged and sanitization collisions cannot occur. Callers are
// expected to skip the call in that mode, but the behavior is safe either way.
//
// This mirrors logic of the vendored translator. On vendor updates of
// github.com/prometheus/prometheus, re-check:
//   - createAttributes and addResourceTargetInfo in storage/remote/otlptranslator/prometheusremotewrite/helper.go
//   - setResourceContext in storage/remote/otlptranslator/prometheusremotewrite/metrics_to_prw.go
//
// Out of scope: data point attribute collisions, job/instance/__name__
// overwrites of attribute-derived labels, and promoted resource attributes and
// scope labels (the translator excludes the latter two from target_info).
func logTargetInfoLabelCollisions(md pmetric.Metrics, opts conversionOptions, logger log.Logger) {
	namer := otlptranslator.LabelNamer{
		UTF8Allowed:                 opts.allowUTF8,
		UnderscoreLabelSanitization: opts.underscoreSanitization,
		PreserveMultipleUnderscores: opts.preserveMultipleUnderscores,
	}

	seen := make(map[string]struct{})
	logged := 0
	suppressed := 0
	affectedResources := 0

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		attrs := rm.Resource().Attributes()

		if !targetInfoWouldBeGenerated(rm, attrs) {
			continue
		}
		resourceSuppressed := false

		job, instance := otelResourceIdentity(attrs)

		// Group original attribute names by their sanitized label name.
		// The translator fails the whole resource on an invalid attribute name,
		// emitting no target_info, so the scan skips it too.
		byLabel := make(map[string][]string, attrs.Len())
		var buildErr error
		attrs.Range(func(key string, _ pcommon.Value) bool {
			if !opts.keepIdentifyingResourceAttributes && isIdentifyingOTelAttr(key) {
				return true
			}
			name, err := namer.Build(key)
			if err != nil {
				buildErr = err
				return false
			}
			byLabel[name] = append(byLabel[name], key)
			return true
		})
		if buildErr != nil {
			continue
		}

		// Sort colliding label names for deterministic output.
		var collidingLabels []string
		for name, attrNames := range byLabel {
			if len(attrNames) > 1 {
				collidingLabels = append(collidingLabels, name)
			}
		}
		slices.Sort(collidingLabels)

		// Metric names are gathered lazily: only when a detail line is actually
		// emitted for this resource, to avoid cost on the no-collision path and
		// for resources whose collisions are all deduped or suppressed.
		var metricNames []string
		var metricsTotal int
		metricNamesGathered := false

		for _, name := range collidingLabels {
			attrNames := byLabel[name]
			slices.Sort(attrNames)
			attributes := strings.Join(attrNames, ",")

			dedupKey := name + "\x00" + attributes + "\x00" + job + "\x00" + instance
			if _, ok := seen[dedupKey]; ok {
				continue
			}
			seen[dedupKey] = struct{}{}

			if logged >= maxLoggedTargetInfoLabelCollisions {
				suppressed++
				resourceSuppressed = true
				continue
			}

			if !metricNamesGathered {
				metricNames, metricsTotal = resourceMetricNames(rm, maxMetricNamesPerCollisionLog)
				metricNamesGathered = true
			}

			logged++
			// TODO: the translator appends the colliding value even when it is
			// identical to the existing one, producing e.g. "val;val". Skipping
			// the append for identical values needs an upstream change in
			// prometheus/prometheus createAttributes (helper.go); revisit this
			// log line's wording if that lands.
			level.Debug(logger).Log(
				"msg", "OTLP resource attributes collide after label name sanitization, values may be concatenated in target_info",
				"label", name,
				"attributes", attributes,
				"job", job,
				"instance", instance,
				"metrics", strings.Join(metricNames, ","),
				"metrics_total", metricsTotal,
			)
		}
		// affectedResources counts resources whose novel (not deduplicated) collisions
		// were suppressed by the cap; a resource whose suppressed collisions were all
		// duplicates of already-counted ones is not counted again.
		if resourceSuppressed {
			affectedResources++
		}
	}

	if suppressed > 0 {
		level.Debug(logger).Log(
			"msg", "additional OTLP resource attribute label name collisions were suppressed",
			"suppressed_collisions", suppressed,
			"affected_resources", affectedResources,
		)
	}
}

// targetInfoWouldBeGenerated mirrors the translator's target_info eligibility
// checks: the resource must have non-identifying attributes, a job or instance
// identity, and at least one data point.
// Known false negative: a literal job or instance resource attribute also
// satisfies the translator's identity check; that rare case is not mirrored.
// Empty service.name and service.instance.id values are treated as absent,
// like the translator's label builder does.
func targetInfoWouldBeGenerated(rm pmetric.ResourceMetrics, attrs pcommon.Map) bool {
	nonIdentifying := attrs.Len()
	for _, a := range []string{otelServiceNameAttr, otelServiceNamespaceAttr, otelServiceInstanceIDAttr} {
		if _, ok := attrs.Get(a); ok {
			nonIdentifying--
		}
	}
	if nonIdentifying == 0 {
		return false
	}

	serviceName, haveServiceName := attrs.Get(otelServiceNameAttr)
	instanceID, haveInstanceID := attrs.Get(otelServiceInstanceIDAttr)
	if (!haveServiceName || serviceName.AsString() == "") && (!haveInstanceID || instanceID.AsString() == "") {
		return false
	}

	return resourceHasDataPoints(rm)
}

func isIdentifyingOTelAttr(key string) bool {
	return key == otelServiceNameAttr || key == otelServiceNamespaceAttr || key == otelServiceInstanceIDAttr
}

// otelResourceIdentity derives the job and instance label values the same way
// as the translator's setResourceContext.
func otelResourceIdentity(attrs pcommon.Map) (job, instance string) {
	if serviceName, ok := attrs.Get(otelServiceNameAttr); ok {
		job = serviceName.AsString()
		if serviceNamespace, ok := attrs.Get(otelServiceNamespaceAttr); ok {
			job = serviceNamespace.AsString() + "/" + job
		}
	}
	if instanceID, ok := attrs.Get(otelServiceInstanceIDAttr); ok {
		instance = instanceID.AsString()
	}
	return job, instance
}

// resourceMetricNames returns up to max metric names of the resource plus the
// total number of metrics it carries, to help locate the source of a collision.
func resourceMetricNames(rm pmetric.ResourceMetrics, max int) (names []string, total int) {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if len(names) < max {
				names = append(names, ms.At(j).Name())
			}
			total++
		}
	}
	return names, total
}

func resourceHasDataPoints(rm pmetric.ResourceMetrics) bool {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			m := ms.At(j)
			switch m.Type() {
			case pmetric.MetricTypeGauge:
				if m.Gauge().DataPoints().Len() > 0 {
					return true
				}
			case pmetric.MetricTypeSum:
				if m.Sum().DataPoints().Len() > 0 {
					return true
				}
			case pmetric.MetricTypeHistogram:
				if m.Histogram().DataPoints().Len() > 0 {
					return true
				}
			case pmetric.MetricTypeExponentialHistogram:
				if m.ExponentialHistogram().DataPoints().Len() > 0 {
					return true
				}
			case pmetric.MetricTypeSummary:
				if m.Summary().DataPoints().Len() > 0 {
					return true
				}
			}
		}
	}
	return false
}
