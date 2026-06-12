// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func collisionScanOpts() conversionOptions {
	return conversionOptions{
		allowUTF8:                   false,
		underscoreSanitization:      true,
		preserveMultipleUnderscores: true,
	}
}

// addGaugeDataPoint adds a gauge metric with one data point so the resource
// qualifies for target_info generation.
func addGaugeDataPoint(rm pmetric.ResourceMetrics) {
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
}

func TestLogTargetInfoLabelCollisions(t *testing.T) {
	const collisionMsg = "collide after label name sanitization"

	for name, tc := range map[string]struct {
		buildMetrics  func() pmetric.Metrics
		opts          conversionOptions
		expectedLines []string // substrings that must each appear in exactly one line
		expectNoLog   bool
	}{
		"collision is detected and logged with resource identity": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("service.namespace", "ns")
				attrs.PutStr("service.instance.id", "inst")
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				addGaugeDataPoint(rm)
				return md
			},
			opts: collisionScanOpts(),
			expectedLines: []string{
				"label=k8s_pod_name attributes=k8s.pod.name,k8s_pod_name job=ns/svc instance=inst",
			},
		},
		"no collision produces no log": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"identifying attributes are excluded by default": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				// Would collide with sanitized "service.name", but identifying
				// attrs are dropped from target_info by default.
				attrs.PutStr("service_name", "x")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"identifying attributes participate when kept": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("service_name", "x")
				addGaugeDataPoint(rm)
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				o.keepIdentifyingResourceAttributes = true
				return o
			}(),
			expectedLines: []string{
				"label=service_name attributes=service.name,service_name job=svc instance=",
			},
		},
		"resource with only identifying attributes is skipped": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("service.namespace", "ns")
				addGaugeDataPoint(rm)
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				// Even with keep enabled, target_info isn't generated when all
				// attributes are identifying, so nothing must be logged.
				o.keepIdentifyingResourceAttributes = true
				return o
			}(),
			expectNoLog: true,
		},
		"resource without job or instance identity is skipped": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"resource without data points is skipped": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				// Metric present but it has no data points.
				m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
				m.SetName("empty_metric")
				m.SetEmptyGauge()
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"identical collisions are deduplicated across resources": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				for i := 0; i < 2; i++ {
					rm := md.ResourceMetrics().AppendEmpty()
					attrs := rm.Resource().Attributes()
					attrs.PutStr("service.name", "svc")
					attrs.PutStr("k8s.pod.name", "foo")
					attrs.PutStr("k8s_pod_name", "bar")
					addGaugeDataPoint(rm)
				}
				return md
			},
			opts: collisionScanOpts(),
			expectedLines: []string{
				"label=k8s_pod_name attributes=k8s.pod.name,k8s_pod_name job=svc instance=",
			},
		},
		"promoted resource attributes are not an extra collision source": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				addGaugeDataPoint(rm)
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				// Promotion re-adds resource attributes to regular series, but
				// the translator nils promotion for target_info; the scan must
				// not treat promoted labels as a collision source.
				o.promoteResourceAttributes = []string{"k8s.pod.name", "k8s_pod_name"}
				return o
			}(),
			expectNoLog: true,
		},
		"colliding scope attributes are ignored, only resource attributes are scanned": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				sm := rm.ScopeMetrics().AppendEmpty()
				sm.Scope().SetName("scope")
				// Scope attributes never end up in target_info (the translator
				// clears scope labels for it), so they must not be scanned.
				sm.Scope().Attributes().PutStr("k8s.pod.name", "x")
				sm.Scope().Attributes().PutStr("k8s_pod_name", "y")
				m := sm.Metrics().AppendEmpty()
				m.SetName("test_metric")
				dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
				dp.SetDoubleValue(1)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				o.promoteScopeMetadata = true
				return o
			}(),
			expectNoLog: true,
		},
		"namer settings are honored: preserveMultipleUnderscores=true keeps names distinct": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("foo.bar", "a")
				attrs.PutStr("foo__bar", "b")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"namer settings are honored: preserveMultipleUnderscores=false collapses to a collision": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("foo.bar", "a")
				attrs.PutStr("foo__bar", "b")
				addGaugeDataPoint(rm)
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				o.preserveMultipleUnderscores = false
				return o
			}(),
			expectedLines: []string{
				"label=foo_bar attributes=foo.bar,foo__bar job=svc instance=",
			},
		},
		"empty identifying values do not count as identity": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "") // empty string is treated as absent
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"resource with an invalid attribute name is skipped like the translator does": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				// "__" normalizes to "__" which has only underscores: Build errors.
				// The translator fails the whole resource on such a name, emitting
				// no target_info, so the scan must not log for this resource either.
				attrs.PutStr("__", "x")
				addGaugeDataPoint(rm)
				return md
			},
			opts:        collisionScanOpts(),
			expectNoLog: true,
		},
		"UTF-8 mode never logs, sanitization collisions cannot occur": {
			buildMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				attrs := rm.Resource().Attributes()
				attrs.PutStr("service.name", "svc")
				attrs.PutStr("k8s.pod.name", "foo")
				attrs.PutStr("k8s_pod_name", "bar")
				addGaugeDataPoint(rm)
				return md
			},
			opts: func() conversionOptions {
				o := collisionScanOpts()
				o.allowUTF8 = true
				return o
			}(),
			expectNoLog: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logTargetInfoLabelCollisions(tc.buildMetrics(), tc.opts, log.NewLogfmtLogger(&buf))

			out := buf.String()
			if tc.expectNoLog {
				assert.Empty(t, out)
				return
			}
			lines := strings.Split(strings.TrimSpace(out), "\n")
			require.Len(t, lines, len(tc.expectedLines))
			for i, expected := range tc.expectedLines {
				assert.Contains(t, lines[i], collisionMsg)
				assert.Contains(t, lines[i], expected)
			}
		})
	}
}

func TestLogTargetInfoLabelCollisionsCap(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "svc")
	// 12 distinct collisions; only 10 detail lines must be logged.
	for i := 0; i < 12; i++ {
		attrs.PutStr(fmt.Sprintf("collide.%c", 'a'+rune(i)), "x")
		attrs.PutStr(fmt.Sprintf("collide_%c", 'a'+rune(i)), "y")
	}
	addGaugeDataPoint(rm)

	var buf bytes.Buffer
	logTargetInfoLabelCollisions(md, collisionScanOpts(), log.NewLogfmtLogger(&buf))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 11) // 10 detail lines + 1 summary.
	for _, line := range lines[:10] {
		assert.Contains(t, line, "collide after label name sanitization")
	}
	assert.Contains(t, lines[10], "suppressed")
	assert.Contains(t, lines[10], "suppressed_collisions=2")
	assert.Contains(t, lines[10], "affected_resources=1")
}

// TestTargetInfoLabelCollisionScanMatchesConverter pins the scan's sanitization
// behavior to the vendored converter's actual target_info output, guarding
// against drift across vendor updates.
func TestTargetInfoLabelCollisionScanMatchesConverter(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "svc")
	attrs.PutStr("service.instance.id", "inst")
	attrs.PutStr("k8s.pod.name", "foo")
	attrs.PutStr("k8s_pod_name", "bar")
	// Colliding attributes with empty values: the converter overwrites instead
	// of concatenating (and drops the empty label); the scan still reports the
	// collision.
	attrs.PutStr("empty.value", "")
	attrs.PutStr("empty_value", "")
	addGaugeDataPoint(rm)

	// Second ResourceMetrics with an identical resource: the converter dedupes
	// target_info samples; the scan must dedupe its log lines.
	rm2 := md.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(rm2.Resource())
	addGaugeDataPoint(rm2)

	opts := collisionScanOpts()

	// Run the actual converter.
	converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
	series, _, dropped, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, md, opts, log.NewNopLogger())
	require.NoError(t, err)
	require.Zero(t, dropped)

	var targetInfoSeries []mimirpb.LabelAdapter
	for _, s := range series {
		for _, l := range s.Labels {
			if l.Name == model.MetricNameLabel && l.Value == "target_info" {
				targetInfoSeries = s.Labels
			}
		}
	}
	require.NotNil(t, targetInfoSeries, "expected a target_info series")

	// The converter concatenated the colliding non-empty values.
	var mergedValue string
	for _, l := range targetInfoSeries {
		if l.Name == "k8s_pod_name" {
			mergedValue = l.Value
		}
	}
	require.Contains(t, mergedValue, ";", "converter should have concatenated colliding values, labels: %v", targetInfoSeries)

	// Cross-check: the scan must report the exact label name the converter merged,
	// regardless of what that name is, so sanitization drift surfaces as a
	// direct mismatch rather than two unrelated failures.
	var converterMergedLabel string
	for _, l := range targetInfoSeries {
		if strings.Contains(l.Value, ";") {
			converterMergedLabel = l.Name
			break
		}
	}
	require.NotEmpty(t, converterMergedLabel, "expected a merged label in target_info, labels: %v", targetInfoSeries)

	// The scan reports the same sanitized label name, deduplicated.
	var buf bytes.Buffer
	logTargetInfoLabelCollisions(md, opts, log.NewLogfmtLogger(&buf))
	out := buf.String()
	assert.Contains(t, out, "label="+converterMergedLabel, "scan must report the same label name the converter merged")
	assert.Contains(t, out, "label=k8s_pod_name")
	assert.Contains(t, out, "attributes=k8s.pod.name,k8s_pod_name")
	assert.Contains(t, out, "label=empty_value")
	assert.Equal(t, 1, strings.Count(out, "label=k8s_pod_name"), "identical collisions across resources must be deduplicated")
}
