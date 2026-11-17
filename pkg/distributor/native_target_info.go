// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// TargetInfoTranslation is how a tenant's OTLP attribute names become label names.
type TargetInfoTranslation struct {
	AllowUTF8, UnderscoreSanitization, PreserveMultipleUnderscores bool
}

// TargetInfoLabels returns the labels of the target_info series that the OTLP conversion writes for a resource with
// these attributes, with the tenant's settings, and false when it writes none: for a resource of only identifying
// attributes, or without job and instance. It runs the conversion itself, on a gauge of the resource, so that
// native target_info has exactly the stored one's labels.
func TargetInfoLabels(ctx context.Context, identifying, descriptive map[string]string, keepIdentifying bool, translation TargetInfoTranslation) (labels.Labels, bool, error) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attributes := rm.Resource().Attributes()
	// Insertion order doesn't change the labels, but sorted keys make the conversion's work the same every time.
	for _, attrs := range []map[string]string{identifying, descriptive} {
		keys := make([]string, 0, len(attrs))
		for k := range attrs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			attributes.PutStr(k, attrs[k])
		}
	}
	gauge := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	gauge.SetName("native_target_info_probe")
	point := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetTimestamp(pcommon.Timestamp(1_000_000_000))
	point.SetDoubleValue(1)

	series, _, _, dropped, err := otelMetricsToSeriesAndMetadata(ctx, newOTLPMimirConverter(otlpappender.NewCombinedAppender()), md, conversionOptions{
		keepIdentifyingResourceAttributes: keepIdentifying,
		allowUTF8:                         translation.AllowUTF8,
		underscoreSanitization:            translation.UnderscoreSanitization,
		preserveMultipleUnderscores:       translation.PreserveMultipleUnderscores,
	}, log.NewNopLogger())
	if err != nil {
		return labels.EmptyLabels(), false, err
	}
	if dropped > 0 {
		return labels.EmptyLabels(), false, fmt.Errorf("converting a resource to its target_info dropped %d metrics", dropped)
	}
	for _, s := range series {
		lbls := mimirpb.FromLabelAdaptersToLabels(s.Labels)
		if lbls.Get(labels.MetricName) == "target_info" {
			// The conversion's labels may refer to its buffers.
			return lbls.Copy(), true, nil
		}
	}
	return labels.EmptyLabels(), false, nil
}
