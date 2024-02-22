// SPDX-License-Identifier: AGPL-3.0-only

package otlp

import (
	"math"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// addSingleGaugeNumberDataPoint converts the Gauge metric data point to a
// Prometheus time series with samples and labels. The result is stored in the
// series map.
func addSingleGaugeNumberDataPoint(
	pt pmetric.NumberDataPoint,
	resource pcommon.Resource,
	metric pmetric.Metric,
	settings Settings,
	series map[uint64]*mimirpb.TimeSeries,
	name string,
) error {
	labels := createAttributes(
		resource,
		pt.Attributes(),
		settings.ExternalLabels,
		model.MetricNameLabel,
		name,
	)
	sample := &mimirpb.Sample{
		// convert ns to ms
		TimestampMs: convertTimeStamp(pt.Timestamp()),
	}
	switch pt.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		sample.Value = float64(pt.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		sample.Value = pt.DoubleValue()
	}
	if pt.Flags().NoRecordedValue() {
		sample.Value = math.Float64frombits(value.StaleNaN)
	}
	_, err := addSample(series, sample, labels, metric.Type().String())
	return err
}

// addSingleSumNumberDataPoint converts the Sum metric data point to a Prometheus
// time series with samples, labels and exemplars. The result is stored in the
// series map.
func addSingleSumNumberDataPoint(
	pt pmetric.NumberDataPoint,
	resource pcommon.Resource,
	metric pmetric.Metric,
	settings Settings,
	series map[uint64]*mimirpb.TimeSeries,
	name string,
) error {
	labels := createAttributes(
		resource,
		pt.Attributes(),
		settings.ExternalLabels,
		model.MetricNameLabel,
		name,
	)
	sample := &mimirpb.Sample{
		// convert ns to ms
		TimestampMs: convertTimeStamp(pt.Timestamp()),
	}
	switch pt.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		sample.Value = float64(pt.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		sample.Value = pt.DoubleValue()
	}
	if pt.Flags().NoRecordedValue() {
		sample.Value = math.Float64frombits(value.StaleNaN)
	}
	sig, err := addSample(series, sample, labels, metric.Type().String())
	if err != nil {
		return err
	}

	if ts := series[sig]; sig != 0 && ts != nil {
		exemplars := getMimirExemplars[pmetric.NumberDataPoint](pt)
		ts.Exemplars = append(ts.Exemplars, exemplars...)
	}

	// add _created time series if needed
	if settings.ExportCreatedMetric && metric.Sum().IsMonotonic() {
		startTimestamp := pt.StartTimestamp()
		if startTimestamp == 0 {
			return nil
		}

		createdLabels := make([]mimirpb.LabelAdapter, len(labels))
		copy(createdLabels, labels)
		for i, l := range createdLabels {
			if l.Name == model.MetricNameLabel {
				createdLabels[i].Value = name + createdSuffix
				break
			}
		}
		if err := addCreatedTimeSeriesIfNeeded(series, createdLabels, startTimestamp, pt.Timestamp(), metric.Type().String()); err != nil {
			return err
		}
	}

	return nil
}
