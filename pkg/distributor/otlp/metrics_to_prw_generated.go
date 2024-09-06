// Code generated from Prometheus sources - DO NOT EDIT.

// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Provenance-includes-location: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/95e8f8fdc2a9dc87230406c9a3cf02be4fd68bea/pkg/translator/prometheusremotewrite/metrics_to_prw.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The OpenTelemetry Authors.

package otlp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	prometheustranslator "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheus"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Settings struct {
	Namespace                           string
	ExternalLabels                      map[string]string
	DisableTargetInfo                   bool
	ExportCreatedMetric                 bool
	AddMetricSuffixes                   bool
	SendMetadata                        bool
	PromoteResourceAttributes           []string
	EnableCreatedTimestampZeroIngestion bool
}

type StartTsAndTs struct {
	StartTs int64
	Ts      int64
	Labels  []mimirpb.LabelAdapter
}

// MimirConverter converts from OTel write format to Mimir remote write format.
type MimirConverter struct {
	unique    map[uint64]*mimirpb.TimeSeries
	conflicts map[uint64][]*mimirpb.TimeSeries
	everyN    everyNTimes
}

func NewMimirConverter() *MimirConverter {
	return &MimirConverter{
		unique:    map[uint64]*mimirpb.TimeSeries{},
		conflicts: map[uint64][]*mimirpb.TimeSeries{},
	}
}

// FromMetrics converts pmetric.Metrics to Mimir remote write format.
func (c *MimirConverter) FromMetrics(ctx context.Context, md pmetric.Metrics, settings Settings, logger log.Logger) (annots annotations.Annotations, errs error) {
	c.everyN = everyNTimes{n: 128}
	resourceMetricsSlice := md.ResourceMetrics()
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		resourceMetrics := resourceMetricsSlice.At(i)
		resource := resourceMetrics.Resource()
		scopeMetricsSlice := resourceMetrics.ScopeMetrics()
		// keep track of the most recent timestamp in the ResourceMetrics for
		// use with the "target" info metric
		var mostRecentTimestamp pcommon.Timestamp
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			metricSlice := scopeMetricsSlice.At(j).Metrics()

			// TODO: decide if instrumentation library information should be exported as labels
			for k := 0; k < metricSlice.Len(); k++ {
				if err := c.everyN.checkContext(ctx); err != nil {
					errs = multierr.Append(errs, err)
					return
				}

				metric := metricSlice.At(k)
				mostRecentTimestamp = max(mostRecentTimestamp, mostRecentTimestampInMetric(metric))

				if !isValidAggregationTemporality(metric) {
					errs = multierr.Append(errs, fmt.Errorf("invalid temporality and type combination for metric %q", metric.Name()))
					continue
				}

				promName := prometheustranslator.BuildCompliantName(metric, settings.Namespace, settings.AddMetricSuffixes)

				// handle individual metrics based on type
				//exhaustive:enforce
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					if dataPoints.Len() == 0 {
						errs = multierr.Append(errs, fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addGaugeNumberDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs = multierr.Append(errs, err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					if dataPoints.Len() == 0 {
						errs = multierr.Append(errs, fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addSumNumberDataPoints(ctx, dataPoints, resource, metric, settings, promName, logger); err != nil {
						errs = multierr.Append(errs, err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					if dataPoints.Len() == 0 {
						errs = multierr.Append(errs, fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addHistogramDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs = multierr.Append(errs, err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					if dataPoints.Len() == 0 {
						errs = multierr.Append(errs, fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					ws, err := c.addExponentialHistogramDataPoints(
						ctx,
						dataPoints,
						resource,
						settings,
						promName,
					)
					annots.Merge(ws)
					if err != nil {
						errs = multierr.Append(errs, err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				case pmetric.MetricTypeSummary:
					dataPoints := metric.Summary().DataPoints()
					if dataPoints.Len() == 0 {
						errs = multierr.Append(errs, fmt.Errorf("empty data points. %s is dropped", metric.Name()))
						break
					}
					if err := c.addSummaryDataPoints(ctx, dataPoints, resource, settings, promName); err != nil {
						errs = multierr.Append(errs, err)
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
					}
				default:
					errs = multierr.Append(errs, errors.New("unsupported metric type"))
				}
			}
		}
		addResourceTargetInfo(resource, settings, mostRecentTimestamp, c)
	}

	return annots, errs
}

func isSameMetric(ts *mimirpb.TimeSeries, lbls []mimirpb.LabelAdapter) bool {
	if len(ts.Labels) != len(lbls) {
		return false
	}
	for i, l := range ts.Labels {
		if l.Name != ts.Labels[i].Name || l.Value != ts.Labels[i].Value {
			return false
		}
	}
	return true
}

// addExemplars adds exemplars for the dataPoint. For each exemplar, if it can find a bucket bound corresponding to its value,
// the exemplar is added to the bucket bound's time series, provided that the time series' has samples.
func (c *MimirConverter) addExemplars(ctx context.Context, dataPoint pmetric.HistogramDataPoint, bucketBounds []bucketBoundsData) error {
	if len(bucketBounds) == 0 {
		return nil
	}

	exemplars, err := getPromExemplars(ctx, &c.everyN, dataPoint)
	if err != nil {
		return err
	}
	if len(exemplars) == 0 {
		return nil
	}

	sort.Sort(byBucketBoundsData(bucketBounds))
	for _, exemplar := range exemplars {
		for _, bound := range bucketBounds {
			if err := c.everyN.checkContext(ctx); err != nil {
				return err
			}
			if len(bound.ts.Samples) > 0 && exemplar.Value <= bound.bound {
				bound.ts.Exemplars = append(bound.ts.Exemplars, exemplar)
				break
			}
		}
	}

	return nil
}

// addSample finds a TimeSeries that corresponds to lbls, and adds sample to it.
// If there is no corresponding TimeSeries already, it's created.
// The corresponding TimeSeries is returned.
// If either lbls is nil/empty or sample is nil, nothing is done.
func (c *MimirConverter) addSample(sample *mimirpb.Sample, lbls []mimirpb.LabelAdapter) *mimirpb.TimeSeries {
	if sample == nil || len(lbls) == 0 {
		// This shouldn't happen
		return nil
	}

	ts, _ := c.getOrCreateTimeSeries(lbls)
	ts.Samples = append(ts.Samples, *sample)
	return ts
}

// TODO(jesus.vazquez) This method is for debugging only and its meant to be removed soon.
// trackStartTimestampForSeries logs the start timestamp for a series if it has been seen before.
func (c *MimirConverter) trackStartTimestampForSeries(startTs, ts int64, lbls []mimirpb.LabelAdapter, logger log.Logger) {
	h := timeSeriesSignature(lbls)
	if _, ok := c.unique[h]; ok {
		var seriesBuilder strings.Builder
		seriesBuilder.WriteString("{")
		for i, l := range lbls {
			if i > 0 {
				seriesBuilder.WriteString(",")
			}
			seriesBuilder.WriteString(fmt.Sprintf("%s=%s", l.Name, l.Value))

		}
		seriesBuilder.WriteString("}")
		level.Debug(logger).Log("labels", seriesBuilder.String(), "start_ts", startTs, "ts", ts)
	}
}
