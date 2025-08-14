/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

import (
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
)

type (
	MetricsOptimizer struct {
		sorter MetricSorter
	}

	MetricsOptimized struct {
		Metrics []*FlattenedMetric
	}

	FlattenedMetric struct {
		// Resource metrics section.
		ResourceMetricsID string
		Resource          pcommon.Resource
		ResourceSchemaUrl string

		// Scope metrics section.
		ScopeMetricsID string
		Scope          pcommon.InstrumentationScope
		ScopeSchemaUrl string

		// Metric section.
		Metric pmetric.Metric
	}

	MetricSorter interface {
		Sort(metrics []*FlattenedMetric)
	}

	MetricsByNothing               struct{}
	MetricsByResourceScopeTypeName struct{}
	MetricsByTypeNameResourceScope struct{}
)

func NewMetricsOptimizer(sorter MetricSorter) *MetricsOptimizer {
	return &MetricsOptimizer{
		sorter: sorter,
	}
}

func (t *MetricsOptimizer) Optimize(metrics pmetric.Metrics) *MetricsOptimized {
	metricsOptimized := &MetricsOptimized{
		Metrics: make([]*FlattenedMetric, 0),
	}

	resMetricsSlice := metrics.ResourceMetrics()
	for i := 0; i < resMetricsSlice.Len(); i++ {
		resMetrics := resMetricsSlice.At(i)
		resource := resMetrics.Resource()
		resourceSchemaUrl := resMetrics.SchemaUrl()
		resMetricID := otlp.ResourceID(resource, resourceSchemaUrl)

		scopeMetricsSlice := resMetrics.ScopeMetrics()
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			scopeMetrics := scopeMetricsSlice.At(j)
			scope := scopeMetrics.Scope()
			scopeSchemaUrl := scopeMetrics.SchemaUrl()
			scopeMetricsID := otlp.ScopeID(scope, scopeSchemaUrl)

			metrics := scopeMetrics.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				metricsOptimized.Metrics = append(metricsOptimized.Metrics, &FlattenedMetric{
					ResourceMetricsID: resMetricID,
					Resource:          resource,
					ResourceSchemaUrl: resourceSchemaUrl,
					ScopeMetricsID:    scopeMetricsID,
					Scope:             scope,
					ScopeSchemaUrl:    scopeSchemaUrl,
					Metric:            metric,
				})
			}
		}
	}

	t.sorter.Sort(metricsOptimized.Metrics)

	return metricsOptimized
}

// No sorting
// ==========

func UnsortedMetrics() *MetricsByNothing {
	return &MetricsByNothing{}
}

func (s *MetricsByNothing) Sort(_ []*FlattenedMetric) {
}

func SortMetricsByResourceScopeTypeName() *MetricsByResourceScopeTypeName {
	return &MetricsByResourceScopeTypeName{}
}

func (s *MetricsByResourceScopeTypeName) Sort(metrics []*FlattenedMetric) {
	sort.Slice(metrics, func(i, j int) bool {
		metricI := metrics[i]
		metricJ := metrics[j]

		if metricI.ResourceMetricsID == metricJ.ResourceMetricsID {
			if metricI.ScopeMetricsID == metricJ.ScopeMetricsID {
				if metricI.Metric.Type() == metricJ.Metric.Type() {
					return metricI.Metric.Name() < metricJ.Metric.Name()
				} else {
					return metricI.Metric.Type() < metricJ.Metric.Type()
				}
			} else {
				return metricI.ScopeMetricsID < metricJ.ScopeMetricsID
			}
		} else {
			return metricI.ResourceMetricsID < metricJ.ResourceMetricsID
		}
	})
}

func SortMetricsByTypeNameResourceScope() *MetricsByTypeNameResourceScope {
	return &MetricsByTypeNameResourceScope{}
}

func (s *MetricsByTypeNameResourceScope) Sort(metrics []*FlattenedMetric) {
	sort.Slice(metrics, func(i, j int) bool {
		metricI := metrics[i]
		metricJ := metrics[j]

		if metricI.Metric.Type() == metricJ.Metric.Type() {
			if metricI.Metric.Name() == metricJ.Metric.Name() {
				if metricI.ResourceMetricsID == metricJ.ResourceMetricsID {
					return metricI.ScopeMetricsID < metricJ.ScopeMetricsID
				} else {
					return metricI.ResourceMetricsID < metricJ.ResourceMetricsID
				}
			} else {
				return metricI.Metric.Name() < metricJ.Metric.Name()
			}
		} else {
			return metricI.Metric.Type() < metricJ.Metric.Type()
		}
	})
}
