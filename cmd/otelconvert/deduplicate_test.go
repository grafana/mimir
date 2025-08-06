package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestDeduplicate(t *testing.T) {
	testCases := []struct {
		name     string
		md       *metricsv1.MetricsData // md will be modified by the test.
		expected *metricsv1.MetricsData
	}{
		{
			name: "happy path",
			md: &metricsv1.MetricsData{
				ResourceMetrics: []*metricsv1.ResourceMetrics{
					{
						// Resource 1
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								keyValue("label1", "foo"),
								keyValue("label2", "bar"),
							},
						},
						ScopeMetrics: []*metricsv1.ScopeMetrics{
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "",
									Version: "",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "foobar",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
												keyValue("endpoint", "/index"),
											},
											[]float64{0, 2, 4, 8, 16, 32},
										),
									},
								},
							},
						},
					},
					{
						// Resource 2
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								keyValue("label1", "foo"),
								keyValue("label2", "bar"),
							},
						},
						ScopeMetrics: []*metricsv1.ScopeMetrics{
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "",
									Version: "",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "foobaz",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{1, 1, 2, 3, 5, 8, 13},
										),
									},
								},
							},
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "alt",
									Version: "v0",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "foobaz",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{0, 0, 0, 0, 0},
										),
									},
								},
							},
						},
					},
					{
						// Resource 3
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								keyValue("label1", "foo"),
								keyValue("label2", "baz"),
							},
						},
						ScopeMetrics: []*metricsv1.ScopeMetrics{
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "",
									Version: "",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "asdf",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{1, 2, 3, 4, 5, 6},
										),
									},
								},
							},
						},
					},
				},
			},
			expected: &metricsv1.MetricsData{
				ResourceMetrics: []*metricsv1.ResourceMetrics{
					{
						// Resource 1 + Resource 2
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								keyValue("label1", "foo"),
								keyValue("label2", "bar"),
							},
						},
						ScopeMetrics: []*metricsv1.ScopeMetrics{
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "",
									Version: "",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "foobar",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
												keyValue("endpoint", "/index"),
											},
											[]float64{0, 2, 4, 8, 16, 32},
										),
									},
									{
										Name: "foobaz",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{1, 1, 2, 3, 5, 8, 13},
										),
									},
								},
							},
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "alt",
									Version: "v0",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "foobaz",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{0, 0, 0, 0, 0},
										),
									},
								},
							},
						},
					},
					{
						// Resource 3
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								keyValue("label1", "foo"),
								keyValue("label2", "baz"),
							},
						},
						ScopeMetrics: []*metricsv1.ScopeMetrics{
							{
								Scope: &commonv1.InstrumentationScope{
									Name:    "",
									Version: "",
								},
								Metrics: []*metricsv1.Metric{
									{
										Name: "asdf",
										Data: gaugeMetric(
											[]*commonv1.KeyValue{
												keyValue("user", "tenant1"),
											},
											[]float64{1, 2, 3, 4, 5, 6},
										),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deduplicate(tc.md)
			require.Equal(t, tc.expected, tc.md)
		})
	}
}

func keyValue(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   k,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
	}
}

func gaugeMetric(attrs []*commonv1.KeyValue, data []float64) *metricsv1.Metric_Gauge {
	dataPoints := make([]*metricsv1.NumberDataPoint, len(data))
	for i, val := range data {
		dataPoints[i] = &metricsv1.NumberDataPoint{
			Attributes: attrs,
			Value: &metricsv1.NumberDataPoint_AsDouble{
				AsDouble: val,
			},
		}
	}

	return &metricsv1.Metric_Gauge{
		Gauge: &metricsv1.Gauge{
			DataPoints: dataPoints,
		},
	}
}
