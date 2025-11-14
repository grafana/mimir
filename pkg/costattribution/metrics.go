// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// descriptor wraps a prometheus.Desc with additional metadata for metric validation and creation.
// This wrapper is needed because prometheus.Desc doesn't expose important information about the
// descriptor itself, such as metric name, label names, and a possible error obtained during the
// descriptor construction. The first two are accessible via the name and labels fields, while
// construction error can be obtained by calling validate().
type descriptor struct {
	desc   *prometheus.Desc
	name   string
	labels []string
}

// newDescriptor creates a new descriptor with the specified metric name, help text, variable labels, and constant labels.
// It returns an error if a creation of the underlying prometheus.Desc is not successful, for example when the metric name
// or any of the label names don't conform to Prometheus naming conventions.
func newDescriptor(name string, help string, labels []string, constLabels prometheus.Labels) (*descriptor, error) {
	desc := &descriptor{
		desc:   prometheus.NewDesc(name, help, labels, constLabels),
		name:   name,
		labels: labels,
	}
	if err := desc.validate(); err != nil {
		return nil, err
	}
	return desc, nil
}

// validate checks whether the underlying prometheus.Desc is valid.
//
// A prometheus.Desc may contain an internal error if created with invalid
// metric or label names. Since this error is unexported, validate detects
// such cases and returns the underlying error if present. Otherwise, it
// returns nil.
func (d *descriptor) validate() error {
	reg := prometheus.NewRegistry()
	if err := reg.Register(d); err != nil {
		return err
	}

	return nil
}

// Describe implements the prometheus.Collector interface by sending this descriptor to the provided channel.
func (d *descriptor) Describe(ch chan<- *prometheus.Desc) {
	ch <- d.desc
}

// Collect implements the prometheus.Collector interface with an empty implementation since we only need Describe for validation.
func (d *descriptor) Collect(_ chan<- prometheus.Metric) {
	// Empty implementation - we only need Describe for validation
}

// gauge creates a new gauge metric with the specified value and label values.
// This has the same effect as calling prometheus.NewConstMetric with value type prometheus.GaugeValue
// but without any validation.
func (d *descriptor) gauge(v float64, labelValues ...string) prometheus.Metric {
	return &constMetric{
		desc: d.desc,
		metric: dto.Metric{
			Label: prometheus.MakeLabelPairs(d.desc, labelValues),
			Gauge: &dto.Gauge{Value: proto.Float64(v)},
		},
	}
}

// counter creates a new counter metric with the specified value and label values.
// This has the same effect as calling prometheus.NewConstMetric with value type prometheus.CounterValue
// but without any validation.
func (d *descriptor) counter(v float64, labelValues ...string) prometheus.Metric {
	return &constMetric{
		desc: d.desc,
		metric: dto.Metric{
			Label:   prometheus.MakeLabelPairs(d.desc, labelValues),
			Counter: &dto.Counter{Value: proto.Float64(v)},
		},
	}
}

// constMetric represents a constant metric that implements the prometheus.Metric interface.
// It corresponds to prometheus.constMetric, which is not exported.
type constMetric struct {
	desc   *prometheus.Desc
	metric dto.Metric
}

func (m *constMetric) Desc() *prometheus.Desc {
	return m.desc
}

func (m *constMetric) Write(out *dto.Metric) error {
	out.Label = m.metric.Label
	out.Counter = m.metric.Counter
	out.Gauge = m.metric.Gauge
	out.Untyped = m.metric.Untyped
	return nil
}
