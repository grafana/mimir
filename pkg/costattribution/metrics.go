// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

type descriptor struct {
	desc   *prometheus.Desc
	name   string
	labels []string
}

func newDescriptor(name string, help string, labels []string, constLabels prometheus.Labels) *descriptor {
	return &descriptor{
		desc:   prometheus.NewDesc(name, help, labels, constLabels),
		name:   name,
		labels: labels,
	}
}

func (d *descriptor) validate() error {
	reg := prometheus.NewRegistry()
	if err := reg.Register(d); err != nil {
		return err
	}

	return nil
}

func (d *descriptor) Describe(ch chan<- *prometheus.Desc) {
	ch <- d.desc
}

func (d *descriptor) Collect(_ chan<- prometheus.Metric) {
	// Empty implementation - we only need Describe for validation
}

func (d *descriptor) newConstGauge(v float64, labelValues ...string) prometheus.Metric {
	return &constMetric{
		desc: d.desc,
		metric: &dto.Metric{
			Label: prometheus.MakeLabelPairs(d.desc, labelValues),
			Gauge: &dto.Gauge{Value: proto.Float64(v)},
		},
	}
}

func (d *descriptor) newConstCounter(v float64, labelValues ...string) prometheus.Metric {
	return &constMetric{
		desc: d.desc,
		metric: &dto.Metric{
			Label: prometheus.MakeLabelPairs(d.desc, labelValues),
			Counter: &dto.Counter{
				Value:            proto.Float64(v),
				Exemplar:         nil,
				CreatedTimestamp: nil,
			},
		},
	}
}

type constMetric struct {
	desc   *prometheus.Desc
	metric *dto.Metric
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
