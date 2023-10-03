// SPDX-License-Identifier: AGPL-3.0-only

package promextra

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var pool = sync.Pool{New: func() any {
	b := make([]string, 2)
	return &b
}}

type CounterVec struct {
	CounterVec *prometheus.CounterVec
}

func (vec CounterVec) WithLabelValue(val string) prometheus.Counter {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val)

	return vec.CounterVec.WithLabelValues(*values...)
}

func (vec CounterVec) WithTwoLabelValues(val1, val2 string) prometheus.Counter {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val1, val2)

	return vec.CounterVec.WithLabelValues(*values...)
}

func (vec CounterVec) DeleteLabelValue(val string) {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val)

	vec.CounterVec.DeleteLabelValues(*values...)
}

func (vec CounterVec) DeleteTwoLabelValues(val1, val2 string) {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val1, val2)

	vec.CounterVec.DeleteLabelValues(*values...)
}

type GaugeVec struct {
	GaugeVec *prometheus.GaugeVec
}

func (vec GaugeVec) WithLabelValue(val string) prometheus.Gauge {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val)

	return vec.GaugeVec.WithLabelValues(*values...)
}

func (vec GaugeVec) WithTwoLabelValues(val1, val2 string) prometheus.Gauge {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val1, val2)

	return vec.GaugeVec.WithLabelValues(*values...)
}

func (vec GaugeVec) DeleteLabelValue(val string) {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val)

	vec.GaugeVec.DeleteLabelValues(*values...)
}

func (vec GaugeVec) DeleteTwoLabelValues(val1, val2 string) {
	values := pool.Get().(*[]string)
	defer pool.Put(values)

	*values = append((*values)[:0], val1, val2)

	vec.GaugeVec.DeleteLabelValues(*values...)
}
