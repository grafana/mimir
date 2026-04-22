// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/client_golang/prometheus/promauto/auto.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package promauto

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus" //lint:ignore faillint this package offers an idempotent alternative to the promauto desired by the lint rule.
)

// mustRegisterOrGet registers a Collector or returns a previously-registered Collector with the same descriptor.
// Like prometheus.MustRegister, but only panics if registration error is not prometheus.AlreadyRegisteredError.
func mustRegisterOrGet[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			return are.ExistingCollector.(T)
		}
		panic(err)
	}
	return c
}

// IdempotentFactory mirrors promauto.Factory, but handles duplicate Collector registration,
// returning the previously-registered Collector for a duplicate descriptor rather than panicking.
type IdempotentFactory struct {
	r prometheus.Registerer
}

func WithIdempotent(reg prometheus.Registerer) IdempotentFactory {
	return IdempotentFactory{r: reg}
}

func (f IdempotentFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labelNames)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	c := prometheus.NewGauge(opts)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	c := prometheus.NewGaugeVec(opts, labelNames)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	c := prometheus.NewHistogram(opts)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(opts, labelNames)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewSummary(opts prometheus.SummaryOpts) prometheus.Summary {
	c := prometheus.NewSummary(opts)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}

func (f IdempotentFactory) NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *prometheus.SummaryVec {
	c := prometheus.NewSummaryVec(opts, labelNames)
	if f.r != nil {
		return mustRegisterOrGet(f.r, c)
	}
	return c
}
