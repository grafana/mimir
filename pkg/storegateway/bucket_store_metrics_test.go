// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_store_metrics_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestIndexHeaderMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	tsdbMetrics := NewIndexHeaderMetrics()
	mainReg.MustRegister(tsdbMetrics)

	tsdbMetrics.AddUserRegistry("user1", populateMockedIndexHeaderMetrics(5328))
	tsdbMetrics.AddUserRegistry("user2", populateMockedIndexHeaderMetrics(6908))
	tsdbMetrics.AddUserRegistry("user3", populateMockedIndexHeaderMetrics(10283))

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
			# HELP cortex_bucket_store_indexheader_lazy_load_duration_seconds Duration of the index-header lazy loading in seconds.
			# TYPE cortex_bucket_store_indexheader_lazy_load_duration_seconds histogram
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.01"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.02"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.05"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.1"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.2"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="0.5"} 0
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="1"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="2"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="5"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_bucket{le="+Inf"} 3
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_sum 1.9500000000000002
			cortex_bucket_store_indexheader_lazy_load_duration_seconds_count 3

			# HELP cortex_bucket_store_indexheader_lazy_load_failed_total Total number of failed index-header lazy load operations.
			# TYPE cortex_bucket_store_indexheader_lazy_load_failed_total counter
			cortex_bucket_store_indexheader_lazy_load_failed_total 1.373659e+06

			# HELP cortex_bucket_store_indexheader_lazy_load_total Total number of index-header lazy load operations.
			# TYPE cortex_bucket_store_indexheader_lazy_load_total counter
			cortex_bucket_store_indexheader_lazy_load_total 1.35114e+06

			# HELP cortex_bucket_store_indexheader_lazy_unload_failed_total Total number of failed index-header lazy unload operations.
			# TYPE cortex_bucket_store_indexheader_lazy_unload_failed_total counter
			cortex_bucket_store_indexheader_lazy_unload_failed_total 1.418697e+06

			# HELP cortex_bucket_store_indexheader_lazy_unload_total Total number of index-header lazy unload operations.
			# TYPE cortex_bucket_store_indexheader_lazy_unload_total counter
			cortex_bucket_store_indexheader_lazy_unload_total 1.396178e+06
`))
	require.NoError(t, err)
}

func BenchmarkMetricsCollections10(b *testing.B) {
	benchmarkMetricsCollection(b, 10)
}

func BenchmarkMetricsCollections100(b *testing.B) {
	benchmarkMetricsCollection(b, 100)
}

func BenchmarkMetricsCollections1000(b *testing.B) {
	benchmarkMetricsCollection(b, 1000)
}

func BenchmarkMetricsCollections10000(b *testing.B) {
	benchmarkMetricsCollection(b, 10000)
}

func benchmarkMetricsCollection(b *testing.B, users int) {
	mainReg := prometheus.NewRegistry()

	tsdbMetrics := NewIndexHeaderMetrics()
	mainReg.MustRegister(tsdbMetrics)

	base := 123456.0
	for i := 0; i < users; i++ {
		tsdbMetrics.AddUserRegistry(fmt.Sprintf("user-%d", i), populateMockedIndexHeaderMetrics(base*float64(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mainReg.Gather()
	}
}

func populateMockedIndexHeaderMetrics(base float64) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	m := newMockedIndexHeaderMetrics(reg)

	m.indexHeaderLazyLoadCount.Add(60 * base)
	m.indexHeaderLazyLoadFailedCount.Add(61 * base)
	m.indexHeaderLazyUnloadCount.Add(62 * base)
	m.indexHeaderLazyUnloadFailedCount.Add(63 * base)
	m.indexHeaderLazyLoadDuration.Observe(0.65)

	return reg
}

type mockedIndexHeaderMetrics struct {
	indexHeaderLazyLoadCount         prometheus.Counter
	indexHeaderLazyLoadFailedCount   prometheus.Counter
	indexHeaderLazyUnloadCount       prometheus.Counter
	indexHeaderLazyUnloadFailedCount prometheus.Counter
	indexHeaderLazyLoadDuration      prometheus.Histogram
}

func newMockedIndexHeaderMetrics(reg prometheus.Registerer) *mockedIndexHeaderMetrics {
	var m mockedIndexHeaderMetrics

	m.indexHeaderLazyLoadCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_load_total",
		Help: "Total number of index-header lazy load operations.",
	})
	m.indexHeaderLazyLoadFailedCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_load_failed_total",
		Help: "Total number of failed index-header lazy load operations.",
	})
	m.indexHeaderLazyUnloadCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_unload_total",
		Help: "Total number of index-header lazy unload operations.",
	})
	m.indexHeaderLazyUnloadFailedCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_indexheader_lazy_unload_failed_total",
		Help: "Total number of failed index-header lazy unload operations.",
	})
	m.indexHeaderLazyLoadDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_indexheader_lazy_load_duration_seconds",
		Help:    "Duration of the index-header lazy loading in seconds.",
		Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5},
	})

	return &m
}
