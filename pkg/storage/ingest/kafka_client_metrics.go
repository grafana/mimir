// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	// interface checks to ensure we implement the hooks properly.
	_ kgo.HookBrokerE2E      = new(KafkaClientExtendedMetrics)
	_ kgo.HookBrokerThrottle = new(KafkaClientExtendedMetrics)
)

// KafkaClientExtendedMetrics holds custom Kafka client metrics.
//
// These metrics are optionally supported in kprom, but they're implemented as classic histograms and segregated by node ID.
// We want to use native histograms and no node ID to keep cardinality low, so we implement our custom ones here.
type KafkaClientExtendedMetrics struct {
	// Writes.
	writeWaitSeconds prometheus.Histogram
	writeTimeSeconds prometheus.Histogram

	// Reads.
	readWaitSeconds prometheus.Histogram
	readTimeSeconds prometheus.Histogram

	// Requests.
	requestDurationE2ESeconds prometheus.Histogram
	requestThrottledSeconds   prometheus.Histogram
}

func NewKafkaClientExtendedMetrics(reg prometheus.Registerer) *KafkaClientExtendedMetrics {
	return &KafkaClientExtendedMetrics{
		writeWaitSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_write_wait_seconds",
			Help:                            "Time spent waiting to write to Kafka backend.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		writeTimeSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_write_time_seconds",
			Help:                            "Time spent writing to Kafka backend.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		readWaitSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_read_wait_seconds",
			Help:                            "Time spent waiting to read from Kafka backend.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		readTimeSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_read_time_seconds",
			Help:                            "Time spent reading from Kafka backend.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		requestDurationE2ESeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_request_duration_e2e_seconds",
			Help:                            "Time from the start of when a Kafka request is written to the end of when the response for that request was fully read from the Kafka backend.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		requestThrottledSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "kafka_request_throttled_seconds",
			Help:                            "How long Kafka requests have been throttled by the Kafka client.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
	}
}

// OnBrokerE2E implements kgo.HookBrokerE2E.
func (m *KafkaClientExtendedMetrics) OnBrokerE2E(_ kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	m.writeWaitSeconds.Observe(e2e.WriteWait.Seconds())
	m.writeTimeSeconds.Observe(e2e.TimeToWrite.Seconds())
	m.readWaitSeconds.Observe(e2e.ReadWait.Seconds())
	m.readTimeSeconds.Observe(e2e.TimeToRead.Seconds())
	m.requestDurationE2ESeconds.Observe(e2e.DurationE2E().Seconds())
}

// OnBrokerThrottle implements kgo.HookBrokerThrottle.
func (m *KafkaClientExtendedMetrics) OnBrokerThrottle(_ kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	m.requestThrottledSeconds.Observe(throttleInterval.Seconds())
}
