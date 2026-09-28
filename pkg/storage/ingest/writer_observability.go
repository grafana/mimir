// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"net"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/time/rate"
)

var (
	_ kgo.HookBrokerE2E        = (*kafkaWriterRequestMetrics)(nil)
	_ kgo.HookBrokerWrite      = (*kafkaWriterDiagnostics)(nil)
	_ kgo.HookBrokerE2E        = (*kafkaWriterDiagnostics)(nil)
	_ kgo.HookBrokerConnect    = (*kafkaWriterDiagnostics)(nil)
	_ kgo.HookBrokerDisconnect = (*kafkaWriterDiagnostics)(nil)
)

var kafkaWriterAPINames = [...]string{"produce", "metadata", "other"}
var kafkaWriterTransportOutcomes = [...]string{"success", "write_error", "read_error"}

func kafkaWriterAPI(key int16) int {
	switch key {
	case kmsg.Produce.Int16():
		return 0
	case kmsg.Metadata.Int16():
		return 1
	default:
		return 2
	}
}

func kafkaWriterTransportOutcome(e2e kgo.BrokerE2E) int {
	if e2e.WriteErr != nil {
		return 1
	}
	if e2e.ReadErr != nil {
		return 2
	}
	return 0
}

type kafkaWriterRequestMetrics struct {
	// Bind the fixed label sets once; broker request completion must not allocate or look up vector children.
	duration  [3][3]prometheus.Observer
	writeWait [3][3]prometheus.Observer
}

func newKafkaWriterRequestMetrics(reg prometheus.Registerer) *kafkaWriterRequestMetrics {
	newHistogram := func(name, help string) *prometheus.HistogramVec {
		return promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            name,
			Help:                            help,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"api", "transport_outcome"})
	}
	duration := newHistogram("kafka_request_duration_by_api_seconds", "Time from starting a Kafka request write to completing its response read or encountering a transport error, excluding write wait. Transport success does not imply Kafka protocol success.")
	writeWait := newHistogram("kafka_request_write_wait_by_api_seconds", "Time from generating a Kafka request to starting its write, observed at transport completion. Excludes earlier record buffering. Transport success does not imply Kafka protocol success.")
	m := &kafkaWriterRequestMetrics{}
	for api, name := range kafkaWriterAPINames {
		for outcome, label := range kafkaWriterTransportOutcomes {
			m.duration[api][outcome] = duration.WithLabelValues(name, label)
			m.writeWait[api][outcome] = writeWait.WithLabelValues(name, label)
		}
	}
	return m
}

func (m *kafkaWriterRequestMetrics) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	api, outcome := kafkaWriterAPI(key), kafkaWriterTransportOutcome(e2e)
	m.duration[api][outcome].Observe(e2e.DurationE2E().Seconds())
	m.writeWait[api][outcome].Observe(e2e.WriteWait.Seconds())
}

type kafkaWriterDiagnostics struct {
	logger   log.Logger
	started  time.Time
	limiters [3]*rate.Limiter
	dropped  [3]prometheus.Counter
}

func newKafkaWriterDiagnostics(logger log.Logger, reg prometheus.Registerer) *kafkaWriterDiagnostics {
	dropped := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_diagnostic_events_dropped_total",
		Help: "Kafka writer diagnostic events suppressed by the per-writer event group rate limit.",
	}, []string{"event_group"})
	d := &kafkaWriterDiagnostics{logger: log.With(logger, "component", "kafka_client"), started: time.Now()}
	for group, name := range []string{"produce", "metadata", "connection"} {
		d.limiters[group] = rate.NewLimiter(100, 100)
		d.dropped[group] = dropped.WithLabelValues(name)
	}
	return d
}

func (d *kafkaWriterDiagnostics) allow(group int) bool {
	if d.limiters[group].Allow() {
		return true
	}
	d.dropped[group].Inc()
	return false
}

func (d *kafkaWriterDiagnostics) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	api := kafkaWriterAPI(key)
	if api == 2 || !d.allow(api) {
		return
	}
	outcome := kafkaWriterTransportOutcome(kgo.BrokerE2E{WriteErr: err})
	level.Debug(d.logger).Log("msg", "Kafka writer request write completed", "observed_at", time.Now().UTC(), "elapsed", time.Since(d.started),
		"api", kafkaWriterAPINames[api], "broker", meta.NodeID, "host", meta.Host, "port", meta.Port,
		"bytes_written", bytesWritten, "write_wait", writeWait, "time_to_write", timeToWrite, "transport_outcome", kafkaWriterTransportOutcomes[outcome])
}

func (d *kafkaWriterDiagnostics) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	api := kafkaWriterAPI(key)
	if api == 2 || !d.allow(api) {
		return
	}
	// These hooks expose neither the Kafka response error nor a request/batch identifier.
	// In particular, receiving a KafkaStorageError response is still a successful transport exchange.
	level.Debug(d.logger).Log("msg", "Kafka writer request transport completed", "observed_at", time.Now().UTC(), "elapsed", time.Since(d.started),
		"api", kafkaWriterAPINames[api], "broker", meta.NodeID, "host", meta.Host, "port", meta.Port,
		"bytes_written", e2e.BytesWritten, "bytes_read", e2e.BytesRead, "write_wait", e2e.WriteWait,
		"time_to_write", e2e.TimeToWrite, "read_wait", e2e.ReadWait, "time_to_read", e2e.TimeToRead,
		"transport_outcome", kafkaWriterTransportOutcomes[kafkaWriterTransportOutcome(e2e)])
}

func (d *kafkaWriterDiagnostics) OnBrokerConnect(meta kgo.BrokerMetadata, initDuration time.Duration, _ net.Conn, err error) {
	if !d.allow(2) {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "connect_error"
	}
	level.Debug(d.logger).Log("msg", "Kafka writer connection attempt completed", "observed_at", time.Now().UTC(), "elapsed", time.Since(d.started),
		"broker", meta.NodeID, "host", meta.Host, "port", meta.Port, "init_duration", initDuration, "transport_outcome", outcome)
}

func (d *kafkaWriterDiagnostics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	if !d.allow(2) {
		return
	}
	level.Debug(d.logger).Log("msg", "Kafka writer connection closed", "observed_at", time.Now().UTC(), "elapsed", time.Since(d.started),
		"broker", meta.NodeID, "host", meta.Host, "port", meta.Port)
}
