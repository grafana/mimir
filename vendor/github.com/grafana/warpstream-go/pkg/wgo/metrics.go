package wgo

import (
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/plugin/kprom"
)

// metrics holds the Prometheus metrics for this client's produce/hedge path.
//
// Metrics fall into two groups, distinguished by how they're registered:
//   - Warpstream-specific metrics (hedging, demotion, direct/attempt
//     accounting, client-boundary record counters) describe behaviour with no
//     franz-go counterpart. They are registered under the "warpstream_" prefix.
//   - Producer-state metrics (produceWire*) use franz-go/kprom-compatible names
//     with no prefix, so this client is a drop-in replacement for a franz-go
//     producer. They're tracked here because this client bypasses franz-go's
//     producer state machine (see the kprom hook wired in newKgoClient).
type metrics struct {
	hedgeAttemptsTotal           prometheus.Counter
	hedgeWinsTotal               prometheus.Counter
	hedgeAttemptsSuppressedTotal *prometheus.CounterVec

	lingerFlushesTotal prometheus.Counter

	produceDirectRequestsTotal         prometheus.Counter
	produceDirectRequestsFailedTotal   *prometheus.CounterVec
	produceRequestsAttemptsSuccess     prometheus.Observer
	produceRequestsAttemptsFailure     prometheus.Observer
	produceDirectRequestLatencySuccess prometheus.Observer
	// produceDirectRequestLatencyFailure is keyed by failure reason; success
	// latency carries no reason label (see produceDirectRequestLatencySuccess).
	produceDirectRequestLatencyFailure prometheus.ObserverVec

	produceRequestsPrimaryTotal prometheus.Counter
	produceRequestsHedgeTotal   prometheus.Counter

	produceRecordsTotal         prometheus.Counter
	produceRecordsFailedTotal   prometheus.Counter
	produceRecordsRejectedTotal *prometheus.CounterVec

	// Producer-state metrics under franz-go/kprom-compatible names.
	produceWireRecordsTotal         prometheus.Counter
	produceWireBatchesTotal         prometheus.Counter
	produceWireBytesTotal           prometheus.Counter
	produceWireCompressedBytesTotal prometheus.Counter
}

// hedge suppression reasons recorded on hedgeAttemptsSuppressedTotal. These
// match the early-return paths in Hedger.shouldHedge.
const (
	hedgeSuppressedNoAgentStats   = "no_agent_stats"
	hedgeSuppressedNoClusterStats = "no_cluster_stats"
	hedgeSuppressedSlowFraction   = "slow_fraction_exceeded"
	hedgeSuppressedFaultyFraction = "faulty_fraction_exceeded"
)

// produce rejection reasons recorded on produceRecordsRejectedTotal. These are
// terminal failures the client returns before any wire dispatch.
const (
	produceRejectedRecordTooLarge  = "record_too_large"
	produceRejectedNoAgentAssigned = "no_agent_assigned"
)

func newMetrics(reg prometheus.Registerer) *metrics {
	// On failures the latency carries a "reason" label; on success the
	// reason is left empty, which Prometheus treats as the label being
	// absent for the success series.
	produceDirectRequestLatency := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "warpstream_produce_direct_request_latency_seconds",
		Help:                            "Latency of a single direct Produce request to a Warpstream agent, by outcome (and by failure reason when the outcome is a failure). Each retry counts as a separate request.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
		Buckets:                         prometheus.DefBuckets,
	}, []string{"outcome", "reason"})

	produceRequestAttempts := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "warpstream_produce_requests_attempts",
		Help:                            "Number of Produce attempts a request took (1 = resolved on the primary, N = resolved after N-1 hedge waves), by outcome.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"outcome"})

	return &metrics{
		hedgeAttemptsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_hedge_attempts_total",
			Help: "Total number of produce requests for which a fanout to per-partition secondaries was attempted. Includes both latency-triggered hedges (primary still in flight) and primary-failure retries.",
		}),
		hedgeWinsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_hedge_wins_total",
			Help: "Total number of produce requests where the per-partition secondary fanout produced the winning response. Includes both races where the secondaries beat the primary and retries where the primary had already failed.",
		}),
		hedgeAttemptsSuppressedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "warpstream_hedge_attempts_suppressed_total",
			Help: "Total number of produce requests where hedging was suppressed.",
		}, []string{"reason"}),
		lingerFlushesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_linger_flushes_total",
			Help: "Total number of partition batch flushes triggered by the linger buffer.",
		}),
		produceDirectRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_produce_direct_requests_total",
			Help: "Total number of direct Produce requests issued to a Warpstream agent. Each retry counts as a separate request.",
		}),
		produceRequestsPrimaryTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_produce_requests_primary_total",
			Help: "Total number of primary Produce wire requests.",
		}),
		produceRequestsHedgeTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_produce_requests_hedge_total",
			Help: "Total number of hedging and retried Produce wire requests.",
		}),
		produceDirectRequestsFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "warpstream_produce_direct_requests_failed_total",
			Help: "Total number of direct Produce requests issued to a Warpstream agent that failed, by failure reason. Each retry counts as a separate request.",
		}, []string{"reason"}),
		produceRecordsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_produce_records_total",
			Help: "Total number of records submitted to the client via Produce and ProduceSync.",
		}),
		produceRecordsFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "warpstream_produce_records_failed_total",
			Help: "Total number of records that failed to be produced after dispatch (wire failures, timeouts, canceled context). Records rejected before dispatch are counted by warpstream_produce_records_rejected_total instead.",
		}),
		produceRecordsRejectedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "warpstream_produce_records_rejected_total",
			Help: "Total number of records rejected by the client before any wire dispatch, by reason (record_too_large, no_agent_assigned).",
		}, []string{"reason"}),
		produceWireRecordsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_records_total",
			Help: "Total number of records written to the wire. Only successful produce requests are counted.",
		}),
		produceWireBatchesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_batches_total",
			Help: "Total number of record batches written to the wire (one per partition per request). Only successful produce requests are counted.",
		}),
		produceWireBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_bytes_total",
			Help: "Total number of uncompressed record bytes written to the wire. Only successful produce requests are counted.",
		}),
		produceWireCompressedBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_compressed_bytes_total",
			Help: "Total number of compressed record bytes written to the wire. Only successful produce requests are counted.",
		}),
		produceRequestsAttemptsSuccess:     produceRequestAttempts.WithLabelValues("success"),
		produceRequestsAttemptsFailure:     produceRequestAttempts.WithLabelValues("failure"),
		produceDirectRequestLatencySuccess: produceDirectRequestLatency.WithLabelValues("success", ""),
		produceDirectRequestLatencyFailure: produceDirectRequestLatency.MustCurryWith(prometheus.Labels{"outcome": "failure"}),
	}
}

// kpromProducerStateMetricNames are the kprom metric names this client tracks
// itself instead of letting kprom emit them. kprom derives them from
// franz-go's producer state machine (OnProduceBatchWritten) and producer
// buffer, neither of which this client uses — it produces via raw
// Broker.Request — so kprom would report them as a constant zero. We register
// the same names with real values (the produceWire* counters above and the
// ClusterBuffer gauges) and drop kprom's versions to avoid a duplicate
// registration.
var kpromProducerStateMetricNames = []string{
	"produce_bytes_total",
	"produce_compressed_bytes_total",
	"produce_batches_total",
	"produce_records_total",
	"buffered_produce_records_total",
	"buffered_produce_bytes",
}

// newKpromMetrics builds the kprom hook with this client's standard
// fetch/produce detail config, registering on reg.
func newKpromMetrics(reg prometheus.Registerer) *kprom.Metrics {
	return kprom.NewMetrics("",
		kprom.Registerer(reg),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))
}

// filteringRegisterer wraps a prometheus.Registerer and silently drops
// registration of collectors whose fully-qualified metric name is blocked.
// kprom registers all of its metrics unconditionally, so this is how a subset
// is suppressed without forking kprom. A nil wrapped registerer makes every
// operation a no-op, so callers can pass one through to disable metrics.
type filteringRegisterer struct {
	prometheus.Registerer
	blocked map[string]struct{}
}

func newFilteringRegisterer(reg prometheus.Registerer, blocked ...string) *filteringRegisterer {
	set := make(map[string]struct{}, len(blocked))
	for _, name := range blocked {
		set[name] = struct{}{}
	}
	return &filteringRegisterer{Registerer: reg, blocked: set}
}

func (r *filteringRegisterer) Register(c prometheus.Collector) error {
	if r.Registerer == nil || r.isBlocked(c) {
		return nil
	}
	return r.Registerer.Register(c)
}

func (r *filteringRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

func (r *filteringRegisterer) Unregister(c prometheus.Collector) bool {
	if r.Registerer == nil {
		return false
	}
	return r.Registerer.Unregister(c)
}

func (r *filteringRegisterer) isBlocked(c prometheus.Collector) bool {
	name, ok := collectorFQName(c)
	if !ok {
		return false
	}
	_, blocked := r.blocked[name]
	return blocked
}

// fqNameRe extracts the fqName from prometheus.Desc.String(), which has no
// public accessor for it.
var fqNameRe = regexp.MustCompile(`fqName: "([^"]*)"`)

// collectorFQName returns the fully-qualified name of a single-metric
// collector. kprom registers one collector per metric, so reading the first
// Desc is sufficient.
func collectorFQName(c prometheus.Collector) (string, bool) {
	ch := make(chan *prometheus.Desc, 2)
	c.Describe(ch)
	select {
	case desc := <-ch:
		if desc == nil {
			return "", false
		}
		m := fqNameRe.FindStringSubmatch(desc.String())
		if len(m) < 2 {
			return "", false
		}
		return m[1], true
	default:
		return "", false
	}
}
