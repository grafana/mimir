package wgo

import (
	"regexp"
	"runtime/debug"
	"slices"
	"sync/atomic"
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

	// flushesInFlightCount backs flushesInFlight's observations; not itself exported.
	// A histogram, not a gauge, so a scrape can't miss a spike between polls.
	flushesInFlightCount atomic.Int64
	flushesInFlight      prometheus.Histogram

	produceDirectRequestsTotal         *prometheus.CounterVec
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

	metadataRefreshResultsTotal *prometheus.CounterVec

	clusterStatsAvailable     prometheus.Gauge
	clusterSlowFraction       prometheus.Gauge
	clusterSlowContributors   prometheus.Gauge
	clusterFaultyFraction     prometheus.Gauge
	clusterFaultyContributors prometheus.Gauge

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

const (
	agentStateHealthy = "healthy"
	agentStateDemoted = "demoted"
)

// metadataRefreshTrigger is why a live AgentPool Metadata refresh ran. Its
// string value is the "trigger" label on metadataRefreshResultsTotal.
type metadataRefreshTrigger string

const (
	metadataRefreshTriggerPeriodic metadataRefreshTrigger = "periodic"
	metadataRefreshTriggerOnDemand metadataRefreshTrigger = "on_demand"
)

const (
	metadataRefreshResultMembershipChanged = "membership_changed"
	metadataRefreshResultUnchanged         = "unchanged"
	metadataRefreshResultFailed            = "failed"
)

func agentStateLabel(state AgentState) string {
	if state == AgentStateDemoted {
		return agentStateDemoted
	}
	return agentStateHealthy
}

const (
	warpstreamGoModulePath = "github.com/grafana/warpstream-go"
	franzGoModulePath      = "github.com/twmb/franz-go"
)

// reModuleVersion matches a well-formed Go module version or pseudo-version,
// same as franz-go's own reVersion in config.go.
var reModuleVersion = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$`)

// clientBuildInfo mirrors franz-go's own version-format check, but also
// prefers dep.Replace so a replace directive reports the version actually
// running, not the pre-replace requirement.
func clientBuildInfo() (version, franzGoVersion string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown", "unknown"
	}
	return buildInfoVersions(info)
}

func buildInfoVersions(info *debug.BuildInfo) (version, franzGoVersion string) {
	version, franzGoVersion = "unknown", "unknown"

	// Only set when warpstream-go is the main module (e.g. its own tests);
	// otherwise it's a dependency and picked up from Deps below.
	if info.Main.Path == warpstreamGoModulePath {
		version = info.Main.Version
	}

	for _, dep := range info.Deps {
		v := dep.Version
		if dep.Replace != nil {
			v = dep.Replace.Version
		}
		if !reModuleVersion.MatchString(v) {
			continue
		}
		switch dep.Path {
		case warpstreamGoModulePath:
			version = v
		case franzGoModulePath:
			franzGoVersion = v
		}
	}

	return version, franzGoVersion
}

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

	version, franzGoVersion := clientBuildInfo()
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        "warpstream_client_build_info",
		Help:        "Build information about this client and the underlying franz-go client it wraps. Always 1.",
		ConstLabels: prometheus.Labels{"version": version, "franz_go_version": franzGoVersion},
	}).Set(1)

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
		flushesInFlight: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "warpstream_produce_flushes_in_flight",
			Help:                            "Distribution of concurrent in-flight AgentBuffer flushes (main and hedge), sampled at each flush start (including itself).",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}),
		produceDirectRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "warpstream_produce_direct_requests_total",
			Help: "Total number of direct Produce requests issued to a Warpstream agent, by routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Each retry counts as a separate request.",
		}, []string{"agent_state"}),
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
			Help: "Total number of direct Produce requests issued to a Warpstream agent that failed, by failure reason and routing-time agent state (healthy, or demoted if any partition in the attempt is a probe). Each retry counts as a separate request.",
		}, []string{"reason", "agent_state"}),
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
		metadataRefreshResultsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "warpstream_metadata_refresh_results_total",
			Help: "Total number of live AgentPool Metadata refreshes, by trigger (periodic, on_demand) and result (membership_changed, unchanged, failed). membership_changed is the sorted Agent NodeID set only; leader-only or topic-only updates are unchanged. The constructor Refresh is not counted.",
		}, []string{"trigger", "result"}),
		clusterStatsAvailable: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "warpstream_cluster_stats_available",
			Help: "Whether the last ClusterStats compute returned a usable cluster view (1) or not (0). 0 until the first compute. While this is 0 the other warpstream_cluster_ gauges hold their last successful values.",
		}),
		clusterSlowFraction: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "warpstream_cluster_slow_fraction",
			Help: "Fraction of agents classified slow from the last successful ClusterStats compute, the value the Hedger's slow-fraction suppression gate is tested against. Held when a later compute has no view (see warpstream_cluster_stats_available). 0 until the first successful compute.",
		}),
		clusterSlowContributors: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "warpstream_cluster_slow_contributors",
			Help: "Denominator of warpstream_cluster_slow_fraction: agents with a measurable latency in the stats window. Held when a later compute has no view (see warpstream_cluster_stats_available). 0 until the first successful compute.",
		}),
		clusterFaultyFraction: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "warpstream_cluster_faulty_fraction",
			Help: "Fraction of agents classified faulty from the last successful ClusterStats compute, the value the Demoter's suppression gate and the Hedger's faulty-fraction gate are tested against. Held when a later compute has no view (see warpstream_cluster_stats_available). 0 until the first successful compute.",
		}),
		clusterFaultyContributors: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "warpstream_cluster_faulty_contributors",
			Help: "Denominator of warpstream_cluster_faulty_fraction: agents with any request tracked by this client in the stats window. Held when a later compute has no view (see warpstream_cluster_stats_available). 0 until the first successful compute.",
		}),
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

// observeMetadataRefresh records one refresh. Membership is the sorted Agent
// NodeID list; leader-only Metadata changes count as unchanged.
func (m *metrics) observeMetadataRefresh(trigger metadataRefreshTrigger, before, after []int32, err error) {
	result := metadataRefreshResultUnchanged
	switch {
	case err != nil:
		result = metadataRefreshResultFailed
	case !slices.Equal(before, after):
		result = metadataRefreshResultMembershipChanged
	}
	m.metadataRefreshResultsTotal.WithLabelValues(string(trigger), result).Inc()
}

// observeClusterStats records one ClusterStats compute. Without a view the
// value gauges keep their last successful reading rather than a fake 0;
// clusterStatsAvailable is what marks them stale.
func (m *metrics) observeClusterStats(stats ClusterStats, ok bool) {
	if !ok {
		m.clusterStatsAvailable.Set(0)
		return
	}
	m.clusterStatsAvailable.Set(1)
	m.clusterSlowFraction.Set(stats.SlowFraction)
	m.clusterSlowContributors.Set(float64(stats.SlowContributorsCount))
	m.clusterFaultyFraction.Set(stats.FaultyFraction)
	m.clusterFaultyContributors.Set(float64(stats.FaultyContributorsCount))
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
