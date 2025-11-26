package cache

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	opAdd            = "add"
	opSet            = "set"
	opGetMulti       = "getmulti"
	opDelete         = "delete"
	opDecrement      = "decrement"
	opIncrement      = "increment"
	opTouch          = "touch"
	opFlush          = "flushall"
	opCompareAndSwap = "compareswap"

	reasonMaxItemSize     = "max-item-size"
	reasonAsyncBufferFull = "async-buffer-full"
	reasonMalformedKey    = "malformed-key"
	reasonInvalidTTL      = "invalid-ttl"
	reasonNotStored       = "not-stored"
	reasonConnectTimeout  = "connect-timeout"
	reasonTimeout         = "request-timeout"
	reasonServerError     = "server-error"
	reasonNetworkError    = "network-error"
	reasonOther           = "other"

	labelCacheName           = "name"
	labelCacheBackend        = "backend"
	backendValueMemcached    = "memcached"
	cacheMetricNamePrefix    = "cache_"
	getMultiMetricNamePrefix = "getmulti_"
	clientInfoMetricName     = "client_info"
)

type clientMetrics struct {
	requests   prometheus.Counter
	hits       prometheus.Counter
	operations *prometheus.CounterVec
	failures   *prometheus.CounterVec
	skipped    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	dataSize   *prometheus.HistogramVec
}

// newClientMetrics creates a new bundle of metrics about an instance of a cache client. Note
// that there may be multiple cache clients at any given time so the prometheus.Registerer passed
// to this method should include labels unique to this particular client (e.g. a name for each
// different cache being used).
func newClientMetrics(reg prometheus.Registerer) *clientMetrics {
	cm := &clientMetrics{}

	cm.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "requests_total",
		Help: "Total number of items requests to cache.",
	})
	cm.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	})
	cm.operations = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operations_total",
		Help: "Total number of operations against cache.",
	}, []string{"operation"})
	cm.operations.WithLabelValues(opGetMulti)
	cm.operations.WithLabelValues(opAdd)
	cm.operations.WithLabelValues(opSet)
	cm.operations.WithLabelValues(opDelete)
	cm.operations.WithLabelValues(opIncrement)
	cm.operations.WithLabelValues(opDecrement)
	cm.operations.WithLabelValues(opTouch)
	cm.operations.WithLabelValues(opCompareAndSwap)
	cm.operations.WithLabelValues(opFlush)

	cm.failures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operation_failures_total",
		Help: "Total number of operations against cache that failed.",
	}, []string{"operation", "reason"})
	for _, op := range []string{opGetMulti, opAdd, opSet, opDelete, opIncrement, opDecrement, opFlush, opTouch, opCompareAndSwap} {
		cm.failures.WithLabelValues(op, reasonConnectTimeout)
		cm.failures.WithLabelValues(op, reasonTimeout)
		cm.failures.WithLabelValues(op, reasonMalformedKey)
		cm.failures.WithLabelValues(op, reasonInvalidTTL)
		cm.failures.WithLabelValues(op, reasonNotStored)
		cm.failures.WithLabelValues(op, reasonServerError)
		cm.failures.WithLabelValues(op, reasonNetworkError)
		cm.failures.WithLabelValues(op, reasonOther)
	}

	cm.skipped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operation_skipped_total",
		Help: "Total number of operations against cache that have been skipped.",
	}, []string{"operation", "reason"})
	cm.skipped.WithLabelValues(opGetMulti, reasonMaxItemSize)
	cm.skipped.WithLabelValues(opAdd, reasonMaxItemSize)
	cm.skipped.WithLabelValues(opSet, reasonMaxItemSize)
	cm.skipped.WithLabelValues(opSet, reasonAsyncBufferFull)

	cm.duration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "operation_duration_seconds",
		Help:    "Duration of operations against cache.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
		// Use defaults recommended by Prometheus for native histograms.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"operation"})
	cm.duration.WithLabelValues(opGetMulti)
	cm.duration.WithLabelValues(opAdd)
	cm.duration.WithLabelValues(opSet)
	cm.duration.WithLabelValues(opDelete)
	cm.duration.WithLabelValues(opIncrement)
	cm.duration.WithLabelValues(opDecrement)
	cm.duration.WithLabelValues(opFlush)
	cm.duration.WithLabelValues(opTouch)
	cm.duration.WithLabelValues(opCompareAndSwap)

	cm.dataSize = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "operation_data_size_bytes",
		Help: "Tracks the size of the data stored in and fetched from cache.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	},
		[]string{"operation"},
	)
	cm.dataSize.WithLabelValues(opGetMulti)
	cm.dataSize.WithLabelValues(opAdd)
	cm.dataSize.WithLabelValues(opSet)
	cm.dataSize.WithLabelValues(opCompareAndSwap)

	return cm
}
