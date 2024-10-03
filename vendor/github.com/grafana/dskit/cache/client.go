package cache

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Common functionality shared between the Memcached and Redis Cache implementations

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
	backendValueRedis        = "redis"
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
	cm.operations.WithLabelValues(opSet)
	cm.operations.WithLabelValues(opDelete)
	cm.operations.WithLabelValues(opIncrement)
	cm.operations.WithLabelValues(opTouch)
	cm.operations.WithLabelValues(opCompareAndSwap)
	cm.operations.WithLabelValues(opFlush)

	cm.failures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "operation_failures_total",
		Help: "Total number of operations against cache that failed.",
	}, []string{"operation", "reason"})
	for _, op := range []string{opGetMulti, opAdd, opSet, opDelete, opIncrement, opFlush, opTouch, opCompareAndSwap} {
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

type baseClient struct {
	logger        log.Logger
	maxItemSize   uint64
	asyncBuffSize int
	asyncQueue    *asyncQueue
	metrics       *clientMetrics
}

func newBaseClient(
	logger log.Logger,
	maxItemSize uint64,
	asyncBuffSize int,
	asyncConcurrency int,
	metrics *clientMetrics,
) *baseClient {
	return &baseClient{
		asyncQueue:    newAsyncQueue(asyncBuffSize, asyncConcurrency),
		logger:        logger,
		maxItemSize:   maxItemSize,
		asyncBuffSize: asyncBuffSize,
		metrics:       metrics,
	}
}

func (c *baseClient) setMultiAsync(data map[string][]byte, ttl time.Duration, f func(key string, buf []byte, ttl time.Duration) error) {
	for key, val := range data {
		c.setAsync(key, val, ttl, f)
	}
}

func (c *baseClient) setAsync(key string, value []byte, ttl time.Duration, f func(key string, buf []byte, ttl time.Duration) error) {
	if c.maxItemSize > 0 && uint64(len(value)) > c.maxItemSize {
		c.metrics.skipped.WithLabelValues(opSet, reasonMaxItemSize).Inc()
		return
	}

	err := c.asyncQueue.submit(func() {
		// Because this operation is executed in a separate goroutine: We run the operation without
		// a context (it is expected to keep running no matter what happens) and we don't return the
		// error (it will be tracked via metrics instead of being returned to the caller).
		_ = c.storeOperation(context.Background(), key, value, ttl, opSet, func(_ context.Context, key string, value []byte, ttl time.Duration) error {
			return f(key, value, ttl)
		})
	})

	if err != nil {
		c.metrics.skipped.WithLabelValues(opSet, reasonAsyncBufferFull).Inc()
		level.Debug(c.logger).Log("msg", "failed to store item to cache because the async buffer is full", "err", err, "size", c.asyncBuffSize)
	}
}

func (c *baseClient) storeOperation(ctx context.Context, key string, value []byte, ttl time.Duration, operation string, f func(ctx context.Context, key string, value []byte, ttl time.Duration) error) error {
	if c.maxItemSize > 0 && uint64(len(value)) > c.maxItemSize {
		c.metrics.skipped.WithLabelValues(operation, reasonMaxItemSize).Inc()
		return nil
	}

	start := time.Now()
	c.metrics.operations.WithLabelValues(operation).Inc()

	err := f(ctx, key, value, ttl)
	if err != nil {
		level.Debug(c.logger).Log(
			"msg", "failed to store item to cache",
			"operation", operation,
			"key", key,
			"sizeBytes", len(value),
			"err", err,
		)
		c.trackError(operation, err)
	}

	c.metrics.dataSize.WithLabelValues(operation).Observe(float64(len(value)))
	c.metrics.duration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	return err
}

// wait submits an async task and blocks until it completes. This can be used during
// tests to ensure that async "sets" have completed before attempting to read them.
func (c *baseClient) wait() error {
	var wg sync.WaitGroup

	wg.Add(1)
	err := c.asyncQueue.submit(func() {
		wg.Done()
	})
	if err != nil {
		return err
	}

	wg.Wait()
	return nil
}

func (c *baseClient) delete(ctx context.Context, key string, f func(ctx context.Context, key string) error) error {
	errCh := make(chan error, 1)

	enqueueErr := c.asyncQueue.submit(func() {
		start := time.Now()
		c.metrics.operations.WithLabelValues(opDelete).Inc()

		err := f(ctx, key)
		if err != nil {
			level.Debug(c.logger).Log(
				"msg", "failed to delete cache item",
				"key", key,
				"err", err,
			)
			c.trackError(opDelete, err)
		} else {
			c.metrics.duration.WithLabelValues(opDelete).Observe(time.Since(start).Seconds())
		}
		errCh <- err
	})

	if errors.Is(enqueueErr, errAsyncQueueFull) {
		c.metrics.skipped.WithLabelValues(opDelete, reasonAsyncBufferFull).Inc()
		level.Debug(c.logger).Log("msg", "failed to delete cache item because the async buffer is full", "err", enqueueErr, "size", c.asyncBuffSize)
		return enqueueErr
	}
	// Wait for the delete operation to complete.
	return <-errCh
}

func (c *baseClient) trackError(op string, err error) {
	var connErr *memcache.ConnectTimeoutError
	var netErr net.Error
	switch {
	case errors.As(err, &connErr):
		c.metrics.failures.WithLabelValues(op, reasonConnectTimeout).Inc()
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			c.metrics.failures.WithLabelValues(op, reasonTimeout).Inc()
		} else {
			c.metrics.failures.WithLabelValues(op, reasonNetworkError).Inc()
		}
	case errors.Is(err, ErrNotStored):
		c.metrics.failures.WithLabelValues(op, reasonNotStored).Inc()
	case errors.Is(err, ErrInvalidTTL):
		c.metrics.failures.WithLabelValues(op, reasonInvalidTTL).Inc()
	case errors.Is(err, memcache.ErrMalformedKey):
		c.metrics.failures.WithLabelValues(op, reasonMalformedKey).Inc()
	case errors.Is(err, memcache.ErrServerError):
		c.metrics.failures.WithLabelValues(op, reasonServerError).Inc()
	default:
		c.metrics.failures.WithLabelValues(op, reasonOther).Inc()
	}
}
