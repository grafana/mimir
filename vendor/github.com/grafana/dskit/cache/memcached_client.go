// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	dstls "github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/gate"
)

const (
	dnsProviderUpdateInterval = 30 * time.Second
	maxTTL                    = 30 * 24 * time.Hour
)

var (
	ErrNoMemcachedAddresses                    = errors.New("no memcached addresses provided")
	ErrMemcachedMaxAsyncConcurrencyNotPositive = errors.New("max async concurrency must be positive")
	ErrInvalidWriteBufferSizeBytes             = errors.New("invalid write buffer size specified (must be greater than 0)")
	ErrInvalidReadBufferSizeBytes              = errors.New("invalid read buffer size specified (must be greater than 0)")

	_ Cache = (*MemcachedClient)(nil)
)

// memcachedClientBackend is an interface used to mock the underlying client in tests.
type memcachedClientBackend interface {
	GetMulti(keys []string, opts ...memcache.Option) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
	Add(item *memcache.Item) error
	Delete(key string) error
	Decrement(key string, delta uint64) (uint64, error)
	Increment(key string, delta uint64) (uint64, error)
	Touch(key string, seconds int32) error
	Close()
	CompareAndSwap(item *memcache.Item) error
	FlushAll() error
}

// updatableServerSelector extends the interface used for picking a memcached server
// for a key to allow servers to be updated at runtime. It allows the selector used
// by the client to be mocked in tests.
type updatableServerSelector interface {
	memcache.ServerSelector

	// SetServers changes a ServerSelector's set of servers at runtime
	// and is safe for concurrent use by multiple goroutines.
	//
	// SetServers returns an error if any of the server names fail to
	// resolve. No attempt is made to connect to the server. If any
	// error occurs, no changes are made to the internal server list.
	SetServers(servers ...string) error
}

// MemcachedClientConfig is the config accepted by RemoteCacheClient.
type MemcachedClientConfig struct {
	// Addresses specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addresses flagext.StringSliceCSV `yaml:"addresses"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"`

	// ConnectTimeout specifies the connection timeout.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`

	// WriteBufferSizeBytes specifies the size of the write buffer (in bytes). The buffer
	// is allocated for each connection.
	WriteBufferSizeBytes int `yaml:"write_buffer_size_bytes" category:"experimental"`

	// ReadBufferSizeBytes specifies the size of the read buffer (in bytes). The buffer
	// is allocated for each connection.
	ReadBufferSizeBytes int `yaml:"read_buffer_size_bytes" category:"experimental"`

	// MinIdleConnectionsHeadroomPercentage specifies the minimum number of idle connections
	// to keep open as a percentage of the number of recently used idle connections.
	// If negative, idle connections are kept open indefinitely.
	MinIdleConnectionsHeadroomPercentage float64 `yaml:"min_idle_connections_headroom_percentage" category:"advanced"`

	// MaxIdleConnections specifies the maximum number of idle connections that
	// will be maintained per address. For better performances, this should be
	// set to a number higher than your peak parallel requests.
	MaxIdleConnections int `yaml:"max_idle_connections" category:"advanced"`

	// MaxAsyncConcurrency specifies the maximum number of SetAsync goroutines.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency" category:"advanced"`

	// MaxAsyncBufferSize specifies the queue buffer size for SetAsync operations.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size" category:"advanced"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency" category:"advanced"`

	// MaxGetMultiBatchSize specifies the maximum number of keys a single underlying
	// GetMulti() should run. If more keys are specified, internally keys are split
	// into multiple batches and fetched concurrently, honoring MaxGetMultiConcurrency parallelism.
	// If set to 0, the max batch size is unlimited.
	MaxGetMultiBatchSize int `yaml:"max_get_multi_batch_size" category:"advanced"`

	// MaxItemSize specifies the maximum size of an item stored in memcached, in bytes.
	// Items bigger than MaxItemSize are skipped. If set to 0, no maximum size is enforced.
	MaxItemSize int `yaml:"max_item_size" category:"advanced"`

	// TLSEnabled enables connecting to Memcached with TLS.
	TLSEnabled bool `yaml:"tls_enabled" category:"advanced"`

	// TLS to use to connect to the Memcached server.
	TLS dstls.ClientConfig `yaml:",inline"`
}

func (c *MemcachedClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&c.Addresses, prefix+"addresses", "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format.")
	f.DurationVar(&c.Timeout, prefix+"timeout", 200*time.Millisecond, "The socket read/write timeout.")
	f.DurationVar(&c.ConnectTimeout, prefix+"connect-timeout", 200*time.Millisecond, "The connection timeout.")
	f.IntVar(&c.WriteBufferSizeBytes, prefix+"write-buffer-size-bytes", 4096, "The size of the write buffer (in bytes). The buffer is allocated for each connection to memcached.")
	f.IntVar(&c.ReadBufferSizeBytes, prefix+"read-buffer-size-bytes", 4096, "The size of the read buffer (in bytes). The buffer is allocated for each connection to memcached.")
	f.Float64Var(&c.MinIdleConnectionsHeadroomPercentage, prefix+"min-idle-connections-headroom-percentage", -1, "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely.")
	f.IntVar(&c.MaxIdleConnections, prefix+"max-idle-connections", 100, "The maximum number of idle connections that will be maintained per address.")
	f.IntVar(&c.MaxAsyncConcurrency, prefix+"max-async-concurrency", 50, "The maximum number of concurrent asynchronous operations can occur.")
	f.IntVar(&c.MaxAsyncBufferSize, prefix+"max-async-buffer-size", 25000, "The maximum number of enqueued asynchronous operations allowed.")
	f.IntVar(&c.MaxGetMultiConcurrency, prefix+"max-get-multi-concurrency", 100, "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&c.MaxGetMultiBatchSize, prefix+"max-get-multi-batch-size", 100, "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited.")
	f.IntVar(&c.MaxItemSize, prefix+"max-item-size", 1024*1024, "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced.")
	f.BoolVar(&c.TLSEnabled, prefix+"tls-enabled", false, "Enable connecting to Memcached with TLS.")
	c.TLS.RegisterFlagsWithPrefix(prefix, f)
}

func (c *MemcachedClientConfig) Validate() error {
	if len(c.Addresses) == 0 {
		return ErrNoMemcachedAddresses
	}
	if c.WriteBufferSizeBytes <= 0 {
		return ErrInvalidWriteBufferSizeBytes
	}
	if c.ReadBufferSizeBytes <= 0 {
		return ErrInvalidReadBufferSizeBytes
	}

	// Set async only available when MaxAsyncConcurrency > 0.
	if c.MaxAsyncConcurrency <= 0 {
		return ErrMemcachedMaxAsyncConcurrencyNotPositive
	}

	return nil
}

type MemcachedClient struct {
	*baseClient

	logger   log.Logger
	config   MemcachedClientConfig
	client   memcachedClientBackend
	selector updatableServerSelector

	// Name provides an identifier for the instantiated Client
	name string

	// Address provider used to keep the memcached servers list updated.
	addressProvider AddressProvider

	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}

	// Gate used to enforce the max number of concurrent GetMulti() operations.
	getMultiGate gate.Gate

	// Tracked metrics.
	clientInfo prometheus.GaugeFunc
}

// AddressProvider performs node address resolution given a list of clusters.
type AddressProvider interface {
	// Resolve resolves the provided list of memcached cluster to the actual nodes
	Resolve(context.Context, []string) error

	// Addresses returns the nodes
	Addresses() []string
}

type memcachedGetMultiResult struct {
	items map[string]*memcache.Item
	err   error
}

// NewMemcachedClientWithConfig makes a new MemcachedClient.
func NewMemcachedClientWithConfig(logger log.Logger, name string, config MemcachedClientConfig, reg prometheus.Registerer) (*MemcachedClient, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// We use a custom servers selector in order to use a jump hash
	// for servers selection.
	selector := &MemcachedJumpHashSelector{}

	client := memcache.NewFromSelector(selector)
	client.Timeout = config.Timeout
	client.ConnectTimeout = config.ConnectTimeout
	client.WriteBufferSizeBytes = config.WriteBufferSizeBytes
	client.ReadBufferSizeBytes = config.ReadBufferSizeBytes
	client.MinIdleConnsHeadroomPercentage = config.MinIdleConnectionsHeadroomPercentage
	client.MaxIdleConns = config.MaxIdleConnections

	if config.TLSEnabled {
		cfg, err := config.TLS.GetTLSConfig()
		if err != nil {
			return nil, errors.Wrapf(err, "TLS configuration")
		}

		client.DialTimeout = func(network, address string, timeout time.Duration) (net.Conn, error) {
			base := new(net.Dialer)
			base.Timeout = timeout

			return tls.DialWithDialer(base, network, address, cfg)
		}
	}

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{labelCacheName: name}, reg)
	}
	return newMemcachedClient(logger, client, selector, config, reg, name)
}

func newMemcachedClient(
	logger log.Logger,
	client memcachedClientBackend,
	selector updatableServerSelector,
	config MemcachedClientConfig,
	reg prometheus.Registerer,
	name string,
) (*MemcachedClient, error) {
	reg = prometheus.WrapRegistererWith(
		prometheus.Labels{labelCacheBackend: backendValueMemcached},
		prometheus.WrapRegistererWithPrefix(cacheMetricNamePrefix, reg))

	addressProvider := dns.NewProvider(
		logger,
		reg,
		dns.MiekgdnsResolverType,
	)

	metrics := newClientMetrics(reg)

	c := &MemcachedClient{
		baseClient:      newBaseClient(logger, uint64(config.MaxItemSize), config.MaxAsyncBufferSize, config.MaxAsyncConcurrency, metrics),
		logger:          log.With(logger, "name", name),
		config:          config,
		client:          client,
		selector:        selector,
		addressProvider: addressProvider,
		stop:            make(chan struct{}, 1),
		getMultiGate: gate.New(
			prometheus.WrapRegistererWithPrefix(getMultiMetricNamePrefix, reg),
			config.MaxGetMultiConcurrency,
		),
	}

	c.clientInfo = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: clientInfoMetricName,
		Help: "A metric with a constant '1' value labeled by configuration options from which memcached client was configured.",
		ConstLabels: prometheus.Labels{
			"timeout":                                  config.Timeout.String(),
			"write_buffer_size_bytes":                  strconv.Itoa(config.WriteBufferSizeBytes),
			"read_buffer_size_bytes":                   strconv.Itoa(config.ReadBufferSizeBytes),
			"min_idle_connections_headroom_percentage": fmt.Sprintf("%f.2", config.MinIdleConnectionsHeadroomPercentage),
			"max_idle_connections":                     strconv.Itoa(config.MaxIdleConnections),
			"max_async_concurrency":                    strconv.Itoa(config.MaxAsyncConcurrency),
			"max_async_buffer_size":                    strconv.Itoa(config.MaxAsyncBufferSize),
			"max_item_size":                            strconv.FormatUint(uint64(config.MaxItemSize), 10),
			"max_get_multi_concurrency":                strconv.Itoa(config.MaxGetMultiConcurrency),
			"max_get_multi_batch_size":                 strconv.Itoa(config.MaxGetMultiBatchSize),
		},
	},
		func() float64 { return 1 },
	)

	// As soon as the client is created it must ensure that memcached server
	// addresses are resolved, so we're going to trigger an initial addresses
	// resolution here.
	if err := c.resolveAddrs(); err != nil {
		return nil, err
	}
	go c.resolveAddrsLoop()

	return c, nil
}

func (c *MemcachedClient) Stop() {
	close(c.stop)

	// Stop running async operations.
	c.asyncQueue.stop()

	// Stop the underlying client.
	c.client.Close()
}

func (c *MemcachedClient) Name() string {
	return c.name
}

func (c *MemcachedClient) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	c.setMultiAsync(data, ttl, c.setSingleItem)
}

func (c *MemcachedClient) SetAsync(key string, value []byte, ttl time.Duration) {
	c.setAsync(key, value, ttl, c.setSingleItem)
}

func (c *MemcachedClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.storeOperation(ctx, key, value, ttl, opSet, func(ctx context.Context, key string, value []byte, ttl time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return c.setSingleItem(key, value, ttl)
		}
	})
}

func (c *MemcachedClient) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.storeOperation(ctx, key, value, ttl, opAdd, func(ctx context.Context, key string, value []byte, ttl time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ttlSeconds, ok := toSeconds(ttl)
			if !ok {
				return fmt.Errorf("%w: for set operation on %s %s", ErrInvalidTTL, key, ttl)
			}

			err := c.client.Add(&memcache.Item{
				Key:        key,
				Value:      value,
				Expiration: ttlSeconds,
			})

			if errors.Is(err, memcache.ErrNotStored) {
				return fmt.Errorf("%w: for add operation on %s", ErrNotStored, key)
			}

			return err
		}
	})
}

func (c *MemcachedClient) setSingleItem(key string, value []byte, ttl time.Duration) error {
	ttlSeconds, ok := toSeconds(ttl)
	if !ok {
		return fmt.Errorf("%w: for set operation on %s %s", ErrInvalidTTL, key, ttl)
	}

	return c.client.Set(&memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: ttlSeconds,
	})
}

// TODO: Docs
func toSeconds(d time.Duration) (int32, bool) {
	if d > maxTTL {
		return 0, false
	}

	secs := int32(d.Seconds())
	if d != 0 && secs <= 0 {
		return 0, false
	}

	return secs, true
}

func toMemcacheOptions(opts ...Option) []memcache.Option {
	if len(opts) == 0 {
		return nil
	}

	base := &Options{}
	for _, opt := range opts {
		opt(base)
	}

	var out []memcache.Option
	if base.Alloc != nil {
		out = append(out, memcache.WithAllocator(base.Alloc))
	}

	return out
}

func (c *MemcachedClient) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}

	c.metrics.requests.Add(float64(len(keys)))
	options := toMemcacheOptions(opts...)
	batches, err := c.getMultiBatched(ctx, keys, options...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		level.Warn(c.logger).Log("msg", "failed to fetch items from memcached", "numKeys", len(keys), "firstKey", keys[0], "err", err)

		// In case we have both results and an error, it means some batch requests
		// failed and other succeeded. In this case we prefer to log it and move on,
		// given returning some results from the cache is better than returning
		// nothing.
		if len(batches) == 0 {
			return nil
		}
	}

	hits := map[string][]byte{}
	for _, items := range batches {
		for key, item := range items {
			hits[key] = item.Value
		}
	}

	c.metrics.hits.Add(float64(len(hits)))
	return hits
}

func (c *MemcachedClient) Delete(ctx context.Context, key string) error {
	return c.delete(ctx, key, func(ctx context.Context, key string) error {
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
			err = c.client.Delete(key)
		}
		return err
	})
}

func (c *MemcachedClient) Increment(ctx context.Context, key string, delta uint64) (uint64, error) {
	return c.incrDecr(ctx, key, opIncrement, func() (uint64, error) {
		return c.client.Increment(key, delta)
	})
}

func (c *MemcachedClient) Decrement(ctx context.Context, key string, delta uint64) (uint64, error) {
	return c.incrDecr(ctx, key, opDecrement, func() (uint64, error) {
		return c.client.Decrement(key, delta)
	})
}

func (c *MemcachedClient) incrDecr(ctx context.Context, key string, operation string, f func() (uint64, error)) (uint64, error) {
	var (
		newValue uint64
		err      error
	)
	start := time.Now()
	c.metrics.operations.WithLabelValues(operation).Inc()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	default:
		newValue, err = f()
	}
	if err != nil {
		level.Debug(c.logger).Log(
			"msg", "failed to incr/decr cache item",
			"operation", operation,
			"key", key,
			"err", err,
		)
		c.trackError(operation, err)
	} else {
		c.metrics.duration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	}

	return newValue, err
}

func (c *MemcachedClient) Touch(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	c.metrics.operations.WithLabelValues(opTouch).Inc()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	default:
		err = c.client.Touch(key, int32(ttl.Seconds()))
	}
	if err != nil {
		level.Debug(c.logger).Log(
			"msg", "failed to touch cache item",
			"key", key,
			"err", err,
		)
		c.trackError(opTouch, err)
	} else {
		c.metrics.duration.WithLabelValues(opTouch).Observe(time.Since(start).Seconds())
	}
	return err
}

func (c *MemcachedClient) CompareAndSwap(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	var err error
	item := &memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: int32(ttl.Seconds()),
	}

	start := time.Now()
	c.metrics.operations.WithLabelValues(opCompareAndSwap).Inc()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	default:
		err = c.client.CompareAndSwap(item)
	}
	if err != nil {
		level.Debug(c.logger).Log(
			"msg", "failed to compareAndSwap cache item",
			"key", key,
			"err", err,
		)
		c.trackError(opCompareAndSwap, err)
	} else {
		c.metrics.dataSize.WithLabelValues(opCompareAndSwap).Observe(float64(len(value)))
		c.metrics.duration.WithLabelValues(opCompareAndSwap).Observe(time.Since(start).Seconds())
	}

	return err
}

func (c *MemcachedClient) FlushAll(ctx context.Context) error {
	var err error
	start := time.Now()
	c.metrics.operations.WithLabelValues(opFlush).Inc()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	default:
		err = c.client.FlushAll()
	}
	if err != nil {
		level.Debug(c.logger).Log(
			"msg", "failed to flush all cache",
			"err", err,
		)
		c.trackError(opFlush, err)
	} else {
		c.metrics.duration.WithLabelValues(opFlush).Observe(time.Since(start).Seconds())
	}

	return err
}

func (c *MemcachedClient) getMultiBatched(ctx context.Context, keys []string, opts ...memcache.Option) ([]map[string]*memcache.Item, error) {
	// Do not batch if the input keys are less than the max batch size.
	if (c.config.MaxGetMultiBatchSize <= 0) || (len(keys) <= c.config.MaxGetMultiBatchSize) {
		// Even if we're not splitting the input into batches, make sure that our single request
		// still counts against the concurrency limit.
		if c.config.MaxGetMultiConcurrency > 0 {
			if err := c.getMultiGate.Start(ctx); err != nil {
				return nil, errors.Wrapf(err, "failed to wait for turn. Instance: %s", c.name)
			}

			defer c.getMultiGate.Done()
		}

		items, err := c.getMultiSingle(ctx, keys, opts...)
		if err != nil {
			return nil, err
		}

		return []map[string]*memcache.Item{items}, nil
	}

	// Calculate the number of expected results.
	batchSize := c.config.MaxGetMultiBatchSize
	numResults := len(keys) / batchSize
	if len(keys)%batchSize != 0 {
		numResults++
	}

	// If max concurrency is disabled, use a nil gate for the doWithBatch method which will
	// not apply any limit to the number goroutines started to make batch requests in that case.
	var getMultiGate gate.Gate
	if c.config.MaxGetMultiConcurrency > 0 {
		getMultiGate = c.getMultiGate
	}

	// Sort keys based on which memcached server they will be sharded to. Sorting keys that
	// are on the same server together before splitting into batches reduces the number of
	// connections required and increases the number of "gets" per connection.
	sortedKeys := c.sortKeysByServer(keys)

	// Allocate a channel to store results for each batch request. The max concurrency will be
	// enforced by doWithBatch.
	results := make(chan *memcachedGetMultiResult, numResults)
	defer close(results)

	// Ignore the error here since it can only be returned by our provided function which
	// always returns nil. NOTE also we are using a background context here for the doWithBatch
	// method. This is to ensure that it runs the expected number of batches _even if_ our
	// context (`ctx`) is canceled since we expect a certain number of batches to be read
	// from `results` below. The wrapped `getMultiSingle` method will still check our context
	// and short-circuit if it has been canceled.
	_ = doWithBatch(context.Background(), len(keys), c.config.MaxGetMultiBatchSize, getMultiGate, func(startIndex, endIndex int) error {
		batchKeys := sortedKeys[startIndex:endIndex]

		res := &memcachedGetMultiResult{}
		res.items, res.err = c.getMultiSingle(ctx, batchKeys, opts...)

		results <- res
		return nil
	})

	// Wait for all batch results. In case of error, we keep
	// track of the last error occurred.
	items := make([]map[string]*memcache.Item, 0, numResults)
	var lastErr error

	for i := 0; i < numResults; i++ {
		result := <-results
		if result.err != nil {
			lastErr = result.err
			continue
		}

		items = append(items, result.items)
	}

	return items, lastErr
}

func (c *MemcachedClient) getMultiSingle(ctx context.Context, keys []string, opts ...memcache.Option) (items map[string]*memcache.Item, err error) {
	start := time.Now()
	c.metrics.operations.WithLabelValues(opGetMulti).Inc()

	select {
	case <-ctx.Done():
		// Make sure our context hasn't been canceled before fetching cache items using
		// cache client backend.
		return nil, ctx.Err()
	default:
		items, err = c.client.GetMulti(keys, opts...)
	}

	if err != nil {
		level.Debug(c.logger).Log("msg", "failed to get multiple items from memcached", "err", err)
		c.trackError(opGetMulti, err)
	} else {
		var total int
		for _, it := range items {
			total += len(it.Value)
		}
		c.metrics.dataSize.WithLabelValues(opGetMulti).Observe(float64(total))
		c.metrics.duration.WithLabelValues(opGetMulti).Observe(time.Since(start).Seconds())
	}

	return items, err
}

// sortKeysByServer sorts cache keys within a slice based on which server they are
// sharded to using a memcache.ServerSelector instance. The keys are ordered so keys
// on the same server are next to each other. Any errors encountered determining which
// server a key should be on will result in returning keys unsorted (in the same order
// they were supplied in). Note that output is not guaranteed to be any particular order
// *except* that keys sharded to the same server will be together. The order of keys
// returned may change from call to call.
func (c *MemcachedClient) sortKeysByServer(keys []string) []string {
	bucketed := make(map[string][]string)

	for _, key := range keys {
		addr, err := c.selector.PickServer(key)
		// If we couldn't determine the correct server, return keys in existing order
		if err != nil {
			return keys
		}

		addrString := addr.String()
		bucketed[addrString] = append(bucketed[addrString], key)
	}

	out := make([]string, 0, len(keys))
	for srv := range bucketed {
		out = append(out, bucketed[srv]...)
	}

	return out
}

func (c *MemcachedClient) resolveAddrsLoop() {
	ticker := time.NewTicker(dnsProviderUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := c.resolveAddrs()
			if err != nil {
				level.Warn(c.logger).Log("msg", "failed update memcached servers list", "err", err)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *MemcachedClient) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// If some dns resolution fails, log the error.
	if err := c.addressProvider.Resolve(ctx, c.config.Addresses); err != nil {
		level.Error(c.logger).Log("msg", "failed to resolve addresses for memcached", "addresses", strings.Join(c.config.Addresses, ","), "err", err)
	}
	// Fail in case no server address is resolved.
	servers := c.addressProvider.Addresses()
	if len(servers) == 0 {
		return fmt.Errorf("no server address resolved for %s", c.name)
	}

	return c.selector.SetServers(servers...)
}
