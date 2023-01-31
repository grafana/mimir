package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/gomemcache/memcache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/gate"
)

var (
	errMemcachedConfigNoAddrs                  = errors.New("no memcached addresses provided")
	errMemcachedDNSUpdateIntervalNotPositive   = errors.New("DNS provider update interval must be positive")
	errMemcachedMaxAsyncConcurrencyNotPositive = errors.New("max async concurrency must be positive")

	_ RemoteCacheClient = (*memcachedClient)(nil)
)

// MemcachedClient for compatible.
type MemcachedClient = RemoteCacheClient

// memcachedClientBackend is an interface used to mock the underlying client in tests.
type memcachedClientBackend interface {
	GetMulti(keys []string, opts ...memcache.Option) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
	Delete(key string) error
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
	Addresses []string `yaml:"addresses"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"`

	// MaxIdleConnections specifies the maximum number of idle connections that
	// will be maintained per address. For better performances, this should be
	// set to a number higher than your peak parallel requests.
	MaxIdleConnections int `yaml:"max_idle_connections"`

	// MaxAsyncConcurrency specifies the maximum number of SetAsync goroutines.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency"`

	// MaxAsyncBufferSize specifies the queue buffer size for SetAsync operations.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency"`

	// MaxItemSize specifies the maximum size of an item stored in memcached.
	// Items bigger than MaxItemSize are skipped.
	// If set to 0, no maximum size is enforced.
	MaxItemSize flagext.Bytes `yaml:"max_item_size"`

	// MaxGetMultiBatchSize specifies the maximum number of keys a single underlying
	// GetMulti() should run. If more keys are specified, internally keys are splitted
	// into multiple batches and fetched concurrently, honoring MaxGetMultiConcurrency parallelism.
	// If set to 0, the max batch size is unlimited.
	MaxGetMultiBatchSize int `yaml:"max_get_multi_batch_size"`

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration `yaml:"dns_provider_update_interval"`

	// AutoDiscovery configures memached client to perform auto-discovery instead of DNS resolution
	AutoDiscovery bool `yaml:"auto_discovery"`
}

func (c *MemcachedClientConfig) validate() error {
	if len(c.Addresses) == 0 {
		return errMemcachedConfigNoAddrs
	}

	// Avoid panic in time ticker.
	if c.DNSProviderUpdateInterval <= 0 {
		return errMemcachedDNSUpdateIntervalNotPositive
	}

	// Set async only available when MaxAsyncConcurrency > 0.
	if c.MaxAsyncConcurrency <= 0 {
		return errMemcachedMaxAsyncConcurrencyNotPositive
	}

	return nil
}

type memcachedClient struct {
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
	// Resolves the provided list of memcached cluster to the actual nodes
	Resolve(context.Context, []string) error

	// Returns the nodes
	Addresses() []string
}

type memcachedGetMultiResult struct {
	items map[string]*memcache.Item
	err   error
}

// NewMemcachedClientWithConfig makes a new RemoteCacheClient.
func NewMemcachedClientWithConfig(logger log.Logger, name string, config MemcachedClientConfig, reg prometheus.Registerer) (RemoteCacheClient, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	// We use a custom servers selector in order to use a jump hash
	// for servers selection.
	selector := &MemcachedJumpHashSelector{}

	client := memcache.NewFromSelector(selector)
	client.Timeout = config.Timeout
	client.MaxIdleConns = config.MaxIdleConnections

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{labelName: name}, reg)
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
) (*memcachedClient, error) {
	newRegisterer := prometheus.WrapRegistererWith(
		prometheus.Labels{labelBackend: backendMemcached},
		prometheus.WrapRegistererWithPrefix(cachePrefix, reg))
	reg = prometheus.WrapRegistererWithPrefix(legacyMemcachedPrefix, reg)

	backwardCompatibleRegs := []prometheus.Registerer{reg, newRegisterer}

	addressProvider := dns.NewProviderWithRegisterers(
		logger,
		backwardCompatibleRegs,
		dns.MiekgdnsResolverType,
	)

	metrics := newClientMetrics(backwardCompatibleRegs)

	c := &memcachedClient{
		baseClient:      newBaseClient(logger, uint64(config.MaxItemSize), config.MaxAsyncBufferSize, config.MaxAsyncConcurrency, metrics),
		logger:          log.With(logger, "name", name),
		config:          config,
		client:          client,
		selector:        selector,
		addressProvider: addressProvider,
		stop:            make(chan struct{}, 1),
		getMultiGate: gate.NewWithRegisterers(
			[]prometheus.Registerer{
				prometheus.WrapRegistererWithPrefix(getMultiPrefix, reg),
				prometheus.WrapRegistererWithPrefix(getMultiPrefix, newRegisterer),
			},
			config.MaxGetMultiConcurrency,
		),
	}

	//lint:ignore faillint need to apply the metric to multiple registerer
	c.clientInfo = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "client_info",
		Help: "A metric with a constant '1' value labeled by configuration options from which memcached client was configured.",
		ConstLabels: prometheus.Labels{
			"timeout":                      config.Timeout.String(),
			"max_idle_connections":         strconv.Itoa(config.MaxIdleConnections),
			"max_async_concurrency":        strconv.Itoa(config.MaxAsyncConcurrency),
			"max_async_buffer_size":        strconv.Itoa(config.MaxAsyncBufferSize),
			"max_item_size":                strconv.FormatUint(uint64(config.MaxItemSize), 10),
			"max_get_multi_concurrency":    strconv.Itoa(config.MaxGetMultiConcurrency),
			"max_get_multi_batch_size":     strconv.Itoa(config.MaxGetMultiBatchSize),
			"dns_provider_update_interval": config.DNSProviderUpdateInterval.String(),
		},
	},
		func() float64 { return 1 },
	)

	for _, reg := range backwardCompatibleRegs {
		reg.MustRegister(c.clientInfo)
	}

	// As soon as the client is created it must ensure that memcached server
	// addresses are resolved, so we're going to trigger an initial addresses
	// resolution here.
	if err := c.resolveAddrs(); err != nil {
		return nil, err
	}
	go c.resolveAddrsLoop()

	return c, nil
}

func (c *memcachedClient) Stop() {
	close(c.stop)

	// Stop running async operations.
	c.asyncQueue.stop()
}

func (c *memcachedClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.setAsync(ctx, key, value, ttl, func(ctx context.Context, key string, buf []byte, ttl time.Duration) error {
		return c.client.Set(&memcache.Item{
			Key:        key,
			Value:      value,
			Expiration: int32(time.Now().Add(ttl).Unix()),
		})
	})
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

func (c *memcachedClient) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}

	options := toMemcacheOptions(opts...)
	batches, err := c.getMultiBatched(ctx, keys, options...)
	if err != nil {
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

	return hits
}

func (c *memcachedClient) Delete(ctx context.Context, key string) error {
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

func (c *memcachedClient) getMultiBatched(ctx context.Context, keys []string, opts ...memcache.Option) ([]map[string]*memcache.Item, error) {
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

func (c *memcachedClient) getMultiSingle(ctx context.Context, keys []string, opts ...memcache.Option) (items map[string]*memcache.Item, err error) {
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
func (c *memcachedClient) sortKeysByServer(keys []string) []string {
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

	var out []string
	for srv := range bucketed {
		out = append(out, bucketed[srv]...)
	}

	return out
}

func (c *memcachedClient) resolveAddrsLoop() {
	ticker := time.NewTicker(c.config.DNSProviderUpdateInterval)
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

func (c *memcachedClient) resolveAddrs() error {
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
