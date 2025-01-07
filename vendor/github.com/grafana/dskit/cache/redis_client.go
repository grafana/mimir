package cache

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	dstls "github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/gate"
)

var (
	ErrRedisConfigNoEndpoint               = errors.New("no redis endpoint provided")
	ErrRedisMaxAsyncConcurrencyNotPositive = errors.New("max async concurrency must be positive")

	_ Cache = (*RedisClient)(nil)
)

// RedisClientConfig is the config accepted by RedisClient.
type RedisClientConfig struct {
	// Endpoint specifies the endpoint of Redis server.
	Endpoint flagext.StringSliceCSV `yaml:"endpoint"`

	// Use the specified Username to authenticate the current connection
	// with one of the connections defined in the ACL list when connecting
	// to a Redis 6.0 instance, or greater, that is using the Redis ACL system.
	Username string `yaml:"username"`

	// Optional password. Must match the password specified in the
	// requirepass server configuration option (if connecting to a Redis 5.0 instance, or lower),
	// or the User Password when connecting to a Redis 6.0 instance, or greater,
	// that is using the Redis ACL system.
	Password flagext.Secret `yaml:"password"`

	// DB Database to be selected after connecting to the server.
	DB int `yaml:"db"`

	// MasterName is Redis Sentinel master name. An empty string for Redis Server or Redis Cluster.
	MasterName string `yaml:"master_name" category:"advanced"`

	// DialTimeout specifies the client dial timeout.
	DialTimeout time.Duration `yaml:"dial_timeout" category:"advanced"`

	// ReadTimeout specifies the client read timeout.
	ReadTimeout time.Duration `yaml:"read_timeout" category:"advanced"`

	// WriteTimeout specifies the client write timeout.
	WriteTimeout time.Duration `yaml:"write_timeout" category:"advanced"`

	// Maximum number of socket connections.
	ConnectionPoolSize int `yaml:"connection_pool_size" category:"advanced"`

	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	ConnectionPoolTimeout time.Duration `yaml:"connection_pool_timeout" category:"advanced"`

	// MinIdleConnections specifies the minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConnections int `yaml:"min_idle_connections" category:"advanced"`

	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// -1 disables idle timeout check.
	IdleTimeout time.Duration `yaml:"idle_timeout" category:"advanced"`

	// MaxConnectionAge is connection age at which client retires (closes) the connection.
	// Default 0 is to not close aged connections.
	MaxConnectionAge time.Duration `yaml:"max_connection_age" category:"advanced"`

	// MaxItemSize specifies the maximum size of an item stored in Redis.
	// Items bigger than MaxItemSize are skipped.
	// If set to 0, no maximum size is enforced.
	MaxItemSize int `yaml:"max_item_size" category:"advanced"`

	// MaxAsyncConcurrency specifies the maximum number of SetAsync goroutines.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency" category:"advanced"`

	// MaxAsyncBufferSize specifies the queue buffer size for SetAsync operations.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size" category:"advanced"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency" category:"advanced"`

	// MaxGetMultiBatchSize specifies the maximum size per batch for mget.
	MaxGetMultiBatchSize int `yaml:"max_get_multi_batch_size" category:"advanced"`

	// TLSEnabled enable TLS for Redis connection.
	TLSEnabled bool `yaml:"tls_enabled" category:"advanced"`

	// TLS to use to connect to the Redis server.
	TLS dstls.ClientConfig `yaml:",inline"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (c *RedisClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&c.Endpoint, prefix+"endpoint", "Redis Server or Cluster configuration endpoint to use for caching. A comma-separated list of endpoints for Redis Cluster or Redis Sentinel.")
	f.StringVar(&c.Username, prefix+"username", "", "Username to use when connecting to Redis.")
	f.Var(&c.Password, prefix+"password", "Password to use when connecting to Redis.")
	f.IntVar(&c.DB, prefix+"db", 0, "Database index.")
	f.StringVar(&c.MasterName, prefix+"master-name", "", "Redis Sentinel master name. An empty string for Redis Server or Redis Cluster.")
	f.DurationVar(&c.DialTimeout, prefix+"dial-timeout", time.Second*5, "Client dial timeout.")
	f.DurationVar(&c.ReadTimeout, prefix+"read-timeout", time.Second*3, "Client read timeout.")
	f.DurationVar(&c.WriteTimeout, prefix+"write-timeout", time.Second*3, "Client write timeout.")
	f.DurationVar(&c.ConnectionPoolTimeout, prefix+"connection-pool-timeout", time.Second*4, "Maximum duration to wait to get a connection from pool.")
	f.IntVar(&c.ConnectionPoolSize, prefix+"connection-pool-size", 100, "Maximum number of connections in the pool.")
	f.IntVar(&c.MinIdleConnections, prefix+"min-idle-connections", 10, "Minimum number of idle connections.")
	f.DurationVar(&c.MaxConnectionAge, prefix+"max-connection-age", 0, "Close connections older than this duration. If the value is zero, then the pool does not close connections based on age.")
	f.DurationVar(&c.IdleTimeout, prefix+"idle-timeout", time.Minute*5, "Amount of time after which client closes idle connections.")
	f.IntVar(&c.MaxAsyncConcurrency, prefix+"max-async-concurrency", 50, "The maximum number of concurrent asynchronous operations can occur.")
	f.IntVar(&c.MaxAsyncBufferSize, prefix+"max-async-buffer-size", 25000, "The maximum number of enqueued asynchronous operations allowed.")
	f.IntVar(&c.MaxGetMultiConcurrency, prefix+"max-get-multi-concurrency", 100, "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&c.MaxGetMultiBatchSize, prefix+"max-get-multi-batch-size", 100, "The maximum size per batch for mget operations.")
	f.IntVar(&c.MaxItemSize, prefix+"max-item-size", 16*1024*1024, "The maximum size of an item stored in Redis. Bigger items are not stored. If set to 0, no maximum size is enforced.")
	f.BoolVar(&c.TLSEnabled, prefix+"tls-enabled", false, "Enable connecting to Redis with TLS.")
	c.TLS.RegisterFlagsWithPrefix(prefix, f)
}

func (c *RedisClientConfig) Validate() error {
	if c.Endpoint.String() == "" {
		return ErrRedisConfigNoEndpoint
	}
	// Set async only available when MaxAsyncConcurrency > 0.
	if c.MaxAsyncConcurrency <= 0 {
		return ErrRedisMaxAsyncConcurrencyNotPositive
	}
	return nil
}

type RedisClient struct {
	*baseClient

	client redis.UniversalClient
	config RedisClientConfig

	// Name provides an identifier for the instantiated Client
	name string

	// getMultiGate used to enforce the max number of concurrent GetMulti() operations.
	getMultiGate gate.Gate

	logger log.Logger

	// Tracked metrics.
	clientInfo prometheus.GaugeFunc
}

// NewRedisClient makes a new RedisClient.
func NewRedisClient(logger log.Logger, name string, config RedisClientConfig, reg prometheus.Registerer) (*RedisClient, error) {
	opts := &redis.UniversalOptions{
		Addrs:        strings.Split(config.Endpoint.String(), ","),
		Username:     config.Username,
		Password:     config.Password.String(),
		DB:           config.DB,
		MasterName:   config.MasterName,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		PoolSize:     config.ConnectionPoolSize,
		PoolTimeout:  config.ConnectionPoolTimeout,
		MinIdleConns: config.MinIdleConnections,
		MaxConnAge:   config.MaxConnectionAge,
		IdleTimeout:  config.IdleTimeout,
	}

	if config.TLSEnabled {
		tlsClientConfig, err := config.TLS.GetTLSConfig()
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsClientConfig
	}

	reg = prometheus.WrapRegistererWith(
		prometheus.Labels{labelCacheName: name, labelCacheBackend: backendValueRedis},
		prometheus.WrapRegistererWithPrefix(cacheMetricNamePrefix, reg))

	metrics := newClientMetrics(reg)

	c := &RedisClient{
		baseClient: newBaseClient(logger, uint64(config.MaxItemSize), config.MaxAsyncBufferSize, config.MaxAsyncConcurrency, metrics),
		client:     redis.NewUniversalClient(opts),
		name:       name,
		config:     config,
		logger:     log.With(logger, "name", name),
	}
	if config.MaxGetMultiConcurrency > 0 {
		c.getMultiGate = gate.New(
			prometheus.WrapRegistererWithPrefix(getMultiMetricNamePrefix, reg),
			config.MaxGetMultiConcurrency,
		)
	}

	c.clientInfo = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: clientInfoMetricName,
		Help: "A metric with a constant '1' value labeled by configuration options from which redis client was configured.",
		ConstLabels: prometheus.Labels{
			"dial_timeout":              config.DialTimeout.String(),
			"read_timeout":              config.ReadTimeout.String(),
			"write_timeout":             config.WriteTimeout.String(),
			"connection_pool_timeout":   config.ConnectionPoolTimeout.String(),
			"connection_pool_size":      strconv.Itoa(config.ConnectionPoolSize),
			"max_async_concurrency":     strconv.Itoa(config.MaxAsyncConcurrency),
			"max_async_buffer_size":     strconv.Itoa(config.MaxAsyncBufferSize),
			"max_item_size":             strconv.FormatUint(uint64(config.MaxItemSize), 10),
			"max_get_multi_concurrency": strconv.Itoa(config.MaxGetMultiConcurrency),
			"max_get_multi_batch_size":  strconv.Itoa(config.MaxGetMultiBatchSize),
		},
	},
		func() float64 { return 1 },
	)
	return c, nil
}

// SetMultiAsync implements Cache.
func (c *RedisClient) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	c.setMultiAsync(data, ttl, func(key string, value []byte, ttl time.Duration) error {
		_, err := c.client.Set(context.Background(), key, value, ttl).Result()
		return err
	})
}

// SetAsync implements Cache.
func (c *RedisClient) SetAsync(key string, value []byte, ttl time.Duration) {
	c.setAsync(key, value, ttl, func(key string, buf []byte, ttl time.Duration) error {
		_, err := c.client.Set(context.Background(), key, buf, ttl).Result()
		return err
	})
}

// Set implements Cache.
func (c *RedisClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.storeOperation(ctx, key, value, ttl, opSet, func(ctx context.Context, key string, value []byte, ttl time.Duration) error {
		_, err := c.client.Set(ctx, key, value, ttl).Result()
		return err
	})
}

// Add implements Cache.
func (c *RedisClient) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.storeOperation(ctx, key, value, ttl, opAdd, func(ctx context.Context, key string, value []byte, ttl time.Duration) error {
		stored, err := c.client.SetNX(ctx, key, value, ttl).Result()
		if err != nil {
			return err
		}
		if !stored {
			return fmt.Errorf("%w: for Set NX operation on %s", ErrNotStored, key)
		}

		return nil
	})
}

// GetMulti implements Cache.
func (c *RedisClient) GetMulti(ctx context.Context, keys []string, _ ...Option) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	var mu sync.Mutex
	results := make(map[string][]byte, len(keys))
	c.metrics.requests.Add(float64(len(keys)))

	err := doWithBatch(ctx, len(keys), c.config.MaxGetMultiBatchSize, c.getMultiGate, func(startIndex, endIndex int) error {
		start := time.Now()
		c.metrics.operations.WithLabelValues(opGetMulti).Inc()

		var cacheHitBytes int

		currentKeys := keys[startIndex:endIndex]
		resp, err := c.client.MGet(ctx, currentKeys...).Result()
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(resp))
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < len(resp); i++ {
			key := currentKeys[i]
			switch val := resp[i].(type) {
			case string:
				cacheHitBytes += len(val)
				results[key] = stringToBytes(val)
			case nil: // miss
			default:
				level.Warn(c.logger).Log("msg",
					fmt.Sprintf("unexpected redis mget result type:%T %v", resp[i], resp[i]))
			}
		}
		c.metrics.dataSize.WithLabelValues(opGetMulti).Observe(float64(cacheHitBytes))
		c.metrics.duration.WithLabelValues(opGetMulti).Observe(time.Since(start).Seconds())
		return nil
	})
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(keys))
		return nil
	}

	c.metrics.hits.Add(float64(len(results)))
	return results
}

// Delete implement RemoteCacheClient.
func (c *RedisClient) Delete(ctx context.Context, key string) error {
	return c.delete(ctx, key, func(ctx context.Context, key string) error {
		return c.client.Del(ctx, key).Err()
	})
}

// Stop implement RemoteCacheClient.
func (c *RedisClient) Stop() {
	// Stop running async operations.
	c.asyncQueue.stop()

	if err := c.client.Close(); err != nil {
		level.Error(c.logger).Log("msg", "failed to close redis client", "err", err)
	}
}

func (c *RedisClient) Name() string {
	return c.name
}

// stringToBytes converts string to byte slice.
func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
