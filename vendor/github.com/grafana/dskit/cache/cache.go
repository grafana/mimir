// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrNotStored  = errors.New("item not stored")
	ErrInvalidTTL = errors.New("invalid TTL")
)

// Cache is a high level interface to interact with a cache.
type Cache interface {
	// GetMulti fetches multiple keys at once from a cache. In case of error,
	// an empty map is returned and the error tracked/logged. One or more Option
	// instances may be passed to modify the behavior of this GetMulti call.
	GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte

	// SetAsync enqueues an operation to store a key into a cache. In case the underlying
	// operation fails, the error will be tracked/logged.
	SetAsync(key string, value []byte, ttl time.Duration)

	// SetMultiAsync enqueues operations to store a keys and values into a cache. In case
	// any underlying async operations fail, the errors will be tracked/logged.
	SetMultiAsync(data map[string][]byte, ttl time.Duration)

	// Set stores a key and value into a cache.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Add stores a key and value into a cache only if it does not already exist. If the
	// item was not stored because an entry already exists in the cache, ErrNotStored will
	// be returned.
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Delete deletes a key from a cache. This is a synchronous operation. If an asynchronous
	// set operation for key is still pending to be processed, it will wait for it to complete
	// before performing deletion.
	Delete(ctx context.Context, key string) error

	// Stop client and release underlying resources.
	Stop()

	// Name returns the name of this particular cache instance.
	Name() string
}

// Options are used to modify the behavior of an individual call to get results
// from a cache backend. They are constructed by applying Option callbacks passed
// to a client method to a default Options instance.
type Options struct {
	Alloc Allocator
}

// Option is a callback used to modify the Options that a particular client
// method uses.
type Option func(opts *Options)

// WithAllocator creates a new Option that makes use of a specific memory Allocator
// for cache result values.
func WithAllocator(alloc Allocator) Option {
	return func(opts *Options) {
		opts.Alloc = alloc
	}
}

// Allocator allows memory for cache result values to be managed by callers instead of by
// a cache client itself. For example, this can be used by callers to implement arena-style
// memory management if a workload tends to be request-centric.
type Allocator interface {
	// Get returns a byte slice with at least sz capacity. Length of the slice is
	// not guaranteed and so must be asserted by callers (cache clients).
	Get(sz int) *[]byte
	// Put returns the byte slice to the underlying allocator. The cache clients
	// will only call this method during error handling when allocated values are
	// not returned to the caller as cache results.
	Put(b *[]byte)
}

const (
	BackendMemcached = "memcached"
	BackendRedis     = "redis"
)

type BackendConfig struct {
	Backend   string                `yaml:"backend"`
	Memcached MemcachedClientConfig `yaml:"memcached"`
	Redis     RedisClientConfig     `yaml:"redis"`
}

// Validate the config.
func (cfg *BackendConfig) Validate() error {
	if cfg.Backend != "" && cfg.Backend != BackendMemcached && cfg.Backend != BackendRedis {
		return fmt.Errorf("unsupported cache backend: %s", cfg.Backend)
	}

	switch cfg.Backend {
	case BackendMemcached:
		return cfg.Memcached.Validate()
	case BackendRedis:
		return cfg.Redis.Validate()
	}
	return nil
}

func CreateClient(cacheName string, cfg BackendConfig, logger log.Logger, reg prometheus.Registerer) (Cache, error) {
	switch cfg.Backend {
	case "":
		// No caching.
		return nil, nil
	case BackendMemcached:
		return NewMemcachedClientWithConfig(logger, cacheName, cfg.Memcached, reg)
	case BackendRedis:
		return NewRedisClient(logger, cacheName, cfg.Redis, reg)
	default:
		return nil, errors.Errorf("unsupported cache type for cache %s: %s", cacheName, cfg.Backend)
	}
}
