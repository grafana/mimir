package cache

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
)

var (
	ErrNoMemcachedAddresses = errors.New("no memcached addresses configured")
)

type MemcachedConfig struct {
	Addresses              string        `yaml:"addresses"`
	Timeout                time.Duration `yaml:"timeout"`
	MaxIdleConnections     int           `yaml:"max_idle_connections" category:"advanced"`
	MaxAsyncConcurrency    int           `yaml:"max_async_concurrency" category:"advanced"`
	MaxAsyncBufferSize     int           `yaml:"max_async_buffer_size" category:"advanced"`
	MaxGetMultiConcurrency int           `yaml:"max_get_multi_concurrency" category:"advanced"`
	MaxGetMultiBatchSize   int           `yaml:"max_get_multi_batch_size" category:"advanced"`
	MaxItemSize            int           `yaml:"max_item_size" category:"advanced"`
}

func (cfg *MemcachedConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format.")
	f.DurationVar(&cfg.Timeout, prefix+"timeout", 200*time.Millisecond, "The socket read/write timeout.")
	f.IntVar(&cfg.MaxIdleConnections, prefix+"max-idle-connections", 100, "The maximum number of idle connections that will be maintained per address.")
	f.IntVar(&cfg.MaxAsyncConcurrency, prefix+"max-async-concurrency", 50, "The maximum number of concurrent asynchronous operations can occur.")
	f.IntVar(&cfg.MaxAsyncBufferSize, prefix+"max-async-buffer-size", 25000, "The maximum number of enqueued asynchronous operations allowed.")
	f.IntVar(&cfg.MaxGetMultiConcurrency, prefix+"max-get-multi-concurrency", 100, "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&cfg.MaxGetMultiBatchSize, prefix+"max-get-multi-batch-size", 100, "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited.")
	f.IntVar(&cfg.MaxItemSize, prefix+"max-item-size", 1024*1024, "The maximum size of an item stored in memcached. Bigger items are not stored. If set to 0, no maximum size is enforced.")
}

func (cfg *MemcachedConfig) GetAddresses() []string {
	if cfg.Addresses == "" {
		return []string{}
	}

	return strings.Split(cfg.Addresses, ",")
}

// Validate the config.
func (cfg *MemcachedConfig) Validate() error {
	if len(cfg.GetAddresses()) == 0 {
		return ErrNoMemcachedAddresses
	}

	return nil
}

func (cfg MemcachedConfig) ToMemcachedClientConfig() MemcachedClientConfig {
	return MemcachedClientConfig{
		Addresses:                 cfg.GetAddresses(),
		Timeout:                   cfg.Timeout,
		MaxIdleConnections:        cfg.MaxIdleConnections,
		MaxAsyncConcurrency:       cfg.MaxAsyncConcurrency,
		MaxAsyncBufferSize:        cfg.MaxAsyncBufferSize,
		MaxGetMultiConcurrency:    cfg.MaxGetMultiConcurrency,
		MaxGetMultiBatchSize:      cfg.MaxGetMultiBatchSize,
		MaxItemSize:               flagext.Bytes(cfg.MaxItemSize),
		DNSProviderUpdateInterval: 30 * time.Second,
	}
}
