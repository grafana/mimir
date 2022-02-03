// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"time"
)

type MemcachedClientConfig struct {
	Host           string        `yaml:"host"`
	Service        string        `yaml:"service"`
	Addresses      string        `yaml:"addresses"` // EXPERIMENTAL.
	Timeout        time.Duration `yaml:"timeout"`
	MaxIdleConns   int           `yaml:"max_idle_conns"`
	MaxItemSize    int           `yaml:"max_item_size"`
	UpdateInterval time.Duration `yaml:"update_interval"`
	ConsistentHash bool          `yaml:"consistent_hash"`
	CBFailures     uint          `yaml:"circuit_breaker_consecutive_failures"`
	CBTimeout      time.Duration `yaml:"circuit_breaker_timeout"`  // reset error count after this long
	CBInterval     time.Duration `yaml:"circuit_breaker_interval"` // remain closed for this long after CBFailures errors
}

func (cfg *MemcachedClientConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Host, prefix+"memcached.hostname", "", description+"Hostname for memcached service to use. If empty and if addresses is unset, no memcached will be used.")
	f.StringVar(&cfg.Service, prefix+"memcached.service", "memcached", description+"SRV service used to discover memcache servers.")
	f.StringVar(&cfg.Addresses, prefix+"memcached.addresses", "", description+"EXPERIMENTAL: Comma separated addresses list in DNS Service Discovery format: https://cortexmetrics.io/docs/configuration/arguments/#dns-service-discovery")
	f.IntVar(&cfg.MaxIdleConns, prefix+"memcached.max-idle-conns", 16, description+"Maximum number of idle connections in pool.")
	f.DurationVar(&cfg.Timeout, prefix+"memcached.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on memcached requests.")
	f.DurationVar(&cfg.UpdateInterval, prefix+"memcached.update-interval", 1*time.Minute, description+"Period with which to poll DNS for memcache servers.")
	f.BoolVar(&cfg.ConsistentHash, prefix+"memcached.consistent-hash", true, description+"Use consistent hashing to distribute to memcache servers.")
	f.UintVar(&cfg.CBFailures, prefix+"memcached.circuit-breaker-consecutive-failures", 10, description+"Trip circuit-breaker after this number of consecutive dial failures (if zero then circuit-breaker is disabled).")
	f.DurationVar(&cfg.CBTimeout, prefix+"memcached.circuit-breaker-timeout", 10*time.Second, description+"Duration circuit-breaker remains open after tripping (if zero then 60 seconds is used).")
	f.DurationVar(&cfg.CBInterval, prefix+"memcached.circuit-breaker-interval", 10*time.Second, description+"Reset circuit-breaker counts after this long (if zero then never reset).")
	f.IntVar(&cfg.MaxItemSize, prefix+"memcached.max-item-size", 0, description+"The maximum size of an item stored in memcached. Bigger items are not stored. If set to 0, no maximum size is enforced.")
}
