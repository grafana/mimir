// Package e2ecache provides pre-built cache service definitions for e2e tests.
package e2ecache

import (
	"github.com/grafana/e2e"
	"github.com/grafana/e2e/images"
)

const (
	MemcachedPort = 11211
	RedisPort     = 6379
)

func NewMemcached() *e2e.ConcreteService {
	return e2e.NewConcreteService(
		"memcached",
		images.Memcached,
		nil,
		e2e.NewTCPReadinessProbe(MemcachedPort),
		MemcachedPort,
	)
}

func NewRedis() *e2e.ConcreteService {
	return e2e.NewConcreteService(
		"redis",
		images.Redis,
		// Set a memory limit, eviction policy, and disable persistence since Redis
		// is used as a cache not a database. 64mb is picked for parity with the Memcached
		// default memory limit.
		e2e.NewCommand(
			"redis-server",
			"--maxmemory", "64mb",
			"--maxmemory-policy", "allkeys-lru",
			"--save", "''",
			"--appendonly", "no",
		),
		e2e.NewTCPReadinessProbe(RedisPort),
		RedisPort,
	)
}
