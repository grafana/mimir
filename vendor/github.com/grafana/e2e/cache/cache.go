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
		nil,
		e2e.NewTCPReadinessProbe(RedisPort),
		RedisPort,
	)
}
