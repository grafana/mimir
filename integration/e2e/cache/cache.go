package e2ecache

import (
	"github.com/grafana/mimir/integration/e2e"
	"github.com/grafana/mimir/integration/e2e/images"
)

const (
	MemcachedPort = 11211
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
