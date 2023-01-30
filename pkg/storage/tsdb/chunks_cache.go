// SPDX-License-Identifier: AGPL-3.0-only

package tsdb

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
)

var (
	errUnsupportedChunksCacheBackend = errors.New("unsupported chunks cache backend")
)

func NewChunksCache(cfg ChunksCacheConfig, logger log.Logger, reg prometheus.Registerer) (chunkscache.ChunksCache, error) {
	switch cfg.Backend {
	case "":
		return nil, nil
	case cache.BackendMemcached:
		client, err := cache.NewMemcachedClientWithConfig(logger, "chunks-cache", cfg.Memcached.ToMemcachedClientConfig(), prometheus.WrapRegistererWithPrefix("thanos_", reg))
		if err != nil {
			return nil, errors.Wrap(err, "create index cache memcached client")
		}
		return chunkscache.NewMemcachedChunksCache(logger, client, reg)
	default:
		return nil, errUnsupportedChunksCacheBackend
	}
}
