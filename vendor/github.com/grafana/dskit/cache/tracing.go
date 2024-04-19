package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/spanlogger"
)

var _ Cache = (*SpanlessTracingCache)(nil)

// SpanlessTracingCache wraps a Cache and logs Fetch operation in the parent spans.
type SpanlessTracingCache struct {
	c        Cache
	resolver spanlogger.TenantResolver
	logger   log.Logger
}

func NewSpanlessTracingCache(cache Cache, logger log.Logger, resolver spanlogger.TenantResolver) Cache {
	return SpanlessTracingCache{c: cache, resolver: resolver, logger: logger}
}

func (t SpanlessTracingCache) StoreAsync(data map[string][]byte, ttl time.Duration) {
	t.c.StoreAsync(data, ttl)
}

func (t SpanlessTracingCache) Fetch(ctx context.Context, keys []string, opts ...Option) (result map[string][]byte) {
	var (
		bytes  int
		logger = spanlogger.FromContext(ctx, t.logger, t.resolver)
	)
	result = t.c.Fetch(ctx, keys, opts...)

	for _, v := range result {
		bytes += len(v)
	}
	level.Debug(logger).Log("msg", "cache_fetch", "name", t.Name(), "requested keys", len(keys), "returned keys", len(result), "returned bytes", bytes)

	return
}

func (t SpanlessTracingCache) Name() string {
	return t.c.Name()
}

func (t SpanlessTracingCache) Delete(ctx context.Context, key string) error {
	return t.c.Delete(ctx, key)
}
