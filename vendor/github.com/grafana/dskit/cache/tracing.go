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
	next     Cache
	resolver spanlogger.TenantResolver
	logger   log.Logger
}

func NewSpanlessTracingCache(cache Cache, logger log.Logger, resolver spanlogger.TenantResolver) *SpanlessTracingCache {
	return &SpanlessTracingCache{next: cache, resolver: resolver, logger: logger}
}

func (t *SpanlessTracingCache) SetAsync(key string, value []byte, ttl time.Duration) {
	t.next.SetAsync(key, value, ttl)
}

func (t *SpanlessTracingCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	t.next.SetMultiAsync(data, ttl)
}

func (t *SpanlessTracingCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return t.next.Set(ctx, key, value, ttl)
}

func (t *SpanlessTracingCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return t.next.Add(ctx, key, value, ttl)
}

func (t *SpanlessTracingCache) GetMulti(ctx context.Context, keys []string, opts ...Option) (result map[string][]byte) {
	var (
		bytes  int
		logger = spanlogger.FromContext(ctx, t.logger, t.resolver)
	)
	result = t.next.GetMulti(ctx, keys, opts...)

	for _, v := range result {
		bytes += len(v)
	}
	level.Debug(logger).Log("msg", "cache_fetch", "name", t.Name(), "requested keys", len(keys), "returned keys", len(result), "returned bytes", bytes)

	return
}

func (t *SpanlessTracingCache) Name() string {
	return t.next.Name()
}

func (t *SpanlessTracingCache) Stop() {
	t.next.Stop()
}

func (t *SpanlessTracingCache) Delete(ctx context.Context, key string) error {
	return t.next.Delete(ctx, key)
}
