// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SpanlessTracingCache logs Fetch operation in the parent spans.
type SpanlessTracingCache struct {
	c      Cache
	logger log.Logger
}

func NewSpanlessTracingCache(cache Cache, logger log.Logger) Cache {
	return SpanlessTracingCache{c: cache, logger: logger}
}

func (t SpanlessTracingCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	t.c.Store(ctx, data, ttl)
}

func (t SpanlessTracingCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	var (
		bytes  int
		logger = spanlogger.FromContext(ctx, t.logger)
	)
	result = t.c.Fetch(ctx, keys)

	for _, v := range result {
		bytes += len(v)
	}
	level.Debug(logger).Log("msg", "cache_fetch", "name", t.Name(), "requested keys", len(keys), "returned keys", len(result), "returned bytes", bytes)

	return
}

func (t SpanlessTracingCache) Name() string {
	return t.c.Name()
}
