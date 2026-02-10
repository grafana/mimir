package indexcache

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

var (
	postingsOffsetCacheKeyLabelHashBufferPool = sync.Pool{New: func() any {
		// We assume the label name/value pair is typically not longer than 1KB.
		b := make([]byte, 1024)
		return &b
	}}
)

type RemotePostingsOffsetTableCache struct {
	valCodec PostingsOffsetCacheCodec

	remote cache.Cache

	logger log.Logger
}

func NewRemotePostingsOffsetTableCache(remote cache.Cache, logger log.Logger) *RemotePostingsOffsetTableCache {
	return &RemotePostingsOffsetTableCache{
		valCodec: BigEndianPostingsOffsetCodec{},
		remote:   remote,
		logger:   logger,
	}
}

func (c *RemotePostingsOffsetTableCache) StorePostingsOffset(
	tenantID string, blockID ulid.ULID, lbl labels.Label, rng index.Range, ttl time.Duration,
) {
	key := PostingsOffsetCacheKey{tenantID, blockID, lbl}
	c.setAsync(key, rng, ttl)
}

func (c *RemotePostingsOffsetTableCache) FetchPostingsOffset(
	ctx context.Context, tenantID string, blockID ulid.ULID, lbl labels.Label,
) (index.Range, bool) {
	key := PostingsOffsetCacheKey{tenantID, blockID, lbl}
	return c.get(ctx, key)
}

func (c *RemotePostingsOffsetTableCache) get(ctx context.Context, key RemoteCacheKey) (index.Range, bool) {
	k := key.Key()
	results := c.remote.GetMulti(ctx, []string{k})
	val, ok := results[k]
	if !ok {
		return index.Range{}, false
	}

	rng, err := c.valCodec.DecodeRange(val)
	if err != nil {
		level.Error(c.logger).Log(
			"msg", "error decoding cache value to index.Range",
			"key", key,
			"value", val,
		)
		if err := c.remote.Delete(ctx, k); err != nil {
			level.Error(c.logger).Log(
				"msg", "error deleting malformed index.Range value from cache",
				"key", key,
				"value", val,
			)
		}
		return index.Range{}, false
	}

	return rng, true
}

func (c *RemotePostingsOffsetTableCache) setAsync(key RemoteCacheKey, rng index.Range, ttl time.Duration) {
	k := key.Key()
	val := c.valCodec.EncodeRange(rng)
	c.remote.SetAsync(k, val, ttl)
}
