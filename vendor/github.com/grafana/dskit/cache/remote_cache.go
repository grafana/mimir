package cache

import (
	"context"
	"time"
)

var (
	_ Cache = (*RemoteCacheAdapter)(nil)
)

// RemoteCacheAdapter is an implementation of the Cache interface that wraps
// RemoteCacheClient instances. This adapter will eventually be removed when
// those interfaces are unified.
type RemoteCacheAdapter struct {
	remoteClient RemoteCacheClient
}

func NewRemoteCacheAdapter(remoteClient RemoteCacheClient) *RemoteCacheAdapter {
	return &RemoteCacheAdapter{
		remoteClient: remoteClient,
	}
}

func (c *RemoteCacheAdapter) StoreAsync(data map[string][]byte, ttl time.Duration) {
	c.remoteClient.SetMultiAsync(data, ttl)
}

func (c *RemoteCacheAdapter) Fetch(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	return c.remoteClient.GetMulti(ctx, keys, opts...)
}

func (c *RemoteCacheAdapter) Delete(ctx context.Context, key string) error {
	return c.remoteClient.Delete(ctx, key)
}

func (c *RemoteCacheAdapter) Name() string {
	return c.remoteClient.Name()
}
