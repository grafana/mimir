// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"time"

	"github.com/grafana/dskit/cache"
)

type Adaptor struct {
	inner cache.Cache
}

// NewAdaptor wraps a dskit cache.Cache to satisfy caching.Backend.
func NewAdaptor(inner cache.Cache) *Adaptor {
	return &Adaptor{inner: inner}
}

func (a *Adaptor) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	return a.inner.GetMulti(ctx, keys, opts...), nil
}

func (a *Adaptor) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	a.inner.SetAsync(key, value, ttl)
	return nil
}
