// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
)

type PrefixGenerator func(ctx context.Context) (string, error)

type PrefixingCache struct {
	inner           Backend
	prefixGenerator PrefixGenerator
}

func NewPrefixingCache(inner Backend, prefixGenerator func(ctx context.Context) (string, error)) *PrefixingCache {
	return &PrefixingCache{inner: inner, prefixGenerator: prefixGenerator}
}

func (p *PrefixingCache) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	prefix, err := p.prefixGenerator(ctx)
	if err != nil {
		return err
	}

	return p.inner.SetAsync(ctx, prefix+key, value, ttl)
}

func (p *PrefixingCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	prefix, err := p.prefixGenerator(ctx)
	if err != nil {
		return nil, err
	}

	prefixedKeys := make([]string, len(keys))

	for i, key := range keys {
		prefixedKeys[i] = prefix + key
	}

	prefixedResults, err := p.inner.GetMulti(ctx, prefixedKeys, opts...)
	if err != nil {
		return nil, err
	}

	results := make(map[string][]byte, len(prefixedResults))
	for key, value := range prefixedResults {
		results[key[len(prefix):]] = value
	}

	return results, nil
}

func TenantPrefixGenerator(ctx context.Context) (string, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:", tenant.JoinTenantIDs(tenantIDs)), nil
}
