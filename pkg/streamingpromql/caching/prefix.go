// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/tenant"
)

// CacheKeyGenerator is a utility which constructs fully qualified cache keys.
// Generated prefixes are appended to keys allowing for the scope of a key to
// be limited.
// Two prefixes are supported - non hashable and plain prefixes.
// A non hashable prefix is not included in the overall hash of the key, but is instead
// prepended to the hashed version of the generated prefix + key.
type CacheKeyGenerator struct {
	nonHashable PrefixGenerator
	prefix      PrefixGenerator
}

func NewCacheKeyGenerator(nonHashable PrefixGenerator, prefix PrefixGenerator) *CacheKeyGenerator {
	return &CacheKeyGenerator{nonHashable: nonHashable, prefix: prefix}
}

// ComputeCacheKey generates a fully qualified cache key which includes the generated prefixes.
// For a given key, both a plain []byte and a hashed version of the fully qualified keys is returned.
// The plain version should be kept for detecting collisions.
// The plain []byte will be of the format; <non_hashable_prefix><prefix><key>
// The hashed version will be of the format; <non_hashable_prefix>hash(<prefix><key>)
func (c *CacheKeyGenerator) ComputeCacheKey(ctx context.Context, key []byte) (plain []byte, hashed string, err error) {
	if c.prefix != nil {
		prefix, err := c.prefix(ctx)
		if err != nil {
			return nil, "", err
		}
		key = append([]byte(prefix), key...)
	}

	hashed = HashCacheKey(key)

	if c.nonHashable != nil {
		prefix, err := c.nonHashable(ctx)
		if err != nil {
			return nil, "", err
		}

		key = append([]byte(prefix), key...)
		hashed = prefix + hashed
	}

	return key, hashed, nil
}

// PrefixGenerator returns the cache key prefix to use for the given context. The prefix
// identifies the namespace a cache entry belongs to, for example the tenant IDs.
// Callers must fold the prefix into both the hashed backend key
// and the value stored for collision verification, so that entries cannot be shared across
// tenants or label policies.
type PrefixGenerator func(ctx context.Context) (string, error)

// TenantPrefixGenerator returns a string of delimited tenantIDs.
// Note that the returned string includes a trailing : suffix.
func TenantPrefixGenerator(ctx context.Context) (string, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:", tenant.JoinTenantIDs(tenantIDs)), nil
}

// VersioningAndItemTypePrefixGenerator returns a PrefixGenerator which will generate
// prefixes which include the given itemType and version.
// The generated prefix will include a trailing :
func VersioningAndItemTypePrefixGenerator(itemType string, version uint) PrefixGenerator {
	return StaticPrefixGenerator(fmt.Sprintf("%s@v%d:", itemType, version))
}

// StaticPrefixGenerator returns a PrefixGenerator which emits the given prefix.
// Note that it is the callers responsibility to add a trailing delimiter on the given prefix.
func StaticPrefixGenerator(prefix string) PrefixGenerator {
	return func(ctx context.Context) (string, error) {
		return prefix, nil
	}
}
