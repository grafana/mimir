// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/tenant"
)

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
// prefixes which include the given itemType, version and generated prefix from the given base PrefixGenerator.
func VersioningAndItemTypePrefixGenerator(base PrefixGenerator, version uint, itemType string) PrefixGenerator {
	return func(ctx context.Context) (string, error) {
		prefix, err := base(ctx)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s@v%d:%s:", itemType, version, prefix), nil
	}
}
