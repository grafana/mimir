// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
)

func TestCacheKeyGenerator_NoNonHashablePrefix(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	prefix := NewCacheKeyGenerator(nil, TenantPrefixGenerator)
	key, hashed, err := prefix.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("tenant-1:my-key"), key)
	require.Equal(t, HashCacheKey(key), hashed)
}

func TestCacheKeyGenerator_WithNonHashablePrefix(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	prefix := NewCacheKeyGenerator(VersioningAndItemTypePrefixGenerator("MQEQR", 1), TenantPrefixGenerator)
	key, hashed, err := prefix.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("MQEQR@v1:tenant-1:my-key"), key)
	require.Equal(t, "MQEQR@v1:"+HashCacheKey([]byte("tenant-1:my-key")), hashed)
}

func TestTenantPrefixGenerator(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	prefix, err := TenantPrefixGenerator(ctx)
	require.NoError(t, err)
	require.Equal(t, "tenant-1:", prefix)

	// Federated queries join multiple tenant IDs.
	ctx = user.InjectOrgID(context.Background(), "tenant-1|tenant-2")
	prefix, err = TenantPrefixGenerator(ctx)
	require.NoError(t, err)
	require.Equal(t, "tenant-1|tenant-2:", prefix)

	// No tenant in the context is an error.
	_, err = TenantPrefixGenerator(context.Background())
	require.Error(t, err)
}
