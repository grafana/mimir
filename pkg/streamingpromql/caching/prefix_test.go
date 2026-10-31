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
	prefix := NewCacheKeyGenerator(nil, TenantPrefixGenerator, false)
	key, hashed, err := prefix.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("tenant-1:my-key"), key)
	require.Equal(t, HashCacheKey(key, false), hashed)
}

func TestCacheKeyGenerator_WithNonHashablePrefix(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	prefix := NewCacheKeyGenerator(VersioningAndItemTypePrefixGenerator("MQEQR", 1), TenantPrefixGenerator, false)
	key, hashed, err := prefix.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("MQEQR@v1:tenant-1:my-key"), key)
	require.Equal(t, "MQEQR@v1:"+HashCacheKey([]byte("tenant-1:my-key"), false), hashed)
}

func TestCacheKeyGenerator_CryptographicHash(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	nonCryptographicGenerator := NewCacheKeyGenerator(VersioningAndItemTypePrefixGenerator("MQEQR", 1), TenantPrefixGenerator, false)
	cryptographicGenerator := NewCacheKeyGenerator(VersioningAndItemTypePrefixGenerator("MQEQR", 1), TenantPrefixGenerator, true)

	nonCryptographicKey, nonCryptographicHash, err := nonCryptographicGenerator.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("MQEQR@v1:tenant-1:my-key"), nonCryptographicKey)

	cryptographicKey, cryptographicHash, err := cryptographicGenerator.ComputeCacheKey(ctx, []byte("my-key"))
	require.NoError(t, err)
	require.Equal(t, nonCryptographicKey, cryptographicKey)
	require.Equal(t, "MQEQR@v1:"+HashCacheKey([]byte("tenant-1:my-key"), true), cryptographicHash)
	require.NotEqual(t, cryptographicHash, nonCryptographicHash)
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
