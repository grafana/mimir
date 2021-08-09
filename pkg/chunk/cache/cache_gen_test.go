// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/cache_gen_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheGenNumCacheKeysPrefix(t *testing.T) {
	keys := []string{"foo", "bar", "baz"}

	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{
			name: "empty-prefix",
		},
		{
			name:   "with-prefix",
			prefix: "prefix",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := InjectCacheGenNumber(context.Background(), tc.prefix)

			prefixedKeys := addCacheGenNumToCacheKeys(ctx, keys)
			for i, key := range prefixedKeys {
				require.Equal(t, tc.prefix+keys[i], key)
			}
			require.Len(t, prefixedKeys, len(keys))

			unprefixedKeys := removeCacheGenNumFromKeys(ctx, prefixedKeys)
			for i, key := range unprefixedKeys {
				require.Equal(t, keys[i], key)
			}
			require.Len(t, unprefixedKeys, len(keys))
		})
	}
}
