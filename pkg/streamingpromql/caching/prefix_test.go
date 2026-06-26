// SPDX-License-Identifier: AGPL-3.0-only

package caching

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
)

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

func TestVersioningAndItemTypePrefixGenerator(t *testing.T) {
	base := func(context.Context) (string, error) {
		return "tenant-1:", nil
	}

	prefix, err := VersioningAndItemTypePrefixGenerator(base, 2, "MQEQR")(context.Background())
	require.NoError(t, err)
	require.Equal(t, "MQEQR@v2:tenant-1::", prefix)
}
