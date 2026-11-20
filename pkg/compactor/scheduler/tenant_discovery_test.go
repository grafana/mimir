// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestTenantDiscoverer_listTenants_ShouldListAllReadBuckets(t *testing.T) {
	ctx := context.Background()
	writeBucket := objstore.NewInMemBucket()
	readBucket := objstore.NewInMemBucket()
	require.NoError(t, writeBucket.Upload(ctx, "tenant-1/placeholder", strings.NewReader("")))
	require.NoError(t, writeBucket.Upload(ctx, "tenant-3/placeholder", strings.NewReader("")))
	require.NoError(t, readBucket.Upload(ctx, "tenant-1/placeholder", strings.NewReader("")))
	require.NoError(t, readBucket.Upload(ctx, "tenant-2/placeholder", strings.NewReader("")))

	discoverer := &TenantDiscoverer{bkts: []objstore.Bucket{writeBucket, readBucket}}
	tenants, err := discoverer.listTenants(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-1", "tenant-2", "tenant-3"}, tenants)
}
