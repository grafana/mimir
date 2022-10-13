// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/testutil/objstore.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func PrepareFilesystemBucket(t testing.TB) (objstore.Bucket, string) {
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	return objstore.BucketWithMetrics("test", bkt, nil), storageDir
}
