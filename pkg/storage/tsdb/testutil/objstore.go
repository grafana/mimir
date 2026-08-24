// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/testutil/objstore.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package testutil

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func PrepareFilesystemBucket(t testing.TB) (objstore.Bucket, string) {
	storageDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	return objstore.WrapWithMetrics(&serializedUploadBucket{Bucket: bkt}, nil, "test"), storageDir
}

// serializedUploadBucket serializes Upload calls because the objstore filesystem provider
// fails concurrent uploads of the same object, unlike real object storages where the last
// writer wins. Serializing uploads avoids this error and allows us to test with concurrency.
// This wrapper can be removed once/if the filesystem Bucket is fixed to allow for this.
type serializedUploadBucket struct {
	objstore.Bucket

	uploadMx sync.Mutex
}

func (b *serializedUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.uploadMx.Lock()
	defer b.uploadMx.Unlock()

	return b.Bucket.Upload(ctx, name, r, opts...)
}
