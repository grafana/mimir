// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/atomicfs"
)

func downloadSparseHeader(
	ctx context.Context,
	id ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	ll log.Logger,
) error {
	localBlockDir := filepath.Join(localTenantDir, id.String())
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	_, err := os.Stat(localSparseHeaderPath)
	if err == nil {
		// The header is already on disk
		return nil
	}
	bucketSparseHeaderBytes, err := getSparseHeaderBytes(ctx, id, bkt, ll)
	if err != nil {
		return err
	}

	return atomicfs.CreateFile(localSparseHeaderPath, bytes.NewReader(bucketSparseHeaderBytes))
}

// getSparseHeaderBytes reads the raw sparse header bytes from object storage with bucket.Get.
// The bucket reader passed in must be prefixed with the tenant ID TSDB path in the object storage.
// This does not write the header bytes to local disk.
func getSparseHeaderBytes(
	ctx context.Context,
	id ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	ll log.Logger,
) ([]byte, error) {
	if tenantBkt == nil {
		return nil, fmt.Errorf("bucket is nil")
	}
	sparseHeaderObjPath := filepath.Join(id.String(), block.SparseIndexHeaderFilename)

	reader, err := tenantBkt.ReaderWithExpectedErrs(tenantBkt.IsObjNotFoundErr).Get(ctx, sparseHeaderObjPath)
	if err != nil {
		return nil, fmt.Errorf("getting sparse index-header from bucket: %w", err)
	}
	defer runutil.CloseWithLogOnErr(ll, reader, "close sparse index-header reader")

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading sparse index-header from bucket: %w", err)
	}

	// Check if we've been canceled after downloading
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	level.Info(ll).Log("msg", "downloaded sparse index-header from bucket")

	return data, nil
}
