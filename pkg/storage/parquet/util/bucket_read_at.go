// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/306c8486441da41d2a655fa29d0e83820437cf23/util/bucket_read_at.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package util

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

type bReadAt struct {
	path string
	obj  objstore.Bucket
	ctx  context.Context
}

func NewBucketReadAt(ctx context.Context, path string, obj objstore.Bucket) io.ReaderAt {
	return &bReadAt{
		path: path,
		obj:  obj,
		ctx:  ctx,
	}
}

func (b *bReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.obj.GetRange(b.ctx, b.path, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()
	n, err = rc.Read(p)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return
}
