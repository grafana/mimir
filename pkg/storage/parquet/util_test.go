// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/storage/parquet/util_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquet

import (
	"bytes"
	"context"
	"io"
)

type BufferReadAt struct {
	buffer *bytes.Buffer
}

func (b BufferReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	n = copy(p, b.buffer.Bytes()[off:off+int64(len(p))])
	return
}

func (b BufferReadAt) CreateReadAtWithContext(_ context.Context) io.ReaderAt {
	return b
}
