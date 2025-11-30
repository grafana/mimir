// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/thanos-io/objstore"
)

// BucketDecbufFactory creates new in-memory decoding buffer instances
// by fetching data directly from object storage.
type BucketDecbufFactory struct {
	ctx        context.Context
	bkt        objstore.BucketReader
	objectPath string // Path to index file in bucket
}

// NewBucketDecbufFactory creates a new BucketDecbufFactory for the given object path.
func NewBucketDecbufFactory(ctx context.Context, bkt objstore.BucketReader, objectPath string) *BucketDecbufFactory {
	return &BucketDecbufFactory{
		ctx:        ctx,
		bkt:        bkt,
		objectPath: objectPath,
	}
}

// NewDecbufAtChecked returns a new bucket-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method checks the CRC of the content and will
// return an error Decbuf if it does not match the expected CRC.
func (bf *BucketDecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	// Read the 4-byte length prefix
	lengthBytes, err := bf.fetchRange(int64(offset), 4)
	if err != nil {
		return Decbuf{E: fmt.Errorf("fetch length prefix: %w", err)}
	}

	contentLength := int(binary.BigEndian.Uint32(lengthBytes))
	bufLength := 4 + contentLength + crc32.Size

	// Fetch the entire section
	// TODO(v): this must return a ReadCloser, not an in-mem buffer.
	data, err := bf.fetchRange(int64(offset), int64(bufLength))
	if err != nil {
		return Decbuf{E: fmt.Errorf("fetch section: %w", err)}
	}

	r := newBufReader(data)
	d := Decbuf{r: r}

	if d.ResetAt(4); d.Err() != nil {
		return d
	}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(4)
	}

	return d
}

// NewDecbufAtUnchecked returns a new bucket-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method does NOT compute the CRC of the content.
// To check the CRC of the content, use NewDecbufAtChecked.
func (bf *BucketDecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return bf.NewDecbufAtChecked(offset, nil)
}

// NewRawDecbuf returns a new bucket-backed decoding buffer positioned at the beginning of the file,
// spanning the entire length of the file. It does not make any assumptions about the contents of the
// file, nor does it perform any form of integrity check.
func (bf *BucketDecbufFactory) NewRawDecbuf() Decbuf {
	return Decbuf{E: fmt.Errorf("NewRawDecbuf is not supported: %w", errors.ErrUnsupported)}
}

// Stop cleans up resources associated with this BucketDecbufFactory.
// For bucket-based implementation, there are no resources to clean up.
func (bf *BucketDecbufFactory) Stop() {
	// Nothing to do for bucket-based implementation
}

// fetchRange fetches a range of bytes from the object storage.
func (bf *BucketDecbufFactory) fetchRange(offset, length int64) ([]byte, error) {
	rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, offset, length)
	if err != nil {
		return nil, fmt.Errorf("get range [%d, %d): %w", offset, offset+length, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	if int64(len(data)) != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, len(data))
	}

	return data, nil
}
