// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/thanos-io/objstore"
)

// BucketDecbufFactory creates new bucket-reader-backed Decbuf instances
// for a specific index file in object storage
type BucketDecbufFactory struct {
	ctx             context.Context
	bkt             objstore.BucketReader
	objectPath      string // Path to index file in bucket
	sectionLenCache map[int]int
	mu              sync.Mutex
}

// NewBucketDecbufFactory creates a new BucketDecbufFactory for the given object path.
func NewBucketDecbufFactory(ctx context.Context, bkt objstore.BucketReader, objectPath string) *BucketDecbufFactory {
	return &BucketDecbufFactory{
		ctx:        ctx,
		bkt:        bkt,
		objectPath: objectPath,
		// Allocate cache to hold the start offsets of Symbols and Postings Offsets tables.
		sectionLenCache: make(map[int]int, 2),
	}
}

func (bf *BucketDecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	attrs, err := bf.bkt.Attributes(bf.ctx, bf.objectPath)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get size from %s: %w", bf.objectPath, err)}
	}
	if offset > int(attrs.Size) {
		return Decbuf{E: fmt.Errorf("offset greater than object size of %s", bf.objectPath)}
	}

	var contentLength int
	bf.mu.Lock()
	if cachedContentLength, ok := bf.sectionLenCache[offset]; ok {
		// Section length is cached
		contentLength = cachedContentLength
	} else {
		// We do not know section length yet;
		// use the lower-level BucketReader to scan the length data
		metaReader := NewBucketReader(
			bf.ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size)-offset,
		)

		lengthBytes := make([]byte, numLenBytes)
		n, err := metaReader.Read(lengthBytes)
		if err != nil {
			bf.mu.Unlock()
			return Decbuf{E: err}
		}
		if n != numLenBytes {
			bf.mu.Unlock()
			return Decbuf{E: fmt.Errorf("insufficient bytes read for size (got %d, wanted %d): %w", n, numLenBytes, ErrInvalidSize)}
		}
		contentLength = int(binary.BigEndian.Uint32(lengthBytes))
		bf.sectionLenCache[offset] = contentLength
	}
	bf.mu.Unlock()

	bufferLength := numLenBytes + contentLength + crc32.Size

	bufReader := NewBucketBufReader(bf.ctx, bf.bkt, bf.objectPath, offset, bufferLength)
	// bufReader is expected start at base offset + 4 after consuming length bytes
	err = bufReader.Skip(numLenBytes)
	if err != nil {
		_ = bufReader.Close()
		return Decbuf{E: err}
	}
	d := Decbuf{r: bufReader}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(numLenBytes)
	}

	return d
}

func (bf *BucketDecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return bf.NewDecbufAtChecked(offset, nil)
}

func (bf *BucketDecbufFactory) NewRawDecbuf() Decbuf {
	const offset = 0

	attrs, err := bf.bkt.Attributes(bf.ctx, bf.objectPath)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get size from %s: %w", bf.objectPath, err)}
	}
	// Create reader from full file range
	r := NewBucketBufReader(
		bf.ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size),
	)
	d := Decbuf{r: r}
	return d
}

// Close cleans up resources associated with this BucketDecbufFactory.
// For bucket-based implementation, there are no resources to clean up;
// the bucket client lifecycle is managed by parent components.
func (bf *BucketDecbufFactory) Close() error {
	// Nothing to do for bucket-based implementation
	return nil
}
