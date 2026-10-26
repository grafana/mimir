// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/thanos-io/objstore"
)

type cachedContentLength struct {
	headerSize int
	length     uint64
}

// BucketDecbufFactory creates new bucket-reader-backed Decbuf instances
// for a specific index file in object storage
type BucketDecbufFactory struct {
	bkt             objstore.BucketReader
	objectPath      string                      // Path to index file in bucket
	sectionLenCache map[int]cachedContentLength // Maps offset -> section header
	mu              sync.Mutex
}

// NewBucketDecbufFactory creates a new BucketDecbufFactory for the given object path.
func NewBucketDecbufFactory(bkt objstore.BucketReader, objectPath string) *BucketDecbufFactory {
	return &BucketDecbufFactory{
		bkt:        bkt,
		objectPath: objectPath,
		// Allocate cache to hold the start offsets of Symbols and Postings Offsets tables.
		sectionLenCache: make(map[int]cachedContentLength, 2),
	}
}

func (bf *BucketDecbufFactory) getCachedContentLength(offset int) (int, uint64, bool) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	val, ok := bf.sectionLenCache[offset]
	return val.headerSize, val.length, ok
}

func (bf *BucketDecbufFactory) getContentLength(ctx context.Context, offset int) (int, uint64, error) {
	if sz, length, ok := bf.getCachedContentLength(offset); ok {
		return sz, length, nil
	}
	attrs, err := bf.bkt.Attributes(ctx, bf.objectPath)
	if err != nil {
		return 0, 0, fmt.Errorf("get size from %s: %w", bf.objectPath, err)
	}
	if offset > int(attrs.Size) {
		return 0, 0, fmt.Errorf("offset greater than object size of %s", bf.objectPath)
	}

	bf.mu.Lock()
	defer bf.mu.Unlock()

	if val, ok := bf.sectionLenCache[offset]; ok {
		return val.headerSize, val.length, nil
	}

	headerSize := numLenBytes
	metaReader := NewBucketReader(
		ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size)-offset,
	)

	lengthBuffer := make([]byte, maxLenHeaderSize)
	n, err := metaReader.Read(lengthBuffer)
	if err != nil {
		return 0, 0, err
	}
	if n != cap(lengthBuffer) {
		return 0, 0, fmt.Errorf("insufficient bytes read for size (got %d, wanted %d): %w", n, cap(lengthBuffer), ErrInvalidSize)
	}
	contentLength := uint64(binary.BigEndian.Uint32(lengthBuffer[:numLenBytes]))
	if contentLength == encoding.OversizedLenMarker {
		// The len field is the "oversized" marker: the remainder stores the real oversized length.
		contentLength = binary.BigEndian.Uint64(lengthBuffer[numLenBytes:])
		headerSize = maxLenHeaderSize
	}

	bf.sectionLenCache[offset] = cachedContentLength{headerSize: headerSize, length: contentLength}
	return headerSize, contentLength, nil
}

func (bf *BucketDecbufFactory) NewDecbufAtChecked(ctx context.Context, offset int, table *crc32.Table) Decbuf {
	headerSize, contentLength, err := bf.getContentLength(ctx, offset)
	if err != nil {
		return Decbuf{E: err}
	}

	bufferLength := headerSize + int(contentLength) + crc32.Size

	bufReader := NewBucketBufReader(ctx, bf.bkt, bf.objectPath, offset, bufferLength)
	// Skip the length header so the buffered reader starts at the content.
	err = bufReader.Skip(headerSize)
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
		d.ResetAt(headerSize)
	}

	return d
}

// NewDecbufInSection creates a Decbuf which can only read a section of a table.
// sectionStartOffset and sectionEndOffset are relative offsets from tableOffset (an absolute offset).
// If sectionEndOffset is beyond the end of the table, length is adjusted to only read to the end of the table.
func (bf *BucketDecbufFactory) NewDecbufInSection(ctx context.Context, tableOffset, sectionStartOffset, sectionEndOffset int) Decbuf {
	headerSize, contentLength, err := bf.getContentLength(ctx, tableOffset)
	if err != nil {
		return Decbuf{E: err}
	}

	tableRelativeEndOffset := headerSize + int(contentLength)

	// Force the reader to stop at the end of the table first
	if tableRelativeEndOffset < sectionEndOffset {
		sectionEndOffset = tableRelativeEndOffset
	}
	sectionLength := sectionEndOffset - sectionStartOffset
	if sectionLength <= 0 {
		return Decbuf{E: fmt.Errorf("section length must be greater than 0")}
	}
	bufReader := NewBucketBufReader(
		ctx,
		bf.bkt,
		bf.objectPath,
		tableOffset+sectionStartOffset,
		sectionLength,
	)

	return Decbuf{r: bufReader}
}

func (bf *BucketDecbufFactory) NewDecbufAtUnchecked(ctx context.Context, offset int) Decbuf {
	return bf.NewDecbufAtChecked(ctx, offset, nil)
}

func (bf *BucketDecbufFactory) NewRawDecbuf(ctx context.Context) Decbuf {
	const offset = 0

	attrs, err := bf.bkt.Attributes(ctx, bf.objectPath)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get size from %s: %w", bf.objectPath, err)}
	}
	// Create reader from full file range
	r := NewBucketBufReader(
		ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size),
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
