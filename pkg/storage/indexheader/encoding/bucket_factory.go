// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/thanos-io/objstore"
)

// BucketDecbufFactory creates new bucket-reader-backed Decbuf instances
// for a specific index-header file in object storage
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

func (bf *BucketDecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	// At this point we don't know the length of the section (length is -1).
	rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, int64(offset), -1)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get range from %s at offset %d: %w", bf.objectPath, offset, err)}
	}

	closeReader := true
	defer func() {
		if closeReader {
			rc.Close()
		}
	}()

	// Consume 4-byte length prefix of the section.
	lengthBytes := make([]byte, 4)
	n, err := io.ReadFull(rc, lengthBytes)
	if err != nil {
		return Decbuf{E: fmt.Errorf("read section length from %s at offset %d: %w", bf.objectPath, offset, err)}
	}
	if n != 4 {
		return Decbuf{E: fmt.Errorf(
			"insufficient bytes read from %s for section length at offset %d (got %d, wanted %d): %w",
			bf.objectPath, offset, n, 4, ErrInvalidSize,
		)}
	}

	contentLength := int(binary.BigEndian.Uint32(lengthBytes))
	bufLength := len(lengthBytes) + contentLength + crc32.Size

	r := newStreamReader(rc, len(lengthBytes), bufLength)
	r.seekReader = func(off int) error {
		rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, int64(offset+off), -1)
		if err != nil {
			return err
		}
		r.rc.Close()
		r.rc = rc
		return nil
	}

	d := Decbuf{r: r}
	closeReader = false

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(4)
	}

	return d
}

func (bf *BucketDecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return bf.NewDecbufAtChecked(offset, nil)
}

func (bf *BucketDecbufFactory) NewRawDecbuf() Decbuf {
	const offset = int64(0)

	rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, offset, -1)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get range from %s at offset %d: %w", bf.objectPath, offset, err)}
	}

	closeReader := true
	defer func() {
		if closeReader {
			rc.Close()
		}
	}()

	attrs, err := bf.bkt.Attributes(bf.ctx, bf.objectPath)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get size from %s: %w", bf.objectPath, err)}
	}

	r := newStreamReader(rc, 0, int(attrs.Size))
	r.seekReader = func(off int) error {
		rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, offset, attrs.Size)
		if err != nil {
			return err
		}
		r.rc.Close()
		r.rc = rc
		return nil
	}

	closeReader = false
	return Decbuf{r: r}
}

// Close cleans up resources associated with this BucketDecbufFactory.
// For bucket-based implementation, there are no resources to clean up;
// the bucket client lifecycle is managed by parent components.
func (bf *BucketDecbufFactory) Close() error {
	// Nothing to do for bucket-based implementation
	return nil
}

// streamReader implements BufReader with a bucket-based io.ReadCloser
type streamReader struct {
	rc     io.ReadCloser
	buf    *bufio.Reader
	pos    int
	length int

	seekReader func(off int) error
}

var netbufPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, 1<<20) // 1MiB buffer to reduce number of network IO
	},
}

// newStreamReader creates a new streamReader that wraps the given io.ReadCloser.
func newStreamReader(rc io.ReadCloser, pos, length int) *streamReader {
	r := &streamReader{
		rc:     rc,
		buf:    netbufPool.Get().(*bufio.Reader),
		pos:    pos,
		length: length,
	}
	r.buf.Reset(r.rc)
	return r
}

func (r *streamReader) Reset() error {
	return r.ResetAt(0)
}

func (r *streamReader) ResetAt(off int) error {
	if off > r.length {
		return ErrInvalidSize
	}

	if dist := off - r.pos; dist > 0 && dist < r.Buffered() {
		// skip ahead by discarding the distance bytes
		return r.Skip(dist)
	}

	// Objstore hides the io.ReadSeekCloser, that the underlying bucket clients implement.
	// So we reimplement it ourselves:
	// 1. Close the r.rc
	// 2. Re-read the object from new offset
	// 3. Reset the r.buf and the rest of the state.
	// TODO: evaluate if we need a more efficient approach
	if err := r.seekReader(off); err != nil {
		return err
	}

	r.buf.Reset(r.rc)
	r.pos = off

	return nil
}

func (r *streamReader) Skip(l int) error {
	if l > r.Len() {
		return ErrInvalidSize
	}

	// TODO: how to make sure we don't trash the cache when skipping
	n, err := r.buf.Discard(l)
	if n > 0 {
		r.pos += n
	}

	return err
}

func (r *streamReader) Peek(n int) ([]byte, error) {
	b, err := r.buf.Peek(n)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if len(b) > 0 {
		return b, nil
	}

	return nil, nil
}

func (r *streamReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := r.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (r *streamReader) ReadInto(b []byte) error {
	n, err := io.ReadFull(r.buf, b)
	if n > 0 {
		r.pos += n
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(b), err)
	} else if err != nil {
		return err
	}

	return nil
}

func (r *streamReader) Offset() int {
	return r.pos
}

func (r *streamReader) Len() int {
	return r.length - r.pos
}

func (r *streamReader) Size() int {
	return r.buf.Size()
}

func (r *streamReader) Buffered() int {
	return r.buf.Buffered()
}

func (r *streamReader) Close() error {
	err := r.rc.Close()
	netbufPool.Put(r.buf)
	return err
}
