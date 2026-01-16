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
	// At this point we don't know the length of the section (length is -1).
	rc, err := bf.bkt.GetRange(bf.ctx, bf.objectPath, int64(offset), -1)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get range at offset %d: %w", offset, err)}
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
		return Decbuf{E: fmt.Errorf("read section length at offset %d: %w", offset, err)}
	}
	if n != 4 {
		return Decbuf{E: fmt.Errorf("insufficient bytes read for size at offset %d (got %d, wanted %d): %w", offset, n, 4, ErrInvalidSize)}
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

// Close cleans up resources associated with this BucketDecbufFactory.
// For bucket-based implementation, there are no resources to clean up.
func (bf *BucketDecbufFactory) Close() error {
	// Nothing to do for bucket-based implementation
	return nil
}

// streamReader wraps an io.ReadCloser and provides the reader interface for streaming data.
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
	// So we reimplement it ourselves: close the r.rc, re-read the object from new offset, reset the r.buf and the rest of the state.
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

	// TODO(v): how to make sure we don't trash the cache when skipping
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
