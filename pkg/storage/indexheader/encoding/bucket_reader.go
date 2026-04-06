// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"fmt"
	"io"

	"github.com/thanos-io/objstore"
)

// BucketReader implements BufReader backed by an objstore.InstrumentedBucketReader.
// The in-memory buffer must be explicitly extended using BufferTo before reading.
type BucketReader struct {
	ctx    context.Context
	bkt    objstore.InstrumentedBucketReader
	name   string
	base   int64
	length int
	off    int
	buf    []byte
}

// NewBucketReader creates a new BucketReader for the segment of the named object
// beginning at base bytes and extending length bytes.
func NewBucketReader(ctx context.Context, bkt objstore.InstrumentedBucketReader, name string, base int64, length int) *BucketReader {
	return &BucketReader{
		ctx:    ctx,
		bkt:    bkt,
		name:   name,
		base:   base,
		length: length,
	}
}

// BufferTo extends the in-memory buffer to cover bytes [base, base+offset) by issuing
// a GetRange call for any bytes not yet buffered. It returns an error if offset is
// before the current cursor position.
func (r *BucketReader) BufferTo(offset int) error {
	if offset < r.off {
		return fmt.Errorf("cannot buffer to offset %d before current offset %d", offset, r.off)
	}
	bufferedEnd := len(r.buf)
	if offset <= bufferedEnd {
		return nil
	}
	fetchLen := offset - bufferedEnd
	rc, err := r.bkt.GetRange(r.ctx, r.name, r.base+int64(bufferedEnd), int64(fetchLen))
	if err != nil {
		return err
	}
	defer rc.Close()
	newData := make([]byte, fetchLen)
	if _, err = io.ReadFull(rc, newData); err != nil {
		return err
	}
	r.buf = append(r.buf, newData...)
	return nil
}

func (r *BucketReader) Reset() error {
	return r.ResetAt(0)
}

func (r *BucketReader) ResetAt(off int) error {
	if off > r.length {
		return ErrInvalidSize
	}
	r.off = off
	return nil
}

func (r *BucketReader) Skip(l int) error {
	if l > r.Len() {
		return ErrInvalidSize
	}
	r.off += l
	return nil
}

func (r *BucketReader) Peek(n int) ([]byte, error) {
	end := r.off + n
	bufferedEnd := len(r.buf)
	if end > bufferedEnd {
		end = bufferedEnd
	}
	if end <= r.off {
		return nil, nil
	}
	return r.buf[r.off:end], nil
}

func (r *BucketReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)
	if err := r.ReadInto(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (r *BucketReader) ReadInto(b []byte) error {
	n := len(b)
	end := r.off + n
	if end > r.length {
		r.off = r.length
		return fmt.Errorf("%w reading %d bytes", ErrInvalidSize, n)
	}
	if end > len(r.buf) {
		r.off = r.length
		return fmt.Errorf("%w reading %d bytes: data not buffered", ErrInvalidSize, n)
	}
	copy(b, r.buf[r.off:end])
	r.off = end
	return nil
}

// Size returns the number of bytes currently held in the in-memory buffer.
func (r *BucketReader) Size() int {
	return len(r.buf)
}

func (r *BucketReader) Len() int {
	return r.length - r.off
}

func (r *BucketReader) Offset() int {
	return r.off
}

func (r *BucketReader) Buffered() int {
	buffered := len(r.buf) - r.off
	if buffered < 0 {
		return 0
	}
	return buffered
}

func (r *BucketReader) Close() error {
	return nil
}
