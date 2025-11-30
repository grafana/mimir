// SPDX-License-Identifier: AGPL-3.0-only

package encoding

// bufReader is an in-memory reader for data fetched from object storage.
// It provides the same interface as fileReader but operates on a byte slice in memory.
type bufReader struct {
	data []byte // Entire section in memory
	pos  int    // Current position in data
	base int    // Base offset (for position tracking)
}

// newBufReader creates a new bufReader with the given data.
// base is the absolute offset in the original file (used for position tracking).
func newBufReader(data []byte) *bufReader {
	return &bufReader{
		data: data,
		pos:  0,
	}
}

// reset moves the cursor position to the beginning of the data segment.
func (b *bufReader) reset() error {
	return b.resetAt(0)
}

// resetAt moves the cursor position to the given offset in the data segment.
// Attempting to resetAt beyond the end of the data segment will return an error.
func (b *bufReader) resetAt(off int) error {
	// Here we compare against the actual buffer and not against the remaining data.
	if off > len(b.data) {
		return ErrInvalidSize
	}
	b.pos = off
	return nil
}

// skip advances the cursor position by the given number of bytes.
// Attempting to skip beyond the end of the data segment will return an error.
func (b *bufReader) skip(l int) error {
	if l > b.len() {
		return ErrInvalidSize
	}
	b.pos += l
	return nil
}

// peek returns at most the given number of bytes from the data segment
// without consuming them. It is valid to peek beyond the end of the segment;
// in this case, the available bytes are returned with a nil error.
func (b *bufReader) peek(n int) ([]byte, error) {
	available := b.len()
	if n > available {
		n = available
	}
	if n == 0 {
		return nil, nil
	}
	return b.data[b.pos : b.pos+n], nil
}

// read returns the given number of bytes from the data segment, consuming them.
// It is NOT valid to read beyond the end of the segment.
func (b *bufReader) read(n int) ([]byte, error) {
	if n > b.len() {
		return nil, ErrInvalidSize
	}
	result := make([]byte, n)
	copy(result, b.data[b.pos:b.pos+n])
	b.pos += n
	return result, nil
}

// readInto reads len(buf) bytes from the data segment into buf, consuming them.
// It is NOT valid to readInto beyond the end of the segment.
func (b *bufReader) readInto(buf []byte) error {
	n := len(buf)
	if n > b.len() {
		return ErrInvalidSize
	}
	copy(buf, b.data[b.pos:b.pos+n])
	b.pos += n
	return nil
}

// size returns the buffer size (for compatibility with fileReader interface).
// Since bufReader operates on in-memory data, this returns a nominal buffer size.
func (b *bufReader) size() int {
	return readerBufferSize
}

// len returns the remaining number of bytes in the data segment.
func (b *bufReader) len() int {
	return len(b.data) - b.pos
}

// position returns the current position.
func (b *bufReader) position() int {
	return b.pos
}

// buffered returns the number of bytes that can be read without I/O.
// For bufReader, all data is already in memory, so this returns the remaining bytes.
func (b *bufReader) buffered() int {
	return b.len()
}

// close cleans up resources. For bufReader, there's nothing to close.
func (b *bufReader) close() error {
	return nil
}
