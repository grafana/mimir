// SPDX-License-Identifier: AGPL-3.0-only

package encoding

// BufReader provides the low-level byte access interface for Decbuf's read operations
type BufReader interface {
	// Reset moves the cursor to the beginning of the data segment owned by the reader,
	// at the base offset configured at reader initialization.
	Reset() error

	// ResetAt moves the cursor to the given offset in the data segment owned by the reader,
	// relative to the base offset configured at reader initialization.
	// CAUTION: This operation may be very expensive and result in the discard of buffered data.
	// Use Skip to move forward to avoid unnecessary buffer discard.
	// ResetAt should only be used to move backwards.
	//
	// Attempting to ResetAt to the end of the data segment is valid.
	// Attempting to ResetAt _beyond_ the end of the data segment will return an error.
	ResetAt(off int) error

	// Skip advances the cursor by the given number of bytes in the data segment.
	// It is valid to skip to exactly the end of the data segment.
	// It is NOT valid to skip beyond the end of the data segment;
	// in this case implementations MUST return an ErrInvalidSize error,
	// but MUST NOT advance the cursor or consume any remaining bytes.
	Skip(l int) error

	// Peek returns at most the given number of bytes from the data segment, without consuming them.
	// It is valid to Peek beyond the end of the data segment;
	// in this case implementations MUST return the available bytes up to the end and a nil error.
	//
	// The byte slice returned MUST remain valid for one subsequent Skip of the returned byte length;
	// callers use a Peek-Skip pattern in place of Read to avoid a slice allocation.
	// It is NOT valid to read the returned byte slice after any subsequent read operation:
	// Peek, Read, ReadInto, Reset, ResetAt, and Skip.
	//
	// Peek is limited to and only must support reads up to the underlying buffer length.
	// Caller checks Size first to see if the next read operation length fits in the underlying buffer,
	// then a Peek-Skip pattern is used to avoid the slice allocation which must occur in Read.
	Peek(n int) ([]byte, error)

	// Read returns the given number of bytes from the data segment, consuming them.
	// It is NOT valid to read beyond the end of the data segment;
	// in this case implementations MUST return a nil byte slice and an ErrInvalidSize error,
	// and the remaining bytes MUST be consumed.
	Read(n int) ([]byte, error)

	// ReadInto reads len(dst) bytes from the data segment into dst, consuming them.
	// It is NOT valid to read beyond the end of the data segment;
	// in this case implementations MUST return a nil byte slice and an ErrInvalidSize error,
	// and the remaining bytes MUST be consumed.
	ReadInto(dst []byte) error

	// Size returns the length of the underlying buffer in bytes.
	Size() int

	// Len returns the remaining number of bytes in the data segment owned by the reader,
	// from the current offset to the length configured at reader initialization.
	Len() int

	// Offset returns the cursor offset in the data segment owned by the reader,
	// relative to the base offset configured at reader initialization.
	Offset() int

	// Buffered returns the number of bytes that can be read from the reader which are already in memory.
	Buffered() int

	Close() error
}
