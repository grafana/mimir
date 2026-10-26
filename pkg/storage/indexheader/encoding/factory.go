// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"hash/crc32"

	promencoding "github.com/prometheus/prometheus/tsdb/encoding"
)

const (
	numLenBytes = promencoding.LenHeaderSize

	// maxLenHeaderSize is the maximum size in bytes the len header field can be. It accounts for oversized index tables.
	maxLenHeaderSize = promencoding.LenHeaderSize + promencoding.OversizedLenHeaderSize
)

type DecbufFactory interface {
	// NewDecbufAtChecked returns a new binary decoding reader positioned after the length header.
	// It expects a big-endian-encoded content length after offset, followed by the contents and expected checksum.
	// A uint32 length of 1 selects an oversized header containing a big-endian uint64 length.
	// This method MUST check the CRC of the content and return an errored Decbuf if validation fails.
	NewDecbufAtChecked(ctx context.Context, offset int, table *crc32.Table) Decbuf

	// NewDecbufAtUnchecked returns a new binary decoding reader positioned after the length header.
	// It supports the same standard and oversized length headers as NewDecbufAtChecked.
	// This method MUST NOT validate or compute the CRC of the content.
	// To check the CRC of the content, use NewDecbufAtChecked.
	NewDecbufAtUnchecked(ctx context.Context, offset int) Decbuf

	// NewRawDecbuf returns a new binary decoding reader positioned at the beginning of the underlying data,
	// and spanning the entire length of the data segment.
	// It MUST NOT make any assumptions about the layout of the underlying data w.r.t checksums, TOC, etc.
	// and it MUST NOT validate or compute the CRC of the content.
	// To create a binary decoding reader for some subset of the data or to perform integrity checks,
	// use NewDecbufAtUnchecked or NewDecbufAtChecked.
	NewRawDecbuf(ctx context.Context) Decbuf

	// NewDecbufInSection returns a new binary decoding reader positioned at tableOffset + sectionStartOffset,
	// spanning to tableOffset+sectionEndOffset or the end of the table, whichever comes first.
	// Reading from a section of the table rather than the whole table allows the reader to leverage
	// shorter read operations, which can improve read latency and cache efficiency (if caching these reads).
	// It supports the same standard and oversized length headers as NewDecbufAtChecked.
	// This method MUST NOT validate or compute the CRC of the content.
	NewDecbufInSection(ctx context.Context, tableOffset, sectionStartOffset, sectionEndOffset int) Decbuf

	Close() error
}
