// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

// TOCCompat unifies the Prometheus TSDB index TOC values available from different index types,
// containing only the TOC offsets required for index-header reads of the Symbols and Postings Offsets.
//
// The StreamBinaryReader reads the index-header BinaryFormatV1 from disk.
// The section offsets for this format differ from a full Prometheus TSDB index
// and the file metadata does not contain all TOC values available from the full TSDB index.
//
// The BucketBinaryReader loads the full Prometheus TSDB index TOC from the block in object storage.
type TOCCompat struct {
	IndexVersion int

	Symbols uint64

	// PostingsListEnd is used by pkg/storage/indexheader/index.PostingOffsetTable
	// to mark the end of the last postings list entry in the index.
	// We refer to the end of the postings list rather than the start of the next index section,
	// as which section is next can vary across Prometheus TSDB index and Mimir index-header versions.
	PostingsListEnd     uint64
	PostingsOffsetTable uint64
}

// TOCFromBucketTSDBIndex builds the TOCCompat from the full Prometheus block index TOC in the bucket.
// A plain bucket reader is used rather than a bucket-based Decbuf to reduce object storage operations,
// as the Decbuf interface and implementations are designed for forward scanning.
func TOCFromBucketTSDBIndex(
	ctx context.Context,
	bkt objstore.BucketReader,
	indexPath string,
	indexAttrs objstore.ObjectAttributes,
) (*TOCCompat, error) {
	headerBytes, err := fetchRange(ctx, bkt, indexPath, 0, index.HeaderLen)
	if err != nil {
		return nil, fmt.Errorf("read index file header: %w", err)
	}

	if len(headerBytes) != index.HeaderLen {
		return nil, fmt.Errorf("unexpected index file header length: %d", len(headerBytes))
	}

	if magic := binary.BigEndian.Uint32(headerBytes[0:4]); magic != index.MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	indexVersion := int(headerBytes[4])
	if indexVersion != index.FormatV2 {
		return nil, fmt.Errorf("unknown or unsupported index format version %d", indexVersion)
	}

	tocBytes, err := fetchRange(ctx, bkt, indexPath, indexAttrs.Size-indexTOCLen-crc32.Size, indexTOCLen+crc32.Size)
	if err != nil {
		return nil, fmt.Errorf("read index TOC: %w", err)
	}

	tsdbTOC, err := index.NewTOCFromByteSlice(realByteSlice(tocBytes))
	if err != nil {
		return nil, fmt.Errorf("parse index TOC: %w", err)
	}

	postingsListEnd := tsdbTOC.LabelIndicesTable

	return &TOCCompat{
		IndexVersion: indexVersion,
		Symbols:      tsdbTOC.Symbols,

		PostingsListEnd:     postingsListEnd,
		PostingsOffsetTable: tsdbTOC.PostingsTable,
	}, nil

}

// TOCFromIndexHeader builds the TOCCompact from the on-disk Mimir index-header BinaryFormatV1.
// While any DecbufFactory implementation will work, including a bucket-based Decbuf,
// the parsing is only compatible with the on-disk format which does not exist in the bucket.
func TOCFromIndexHeader(
	castagnoliTable *crc32.Table,
	decbufFactory streamencoding.DecbufFactory,
	ll log.Logger,
) (toc *TOCCompat, indexHeaderVersion int, err error) {
	// Create a new raw decoding buffer with access to the entire index-header file to
	// read initial version information and the table of contents.
	decbuf := decbufFactory.NewRawDecbuf()
	defer runutil.CloseWithErrCapture(&err, &decbuf, "index TOC from index header")
	if err = decbuf.Err(); err != nil {
		return nil, 0, fmt.Errorf("cannot create decoding buffer: %w", err)
	}

	// Grab the full length of the index header before we read any of it. This is needed
	// so that we can skip directly to the table of contents at the end of file.
	indexHeaderSize := decbuf.Len()
	if magic := decbuf.Be32(); magic != MagicIndex {
		return nil, 0, fmt.Errorf("invalid magic number %x", magic)
	}

	level.Debug(ll).Log("msg", "index header file size", "bytes", indexHeaderSize)

	indexHeaderVersion = int(decbuf.Byte())
	indexVersion := int(decbuf.Byte())
	if indexVersion != index.FormatV2 {
		return nil, 0, fmt.Errorf("unknown or unsupported index format version %d", indexVersion)
	}

	// As of now this value is also the actual end of the last posting list. In the future
	// it may be some bytes after the actual end (e.g. in case Prometheus starts adding padding
	// after the last posting list).
	// This value used to be the offset of the postings offset table up to and including Mimir 2.7.
	// After that this is the offset of the label indices table.
	// So what we read here will depend on what version of Mimir created the index header file.
	postingsListEnd := decbuf.Be64()

	if err = decbuf.Err(); err != nil {
		return nil, 0, fmt.Errorf("cannot read version and index version: %w", err)
	}

	if indexHeaderVersion != BinaryFormatV1 {
		return nil, 0, fmt.Errorf("unknown index-header file version %d", indexHeaderVersion)
	}

	indexHeaderTOCOffset := indexHeaderSize - BinaryTOCLen
	if decbuf.ResetAt(indexHeaderTOCOffset); decbuf.Err() != nil {
		return nil, 0, decbuf.Err()
	}

	if decbuf.CheckCrc32(castagnoliTable); decbuf.Err() != nil {
		return nil, 0, decbuf.Err()
	}
	decbuf.ResetAt(indexHeaderTOCOffset)
	symbols := decbuf.Be64()
	postingsOffsetTable := decbuf.Be64()

	if err = decbuf.Err(); err != nil {
		return nil, 0, err
	}

	toc = &TOCCompat{
		IndexVersion: indexVersion,
		Symbols:      symbols,

		PostingsListEnd:     postingsListEnd,
		PostingsOffsetTable: postingsOffsetTable,
	}
	return toc, indexHeaderVersion, nil
}
