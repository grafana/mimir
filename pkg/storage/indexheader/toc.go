// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/filepool"
)

// TOCCompat unifies the Prometheus TSDB index TOC values available from different index types,
// containing only the TOC offsets required for index-header reads of the Symbols and Postings Offsets.
//
// The StreamBinaryReader can use either file-backed or bucket-backed DecbufFactory to read the index-header.
//
// The FilePoolDecbufFactory reads the index-header BinaryFormatV1 from disk.
// This index-header format differs from the full block index, as it only contains the Symbols and Postings Offsets:
//   - The section offsets differ from a full Prometheus TSDB index file
//   - The file metadata does not contain a full Prometheus block TOC, since not all index sections are present.
//
// The BucketDecbufFactory loads the full Prometheus TSDB index TOC from the block in object storage.
type TOCCompat struct {
	IndexVersion int

	Symbols uint64

	// PostingsListEnd is not a standard part of a block index TOC,
	// but is required by pkg/storage/indexheader/index.PostingOffsetTable
	// to mark the end of the last postings list entry in the index.
	//
	// Mimir currently only supports Prometheus index.FormatV2,
	// in which the end of the Postings list is the beginning of the Label Indices table.
	// Prometheus block index TOC only contains start offsets for sections, not end offsets,
	// so we use Label Indices Table offset as the end bound of the Postings List.
	PostingsListEnd     uint64
	PostingsOffsetTable uint64
}

// TOCFromBucketTSDBIndex builds the TOCCompat from the full Prometheus block index TOC in the bucket.
// This can be used to generate sparse headers in components without a full block index on disk, e.g. store-gateways.
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

// TOCFromIndexHeader builds a TOCCompat from the on-disk Mimir BinaryFormatV1.
// This format currently only exists on-disk in the store-gateways.
// The BinaryFormatV1 only has two main sections, which are copies of the Symbols and PostingsOffsets tables,
// and it has a different layout for the header/metadata and TOC.
// This results in different offsets for the relevant sections than a full Prometheus block index in the bucket.
func TOCFromIndexHeader(
	castagnoliTable *crc32.Table,
	decbufFactory streamencoding.DecbufFactory,
	l log.Logger,
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

	level.Debug(l).Log("msg", "index header file size", "bytes", indexHeaderSize)

	indexHeaderVersion = int(decbuf.Byte())
	if indexHeaderVersion != BinaryFormatV1 {
		return nil, 0, fmt.Errorf("unknown or unsupported index header format version %d", indexHeaderVersion)
	}

	indexVersion := int(decbuf.Byte())
	if indexVersion != index.FormatV2 {
		return nil, 0, fmt.Errorf("unknown or unsupported index format version %d", indexVersion)
	}

	postingsListEnd := decbuf.Be64()
	if err = decbuf.Err(); err != nil {
		return nil, 0, fmt.Errorf("cannot read version and index version: %w", err)
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

	if err := decbuf.Err(); err != nil {
		return nil, 0, err
	}

	return &TOCCompat{
		IndexVersion: indexVersion,
		Symbols:      symbols,

		PostingsListEnd:     postingsListEnd,
		PostingsOffsetTable: postingsOffsetTable,
	}, indexHeaderVersion, nil
}

func fetchRange(ctx context.Context, bkt objstore.BucketReader, objectPath string, offset, length int64) (data []byte, err error) {
	rc, err := bkt.GetRange(ctx, objectPath, offset, length)
	if err != nil {
		return nil, fmt.Errorf("get range [%d, %d): %w", offset, offset+length, err)
	}
	defer runutil.CloseWithErrCapture(&err, rc, "close range reader %s", objectPath)

	data, err = io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read range data: %w", err)
	}

	if int64(len(data)) != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, len(data))
	}

	return data, err
}

// tocFromDiskTSDBIndex builds a TOCCompat from the full Prometheus block index TOC on disk.
// This can be used to generate sparse headers in components with a full block index on disk:
// classic ingesters, block-builders, and compactors.
func tocFromDiskTSDBIndex(blockID ulid.ULID, dir string) (toc *TOCCompat, err error) {
	localBlockDir := filepath.Join(dir, blockID.String())
	localIndexPath := filepath.Join(localBlockDir, block.IndexFilename)

	f, err := os.Open(localIndexPath) // file will be closed by streamencoding.FileReader wrapper
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %w", err)
	}
	defer runutil.CloseWithErrCapture(&err, f, "index TOC from block index file")

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot stat index file: %w", err)
	}
	indexSize := stat.Size()

	indexReader, err := streamencoding.NewFileReader(f, 0, int(indexSize), filepool.SingleFilePoolNoopCloser{})
	if err != nil {
		return nil, fmt.Errorf("cannot create reader at offset 0 from index file: %w", err)
	}
	defer runutil.CloseWithErrCapture(&err, indexReader, "index TOC from block index file")

	magicBytes, err := indexReader.Read(4)
	if err != nil {
		return nil, fmt.Errorf("cannot read magic number bytes: %w", err)
	}

	if magic := binary.BigEndian.Uint32(magicBytes); int(magic) != index.MagicIndex {
		return nil, fmt.Errorf("invalid magic number %x", magic)
	}

	indexVersionByte, err := indexReader.Read(1)
	if err != nil {
		return nil, fmt.Errorf("cannot read index version byte: %w", err)
	}

	indexVersion := int(indexVersionByte[0])
	if indexVersion != index.FormatV2 {
		return nil, fmt.Errorf("unknown or unsupported index format version %d", indexVersion)
	}

	tocStartOffset := indexSize - indexTOCLen - crc32.Size
	tocLen := indexTOCLen + crc32.Size

	if err := indexReader.ResetAt(int(tocStartOffset)); err != nil {
		return nil, fmt.Errorf("cannot reset reader to index TOC offset: %w", err)
	}

	tocBytes, err := indexReader.Read(tocLen)
	if err != nil {
		return nil, fmt.Errorf("cannot read toc bytes: %w", err)
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
