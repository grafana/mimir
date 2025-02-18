// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"bufio"
	"context"
	"encoding/binary"
	"golang.org/x/sync/errgroup"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const (
	// BinaryFormatV1 represents first version of index-header file.
	BinaryFormatV1 = 1

	indexTOCLen  = 6*8 + crc32.Size
	binaryTOCLen = 2*8 + crc32.Size
	// headerLen represents number of bytes reserved of index header for header.
	// At present, it is:
	// - 4 bytes for MagicIndex
	// - 1 byte for index header version
	// - 1 byte for TSDB index version
	// - 8 bytes for an offset in the TSDB index after the last posting list.
	headerLen = 4 + 1 + 1 + 8

	// MagicIndex are 4 bytes at the head of an index-header file.
	MagicIndex = 0xBAAAD792
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// BinaryTOC is a table of content for index-header file.
type BinaryTOC struct {
	// Symbols holds start to the same symbols section as index related to this index header.
	Symbols uint64
	// PostingsOffsetTable holds start to the same Postings Offset Table section as index related to this index header.
	PostingsOffsetTable uint64
}

// WriteBinary build index-header file from the pieces of index in object storage.
func WriteBinary(ctx context.Context, bkt objstore.BucketReader, id ulid.ULID, filename string) (err error) {
	ir, indexVersion, err := newChunkedIndexReader(ctx, bkt, id)
	if err != nil {
		return errors.Wrap(err, "new index reader")
	}
	tmpFilename := filename + ".tmp"

	// Buffer for copying and encbuffers.
	// This also will control the size of file writer buffer.
	buf := make([]byte, 32*1024)
	bw, err := newBinaryWriter(tmpFilename, buf)
	if err != nil {
		return errors.Wrap(err, "new binary index header writer")
	}
	defer runutil.CloseWithErrCapture(&err, bw, "close binary writer for %s", tmpFilename)

	// We put the end of the last posting list as the beginning of the label indices table.
	// As of now this value is also the actual end of the last posting list. In the future
	// it may be some bytes after the actual end (e.g. in case Prometheus starts adding padding
	// after the last posting list).
	if err := bw.AddIndexMeta(indexVersion, ir.toc.LabelIndicesTable); err != nil {
		return errors.Wrap(err, "add index meta")
	}

	// Copying symbols and posting offsets into the encbuffer both require a range query against the provider.
	// We make these calls in parallel and then syncronize before writing to buf
	var g errgroup.Group
	var sym, tbl io.ReadCloser
	var symerr, tblerr error
	g.Go(func() (err error) {
		sym, symerr = ir.bkt.GetRange(ir.ctx, ir.path, int64(ir.toc.Symbols), int64(ir.toc.Series-ir.toc.Symbols))
		if symerr != nil {
			return errors.Wrapf(err, "get symbols from object storage of %s", ir.path)
		}
		return
	})

	g.Go(func() (err error) {
		tbl, tblerr = ir.bkt.GetRange(ir.ctx, ir.path, int64(ir.toc.PostingsTable), int64(ir.size-ir.toc.PostingsTable))
		if tblerr != nil {
			return errors.Wrapf(err, "get posting offset table from object storage of %s", ir.path)
		}
		return
	})

	defer func() {
		runutil.CloseWithErrCapture(&symerr, sym, "close symbol reader")
		runutil.CloseWithErrCapture(&tblerr, tbl, "close posting offsets reader")
	}()

	if err := g.Wait(); err != nil {
		return err
	}

	if _, err := io.CopyBuffer(bw.SymbolsWriter(), sym, buf); err != nil {
		return errors.Wrap(err, "copy posting offsets")
	}

	if err := bw.f.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	if _, err := io.CopyBuffer(bw.PostingOffsetsWriter(), tbl, buf); err != nil {
		return errors.Wrap(err, "copy posting offsets")
	}

	if err := bw.f.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	if err := bw.WriteTOC(); err != nil {
		return errors.Wrap(err, "write index header TOC")
	}

	if err := bw.f.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	if err := bw.f.f.Sync(); err != nil {
		return errors.Wrap(err, "sync")
	}

	// Create index-header in atomic way, to avoid partial writes (e.g during restart or crash of store GW).
	return os.Rename(tmpFilename, filename)
}

type chunkedIndexReader struct {
	ctx  context.Context
	path string
	size uint64
	bkt  objstore.BucketReader
	toc  *index.TOC
}

func newChunkedIndexReader(ctx context.Context, bkt objstore.BucketReader, id ulid.ULID) (*chunkedIndexReader, int, error) {
	indexFilepath := filepath.Join(id.String(), block.IndexFilename)
	attrs, err := bkt.Attributes(ctx, indexFilepath)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get object attributes of %s", indexFilepath)
	}

	rc, err := bkt.GetRange(ctx, indexFilepath, 0, index.HeaderLen)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get TOC from object storage of %s", indexFilepath)
	}

	b, err := io.ReadAll(rc)
	if err != nil {
		runutil.CloseWithErrCapture(&err, rc, "close reader")
		return nil, 0, errors.Wrapf(err, "get header from object storage of %s", indexFilepath)
	}

	if err := rc.Close(); err != nil {
		return nil, 0, errors.Wrap(err, "close reader")
	}

	if m := binary.BigEndian.Uint32(b[0:4]); m != index.MagicIndex {
		return nil, 0, errors.Errorf("invalid magic number %x for %s", m, indexFilepath)
	}

	version := int(b[4:5][0])

	if version != index.FormatV1 && version != index.FormatV2 {
		return nil, 0, errors.Errorf("not supported index file version %d of %s", version, indexFilepath)
	}

	ir := &chunkedIndexReader{
		ctx:  ctx,
		path: indexFilepath,
		size: uint64(attrs.Size),
		bkt:  bkt,
	}

	toc, err := ir.readTOC()
	if err != nil {
		return nil, 0, err
	}
	ir.toc = toc

	return ir, version, nil
}

func (r *chunkedIndexReader) readTOC() (*index.TOC, error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.size-indexTOCLen-crc32.Size), indexTOCLen+crc32.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "get TOC from object storage of %s", r.path)
	}

	tocBytes, err := io.ReadAll(rc)
	if err != nil {
		runutil.CloseWithErrCapture(&err, rc, "close toc reader")
		return nil, errors.Wrapf(err, "get TOC from object storage of %s", r.path)
	}

	if err := rc.Close(); err != nil {
		return nil, errors.Wrap(err, "close toc reader")
	}

	toc, err := index.NewTOCFromByteSlice(realByteSlice(tocBytes))
	if err != nil {
		return nil, errors.Wrap(err, "new TOC")
	}
	return toc, nil
}

func (r *chunkedIndexReader) CopySymbols(w io.Writer, buf []byte) (err error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.toc.Symbols), int64(r.toc.Series-r.toc.Symbols))
	if err != nil {
		return errors.Wrapf(err, "get symbols from object storage of %s", r.path)
	}
	defer runutil.CloseWithErrCapture(&err, rc, "close symbol reader")

	if _, err := io.CopyBuffer(w, rc, buf); err != nil {
		return errors.Wrap(err, "copy symbols")
	}

	return nil
}

func (r *chunkedIndexReader) CopyPostingsOffsets(w io.Writer, buf []byte) (err error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.toc.PostingsTable), int64(r.size-r.toc.PostingsTable))
	if err != nil {
		return errors.Wrapf(err, "get posting offset table from object storage of %s", r.path)
	}
	defer runutil.CloseWithErrCapture(&err, rc, "close posting offsets reader")

	if _, err := io.CopyBuffer(w, rc, buf); err != nil {
		return errors.Wrap(err, "copy posting offsets")
	}

	return nil
}

// TODO(bwplotka): Add padding for efficient read.
type binaryWriter struct {
	f *FileWriter

	toc BinaryTOC

	// Reusable memory.
	buf encoding.Encbuf

	crc32 hash.Hash
}

func newBinaryWriter(fn string, buf []byte) (w *binaryWriter, err error) {
	dir := filepath.Dir(fn)

	df, err := fileutil.OpenDir(dir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
		df, err = fileutil.OpenDir(dir)
	}
	if err != nil {

		return nil, err
	}

	defer runutil.CloseWithErrCapture(&err, df, "dir close")

	if err := os.RemoveAll(fn); err != nil {
		return nil, errors.Wrap(err, "remove any existing index at path")
	}

	// We use file writer for buffers not larger than reused one.
	f, err := NewFileWriter(fn, len(buf))
	if err != nil {
		return nil, err
	}
	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	w = &binaryWriter{
		f: f,

		// Reusable memory.
		buf:   encoding.Encbuf{B: buf},
		crc32: newCRC32(),
	}

	w.buf.Reset()
	w.buf.PutBE32(MagicIndex)
	w.buf.PutByte(BinaryFormatV1)

	return w, w.f.Write(w.buf.Get())
}

type FileWriter struct {
	f    *os.File
	fbuf *bufio.Writer
	pos  uint64
	name string
}

// TODO(bwplotka): Added size to method, upstream this.
func NewFileWriter(name string, size int) (*FileWriter, error) {
	f, err := os.OpenFile(filepath.Clean(name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &FileWriter{
		f:    f,
		fbuf: bufio.NewWriterSize(f, size),
		pos:  0,
		name: name,
	}, nil
}

func (fw *FileWriter) Pos() uint64 {
	return fw.pos
}

func (fw *FileWriter) Write(bufs ...[]byte) error {
	for _, b := range bufs {
		n, err := fw.fbuf.Write(b)
		fw.pos += uint64(n)
		if err != nil {
			return err
		}
		// For now the index file must not grow beyond 64GiB. Some of the fixed-sized
		// offset references in v1 are only 4 bytes large.
		// Once we move to compressed/varint representations in those areas, this limitation
		// can be lifted.
		if fw.pos > 16*math.MaxUint32 {
			return errors.Errorf("%q exceeding max size of 64GiB", fw.name)
		}
	}
	return nil
}

func (fw *FileWriter) Flush() error {
	return fw.fbuf.Flush()
}

func (fw *FileWriter) WriteAt(buf []byte, pos uint64) error {
	if err := fw.Flush(); err != nil {
		return err
	}
	_, err := fw.f.WriteAt(buf, int64(pos))
	return err
}

// AddPadding adds zero byte padding until the file size is a multiple size.
func (fw *FileWriter) AddPadding(size int) error {
	p := fw.pos % uint64(size)
	if p == 0 {
		return nil
	}
	p = uint64(size) - p

	if err := fw.Write(make([]byte, p)); err != nil {
		return errors.Wrap(err, "add padding")
	}
	return nil
}

func (fw *FileWriter) Close() error {
	if err := fw.Flush(); err != nil {
		return err
	}
	if err := fw.f.Sync(); err != nil {
		return err
	}
	return fw.f.Close()
}

func (fw *FileWriter) Remove() error {
	return os.Remove(fw.name)
}

func (w *binaryWriter) AddIndexMeta(indexVersion int, indexLastPostingListEndBound uint64) error {
	w.buf.Reset()
	w.buf.PutByte(byte(indexVersion))
	// This value used to be the offset of the postings offset table up to and including Mimir 2.7.
	// After that this is the offset of the label indices table.
	w.buf.PutBE64(indexLastPostingListEndBound)
	return w.f.Write(w.buf.Get())
}

func (w *binaryWriter) SymbolsWriter() io.Writer {
	w.toc.Symbols = w.f.Pos()
	return w
}

func (w *binaryWriter) PostingOffsetsWriter() io.Writer {
	w.toc.PostingsOffsetTable = w.f.Pos()
	return w
}

func (w *binaryWriter) WriteTOC() error {
	w.buf.Reset()

	w.buf.PutBE64(w.toc.Symbols)
	w.buf.PutBE64(w.toc.PostingsOffsetTable)

	w.buf.PutHash(w.crc32)

	return w.f.Write(w.buf.Get())
}

func (w *binaryWriter) Write(p []byte) (int, error) {
	n := w.f.Pos()
	err := w.f.Write(p)
	return int(w.f.Pos() - n), err
}

func (w *binaryWriter) Close() error {
	return w.f.Close()
}

const valueSymbolsCacheSize = 1024

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) index.ByteSlice {
	return b[start:end]
}
