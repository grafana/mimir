// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"

	"github.com/dennwc/varint"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

type bucketChunkReader struct {
	ctx   context.Context
	block *bucketBlock

	toLoad [][]loadIdx
}

func newBucketChunkReader(ctx context.Context, block *bucketBlock) *bucketChunkReader {
	return &bucketChunkReader{
		ctx:    ctx,
		block:  block,
		toLoad: make([][]loadIdx, len(block.chunkObjs)),
	}
}

func (r *bucketChunkReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// reset resets the chunks scheduled for loading. It does not release any loaded chunks.
func (r *bucketChunkReader) reset() {
	for i := range r.toLoad {
		r.toLoad[i] = r.toLoad[i][:0]
	}
}

// addLoad adds the chunk with id to the data set to be fetched.
// Chunk will be fetched and saved to res[seriesChunks][chunk] upon r.load(res, <...>) call.
func (r *bucketChunkReader) addLoad(id chunks.ChunkRef, seriesEntry, chunkEntry int, length uint32) error {
	var (
		seq = chunkSegmentFile(id)
		off = chunkOffset(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{
		offset:      off,
		length:      max(varint.MaxLen32, length), // If the length is 0, we need to at least fetch the length of the chunk.
		seriesEntry: seriesEntry,
		chunkEntry:  chunkEntry,
	})
	return nil
}

func chunkSegmentFile(id chunks.ChunkRef) int { return int(id >> 32) }
func chunkOffset(id chunks.ChunkRef) uint32   { return uint32(id) }

func chunkRef(segmentFile, offset uint32) chunks.ChunkRef {
	return chunks.ChunkRef(uint64(segmentFile)<<32 | uint64(offset))
}

// load all added chunks and saves resulting chunks to res.
func (r *bucketChunkReader) load(res []seriesChunks, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	g, ctx := errgroup.WithContext(r.ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioners.chunks.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + uint64(pIdxs[i].length)
		})

		for _, p := range parts {
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunks(ctx, res, seq, p, indices, chunksPool, stats)
			})
		}
	}
	return g.Wait()
}

var chunksOffsetReaders = &sync.Pool{New: func() any {
	return &offsetTrackingReader{r: bufio.NewReaderSize(nil, tsdb.EstimatedMaxChunkSize)}
}}

// loadChunks will read range [start, end] from the segment file with sequence number seq.
// This data range covers chunks starting at supplied offsets.
//
// This function is called concurrently and the same instance of res, part of pIdx is
// passed to multiple concurrent invocations. However, this shouldn't require a mutex
// because part and pIdxs is only read, and different calls are expected to write to
// different chunks in the res.
func (r *bucketChunkReader) loadChunks(ctx context.Context, res []seriesChunks, seq int, part Part, pIdxs []loadIdx, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	// Get a reader for the required range.
	bucketReader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, bucketReader, "readChunkRange close range reader")

	// Since we may load many chunks, to avoid having to lock very frequently we accumulate
	// all stats in a local instance and then merge it in the defer.
	localStats := newQueryStats()
	defer stats.merge(localStats)

	localStats.chunksFetched += len(pIdxs)
	localStats.chunksFetchedSizeSum += int(part.End - part.Start)
	reader := chunksOffsetReaders.Get().(*offsetTrackingReader)
	defer chunksOffsetReaders.Put(reader)

	reader.Reset(uint64(pIdxs[0].offset), bucketReader)
	defer reader.Release()

	for i, pIdx := range pIdxs {
		// Fast-forward range reader to the next chunk start in case of sparse (for our purposes) byte range.
		if err = reader.SkipTo(uint64(pIdx.offset)); err != nil {
			return errors.Wrap(err, "fast forward range reader")
		}
		chunkDataLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return errors.Wrap(err, "parsing chunk length")
		}
		// We ignore the crc32 after the chunk data.
		chunkEncDataLen := chunks.ChunkEncodingSize + int(chunkDataLen)
		cb := chunksPool.Get(chunkEncDataLen)

		fullyRead, err := io.ReadFull(reader, cb)
		// We get io.EOF when there are 0 bytes read and io.UnexpectedEOF when there are some but not all read.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if chunksLeft := len(pIdxs) - 1 - i; chunksLeft != 0 {
				// Unexpected EOF for last chunk could be a valid case if we have underestimated the length of the chunk.
				// Any other errors are definitely unexpected.
				return fmt.Errorf("underread with %d more remaining chunks in seq %d start %d end %d", chunksLeft, seq, part.Start, part.End)
			}
			if err = r.fetchChunkRemainder(ctx, seq, int64(reader.offset), int64(chunkEncDataLen-fullyRead), cb[fullyRead:], localStats); err != nil {
				return errors.Wrapf(err, "refetching chunk seq %d offset %x length %d", seq, pIdx.offset, pIdx.length)
			}
		} else if err != nil {
			return errors.Wrap(err, "read chunk")
		}

		err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunkEntry]), cb)
		if err != nil {
			return errors.Wrap(err, "populate chunk")
		}
		localStats.chunksTouched++
		// Also account for the crc32 at the end. We ignore the bytes, but include the size of crc32 and the length varint size encoding.
		// By including them we can have a more accurate ratio of touched/returned for small chunks,
		// where the crc32 + length varint size are a substantial part of the chunk.
		localStats.chunksTouchedSizeSum += varint.UvarintSize(chunkDataLen) + chunkEncDataLen + crc32.Size
	}
	return nil
}

func (r *bucketChunkReader) fetchChunkRemainder(ctx context.Context, seq int, offset, length int64, dest []byte, localStats *queryStats) error {
	if length != int64(len(dest)) {
		return fmt.Errorf("byte range length (%d) different from destination length (%d)", length, len(dest))
	}
	refetchReader, err := r.block.chunkRangeReader(ctx, seq, offset, length)
	if err != nil {
		return errors.Wrap(err, "open chunk reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, refetchReader, "close refetch chunk reader")
	refetchedRead, err := io.ReadFull(refetchReader, dest)
	if err != nil {
		return errors.Wrap(err, "read refetched chunk")
	}
	localStats.chunksRefetched++
	localStats.chunksRefetchedSizeSum += refetchedRead
	return nil
}

// populateChunk retains in.Bytes() in out.Raw.
func populateChunk(out *storepb.AggrChunk, in rawChunk) error {
	var enc storepb.Chunk_Encoding
	switch in.Encoding() {
	case chunkenc.EncXOR:
		enc = storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		enc = storepb.Chunk_Histogram
	case chunkenc.EncFloatHistogram:
		enc = storepb.Chunk_FloatHistogram
	default:
		return errors.Errorf("unsupported chunk encoding %d", in.Encoding())
	}

	out.Raw = storepb.Chunk{Type: enc, Data: in.Bytes()}
	return nil
}

type loadIdx struct {
	offset uint32
	// length is the estimated length of the chunk in the segment file.
	// If the length is overestimated, the unnecessary bytes will be discarded.
	// If the length is underestimated, the chunk will be refetched with the correct length.
	length uint32
	// Indices, not actual entries and chunks.
	seriesEntry int
	chunkEntry  int
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(b[0])
}

func (b rawChunk) Bytes() []byte {
	return b[1:]
}
func (b rawChunk) Compact() {}

func (b rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b rawChunk) NumSamples() int {
	panic("invalid call")
}

// bucketChunkReaders holds a collection of chunkReader's for multiple blocks
// and selects the correct chunk reader to use on each call to addLoad
type bucketChunkReaders struct {
	readers map[ulid.ULID]chunkReader
}

type chunkReader interface {
	io.Closer

	// addLoad prepares to load the chunk. If length is unknown, it can be 0.
	addLoad(id chunks.ChunkRef, seriesEntry, chunkEntry int, length uint32) error
	load(result []seriesChunks, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error
	reset()
}

func newChunkReaders(readersMap map[ulid.ULID]chunkReader) *bucketChunkReaders {
	return &bucketChunkReaders{
		readers: readersMap,
	}
}

// addLoad prepares to load the chunk for the chunk reader of the provided block. If length is unknown, it can be 0.
func (r bucketChunkReaders) addLoad(blockID ulid.ULID, id chunks.ChunkRef, seriesEntry, chunk int, length uint32) error {
	reader, ok := r.readers[blockID]
	if !ok {
		return fmt.Errorf("trying to add a chunk for an unknown block %s", blockID.String())
	}
	return reader.addLoad(id, seriesEntry, chunk, length)
}

func (r bucketChunkReaders) load(entries []seriesChunks, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	g := &errgroup.Group{}
	for blockID, reader := range r.readers {
		blockID, reader := blockID, reader
		g.Go(func() error {
			// We don't need synchronisation on the access to entries because each chunk in
			// every series will be loaded by exactly one reader. Since the chunks slices are already
			// initialized to the correct length, they don't need to be resized and can just be accessed.
			if err := reader.load(entries, chunksPool, stats); err != nil {
				return errors.Wrapf(err, "block %s", blockID.String())
			}
			return nil
		})
	}

	// Block until all goroutines are done. We need to wait for all goroutines and
	// can't return on first error, otherwise a subsequent release of the bytes pool
	// could cause a race condition.
	return g.Wait()
}

// reset the chunks scheduled for loading.
func (r *bucketChunkReaders) reset() {
	for _, reader := range r.readers {
		reader.reset()
	}
}
