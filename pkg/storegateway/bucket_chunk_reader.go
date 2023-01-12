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
	"io"
	"sort"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/objstore/tracing"
	"golang.org/x/sync/errgroup"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
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
// Chunk will be fetched and saved to res[seriesEntry][chunk] upon r.load(res, <...>) call.
func (r *bucketChunkReader) addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error {
	var (
		seq = chunkSegmentFile(id)
		off = chunkOffset(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{off, seriesEntry, chunk})
	return nil
}

func chunkSegmentFile(id chunks.ChunkRef) int { return int(id >> 32) }
func chunkOffset(id chunks.ChunkRef) uint32   { return uint32(id) }

func chunkRef(segmentFile, offset uint32) chunks.ChunkRef {
	return chunks.ChunkRef(uint64(segmentFile)<<32 | uint64(offset))
}

// load all added chunks and saves resulting chunks to res.
func (r *bucketChunkReader) load(res []seriesEntry, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	g, ctx := errgroup.WithContext(r.ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioners.chunks.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + mimir_tsdb.EstimatedMaxChunkSize
		})

		for _, p := range parts {
			seq := seq
			p := p
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunks(ctx, res, seq, p, indices, chunksPool, stats)
			})
		}
	}
	return g.Wait()
}

// loadChunks will read range [start, end] from the segment file with sequence number seq.
// This data range covers chunks starting at supplied offsets.
//
// This function is called concurrently and the same instance of res, part of pIdx is
// passed to multiple concurrent invocations. However, this shouldn't require a mutex
// because part and pIdxs is only read, and different calls are expected to write to
// different chunks in the res.
func (r *bucketChunkReader) loadChunks(ctx context.Context, res []seriesEntry, seq int, part Part, pIdxs []loadIdx, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	fetchBegin := time.Now()

	// Get a reader for the required range.
	reader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, reader, "readChunkRange close range reader")
	bufReader := bufio.NewReaderSize(reader, mimir_tsdb.EstimatedMaxChunkSize)

	// Since we may load many chunks, to avoid having to lock very frequently we accumulate
	// all stats in a local instance and then merge it in the defer.
	localStats := queryStats{}
	defer stats.merge(&localStats)

	localStats.chunksFetchCount++
	localStats.chunksFetched += len(pIdxs)
	localStats.chunksFetchDurationSum += time.Since(fetchBegin)
	localStats.chunksFetchedSizeSum += int(part.End - part.Start)

	var (
		buf        = make([]byte, mimir_tsdb.EstimatedMaxChunkSize)
		readOffset = int(pIdxs[0].offset)

		// Save a few allocations.
		written  int64
		diff     uint32
		chunkLen int
		n        int
	)

	for i, pIdx := range pIdxs {
		// Fast forward range reader to the next chunk start in case of sparse (for our purposes) byte range.
		for readOffset < int(pIdx.offset) {
			written, err = io.CopyN(io.Discard, bufReader, int64(pIdx.offset)-int64(readOffset))
			if err != nil {
				return errors.Wrap(err, "fast forward range reader")
			}
			readOffset += int(written)
		}
		// Presume chunk length to be reasonably large for common use cases.
		// However, declaration for EstimatedMaxChunkSize warns us some chunks could be larger in some rare cases.
		// This is handled further down below.
		chunkLen = mimir_tsdb.EstimatedMaxChunkSize
		if i+1 < len(pIdxs) {
			if diff = pIdxs[i+1].offset - pIdx.offset; int(diff) < chunkLen {
				chunkLen = int(diff)
			}
		}
		cb := buf[:chunkLen]
		n, err = io.ReadFull(bufReader, cb)
		readOffset += n
		// Unexpected EOF for last chunk could be a valid case. Any other errors are definitely real.
		if err != nil && !(errors.Is(err, io.ErrUnexpectedEOF) && i == len(pIdxs)-1) {
			return errors.Wrapf(err, "read range for seq %d offset %x", seq, pIdx.offset)
		}

		chunkDataLen, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.New("reading chunk length failed")
		}
		// Chunk length is n (number of bytes used to encode chunk data), 1 for chunk encoding and chunkDataLen for actual chunk data.
		// There is also crc32 after the chunk, but we ignore that.
		chunkLen = n + 1 + int(chunkDataLen)
		if chunkLen <= len(cb) {
			err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk(cb[n:chunkLen]), chunksPool)
			if err != nil {
				return errors.Wrap(err, "populate chunk")
			}
			localStats.chunksTouched++
			localStats.chunksTouchedSizeSum += int(chunkDataLen)
			continue
		}

		fetchBegin = time.Now()

		// Read entire chunk into new buffer.
		// TODO: readChunkRange call could be avoided for any chunk but last in this particular part.
		nb, err := r.block.readChunkRange(ctx, seq, int64(pIdx.offset), int64(chunkLen), []byteRange{{offset: 0, length: chunkLen}})
		if err != nil {
			return errors.Wrapf(err, "preloaded chunk too small, expecting %d, and failed to fetch full chunk", chunkLen)
		}
		if len(*nb) != chunkLen {
			return errors.Errorf("preloaded chunk too small, expecting %d", chunkLen)
		}

		localStats.chunksFetchCount++
		localStats.chunksFetchDurationSum += time.Since(fetchBegin)
		localStats.chunksFetchedSizeSum += len(*nb)
		err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk((*nb)[n:]), chunksPool)
		if err != nil {
			r.block.chunkPool.Put(nb)
			return errors.Wrap(err, "populate chunk")
		}
		localStats.chunksTouched++
		localStats.chunksTouchedSizeSum += int(chunkDataLen)

		r.block.chunkPool.Put(nb)
	}
	return nil
}

func populateChunk(out *storepb.AggrChunk, in chunkenc.Chunk, chunksPool *pool.SafeSlabPool[byte]) error {
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

	b := saveChunk(in.Bytes(), chunksPool)
	out.Raw = &storepb.Chunk{Type: enc, Data: b}
	return nil
}

// saveChunk saves a copy of b's payload to a buffer pulled from chunksPool.
// The buffer containing the chunk data is returned.
// The returned slice becomes invalid once chunksPool is released.
func saveChunk(b []byte, chunksPool *pool.SafeSlabPool[byte]) []byte {
	dst := chunksPool.Get(len(b))
	copy(dst, b)
	return dst
}

type loadIdx struct {
	offset uint32
	// Indices, not actual entries and chunks.
	seriesEntry int
	chunk       int
}

type rangeLoadIdx struct {
	offset uint32
	estLen int64
	// seriesEntry and rangeEntry are indices for where to put the result,
	// they are not actual refs of series or chunks
	seriesEntry int
	rangeEntry  int
}

type bucketChunksRangesReader struct {
	ctx    context.Context
	block  *bucketBlock
	toLoad [][]rangeLoadIdx
}

func newBucketChunksRangesReader(ctx context.Context, block *bucketBlock) *bucketChunksRangesReader {
	return &bucketChunksRangesReader{
		ctx:    ctx,
		block:  block,
		toLoad: make([][]rangeLoadIdx, len(block.chunkObjs)),
	}
}

func (r *bucketChunksRangesReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// reset resets the ranges scheduled for loading. It does not release any loaded ranges.
func (r *bucketChunksRangesReader) reset() {
	for i := range r.toLoad {
		r.toLoad[i] = r.toLoad[i][:0]
	}
}

func (r *bucketChunksRangesReader) addLoadRange(g seriesChunkRefsRange, seriesEntry, rangeEntry int) error {
	var (
		seq = g.segmentFile
		off = g.refs[0].segFileOffset
	)
	if seq >= uint32(len(r.toLoad)) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}

	var totalLen int64
	for _, c := range g.refs {
		totalLen += int64(c.length)
	}

	r.toLoad[seq] = append(r.toLoad[seq], rangeLoadIdx{offset: off, seriesEntry: seriesEntry, rangeEntry: rangeEntry, estLen: totalLen})
	return nil
}

func (r *bucketChunksRangesReader) loadRanges(partialSeries []partialSeriesChunksSet, stats *safeQueryStats) error {
	g, ctx := errgroup.WithContext(r.ctx)
	s, ctx := tracing.StartSpan(ctx, "loadRanges")
	defer s.Finish()

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioners.chunks.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + uint64(pIdxs[i].estLen)
		})

		for _, p := range parts {
			seq := seq
			p := p
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunksRanges(ctx, partialSeries, seq, p, indices, stats)
			})
		}
	}
	return g.Wait()
}

// loadChunksRanges will read the part [start, end] from the segment file with sequence number seq.
// This data range covers ranges starting at supplied chunks ranges.
func (r *bucketChunksRangesReader) loadChunksRanges(ctx context.Context, partialSeries []partialSeriesChunksSet, seq int, part Part, ranges []rangeLoadIdx, stats *safeQueryStats) error {
	// Get a reader for the required range.
	reader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, reader, "readChunkRange close range reader")
	bufReader := bufio.NewReaderSize(reader, mimir_tsdb.EstimatedMaxChunkSize)

	stats.update(func(stats *queryStats) {
		stats.chunksFetchedSizeSum += int(part.End - part.Start)
	})

	readOffset := int(ranges[0].offset)

	for rIdx, rng := range ranges {
		// Fast forward range reader to the next chunk range start in case of sparse byte range.
		for readOffset < int(rng.offset) {
			n, err := io.CopyN(io.Discard, bufReader, int64(rng.offset)-int64(readOffset))
			if err != nil {
				return fmt.Errorf("fast forward range reader (block %s segment %d offset %d read offset %d): %w", r.block.meta.ULID, seq, rng.offset, readOffset, err)
			}
			readOffset += int(n)
		}
		rLen := int(rng.estLen)
		if rIdx+1 < len(ranges) {
			if diff := ranges[rIdx+1].offset - rng.offset; int(diff) < rLen {
				// When a range contains a chunk with an estimated length, a range can overlap with the next one.
				// In this case we want to clamp it to beginning of the next range.
				rLen = int(diff)
			}
		}
		rangeBuf := make([]byte, rLen)
		n, err := io.ReadFull(bufReader, rangeBuf)
		// EOF for last chunk could be a valid case. Any other errors are definitely unexpected.
		if err != nil && !(errors.Is(err, io.ErrUnexpectedEOF) && rIdx == len(ranges)-1) {
			return fmt.Errorf("read chunk range (block %s segment %d offset %d read offset %d): %w", r.block.meta.ULID, seq, rng.offset, readOffset, err)
		}
		readOffset += n
		partialSeries[rng.seriesEntry].rawRanges[rng.rangeEntry] = rangeBuf
	}
	return nil
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

	addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error
	load(result []seriesEntry, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error
	reset()
}

func newChunkReaders(readersMap map[ulid.ULID]chunkReader) *bucketChunkReaders {
	return &bucketChunkReaders{
		readers: readersMap,
	}
}

func (r bucketChunkReaders) addLoad(blockID ulid.ULID, id chunks.ChunkRef, seriesEntry, chunk int) error {
	return r.readers[blockID].addLoad(id, seriesEntry, chunk)
}

func (r bucketChunkReaders) load(entries []seriesEntry, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	g := &errgroup.Group{}
	for _, reader := range r.readers {
		reader := reader
		g.Go(func() error {
			// We don't need synchronisation on the access to entries because each chunk in
			// every series will be loaded by exactly one reader. Since the chunks slices are already
			// initialized to the correct length, they don't need to be resized and can just be accessed.
			return reader.load(entries, chunksPool, stats)
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

// bucketChunkRangesReaders holds a collection of chunkRangeReader's for multiple blocks
// and selects the correct chunk reader to use on each call to addLoad
type bucketChunkRangesReaders struct {
	readers map[ulid.ULID]chunkRangeReader
}

type chunkRangeReader interface {
	io.Closer

	addLoadRange(g seriesChunkRefsRange, seriesEntry, rangeEntry int) error
	loadRanges(partialSeries []partialSeriesChunksSet, stats *safeQueryStats) error
	reset()
}

func newChunkRangeReaders(readersMap map[ulid.ULID]chunkRangeReader) *bucketChunkRangesReaders {
	return &bucketChunkRangesReaders{
		readers: readersMap,
	}
}

func (r bucketChunkRangesReaders) addLoadRange(blockID ulid.ULID, g seriesChunkRefsRange, seriesEntry int, rangeEntry int) error {
	return r.readers[blockID].addLoadRange(g, seriesEntry, rangeEntry)
}

func (r bucketChunkRangesReaders) loadRanges(partialSeries []partialSeriesChunksSet, stats *safeQueryStats) error {
	g := &errgroup.Group{}
	for _, reader := range r.readers {
		reader := reader
		g.Go(func() error {
			// We don't need synchronisation on the access to entries because each chunk in
			// every series will be loaded by exactly one reader. Since the ranges slices are already
			// initialized to the correct length, they don't need to be resized and can just be accessed.
			return reader.loadRanges(partialSeries, stats)
		})
	}

	// Block until all goroutines are done. We need to wait for all goroutines and
	// can't return on first error, otherwise a subsequent release of the bytes pool
	// could cause a race condition.
	return g.Wait()
}

// reset the chunks scheduled for loading.
func (r bucketChunkRangesReaders) reset() {
	for _, reader := range r.readers {
		reader.reset()
	}
}
