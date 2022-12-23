// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sort"
	"time"

	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
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
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{off, seriesEntry, chunk})
	return nil
}

// load all added chunks and saves resulting chunks to res.
func (r *bucketChunkReader) load(res []seriesEntry, chunksPool *pool.SafeSlabPool[byte], stats *safeQueryStats) error {
	g, ctx := errgroup.WithContext(r.ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioner.Partition(len(pIdxs), func(i int) (start, end uint64) {
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
	if in.Encoding() == chunkenc.EncXOR {
		b := saveChunk(in.Bytes(), chunksPool)
		out.Raw = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: b}
		return nil
	}
	return errors.Errorf("unsupported chunk encoding %d", in.Encoding())
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
