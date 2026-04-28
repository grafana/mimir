// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/filepool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type StreamBinaryReaderMetrics struct {
	filePool *filepool.FilePoolMetrics
}

func NewStreamBinaryReaderMetrics(reg prometheus.Registerer) *StreamBinaryReaderMetrics {
	reg = prometheus.WrapRegistererWithPrefix("indexheader_", reg)
	return &StreamBinaryReaderMetrics{
		filePool: filepool.NewFilePoolMetrics(reg),
	}
}

// StreamBinaryReader reads each index-header section from the configured source.
//
// The two main sections of the index-header are the Symbols table and Postings Offsets table.
// The reader currently supports either:
//  1. Reading both sections from the v1 index-header file format on disk,
//     which is built from the Symbols table and Postings Offsets table
//     of the full block index in the bucket, or
//  2. Reading only the Symbols table from the v1 index-header file format on disk,
//     and reading the Postings Offset table directly from the full block index in the bucket.
type StreamBinaryReader struct {
	symbolsTOC         *TOCCompat
	postingsOffsetsTOC *TOCCompat

	symbolsDecbufFactory         streamencoding.DecbufFactory
	postingsOffsetsDecbufFactory streamencoding.DecbufFactory

	symbolsTable        *streamindex.SymbolsTable
	postingsOffsetTable streamindex.PostingsOffsetsTable
	sparseSampleFactor  int

	// In-memory table of symbol lookups for all label names present in block;
	// Populated in the constructor by pulling all label names from the Postings Offsets table
	// and performing reverse symbol lookups with the Symbols Table.
	//
	// The reverse lookups require significant I/O on the Symbols Table,
	// which is the reason why reading the Symbols Table from the bucket is not yet supported.
	//
	// Label names account for ~half of all symbol lookups and total size of this table is minimal.
	nameSymbols map[uint32]string

	// Direct cache of resolved symbol lookups values;
	// Much faster than an LRU cache and still provides a reasonable cache hit ratio.
	valueSymbolsMx sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		index  uint32
		symbol string
	}
}

func NewStreamBinaryReader(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	dir string,
	cfg Config,
	sparseSampleFactor int,
	l log.Logger,
	metrics *StreamBinaryReaderMetrics,
) (*StreamBinaryReader, error) {
	var err error
	spanLog, ctx := spanlogger.New(ctx, l, tracer, "indexheader.NewStreamBinaryReader")
	defer spanLog.Finish()

	localBlockDir := filepath.Join(dir, blockID.String())
	// Ensure local block directory exists on disk to hold the sparse index-header
	// and any sections of the full index-header which are not read from the bucket.
	if f, err := os.Open(localBlockDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(localBlockDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create local block dir: %w", err)
		}
	} else {
		_ = f.Close()
	}

	localIndexHeaderPath := filepath.Join(localBlockDir, block.IndexHeaderFilename)
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// Attempt to load existing sparse index-header from previous write to local disk or from bucket.
	// Track whether we loaded it with a boolean -
	// we cannot necessarily rely on err != nil or the sparse values == nil,
	// depending on what the failure mode of loading the existing sparse header was.
	sparseHeaderLoaded := false
	allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err := DownloadAndLoadSparseHeader(
		ctx, blockID, bkt, dir, sparseSampleFactor, l,
	)
	if err != nil {
		level.Info(spanLog).Log(
			"msg", "failed to load sparse header from local disk or bucket; must recreate")
	} else {
		sparseHeaderLoaded = true
	}

	// Ensure full index-header is downloaded to local block directory.
	// If we did not get an existing sparse index-header from disk or bucket,
	// we have to consume the full index-header to build the sparse header.
	if _, err = os.Stat(localIndexHeaderPath); err != nil {
		level.Info(spanLog).Log(
			"msg", "index-header not found on local disk; will create from bucket block index",
			"path", localIndexHeaderPath, "err", err,
		)
		start := time.Now()
		if err = WriteBinary(ctx, bkt, blockID, localIndexHeaderPath); err != nil {
			return nil, fmt.Errorf("failed to write index header: %w", err)
		}
		level.Info(spanLog).Log(
			"msg", "created index-header on local disk from bucket block index",
			"path", localIndexHeaderPath, "elapsed", time.Since(start),
		)
	}

	// With full index-header now on disk, initialize the local-disk-backed decbuf factory and read the TOC.
	// The index-header's TOC is also required to build the sparse index-header if one does not already exist.
	filePoolDecbufFactory := streamencoding.NewFilePoolDecbufFactory(
		localIndexHeaderPath, cfg.MaxIdleFileHandles, metrics.filePool,
	)
	indexHeaderTOC, _, err := TOCFromIndexHeader(castagnoliTable, filePoolDecbufFactory, l)
	if err != nil {
		// TOC read checks CRC32; assume a failure here is file corruption and attempt to recreate the index-header.
		level.Debug(spanLog).Log(
			"msg", "failed to read table of contents from index-header on disk; will recreate from bucket block index",
			"path", localIndexHeaderPath, "err", err,
		)
		start := time.Now()
		if err = WriteBinary(ctx, bkt, blockID, localIndexHeaderPath); err != nil {
			return nil, fmt.Errorf("failed to write index header: %w", err)
		}
		level.Info(spanLog).Log(
			"msg", "created index-header on local disk from bucket block index",
			"path", localIndexHeaderPath, "elapsed", time.Since(start),
		)
		indexHeaderTOC, _, err = TOCFromIndexHeader(castagnoliTable, filePoolDecbufFactory, l)
		if err != nil {
			// Failure after recreating index-header from bucket; assume this is unrecoverable.
			return nil, fmt.Errorf("failed to read table of contents from index-header on disk after recreate from bucket block index: %w", err)
		}
	}

	// Full index-header is now on disk.
	// If we previously failed to load the sparse index-header, build it now from full header.
	if !sparseHeaderLoaded {
		start := time.Now()
		allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err = buildInMemorySparseHeaderFromIndexHeader(
			indexHeaderTOC, filePoolDecbufFactory, sparseSampleFactor, cfg.VerifyOnLoad, l,
		)
		if err != nil {
			// Exhausted all options to load sparse index-header to memory. Not recoverable.
			return nil, fmt.Errorf("cannot build sparse index-header values from full index-header: %w", err)
		}

		level.Info(spanLog).Log("msg", "built sparse index-header values from full index-header",
			"elapsed", time.Since(start),
		)

		// Try to write to disk so we do not have to repeat this all again.
		sparseHeaderProto := &indexheaderpb.Sparse{
			Symbols:             streamindex.SparseSymbolsToProto(allSymbolsCount, sparseSymbolsOffsets),
			PostingsOffsetTable: streamindex.SparsePostingsOffsetsTableToProto(sparsePostingsOffsets, sparseSampleFactor),
		}
		if err = writeSparseHeaderProtoToDisk(localSparseHeaderPath, sparseHeaderProto, l); err != nil {
			// Log an error in case there are disk issues, but we can still continue.
			level.Error(spanLog).Log(
				"msg", "failed to write bucket sparse index-header to disk", "err", err,
			)
		}
	}

	// Everything is now loaded from bucket or disk.
	streamBinaryReader := &StreamBinaryReader{
		sparseSampleFactor: sparseSampleFactor,
	}

	// Set up each of the Symbols table and Postings Offsets table readers
	// and their respective table of contents and DecbufFactory implementation.
	// Currently, the only supported index-header section to read from the bucket is SectionPostingsOffsetTable.
	// Symbols are always read from disk, so we can assign the TOC and DecbufFactory here already.
	streamBinaryReader.symbolsDecbufFactory = filePoolDecbufFactory
	streamBinaryReader.symbolsTOC = indexHeaderTOC

	if cfg.BucketReader.Enabled {
		bucketBlockIndexPath := filepath.Join(blockID.String(), block.IndexFilename)
		bucketBlockIndexDecbufFactory := streamencoding.NewBucketDecbufFactory(ctx, bkt, bucketBlockIndexPath)
		indexAttrs, err := bkt.Attributes(ctx, bucketBlockIndexPath)
		if err != nil {
			return nil, fmt.Errorf("get index file attributes: %w", err)
		}
		bucketBlockTOC, err := TOCFromBucketTSDBIndex(ctx, bkt, bucketBlockIndexPath, indexAttrs)
		if err != nil {
			return nil, err
		}

		switch cfg.BucketReader.BucketIndexSections {
		case SectionPostingsOffsetsTable:
			streamBinaryReader.postingsOffsetsDecbufFactory = bucketBlockIndexDecbufFactory
			streamBinaryReader.postingsOffsetsTOC = bucketBlockTOC
		default:
			// Invalid BucketIndexSections should already be rejected by config validation; protect anyway.
			return nil, errInvalidIndexHeaderSection
		}
	} else {
		// We will read everything from full index-header on disk
		streamBinaryReader.postingsOffsetsDecbufFactory = filePoolDecbufFactory
		streamBinaryReader.postingsOffsetsTOC = indexHeaderTOC
	}

	// DecbufFactory and TOC for each section are now assigned according to their configured sources.
	// Finally, initialize the readers for each index-header section.
	streamBinaryReader.postingsOffsetTable, err = streamindex.NewPostingsOffsetsTableReader(
		streamBinaryReader.postingsOffsetsTOC.IndexVersion,
		streamBinaryReader.postingsOffsetsDecbufFactory,
		int(streamBinaryReader.postingsOffsetsTOC.PostingsOffsetTable),
		sparsePostingsOffsets, sparseSampleFactor,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postings offset table reader: %w", err)
	}

	// Preload the symbols cache for all label names in the block.
	labelNames, err := streamBinaryReader.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("load label names: %w", err)
	}
	if streamBinaryReader.symbolsTable, err = streamindex.NewSymbolsTableReader(
		streamBinaryReader.symbolsTOC.IndexVersion,
		streamBinaryReader.symbolsDecbufFactory,
		int(streamBinaryReader.symbolsTOC.Symbols),
		allSymbolsCount, sparseSymbolsOffsets,
	); err != nil {
		return nil, fmt.Errorf("failed to initialize symbols table reader: %w", err)
	}

	streamBinaryReader.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = streamBinaryReader.symbolsTable.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		streamBinaryReader.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to build symbols cache for label names: %w", err)
	}

	return streamBinaryReader, nil
}

func (r *StreamBinaryReader) IndexVersion(context.Context) (int, error) {
	return r.symbolsTOC.IndexVersion, nil
}

func (r *StreamBinaryReader) IndexHeaderVersion() int {
	return BinaryFormatV1
}

func (r *StreamBinaryReader) PostingsOffset(_ context.Context, name string, value string) (index.Range, error) {
	rng, found, err := r.postingsOffsetTable.PostingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if !found {
		return index.Range{}, NotFoundRangeErr
	}
	return rng, nil
}

func (r *StreamBinaryReader) LookupSymbol(_ context.Context, o uint32) (string, error) {
	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}

	cacheIndex := o % valueSymbolsCacheSize
	r.valueSymbolsMx.Lock()
	if cached := r.valueSymbols[cacheIndex]; cached.index == o && cached.symbol != "" {
		v := cached.symbol
		r.valueSymbolsMx.Unlock()
		return v, nil
	}
	r.valueSymbolsMx.Unlock()

	s, err := r.symbolsTable.Lookup(o)
	if err != nil {
		return s, err
	}

	r.valueSymbolsMx.Lock()
	r.valueSymbols[cacheIndex].index = o
	r.valueSymbols[cacheIndex].symbol = s
	r.valueSymbolsMx.Unlock()

	return s, nil
}

type cachedLabelNamesSymbolsReader struct {
	labelNames map[uint32]string
	r          streamindex.SymbolsReader
}

func (c cachedLabelNamesSymbolsReader) Close() error {
	return c.r.Close()
}

func (c cachedLabelNamesSymbolsReader) Read(u uint32) (string, error) {
	if s, ok := c.labelNames[u]; ok {
		return s, nil
	}
	return c.r.Read(u)
}

func (r *StreamBinaryReader) SymbolsReader(context.Context) (streamindex.SymbolsReader, error) {
	return cachedLabelNamesSymbolsReader{
		labelNames: r.nameSymbols,
		r:          r.symbolsTable.Reader(),
	}, nil
}

func (r *StreamBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	return r.postingsOffsetTable.LabelValuesOffsets(ctx, name, prefix, filter)
}

func (r *StreamBinaryReader) LabelNames(context.Context) ([]string, error) {
	return r.postingsOffsetTable.LabelNames()
}

func (r *StreamBinaryReader) Close() error {
	merr := multierror.New()
	if r.symbolsDecbufFactory != nil {
		merr.Add(r.symbolsDecbufFactory.Close())
	}
	if r.postingsOffsetsDecbufFactory != nil && r.postingsOffsetsDecbufFactory != r.symbolsDecbufFactory {
		// When both Symbols and Postings offset are read from disk,
		// they use the same DecbufFactory object; avoid double-close.
		merr.Add(r.postingsOffsetsDecbufFactory.Close())
	}
	return merr.Err()
}

// TOC returns the table of contents for the index-header.
func (r *StreamBinaryReader) TOC() *TOCCompat {
	return r.symbolsTOC
}
