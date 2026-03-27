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
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/filepool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type StreamBinaryReaderMetrics struct {
	filePool *filepool.FilePoolMetrics
}

func NewStreamBinaryReaderMetrics(reg prometheus.Registerer) *StreamBinaryReaderMetrics {
	return &StreamBinaryReaderMetrics{
		filePool: filepool.NewFilePoolMetrics(reg),
	}
}

type StreamBinaryReader struct {
	indexHeaderVersion int
	toc                *TOCCompat

	decbufFactory streamencoding.DecbufFactory

	// Symbols struct that keeps only 1/postingOffsetsInMemSampling in the memory, then looks up the
	// rest via seeking to offsets in the index-header.
	symbolsTable *streamindex.SymbolsTableReader

	// In-memory table label name symbol lookups;
	// total size is minimal and label names account for ~half of all symbol lookups.
	nameSymbols map[uint32]string
	// Direct cache of values. This is much faster than an LRU cache and still provides
	// a reasonable cache hit ratio.
	valueSymbolsMx sync.Mutex
	valueSymbols   [valueSymbolsCacheSize]struct {
		// index in TSDB v1 is the offset of the symbol in the index-header file.
		// In TSDB v2 it is the sequence number of the symbol in the TSDB index (starting at 0).
		index  uint32
		symbol string
	}
	postingsOffsetTable streamindex.PostingsOffsetsTableReader
}

// NewStreamBinaryReader loads or builds new index-header if not present on disk.
func NewStreamBinaryReader(
	ctx context.Context,
	blockID ulid.ULID,
	tenantBkt objstore.InstrumentedBucketReader,
	localTenantDir string,
	cfg Config,
	sparseSampleFactor int,
	ll log.Logger,
	metrics *StreamBinaryReaderMetrics,
) (*StreamBinaryReader, error) {
	spanLog, ctx := spanlogger.New(ctx, ll, tracer, "indexheader.NewStreamBinaryReader")
	defer spanLog.Finish()

	localTenantDir = filepath.Join(localTenantDir, blockID.String())
	if df, err := os.Open(localTenantDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(localTenantDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create index-header dir: %w", err)
		}
	} else {
		_ = df.Close()
	}

	localBlockDir := filepath.Join(localTenantDir, blockID.String())
	localIndexHeaderPath := filepath.Join(localBlockDir, block.IndexHeaderFilename)
	localSparseHeaderPath := filepath.Join(localBlockDir, block.SparseIndexHeaderFilename)

	// Ensure local block directory exists on disk to hold the sparse index-header
	// and any sections of the full index-header which are not read from the bucket.
	if f, err := os.Open(localBlockDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(localBlockDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create local block dir: %w", err)
		}
	} else {
		_ = f.Close()
	}

	// Attempt to load existing sparse index-header from previous write to local disk or from bucket
	sparseHeaderLoaded := false
	allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err := LoadExistingSparseHeader(
		ctx, blockID, tenantBkt, localTenantDir, sparseSampleFactor, ll,
	)
	if err != nil {
		level.Info(spanLog).Log(
			"msg", "failed to load sparse header from local disk or bucket; must recreate")
	} else {
		sparseHeaderLoaded = true
	}

	// Ensure full index-header is downloaded to local block directory
	if _, err := os.Stat(localIndexHeaderPath); err != nil {
		level.Info(spanLog).Log(
			"msg", "index-header not found on local disk; will create from bucket block index",
			"path", localIndexHeaderPath, "err", err,
		)
		start := time.Now()
		if err := WriteBinary(ctx, tenantBkt, blockID, localIndexHeaderPath); err != nil {
			return nil, fmt.Errorf("failed to write index header: %w", err)
		}
		level.Info(spanLog).Log(
			"msg", "created index-header on local disk from bucket block index",
			"path", localIndexHeaderPath, "elapsed", time.Since(start),
		)
	}

	// With full index-header now on disk, initialize the local-disk-backed decbuf factory and read the TOC
	filePoolDecbufFactory := streamencoding.NewFilePoolDecbufFactory(
		localIndexHeaderPath, cfg.MaxIdleFileHandles, metrics.filePool,
	)
	indexHeaderTOC, _, err := TOCFromIndexHeader(castagnoliTable, filePoolDecbufFactory, ll)
	if err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents from index-header on disk: %w", err)
	}

	// Full index-header is now on disk.
	// If we previously failed to load the sparse index-header, recreate from full header.
	if !sparseHeaderLoaded {
		start := time.Now()
		allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, err = LoadSparseHeaderFromIndexHeader(
			indexHeaderTOC, filePoolDecbufFactory, sparseSampleFactor, cfg.VerifyOnLoad, ll,
		)
		if err != nil {
			// Exhausted all options to load sparse index-header to memory.
			return nil, fmt.Errorf("cannot build sparse index-header values from full index-header: %w", err)
		}
		level.Info(spanLog).Log("msg", "built sparse index-header values from full index-header",
			"elapsed", time.Since(start),
		)

		// Try to write to disk so we do not have to repeat this all again.
		sparseHeaderProto := InMemorySparseHeaderToProto(
			allSymbolsCount, sparseSymbolsOffsets, sparsePostingsOffsets, sparseSampleFactor,
		)
		if err := writeSparseHeaderProtoToDisk(localSparseHeaderPath, sparseHeaderProto, spanLog); err != nil {
			// Log an error in case there are disk issues, but we can still continue.
			level.Error(spanLog).Log(
				"msg", "failed to write bucket sparse index-header to disk", "err", err,
			)
		}
	}

	// Everything is now loaded from bucket or disk.
	streamBinaryReader := &StreamBinaryReader{
		decbufFactory: filePoolDecbufFactory,
		toc:           indexHeaderTOC,
	}

	streamBinaryReader.postingsOffsetTable, err = streamindex.NewPostingsOffsetTableReader(
		indexHeaderTOC.IndexVersion, filePoolDecbufFactory, int(indexHeaderTOC.PostingsOffsetTable),
		sparsePostingsOffsets, sparseSampleFactor,
	)
	if err != nil {
		return nil, err
	}

	labelNames, err := streamBinaryReader.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("load label names: %w", err)
	}

	streamBinaryReader.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = streamBinaryReader.symbolsTable.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		streamBinaryReader.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cache label names: %w", err)
	}

	return streamBinaryReader, nil

}

func (r *StreamBinaryReader) IndexVersion(context.Context) (int, error) {
	return r.toc.IndexVersion, nil
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
	return r.decbufFactory.Close()
}

// TOC returns the table of contents for the index-header.
func (r *StreamBinaryReader) TOC() *TOCCompat {
	return r.toc
}

// IndexHeaderVersion returns the version of the index-header file format.
func (r *StreamBinaryReader) IndexHeaderVersion() int {
	return r.indexHeaderVersion
}
