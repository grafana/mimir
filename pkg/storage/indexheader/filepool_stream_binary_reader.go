package indexheader

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

// NewFileStreamBinaryReader loads sparse index-headers from disk, then from the bucket, or constructs it from the index-header if neither of the two available.
func NewFileStreamBinaryReader(ctx context.Context, binPath string, id ulid.ULID, sparseHeadersPath string, postingOffsetsInMemSampling int, logger log.Logger, bkt objstore.InstrumentedBucketReader, metrics *StreamBinaryReaderMetrics, cfg Config) (bw *StreamBinaryReader, err error) {
	logger = log.With(logger, "path", sparseHeadersPath, "inmem_sampling_rate", postingOffsetsInMemSampling)

	r := &StreamBinaryReader{
		factory: streamencoding.NewFilePoolDecbufFactory(binPath, cfg.MaxIdleFileHandles, metrics.filePool),
	}

	if r.toc, r.indexHeaderVersion, err = TOCFromIndexHeader(castagnoliTable, r.factory, logger); err != nil {
		return nil, fmt.Errorf("cannot read table-of-contents: %w", err)
	}

	// Load symbols and postings offset table
	if err = r.loadSparseHeader(ctx, logger, cfg, postingOffsetsInMemSampling, sparseHeadersPath, bkt, id); err != nil {
		return nil, fmt.Errorf("cannot load sparse index-header: %w", err)
	}

	labelNames, err := r.postingsOffsetTable.LabelNames()
	if err != nil {
		return nil, fmt.Errorf("cannot load label names from postings offset table: %w", err)
	}

	r.nameSymbols = make(map[uint32]string, len(labelNames))
	if err = r.symbols.ForEachSymbol(labelNames, func(sym string, offset uint32) error {
		r.nameSymbols[offset] = sym
		return nil
	}); err != nil {
		return nil, err
	}

	return r, err
}
