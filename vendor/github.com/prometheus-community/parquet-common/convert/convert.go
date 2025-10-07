// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package convert

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus-community/parquet-common/schema"
)

var DefaultConvertOpts = convertOpts{
	name:               "block",
	rowGroupSize:       1e6,
	colDuration:        time.Hour * 8,
	numRowGroups:       math.MaxInt32,
	sortedLabels:       []string{labels.MetricName},
	bloomfilterLabels:  []string{labels.MetricName},
	pageBufferSize:     parquet.DefaultPageBufferSize,
	writeBufferSize:    parquet.DefaultWriteBufferSize,
	columnPageBuffers:  parquet.DefaultWriterConfig().ColumnPageBuffers,
	readConcurrency:    runtime.GOMAXPROCS(0),
	writeConcurrency:   runtime.GOMAXPROCS(0),
	maxSamplesPerChunk: tsdb.DefaultSamplesPerChunk,
}

type Convertible interface {
	Index() (tsdb.IndexReader, error)
	Chunks() (tsdb.ChunkReader, error)
	Tombstones() (tombstones.Reader, error)
	Meta() tsdb.BlockMeta
}

type convertOpts struct {
	numRowGroups          int
	rowGroupSize          int
	colDuration           time.Duration
	name                  string
	sortedLabels          []string
	bloomfilterLabels     []string
	pageBufferSize        int
	writeBufferSize       int
	columnPageBuffers     parquet.BufferPool
	readConcurrency       int
	writeConcurrency      int
	maxSamplesPerChunk    int
	labelsCompressionOpts []schema.CompressionOpts
	chunksCompressionOpts []schema.CompressionOpts
}

func (cfg convertOpts) buildBloomfilterColumns() []parquet.BloomFilterColumn {
	cols := make([]parquet.BloomFilterColumn, 0, len(cfg.bloomfilterLabels))
	for _, label := range cfg.bloomfilterLabels {
		cols = append(cols, parquet.SplitBlockFilter(10, schema.LabelToColumn(label)))
	}

	return cols
}

func (cfg convertOpts) buildSortingColumns() []parquet.SortingColumn {
	cols := make([]parquet.SortingColumn, 0, len(cfg.sortedLabels))

	for _, label := range cfg.sortedLabels {
		cols = append(cols, parquet.Ascending(schema.LabelToColumn(label)))
	}

	return cols
}

type ConvertOption func(*convertOpts)

// WithSortBy configures the labels used for sorting time series data in the output Parquet files.
// The specified labels determine the sort order of rows within row groups, which can significantly
// improve query performance for filters on these labels. By default, data is sorted by __name__.
//
// Parameters:
//   - labels: Label names to sort by, in order of precedence
//
// Example:
//
//	WithSortBy("__name__", "job", "instance")
func WithSortBy(labels ...string) ConvertOption {
	return func(opts *convertOpts) {
		opts.sortedLabels = labels
	}
}

// WithColDuration sets the time duration for each column in the Parquet schema.
// This determines how time series data is partitioned across columns, affecting
// both storage efficiency and query performance. Shorter durations create more
// columns but allow for more precise time-based filtering.
//
// Parameters:
//   - d: Duration for each time column (default: 8 hours)
//
// Example:
//
//	WithColDuration(4 * time.Hour)  // 4-hour columns
func WithColDuration(d time.Duration) ConvertOption {
	return func(opts *convertOpts) {
		opts.colDuration = d
	}
}

// WithWriteBufferSize configures the buffer size used for writing Parquet data.
// Larger buffers can improve write performance by reducing I/O operations,
// but consume more memory during the conversion process.
//
// Parameters:
//   - s: Buffer size in bytes (default: parquet.DefaultWriteBufferSize)
//
// Example:
//
//	WithWriteBufferSize(64 * 1024)  // 64KB buffer
func WithWriteBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeBufferSize = s
	}
}

// WithPageBufferSize sets the buffer size for Parquet page operations.
// This affects how data is buffered when reading and writing individual pages
// within the Parquet file format. Larger page buffers can improve performance
// for large datasets but increase memory usage.
//
// Parameters:
//   - s: Page buffer size in bytes (default: parquet.DefaultPageBufferSize)
//
// Example:
//
//	WithPageBufferSize(128 * 1024)  // 128KB page buffer
func WithPageBufferSize(s int) ConvertOption {
	return func(opts *convertOpts) {
		opts.pageBufferSize = s
	}
}

// WithName sets the base name used for generated Parquet files.
// This name is used as a prefix for the output files in the object store bucket.
//
// Parameters:
//   - name: Base name for output files (default: "block")
//
// Example:
//
//	WithName("prometheus-data")  // Files will be named prometheus-data-*
func WithName(name string) ConvertOption {
	return func(opts *convertOpts) {
		opts.name = name
	}
}

// WithNumRowGroups limits the maximum number of row groups to create during conversion.
// Row groups are the primary unit of parallelization in Parquet files. More row groups
// allow for better parallelization but may increase metadata overhead.
//
// Parameters:
//   - n: Maximum number of row groups (default: math.MaxInt32, effectively unlimited)
//
// Example:
//
//	WithNumRowGroups(100)  // Limit to 100 row groups
func WithNumRowGroups(n int) ConvertOption {
	return func(opts *convertOpts) {
		opts.numRowGroups = n
	}
}

// WithRowGroupSize sets the target number of rows per row group in the output Parquet files.
// Larger row groups improve compression and reduce metadata overhead, but require more
// memory during processing and may reduce parallelization opportunities.
//
// Parameters:
//   - size: Target number of rows per row group (default: 1,000,000)
//
// Example:
//
//	WithRowGroupSize(500000)  // 500K rows per row group
func WithRowGroupSize(size int) ConvertOption {
	return func(opts *convertOpts) {
		opts.rowGroupSize = size
	}
}

// WithReadConcurrency sets the number of concurrent goroutines used to read TSDB series during conversion.
// Higher concurrency can improve performance on multi-core systems but increases
// memory usage. The optimal value depends on available CPU cores and memory.
//
// Parameters:
//   - concurrency: Number of concurrent workers (default: runtime.GOMAXPROCS(0))
//
// Example:
//
//	WithReadConcurrency(8)  // Use 8 concurrent workers
func WithReadConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.readConcurrency = concurrency
	}
}

// WithWriteConcurrency sets the number of concurrent goroutines used to write Parquet shards during conversion.
// Higher concurrency can improve conversion time on multi-core systems but increases
// CPU and memory usage. The optimal value depends on available CPU cores and memory.
//
// Parameters:
//   - concurrency: Number of concurrent workers (default: runtime.GOMAXPROCS(0))
//
// Example:
//
//	WithWriteConcurrency(8)  // Use 8 concurrent workers
func WithWriteConcurrency(concurrency int) ConvertOption {
	return func(opts *convertOpts) {
		opts.writeConcurrency = concurrency
	}
}

// WithMaxSamplesPerChunk sets the maximum number of samples to include in each chunk
// during the encoding process. This affects how time series data is chunked and can
// impact both compression efficiency and query performance.
//
// Parameters:
//   - samplesPerChunk: Maximum samples per chunk (default: tsdb.DefaultSamplesPerChunk)
//
// Example:
//
//	WithMaxSamplesPerChunk(240)  // Limit chunks to 240 samples each
func WithMaxSamplesPerChunk(samplesPerChunk int) ConvertOption {
	return func(opts *convertOpts) {
		opts.maxSamplesPerChunk = samplesPerChunk
	}
}

func WithColumnPageBuffers(buffers parquet.BufferPool) ConvertOption {
	return func(opts *convertOpts) {
		opts.columnPageBuffers = buffers
	}
}

// WithCompression adds compression options to the conversion process.
// These options will be applied to both labels and chunks projections.
// For separate configuration, use WithLabelsCompression and WithChunksCompression.
func WithCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelsCompressionOpts = compressionOpts
		opts.chunksCompressionOpts = compressionOpts
	}
}

// WithLabelsCompression configures compression options specifically for the labels projection.
// This allows fine-grained control over how label data is compressed in the output Parquet files,
// which can be optimized differently from chunk data due to different access patterns and data characteristics.
//
// Parameters:
//   - compressionOpts: optional compression options to apply to the projection schema
//
// Example:
//
//	WithLabelsCompression(schema.WithSnappyCompression())
func WithLabelsCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.labelsCompressionOpts = compressionOpts
	}
}

// WithChunksCompression configures compression options specifically for the chunks projection.
// This allows optimization of compression settings for time series chunk data, which typically
// has different compression characteristics compared to label metadata.
//
// Parameters:
//   - compressionOpts: optional compression options to apply to the projection schema
//
// Example:
//
//	WithChunksCompression(schema.WithGzipCompression())
func WithChunksCompression(compressionOpts ...schema.CompressionOpts) ConvertOption {
	return func(opts *convertOpts) {
		opts.chunksCompressionOpts = compressionOpts
	}
}

// ConvertTSDBBlock converts one or more TSDB blocks to Parquet format and writes them to an object store bucket.
// It processes time series data within the specified time range and outputs sharded Parquet files.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - bkt: Object store bucket where the converted Parquet files will be written
//   - mint: Minimum timestamp (inclusive) for the time range to convert
//   - maxt: Maximum timestamp (exclusive) for the time range to convert
//   - blks: Slice of Convertible blocks (typically TSDB blocks) to be converted
//   - opts: Optional configuration options to customize the conversion process
//
// Returns:
//   - int: The current shard number after conversion
//   - error: Any error that occurred during the conversion process
//
// The function creates a row reader from the TSDB blocks, generates both labels and chunks
// projections with optional compression, and writes the data to the bucket using a sharded
// writer approach for better performance and parallelization.
func ConvertTSDBBlock(
	ctx context.Context,
	bkt objstore.Bucket,
	mint, maxt int64,
	blks []Convertible,
	opts ...ConvertOption,
) (int, error) {
	cfg := DefaultConvertOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	rr, err := NewTsdbRowReader(ctx, mint, maxt, cfg.colDuration.Milliseconds(), blks, cfg)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rr.Close() }()

	labelsProjection, err := rr.Schema().LabelsProjection(cfg.labelsCompressionOpts...)
	if err != nil {
		return 0, errors.Wrap(err, "error getting labels projection from tsdb schema")
	}
	chunksProjection, err := rr.Schema().ChunksProjection(cfg.chunksCompressionOpts...)
	if err != nil {
		return 0, errors.Wrap(err, "error getting chunks projection from tsdb schema")
	}
	outSchemaProjections := []*schema.TSDBProjection{
		labelsProjection, chunksProjection,
	}

	pipeReaderWriter := NewPipeReaderBucketWriter(bkt)
	w := NewShardedWrite(rr, rr.Schema(), outSchemaProjections, pipeReaderWriter, &cfg)
	return w.currentShard, errors.Wrap(w.Write(ctx), "error writing block")
}

func ConvertTSDBBlockParallel(
	ctx context.Context,
	bkt objstore.Bucket,
	mint, maxt int64,
	blks []Convertible,
	opts ...ConvertOption,
) (int, error) {
	cfg := DefaultConvertOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	shardedRowReaders, err := NewShardedTSDBRowReaders(ctx, mint, maxt, cfg.colDuration.Milliseconds(), blks, &cfg)
	if err != nil {
		return 0, errors.Wrap(err, "failed to create sharded TSDB row readers")
	}
	defer func() {
		for _, rr := range shardedRowReaders {
			_ = rr.Close()
		}
	}()

	errGroup := &errgroup.Group{}
	errGroup.SetLimit(cfg.writeConcurrency)
	for shard, rr := range shardedRowReaders {
		errGroup.Go(func() error {
			labelsProjection, err := rr.Schema().LabelsProjection(cfg.labelsCompressionOpts...)
			if err != nil {
				return errors.Wrap(err, "error getting labels projection from tsdb schema")
			}
			chunksProjection, err := rr.Schema().ChunksProjection(cfg.chunksCompressionOpts...)
			if err != nil {
				return errors.Wrap(err, "error getting chunks projection from tsdb schema")
			}
			outSchemaProjections := []*schema.TSDBProjection{
				labelsProjection, chunksProjection,
			}

			w := &PreShardedWriter{
				shard:                shard,
				rr:                   rr,
				schema:               rr.Schema(),
				outSchemaProjections: outSchemaProjections,
				pipeReaderWriter:     NewPipeReaderBucketWriter(bkt),
				opts:                 &cfg,
			}
			err = w.Write(ctx)
			if err != nil {
				return errors.Wrap(err, "error writing shard for block")
			}
			return nil
		})
	}

	err = errGroup.Wait()
	if err != nil {
		return 0, errors.Wrap(err, "failed to convert shards in parallel")
	}
	return len(shardedRowReaders), nil
}

var _ parquet.RowReader = &TSDBRowReader{}

type TSDBRowReader struct {
	ctx context.Context

	closers []io.Closer

	seriesSet storage.ChunkSeriesSet

	rowBuilder *parquet.RowBuilder
	tsdbSchema *schema.TSDBSchema

	encoder     *schema.PrometheusParquetChunksEncoder
	totalRead   int64
	concurrency int
}

func NewShardedTSDBRowReaders(
	ctx context.Context,
	mint, maxt, colDuration int64,
	blocks []Convertible,
	opts *convertOpts,
) ([]*TSDBRowReader, error) {
	blocksByID := make(map[ulid.ULID]Convertible, len(blocks))
	blockIndexRs := make(map[ulid.ULID]tsdb.IndexReader, len(blocks))
	// Simpler to track and close these readers separate from those used by shard conversion reader/writers.
	defer func() {
		for i := range blockIndexRs {
			_ = blockIndexRs[i].Close()
		}
	}()
	for _, blk := range blocks {
		blocksByID[blk.Meta().ULID] = blk
		indexr, err := blk.Index()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get index reader from block")
		}
		blockIndexRs[blk.Meta().ULID] = indexr
	}

	uniqueSeriesCount, shardedSeries, err := shardSeries(ctx, blockIndexRs, mint, maxt, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine unique series count")
	}
	if uniqueSeriesCount == 0 {
		return nil, errors.Wrap(err, "no series found in the specified time range")
	}

	shardTSDBRowReaders := make([]*TSDBRowReader, len(shardedSeries))
	// For each shard, create a TSDBRowReader with:
	//	* a MergeChunkSeriesSet of all blocks' series sets for the shard
	//	* a schema built from only the label names present in the shard
	for shardIdx, shardSeries := range shardedSeries {
		// An index, chunk, and tombstone reader per block each must be closed after usage
		// in order for the prometheus block reader to not hang indefinitely when closed.
		closers := make([]io.Closer, 0, len(shardSeries)*3)
		seriesSets := make([]storage.ChunkSeriesSet, 0, len(blocks))
		schemaBuilder := schema.NewBuilder(mint, maxt, colDuration)

		// For each block, init readers and postings list to create a tsdb.blockChunkSeriesSet;
		// series sets from all blocks for the shard will be merged by NewMergeChunkSeriesSet.
		for blockID, blockSeries := range shardSeries {
			blk := blocksByID[blockID]

			// Init all readers for block & add to closers

			// Init separate index readers from above blockIndexRs to simplify closing logic
			indexr, err := blk.Index()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get index reader from block")
			}
			closers = append(closers, indexr)

			chunkr, err := blk.Chunks()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get chunk reader from block")
			}
			closers = append(closers, chunkr)

			tombsr, err := blk.Tombstones()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get tombstone reader from block")
			}
			closers = append(closers, tombsr)

			// Flatten series refs and add all label columns to schema for the shard
			refs := make([]storage.SeriesRef, 0, len(blockSeries))
			for _, series := range blockSeries {
				refs = append(refs, series.ref)
				series.labels.Range(func(l labels.Label) {
					schemaBuilder.AddLabelNameColumn(l.Name)
				})
			}
			postings := index.NewListPostings(refs)
			seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
			seriesSets = append(seriesSets, seriesSet)
		}

		mergeSeriesSet := NewMergeChunkSeriesSet(
			seriesSets, compareBySortedLabelsFunc(opts.sortedLabels), storage.NewConcatenatingChunkSeriesMerger(),
		)

		s, err := schemaBuilder.Build()
		if err != nil {
			return nil, fmt.Errorf("unable to build schema reader from block: %w", err)
		}

		rr := &TSDBRowReader{
			ctx:         ctx,
			seriesSet:   mergeSeriesSet,
			closers:     closers,
			tsdbSchema:  s,
			concurrency: opts.readConcurrency,

			rowBuilder: parquet.NewRowBuilder(s.Schema),
			encoder:    schema.NewPrometheusParquetChunksEncoder(s, opts.maxSamplesPerChunk),
		}
		shardTSDBRowReaders[shardIdx] = rr
	}

	return shardTSDBRowReaders, nil
}

func NewTsdbRowReader(ctx context.Context, mint, maxt, colDuration int64, blks []Convertible, ops convertOpts) (*TSDBRowReader, error) {
	var (
		seriesSets = make([]storage.ChunkSeriesSet, 0, len(blks))
		closers    = make([]io.Closer, 0, len(blks))
		ok         = false
	)
	// If we fail to build the row reader, make sure we release resources.
	// This could be either a controlled error or a panic.
	defer func() {
		if !ok {
			for i := range closers {
				_ = closers[i].Close()
			}
		}
	}()

	b := schema.NewBuilder(mint, maxt, colDuration)

	compareFunc := func(a, b labels.Labels) int {
		for _, lb := range ops.sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a, b)
	}

	for _, blk := range blks {
		indexr, err := blk.Index()
		if err != nil {
			return nil, fmt.Errorf("unable to get index reader from block: %w", err)
		}
		closers = append(closers, indexr)

		chunkr, err := blk.Chunks()
		if err != nil {
			return nil, fmt.Errorf("unable to get chunk reader from block: %w", err)
		}
		closers = append(closers, chunkr)

		tombsr, err := blk.Tombstones()
		if err != nil {
			return nil, fmt.Errorf("unable to get tombstone reader from block: %w", err)
		}
		closers = append(closers, tombsr)

		lblns, err := indexr.LabelNames(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get label names from block: %w", err)
		}

		postings := sortedPostings(ctx, indexr, mint, maxt, ops.sortedLabels...)
		seriesSet := tsdb.NewBlockChunkSeriesSet(blk.Meta().ULID, indexr, chunkr, tombsr, postings, mint, maxt, false)
		seriesSets = append(seriesSets, seriesSet)

		b.AddLabelNameColumn(lblns...)
	}

	cseriesSet := NewMergeChunkSeriesSet(seriesSets, compareFunc, storage.NewConcatenatingChunkSeriesMerger())

	s, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("unable to build index reader from block: %w", err)
	}

	rr := &TSDBRowReader{
		ctx:         ctx,
		seriesSet:   cseriesSet,
		closers:     closers,
		tsdbSchema:  s,
		concurrency: ops.readConcurrency,

		rowBuilder: parquet.NewRowBuilder(s.Schema),
		encoder:    schema.NewPrometheusParquetChunksEncoder(s, ops.maxSamplesPerChunk),
	}
	ok = true
	return rr, nil
}

func (rr *TSDBRowReader) Close() error {
	err := &multierror.Error{}
	for i := range rr.closers {
		err = multierror.Append(err, rr.closers[i].Close())
	}
	return err.ErrorOrNil()
}

func (rr *TSDBRowReader) Schema() *schema.TSDBSchema {
	return rr.tsdbSchema
}

func sortedPostings(ctx context.Context, indexr tsdb.IndexReader, mint, maxt int64, sortedLabels ...string) index.Postings {
	p := tsdb.AllSortedPostings(ctx, indexr)

	if len(sortedLabels) == 0 {
		return p
	}

	type s struct {
		ref    storage.SeriesRef
		idx    int
		labels labels.Labels
	}
	series := make([]s, 0, 128)
	chks := make([]chunks.Meta, 0, 128)

	scratchBuilder := labels.NewScratchBuilder(10)
	lb := labels.NewBuilder(labels.EmptyLabels())
	i := 0
P:
	for p.Next() {
		scratchBuilder.Reset()
		chks = chks[:0]
		if err := indexr.Series(p.At(), &scratchBuilder, &chks); err != nil {
			return index.ErrPostings(fmt.Errorf("unable to expand series: %w", err))
		}
		hasChunks := slices.ContainsFunc(chks, func(chk chunks.Meta) bool {
			return mint <= chk.MaxTime && chk.MinTime <= maxt
		})
		if !hasChunks {
			continue P
		}

		lb.Reset(scratchBuilder.Labels())

		series = append(series, s{labels: lb.Keep(sortedLabels...).Labels(), ref: p.At(), idx: i})
		i++
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(fmt.Errorf("expand postings: %w", err))
	}

	slices.SortFunc(series, func(a, b s) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.labels.Get(lb), b.labels.Get(lb)); c != 0 {
				return c
			}
		}
		if a.idx < b.idx {
			return -1
		} else if a.idx > b.idx {
			return 1
		}
		return 0
	})

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

func (rr *TSDBRowReader) ReadRows(buf []parquet.Row) (int, error) {
	type chkBytesOrError struct {
		chkBytes [][]byte
		err      error
	}
	type chunkSeriesPromise struct {
		s storage.ChunkSeries
		c chan chkBytesOrError
	}

	c := make(chan chunkSeriesPromise, rr.concurrency)

	go func() {
		i := 0
		defer close(c)
		for i < len(buf) && rr.seriesSet.Next() {
			s := rr.seriesSet.At()
			it := s.Iterator(nil)

			promise := chunkSeriesPromise{
				s: s,
				c: make(chan chkBytesOrError, 1),
			}

			select {
			case c <- promise:
			case <-rr.ctx.Done():
				return
			}
			go func() {
				chkBytes, err := rr.encoder.Encode(it)
				promise.c <- chkBytesOrError{chkBytes: chkBytes, err: err}
			}()
			i++
		}
	}()

	i, j := 0, 0
	lblsIdxs := []int{}
	colIndex, ok := rr.tsdbSchema.Schema.Lookup(schema.ColIndexesColumn)
	if !ok {
		return 0, fmt.Errorf("unable to find indexes")
	}
	seriesHashIndex, ok := rr.tsdbSchema.Schema.Lookup(schema.SeriesHashColumn)
	if !ok {
		return 0, fmt.Errorf("unable to find series hash column")
	}

	for promise := range c {
		j++

		chkBytesOrErr := <-promise.c
		if err := chkBytesOrErr.err; err != nil {
			return 0, fmt.Errorf("unable encode chunks: %w", err)
		}
		chkBytes := chkBytesOrErr.chkBytes

		rr.rowBuilder.Reset()
		lblsIdxs = lblsIdxs[:0]

		seriesLabels := promise.s.Labels()
		seriesLabels.Range(func(l labels.Label) {
			colName := schema.LabelToColumn(l.Name)
			lc, _ := rr.tsdbSchema.Schema.Lookup(colName)
			rr.rowBuilder.Add(lc.ColumnIndex, parquet.ValueOf(l.Value))
			lblsIdxs = append(lblsIdxs, lc.ColumnIndex)
		})

		rr.rowBuilder.Add(colIndex.ColumnIndex, parquet.ValueOf(schema.EncodeIntSlice(lblsIdxs)))

		// Compute and store the series hash as a byte slice in big-endian format
		seriesHashValue := labels.StableHash(seriesLabels)
		seriesHashBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seriesHashBytes, seriesHashValue)
		rr.rowBuilder.Add(seriesHashIndex.ColumnIndex, parquet.ValueOf(seriesHashBytes))

		// skip series that have no chunks in the requested time
		if allChunksEmpty(chkBytes) {
			continue
		}

		for idx, chk := range chkBytes {
			if len(chk) == 0 {
				continue
			}
			rr.rowBuilder.Add(rr.tsdbSchema.DataColsIndexes[idx], parquet.ValueOf(chk))
		}
		buf[i] = rr.rowBuilder.AppendRow(buf[i][:0])
		i++
	}
	rr.totalRead += int64(i)

	if rr.ctx.Err() != nil {
		return i, rr.ctx.Err()
	}

	if j < len(buf) {
		return i, io.EOF
	}

	return i, rr.seriesSet.Err()
}

type blockSeries struct {
	blockID ulid.ULID
	idx     int
	ref     storage.SeriesRef
	labels  labels.Labels
}

func compareBlockSeriesBySortedLabelsFunc(sortedLabels []string) func(a, b blockSeries) int {
	return func(a, b blockSeries) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.labels.Get(lb), b.labels.Get(lb)); c != 0 {
				return c
			}
		}
		if a.blockID.Compare(b.blockID) < 0 {
			return -1
		} else if a.blockID.Compare(b.blockID) > 0 {
			return 1
		}

		if a.idx < b.idx {
			return -1
		} else if a.idx > b.idx {
			return 1
		}
		return 0
	}
}

func shardSeries(
	ctx context.Context,
	blockIndexReaders map[ulid.ULID]tsdb.IndexReader,
	mint, maxt int64,
	opts *convertOpts,
) (int, []map[ulid.ULID][]blockSeries, error) {
	type reader struct {
		blockID      ulid.ULID
		indexr       tsdb.IndexReader
		postingsIter index.Postings
	}

	readers := make([]reader, 0, len(blockIndexReaders))
	for blockID, indexr := range blockIndexReaders {
		readers = append(readers, reader{
			blockID:      blockID,
			indexr:       indexr,
			postingsIter: tsdb.AllSortedPostings(ctx, indexr),
		})
	}

	chks := make([]chunks.Meta, 0, 128)
	allSeries := make([]blockSeries, 0, 128*len(readers))
	// Collect all series from all blocks with chunks in the time range
	for _, reader := range readers {
		i := 0
		scratchBuilder := labels.NewScratchBuilder(10)

		for reader.postingsIter.Next() {
			scratchBuilder.Reset()
			chks = chks[:0]

			if err := reader.indexr.Series(reader.postingsIter.At(), &scratchBuilder, &chks); err != nil {
				return 0, nil, errors.Wrap(err, "unable to expand series")
			}

			hasChunks := slices.ContainsFunc(chks, func(chk chunks.Meta) bool {
				return mint <= chk.MaxTime && chk.MinTime <= maxt
			})
			if !hasChunks {
				continue
			}

			scratchBuilderLabels := scratchBuilder.Labels()
			allSeries = append(allSeries, blockSeries{
				blockID: reader.blockID,
				idx:     i,
				ref:     reader.postingsIter.At(),
				labels:  scratchBuilderLabels,
			})
		}
	}

	if len(allSeries) == 0 {
		return 0, nil, nil
	}

	slices.SortFunc(allSeries, compareBlockSeriesBySortedLabelsFunc(opts.sortedLabels))

	// Count how many unique series will exist after merging across blocks.
	uniqueSeriesCount := 1
	for i := 1; i < len(allSeries); i++ {
		if labels.Compare(allSeries[i].labels, allSeries[i-1].labels) != 0 {
			uniqueSeriesCount++
		}
	}

	// Divide rows evenly across shards to avoid one small shard at the end;
	// use floats & round up so integer division does not cut off the remainder series.
	totalShards := int(math.Ceil(float64(uniqueSeriesCount) / float64(opts.numRowGroups*opts.rowGroupSize)))
	rowsPerShard := int(math.Ceil(float64(uniqueSeriesCount) / float64(totalShards)))

	// For each shard index i, shardSeries[i] is a map of blockID -> []series.
	shardSeries := make([]map[ulid.ULID][]blockSeries, totalShards)
	for i := range shardSeries {
		shardSeries[i] = make(map[ulid.ULID][]blockSeries)
	}

	shardIdx := 0
	allSeriesIdx := 0
	for shardIdx < totalShards {
		seriesToShard := allSeries[allSeriesIdx:]

		// First series in a shard will always be unique.
		uniqueCount := 1
		shardSeries[shardIdx][seriesToShard[0].blockID] = append(
			shardSeries[shardIdx][seriesToShard[0].blockID], seriesToShard[0],
		)
		allSeriesIdx++

		// Split all series into shards, counting unique series until we reach rowsPerShard.
		// Nothing gets dropped here as all series are already unique per block; merge happens later.
		for i := 1; i < len(seriesToShard); i++ {
			prev := seriesToShard[i-1]
			curr := seriesToShard[i]
			shardSeries[shardIdx][curr.blockID] = append(shardSeries[shardIdx][curr.blockID], curr)

			if labels.Compare(curr.labels, prev.labels) != 0 {
				uniqueCount++
			}
			allSeriesIdx++
			if uniqueCount >= rowsPerShard {
				break
			}
		}
		shardIdx++
	}

	return uniqueSeriesCount, shardSeries, nil
}

func compareBySortedLabelsFunc(sortedLabels []string) func(a, b labels.Labels) int {
	return func(a, b labels.Labels) int {
		for _, lb := range sortedLabels {
			if c := strings.Compare(a.Get(lb), b.Get(lb)); c != 0 {
				return c
			}
		}

		return labels.Compare(a, b)
	}
}

func allChunksEmpty(chkBytes [][]byte) bool {
	for _, chk := range chkBytes {
		if len(chk) != 0 {
			return false
		}
	}
	return true
}
