// SPDX-License-Identifier: AGPL-3.0-only

package parquet

import (
	"context"
	"runtime"
	"strings"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/queryable"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"
)

var tracer = otel.Tracer("pkg/storage/parquet")

type querierOpts struct {
	concurrency                      int
	rowCountLimitFunc                search.QuotaLimitFunc
	chunkBytesLimitFunc              search.QuotaLimitFunc
	dataBytesLimitFunc               search.QuotaLimitFunc
	materializedSeriesCallback       search.MaterializedSeriesFunc
	materializedLabelsFilterCallback search.MaterializedLabelsFilterCallback
}

var DefaultQuerierOpts = querierOpts{
	concurrency:                      runtime.GOMAXPROCS(0),
	rowCountLimitFunc:                search.NoopQuotaLimitFunc,
	chunkBytesLimitFunc:              search.NoopQuotaLimitFunc,
	dataBytesLimitFunc:               search.NoopQuotaLimitFunc,
	materializedSeriesCallback:       search.NoopMaterializedSeriesFunc,
	materializedLabelsFilterCallback: search.NoopMaterializedLabelsFilterCallback,
}

type QuerierOpts func(*querierOpts)

// WithConcurrency set the concurrency that can be used to run the query
func WithConcurrency(concurrency int) QuerierOpts {
	return func(opts *querierOpts) {
		opts.concurrency = concurrency
	}
}

// WithRowCountLimitFunc sets a callback function to get limit for matched row count.
func WithRowCountLimitFunc(fn search.QuotaLimitFunc) QuerierOpts {
	return func(opts *querierOpts) {
		opts.rowCountLimitFunc = fn
	}
}

// WithChunkBytesLimitFunc sets a callback function to get limit for chunk column page bytes fetched.
func WithChunkBytesLimitFunc(fn search.QuotaLimitFunc) QuerierOpts {
	return func(opts *querierOpts) {
		opts.chunkBytesLimitFunc = fn
	}
}

// WithDataBytesLimitFunc sets a callback function to get limit for data (including label and chunk)
// column page bytes fetched.
func WithDataBytesLimitFunc(fn search.QuotaLimitFunc) QuerierOpts {
	return func(opts *querierOpts) {
		opts.dataBytesLimitFunc = fn
	}
}

// WithMaterializedSeriesCallback sets a callback function to process the materialized series.
func WithMaterializedSeriesCallback(fn search.MaterializedSeriesFunc) QuerierOpts {
	return func(opts *querierOpts) {
		opts.materializedSeriesCallback = fn
	}
}

// WithMaterializedLabelsFilterCallback sets a callback function to create a filter that can be used
// to filter series based on their labels before materializing chunks.
func WithMaterializedLabelsFilterCallback(cb search.MaterializedLabelsFilterCallback) QuerierOpts {
	return func(opts *querierOpts) {
		opts.materializedLabelsFilterCallback = cb
	}
}

func NewParquetChunkQuerier(d *schema.PrometheusParquetChunksDecoder, shardFinder queryable.ShardsFinderFunction, opts ...QuerierOpts) (prom_storage.ChunkQuerier, error) {
	cfg := DefaultQuerierOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	return &parquetChunkQuerier{
		shardsFinder: shardFinder,
		d:            d,
		opts:         &cfg,
	}, nil
}

type parquetChunkQuerier struct {
	mint, maxt   int64
	shardsFinder queryable.ShardsFinderFunction
	d            *schema.PrometheusParquetChunksDecoder
	opts         *querierOpts
}

func (p parquetChunkQuerier) LabelValues(ctx context.Context, name string, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO implement me
	panic("implement me")
}

func (p parquetChunkQuerier) LabelNames(ctx context.Context, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	// TODO implement me
	panic("implement me")
}

func (p parquetChunkQuerier) Close() error {
	// TODO implement me
	return nil
}

func (p parquetChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *prom_storage.SelectHints, matchers ...*labels.Matcher) prom_storage.ChunkSeriesSet {
	ctx, span := tracer.Start(ctx, "parquetChunkQuerier.Select")
	var err error
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	span.SetAttributes(
		attribute.Bool("sorted", sortSeries),
		attribute.Int64("mint", p.mint),
		attribute.Int64("maxt", p.maxt),
		attribute.String("matchers", matchersToString(matchers)),
	)

	if hints != nil {
		span.SetAttributes(
			attribute.Int64("select_start", hints.Start),
			attribute.Int64("select_end", hints.End),
			attribute.String("select_func", hints.Func),
		)
	}

	shards, err := p.queryableShards(ctx, p.mint, p.maxt)
	if err != nil {
		return prom_storage.ErrChunkSeriesSet(err)
	}
	seriesSet := make([]prom_storage.ChunkSeriesSet, len(shards))

	minT, maxT := p.mint, p.maxt
	if hints != nil {
		minT, maxT = hints.Start, hints.End
	}
	skipChunks := hints != nil && hints.Func == "series"
	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.opts.concurrency)

	for i, shard := range shards {
		errGroup.Go(func() error {
			ss, err := shard.Query(ctx, sortSeries, hints, minT, maxT, skipChunks, matchers)
			seriesSet[i] = ss
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		return prom_storage.ErrChunkSeriesSet(err)
	}

	span.SetAttributes(
		attribute.Int("shards_count", len(shards)),
		attribute.Bool("skip_chunks", skipChunks),
	)

	return convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())
}

func (p parquetChunkQuerier) queryableShards(ctx context.Context, mint, maxt int64) ([]*queryableShard, error) {
	shards, err := p.shardsFinder(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	qBlocks := make([]*queryableShard, len(shards))
	rowCountQuota := search.NewQuota(p.opts.rowCountLimitFunc(ctx))
	chunkBytesQuota := search.NewQuota(p.opts.chunkBytesLimitFunc(ctx))
	dataBytesQuota := search.NewQuota(p.opts.dataBytesLimitFunc(ctx))
	for i, shard := range shards {
		qb, err := newQueryableShard(p.opts, shard, p.d, rowCountQuota, chunkBytesQuota, dataBytesQuota)
		if err != nil {
			return nil, err
		}
		qBlocks[i] = qb
	}
	return qBlocks, nil
}

func matchersToString(matchers []*labels.Matcher) string {
	if len(matchers) == 0 {
		return "[]"
	}
	var matcherStrings []string
	for _, m := range matchers {
		matcherStrings = append(matcherStrings, m.String())
	}
	return "[" + strings.Join(matcherStrings, ",") + "]"
}
