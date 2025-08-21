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

func NewParquetChunkQuerier(d *schema.PrometheusParquetChunksDecoder, shardFinder queryable.ShardsFinderFunction, opts ...QuerierOpts) (*parquetChunkQuerier, error) {
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

func (p parquetChunkQuerier) Close() error {
	// TODO implement me
	return nil
}

func (p parquetChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *prom_storage.SelectHints, matchers ...*labels.Matcher) (labelsSet prom_storage.ChunkSeriesSet, seriesSet prom_storage.ChunkSeriesSet) {
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
		errSet := prom_storage.ErrChunkSeriesSet(err)
		return errSet, errSet
	}
	labelsSets := make([]prom_storage.ChunkSeriesSet, len(shards))
	seriesSets := make([]prom_storage.ChunkSeriesSet, len(shards))

	minT, maxT := p.mint, p.maxt
	if hints != nil {
		minT, maxT = hints.Start, hints.End
	}
	skipChunks := hints != nil && hints.Func == "series"
	errGroup, _ := errgroup.WithContext(ctx)
	errGroup.SetLimit(p.opts.concurrency)

	for i, shard := range shards {
		errGroup.Go(func() error {
			// TODO
			// There's a problem here: the iterator we get (and return) will be tied to the context
			// we pass here. If we pass the group's context, it will always be canceled by the time
			// of iterator. If we pass the parent ctx (as we are currently doing), goroutines
			// are not canceled early if one of them fails. The latter is not ideal, but at least
			// it works. Ideally we'd have different contexts for each step: the creation of the
			// iterator, and the iteration itself.
			ls, ss, err := shard.Query(ctx, sortSeries, hints, minT, maxT, skipChunks, matchers)
			labelsSets[i] = ls
			seriesSets[i] = ss
			return err
		})
	}

	if err := errGroup.Wait(); err != nil {
		errSet := prom_storage.ErrChunkSeriesSet(err)
		return errSet, errSet
	}

	span.SetAttributes(
		attribute.Int("shards_count", len(shards)),
		attribute.Bool("skip_chunks", skipChunks),
	)

	if skipChunks {
		return convert.NewMergeChunkSeriesSet(labelsSets, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger()), nil
	}

	return convert.NewMergeChunkSeriesSet(labelsSets, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger()), convert.NewMergeChunkSeriesSet(seriesSets, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())
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
