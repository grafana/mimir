// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/distributor_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkinfologger"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	QueryStream(ctx context.Context, queryMetrics *stats.QueryMetrics, from, to model.Time, projectionInclude bool, projectionLabels []string, matchers ...*labels.Matcher) (client.CombinedQueryStreamResponse, error)
	QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error)
	LabelValuesForLabelName(ctx context.Context, from, to model.Time, label model.LabelName, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error)
	LabelNames(ctx context.Context, from model.Time, to model.Time, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]labels.Labels, error)
	MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error)
	LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*client.LabelNamesAndValuesResponse, error)
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
	ActiveSeries(ctx context.Context, matchers []*labels.Matcher) ([]labels.Labels, error)
	ActiveNativeHistogramMetrics(ctx context.Context, matchers []*labels.Matcher) (*cardinality.ActiveNativeHistogramMetricsResponse, error)
}

func NewDistributorQueryable(distributor Distributor, cfgProvider distributorQueryableConfigProvider, queryMetrics *stats.QueryMetrics, logger log.Logger) storage.Queryable {
	return distributorQueryable{
		logger:       logger,
		distributor:  distributor,
		cfgProvider:  cfgProvider,
		queryMetrics: queryMetrics,
	}
}

type distributorQueryableConfigProvider interface {
	QueryIngestersWithin(userID string) time.Duration
}

type distributorQueryable struct {
	logger       log.Logger
	distributor  Distributor
	cfgProvider  distributorQueryableConfigProvider
	queryMetrics *stats.QueryMetrics
}

func (d distributorQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &distributorQuerier{
		logger:       d.logger,
		distributor:  d.distributor,
		mint:         mint,
		maxt:         maxt,
		queryMetrics: d.queryMetrics,
		cfgProvider:  d.cfgProvider,
	}, nil
}

type distributorQuerier struct {
	logger       log.Logger
	distributor  Distributor
	mint, maxt   int64
	cfgProvider  distributorQueryableConfigProvider
	queryMetrics *stats.QueryMetrics

	streamReadersMtx sync.Mutex
	closed           bool
	streamReaders    []*client.SeriesChunksStreamReader
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *distributorQuerier) Select(ctx context.Context, _ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.Select")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	minT, maxT := q.mint, q.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		spanLog.DebugLog("msg", "not querying ingesters; query time range ends before the query-ingesters-within limit")
		return storage.EmptySeriesSet()
	}

	memoryTracker, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	now := time.Now().UnixMilli()
	minT = clampMinTime(spanLog, minT, now, -queryIngestersWithin, "query ingesters within")

	if sp != nil && sp.Func == "series" {
		ms, err := q.distributor.MetricsForLabelMatchers(ctx, model.Time(minT), model.Time(maxT), sp, matchers...)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return series.LabelsToSeriesSet(ms)
	}

	var (
		projectionInclude bool
		projectionLabels  []string
	)
	if sp != nil {
		projectionInclude = sp.ProjectionInclude
		projectionLabels = sp.ProjectionLabels
	}

	return series.NewMemoryTrackingSeriesSet(q.streamingSelect(ctx, minT, maxT, projectionInclude, projectionLabels, matchers), memoryTracker)
}

func (q *distributorQuerier) streamingSelect(ctx context.Context, minT, maxT int64, projectionInclude bool, projectionLabels []string, matchers []*labels.Matcher) storage.SeriesSet {
	results, err := q.distributor.QueryStream(ctx, q.queryMetrics, model.Time(minT), model.Time(maxT), projectionInclude, projectionLabels, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	var chunkInfo *chunkinfologger.ChunkInfoLogger
	if chunkinfologger.IsChunkInfoLoggingEnabled(ctx) {
		traceID, spanID, _ := tracing.ExtractTraceSpanID(ctx)
		chunkInfo = chunkinfologger.NewChunkInfoLogger("ingester message", traceID, spanID, q.logger, chunkinfologger.ChunkInfoLoggingFromContext(ctx))
		chunkInfo.LogSelect("ingester", minT, maxT)
		chunkInfo.SetMsg("ingester streaming")
	}

	streamingSeries := make([]storage.Series, 0, len(results.StreamingSeries))
	streamingChunkSeriesConfig := &streamingChunkSeriesContext{
		queryMetrics: q.queryMetrics,
		queryStats:   stats.FromContext(ctx),
	}

	for i, s := range results.StreamingSeries {
		streamingSeries = append(streamingSeries, &streamingChunkSeries{
			labels:    s.Labels,
			sources:   s.Sources,
			context:   streamingChunkSeriesConfig,
			lastOne:   i == len(results.StreamingSeries)-1,
			chunkInfo: chunkInfo,
		})
	}

	q.streamReadersMtx.Lock()
	defer q.streamReadersMtx.Unlock()

	if q.closed {
		// We were closed while loading, give up now.
		for _, r := range results.StreamReaders {
			r.FreeBuffer()
		}

		return storage.ErrSeriesSet(errAlreadyClosed)
	}

	// Store the stream readers so we can free their buffers when we're done using this Querier.
	q.streamReaders = append(q.streamReaders, results.StreamReaders...)

	return series.NewConcreteSeriesSetFromSortedSeries(streamingSeries)
}

func (q *distributorQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.LabelValues")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		level.Debug(spanLog).Log("msg", "not querying ingesters; query time range ends before the query-ingesters-within limit")
		return nil, nil, nil
	}

	now := time.Now().UnixMilli()
	q.mint = clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	lvs, err := q.distributor.LabelValuesForLabelName(ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), hints, matchers...)

	return lvs, nil, err
}

func (q *distributorQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.LabelNames")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		level.Debug(spanLog).Log("msg", "not querying ingesters; query time range ends before the query-ingesters-within limit")
		return nil, nil, nil
	}

	now := time.Now().UnixMilli()
	q.mint = clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	ln, err := q.distributor.LabelNames(ctx, model.Time(q.mint), model.Time(q.maxt), hints, matchers...)
	return ln, nil, err
}

func (q *distributorQuerier) Close() error {
	q.streamReadersMtx.Lock()
	defer q.streamReadersMtx.Unlock()

	q.closed = true

	for _, r := range q.streamReaders {
		r.FreeBuffer()
	}

	return nil
}

type distributorExemplarQueryable struct {
	distributor Distributor
	logger      log.Logger
}

func newDistributorExemplarQueryable(d Distributor, logger log.Logger) storage.ExemplarQueryable {
	return &distributorExemplarQueryable{
		distributor: d,
		logger:      logger,
	}
}

func (d distributorExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &distributorExemplarQuerier{
		distributor: d.distributor,
		ctx:         ctx,
		logger:      d.logger,
	}, nil
}

type distributorExemplarQuerier struct {
	distributor Distributor
	ctx         context.Context
	logger      log.Logger
}

// Select querys for exemplars, prometheus' storage.ExemplarQuerier's Select function takes the time range as two int64 values.
func (q *distributorExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	spanlog, ctx := spanlogger.New(q.ctx, q.logger, tracer, "distributorExemplarQuerier.Select")
	defer spanlog.Finish()

	spanlog.DebugLog(
		"start", util.TimeFromMillis(start).UTC().String(),
		"end", util.TimeFromMillis(end).UTC().String(),
		"matchers", util.MultiMatchersStringer(matchers),
	)
	allResults, err := q.distributor.QueryExemplars(ctx, model.Time(start), model.Time(end), matchers...)
	if err != nil {
		return nil, err
	}

	var numExemplars int
	var e exemplar.QueryResult
	ret := make([]exemplar.QueryResult, len(allResults.Timeseries))
	for i, ts := range allResults.Timeseries {
		e.SeriesLabels = mimirpb.FromLabelAdaptersToLabels(ts.Labels)
		e.Exemplars = mimirpb.FromExemplarProtosToExemplars(ts.Exemplars)
		ret[i] = e

		numExemplars += len(e.Exemplars)
	}

	spanlog.DebugLog("numSeries", len(ret), "numExemplars", numExemplars)
	return ret, nil
}
