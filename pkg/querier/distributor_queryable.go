// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/distributor_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (client.CombinedQueryStreamResponse, error)
	QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error)
	LabelValuesForLabelName(ctx context.Context, from, to model.Time, label model.LabelName, matchers ...*labels.Matcher) ([]string, error)
	LabelNames(ctx context.Context, from model.Time, to model.Time, matchers ...*labels.Matcher) ([]string, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error)
	MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error)
	LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error)
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error)
}

func newDistributorQueryable(distributor Distributor, iteratorFn chunkIteratorFunc, cfgProvider distributorQueryableConfigProvider, queryMetrics *stats.QueryMetrics, logger log.Logger) QueryableWithFilter {
	return distributorQueryable{
		logger:       logger,
		distributor:  distributor,
		iteratorFn:   iteratorFn,
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
	iteratorFn   chunkIteratorFunc
	cfgProvider  distributorQueryableConfigProvider
	queryMetrics *stats.QueryMetrics
}

func (d distributorQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	queryIngestersWithin := d.cfgProvider.QueryIngestersWithin(userID)
	now := time.Now()

	// Don't create distributorQuerier if maxt is not within QueryIngestersWithin w.r.t. current time.
	if queryIngestersWithin != 0 && maxt < util.TimeToMillis(now.Add(-queryIngestersWithin)) {
		return storage.NoopQuerier(), nil
	}

	return &distributorQuerier{
		logger:               d.logger,
		distributor:          d.distributor,
		ctx:                  ctx,
		mint:                 mint,
		maxt:                 maxt,
		chunkIterFn:          d.iteratorFn,
		queryIngestersWithin: queryIngestersWithin,
		queryMetrics:         d.queryMetrics,
	}, nil
}

func (d distributorQueryable) UseQueryable(_ time.Time, _, _ int64) bool {
	// Always returns true. The proper check is done in `distributorQueryable.Querier()` - if the time range being
	// queried doesn't overlap with queryIngestersWithin, a noopQuerier will be returned instead.
	// This code could be simplified and `UseQueryable()` removed, see
	// https://github.com/grafana/mimir/pull/4287#discussion_r1132488638
	return true
}

type distributorQuerier struct {
	logger               log.Logger
	distributor          Distributor
	ctx                  context.Context
	mint, maxt           int64
	chunkIterFn          chunkIteratorFunc
	queryIngestersWithin time.Duration
	queryMetrics         *stats.QueryMetrics
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *distributorQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	spanlog, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "distributorQuerier.Select")
	defer spanlog.Finish()

	minT, maxT := q.mint, q.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	// If queryIngestersWithin is enabled, we do manipulate the query mint to query samples up until
	// now - queryIngestersWithin, because older time ranges are covered by the storage. This
	// optimization is particularly important for the blocks storage where the blocks retention in the
	// ingesters could be way higher than queryIngestersWithin.
	minT = int64(clampTime(q.ctx, model.Time(minT), q.queryIngestersWithin, model.Now().Add(-q.queryIngestersWithin), true, "min", "query ingesters within", spanlog))

	if minT > maxT {
		level.Debug(spanlog).Log("msg", "empty query time range after min time manipulation")
		return storage.EmptySeriesSet()
	}

	if sp != nil && sp.Func == "series" {
		ms, err := q.distributor.MetricsForLabelMatchers(ctx, model.Time(minT), model.Time(maxT), matchers...)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return series.LabelsToSeriesSet(ms)
	}

	return q.streamingSelect(ctx, minT, maxT, matchers)
}

func (q *distributorQuerier) streamingSelect(ctx context.Context, minT, maxT int64, matchers []*labels.Matcher) storage.SeriesSet {
	results, err := q.distributor.QueryStream(ctx, model.Time(minT), model.Time(maxT), matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	sets := []storage.SeriesSet(nil)
	if len(results.Timeseries) > 0 {
		sets = append(sets, newTimeSeriesSeriesSet(results.Timeseries))
	}

	serieses := make([]storage.Series, 0, len(results.Chunkseries))
	for _, result := range results.Chunkseries {
		// Sometimes the ingester can send series that have no data.
		if len(result.Chunks) == 0 {
			continue
		}

		ls := mimirpb.FromLabelAdaptersToLabels(result.Labels)

		chunks, err := client.FromChunks(ls, result.Chunks)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		serieses = append(serieses, &chunkSeries{
			labels:            ls,
			chunks:            chunks,
			chunkIteratorFunc: q.chunkIterFn,
			mint:              minT,
			maxt:              maxT,
		})
	}

	if len(serieses) > 0 {
		sets = append(sets, series.NewConcreteSeriesSetFromUnsortedSeries(serieses))
	}

	if len(results.StreamingSeries) > 0 {
		streamingSeries := make([]storage.Series, 0, len(results.StreamingSeries))
		streamingChunkSeriesConfig := &streamingChunkSeriesContext{
			chunkIteratorFunc: q.chunkIterFn,
			mint:              minT,
			maxt:              maxT,
			queryMetrics:      q.queryMetrics,
			queryStats:        stats.FromContext(ctx),
		}

		for _, s := range results.StreamingSeries {
			streamingSeries = append(streamingSeries, &streamingChunkSeries{
				labels:  s.Labels,
				sources: s.Sources,
				context: streamingChunkSeriesConfig,
			})
		}

		sets = append(sets, series.NewConcreteSeriesSetFromSortedSeries(streamingSeries))
	}

	if len(sets) == 0 {
		return storage.EmptySeriesSet()
	}
	if len(sets) == 1 {
		return sets[0]
	}
	// Sets need to be sorted. Both series.NewConcreteSeriesSetFromUnsortedSeries and newTimeSeriesSeriesSet take care of that.
	return storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
}

func (q *distributorQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	minT := clampTime(q.ctx, model.Time(q.mint), q.queryIngestersWithin, model.Now().Add(-q.queryIngestersWithin), true, "min", "query ingesters within", q.logger)

	if minT > model.Time(q.maxt) {
		level.Debug(q.logger).Log("msg", "empty time range after min time manipulation")
		return nil, nil, nil
	}

	lvs, err := q.distributor.LabelValuesForLabelName(q.ctx, minT, model.Time(q.maxt), model.LabelName(name), matchers...)

	return lvs, nil, err
}

func (q *distributorQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "distributorQuerier.LabelNames")
	defer log.Span.Finish()

	minT := clampTime(q.ctx, model.Time(q.mint), q.queryIngestersWithin, model.Now().Add(-q.queryIngestersWithin), true, "min", "query ingesters within", log)

	if minT > model.Time(q.maxt) {
		level.Debug(q.logger).Log("msg", "empty time range after min time manipulation")
		return nil, nil, nil
	}

	ln, err := q.distributor.LabelNames(ctx, minT, model.Time(q.maxt), matchers...)
	return ln, nil, err
}

func (q *distributorQuerier) Close() error {
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
	spanlog, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "distributorExemplarQuerier.Select")
	defer spanlog.Finish()

	level.Debug(spanlog).Log(
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

	level.Debug(spanlog).Log("numSeries", len(ret), "numExemplars", numExemplars)
	return ret, nil
}
