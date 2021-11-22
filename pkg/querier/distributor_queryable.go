// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/distributor_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/prom1/storage/metric"
	"github.com/grafana/mimir/pkg/querier/series"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
	"github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error)
	QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error)
	LabelValuesForLabelName(ctx context.Context, from, to model.Time, label model.LabelName, matchers ...*labels.Matcher) ([]string, error)
	LabelNames(ctx context.Context, from model.Time, to model.Time, matchers ...*labels.Matcher) ([]string, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error)
	MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error)
	LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error)
	LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (uint64, *client.LabelValuesCardinalityResponse, error)
}

func newDistributorQueryable(distributor Distributor, iteratorFn chunkIteratorFunc, queryIngestersWithin time.Duration, queryLabelNamesWithMatchers bool, logger log.Logger) QueryableWithFilter {
	return distributorQueryable{
		logger:                      logger,
		distributor:                 distributor,
		iteratorFn:                  iteratorFn,
		queryIngestersWithin:        queryIngestersWithin,
		queryLabelNamesWithMatchers: queryLabelNamesWithMatchers,
	}
}

type distributorQueryable struct {
	logger               log.Logger
	distributor          Distributor
	iteratorFn           chunkIteratorFunc
	queryIngestersWithin time.Duration

	queryLabelNamesWithMatchers bool
}

func (d distributorQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &distributorQuerier{
		logger:               d.logger,
		distributor:          d.distributor,
		ctx:                  ctx,
		mint:                 mint,
		maxt:                 maxt,
		chunkIterFn:          d.iteratorFn,
		queryIngestersWithin: d.queryIngestersWithin,

		queryLabelNamesWithMatchers: d.queryLabelNamesWithMatchers,
	}, nil
}

func (d distributorQueryable) UseQueryable(now time.Time, _, queryMaxT int64) bool {
	// Include ingester only if maxt is within QueryIngestersWithin w.r.t. current time.
	return d.queryIngestersWithin == 0 || queryMaxT >= util.TimeToMillis(now.Add(-d.queryIngestersWithin))
}

type distributorQuerier struct {
	logger               log.Logger
	distributor          Distributor
	ctx                  context.Context
	mint, maxt           int64
	chunkIterFn          chunkIteratorFunc
	queryIngestersWithin time.Duration

	queryLabelNamesWithMatchers bool
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *distributorQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "distributorQuerier.Select")
	defer log.Span.Finish()

	if sp.Func == "series" {
		ms, err := q.distributor.MetricsForLabelMatchers(ctx, model.Time(sp.Start), model.Time(sp.End), matchers...)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return series.MetricsToSeriesSet(ms)
	}

	minT, maxT := sp.Start, sp.End

	// If queryIngestersWithin is enabled, we do manipulate the query mint to query samples up until
	// now - queryIngestersWithin, because older time ranges are covered by the storage. This
	// optimization is particularly important for the blocks storage where the blocks retention in the
	// ingesters could be way higher than queryIngestersWithin.
	if q.queryIngestersWithin > 0 {
		now := time.Now()
		origMinT := minT
		minT = math.Max64(minT, util.TimeToMillis(now.Add(-q.queryIngestersWithin)))

		if origMinT != minT {
			level.Debug(log).Log("msg", "the min time of the query to ingesters has been manipulated", "original", origMinT, "updated", minT)
		}

		if minT > maxT {
			level.Debug(log).Log("msg", "empty query time range after min time manipulation")
			return storage.EmptySeriesSet()
		}
	}

	return q.streamingSelect(ctx, minT, maxT, matchers)
}

func (q *distributorQuerier) streamingSelect(ctx context.Context, minT, maxT int64, matchers []*labels.Matcher) storage.SeriesSet {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

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
		sort.Sort(ls)

		chunks, err := chunkcompat.FromChunks(userID, ls, result.Chunks)
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
		sets = append(sets, series.NewConcreteSeriesSet(serieses))
	}

	if len(sets) == 0 {
		return storage.EmptySeriesSet()
	}
	if len(sets) == 1 {
		return sets[0]
	}
	// Sets need to be sorted. Both series.NewConcreteSeriesSet and newTimeSeriesSeriesSet take care of that.
	return storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
}

func (q *distributorQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	lvs, err := q.distributor.LabelValuesForLabelName(q.ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), matchers...)

	return lvs, nil, err
}

func (q *distributorQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "distributorQuerier.LabelNames")
	defer log.Span.Finish()

	if len(matchers) > 0 && !q.queryLabelNamesWithMatchers {
		return q.legacyLabelNamesWithMatchersThroughMetricsCall(ctx, matchers...)
	}

	ln, err := q.distributor.LabelNames(ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	return ln, nil, err
}

// legacyLabelNamesWithMatchersThroughMetricsCall performs the LabelNames call in _the old way_, by calling ingester's MetricsForLabelMatchers method
// this is used when the LabelNames with matchers feature is first deployed, and some ingesters may have not been updated yet, so they could be ignoring
// the matchers, leading to wrong results.
func (q *distributorQuerier) legacyLabelNamesWithMatchersThroughMetricsCall(ctx context.Context, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, q.logger, "distributorQuerier.legacyLabelNamesWithMatchersThroughMetricsCall")
	defer log.Span.Finish()
	ms, err := q.distributor.MetricsForLabelMatchers(ctx, model.Time(q.mint), model.Time(q.maxt), matchers...)
	if err != nil {
		return nil, nil, err
	}
	namesMap := make(map[string]struct{})

	for _, m := range ms {
		for name := range m.Metric {
			namesMap[string(name)] = struct{}{}
		}
	}

	names := make([]string, 0, len(namesMap))
	for name := range namesMap {
		names = append(names, name)
	}
	sort.Strings(names)

	return names, nil, nil
}

func (q *distributorQuerier) Close() error {
	return nil
}

type distributorExemplarQueryable struct {
	distributor Distributor
}

func newDistributorExemplarQueryable(d Distributor) storage.ExemplarQueryable {
	return &distributorExemplarQueryable{
		distributor: d,
	}
}

func (d distributorExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &distributorExemplarQuerier{
		distributor: d.distributor,
		ctx:         ctx,
	}, nil
}

type distributorExemplarQuerier struct {
	distributor Distributor
	ctx         context.Context
}

// Select querys for exemplars, prometheus' storage.ExemplarQuerier's Select function takes the time range as two int64 values.
func (q *distributorExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	allResults, err := q.distributor.QueryExemplars(q.ctx, model.Time(start), model.Time(end), matchers...)

	if err != nil {
		return nil, err
	}

	var e exemplar.QueryResult
	ret := make([]exemplar.QueryResult, len(allResults.Timeseries))
	for i, ts := range allResults.Timeseries {
		e.SeriesLabels = mimirpb.FromLabelAdaptersToLabels(ts.Labels)
		e.Exemplars = mimirpb.FromExemplarProtosToExemplars(ts.Exemplars)
		ret[i] = e
	}
	return ret, nil
}
