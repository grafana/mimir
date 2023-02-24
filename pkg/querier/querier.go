// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/querier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/querier/engine"
	"github.com/grafana/mimir/pkg/querier/iterators"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Config contains the configuration require to create a querier
type Config struct {
	Iterators      bool `yaml:"iterators" category:"advanced"`
	BatchIterators bool `yaml:"batch_iterators" category:"advanced"`

	// QueryStoreAfter the time after which queries should also be sent to the store and not just ingesters.
	QueryStoreAfter    time.Duration `yaml:"query_store_after" category:"advanced"`
	MaxQueryIntoFuture time.Duration `yaml:"max_query_into_future" category:"advanced"`

	StoreGatewayClient ClientConfig `yaml:"store_gateway_client"`

	ShuffleShardingIngestersEnabled bool `yaml:"shuffle_sharding_ingesters_enabled" category:"advanced"`

	// PromQL engine config.
	EngineConfig engine.Config `yaml:",inline"`
}

const (
	queryStoreAfterFlag = "querier.query-store-after"
)

var (
	errBadLookbackConfigs = fmt.Errorf("the -%s setting must be greater than -%s otherwise queries might return partial results", validation.QueryIngestersWithinFlag, queryStoreAfterFlag)
	errEmptyTimeRange     = errors.New("empty time range")
)

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.StoreGatewayClient.RegisterFlagsWithPrefix("querier.store-gateway-client", f)
	f.BoolVar(&cfg.Iterators, "querier.iterators", false, "Use iterators to execute query, as opposed to fully materialising the series in memory.")
	f.BoolVar(&cfg.BatchIterators, "querier.batch-iterators", true, "Use batch iterators to execute query, as opposed to fully materialising the series in memory.  Takes precedent over the -querier.iterators flag.")
	f.DurationVar(&cfg.MaxQueryIntoFuture, "querier.max-query-into-future", 10*time.Minute, "Maximum duration into the future you can query. 0 to disable.")
	f.DurationVar(&cfg.QueryStoreAfter, queryStoreAfterFlag, 12*time.Hour, "The time after which a metric should be queried from storage and not just ingesters. 0 means all queries are sent to store. If this option is enabled, the time range of the query sent to the store-gateway will be manipulated to ensure the query end is not more recent than 'now - query-store-after'.")
	f.BoolVar(&cfg.ShuffleShardingIngestersEnabled, "querier.shuffle-sharding-ingesters-enabled", true, fmt.Sprintf("Fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since -%s. If this setting is false or -%s is '0', queriers always query all ingesters (ingesters shuffle sharding on read path is disabled).", validation.QueryIngestersWithinFlag, validation.QueryIngestersWithinFlag))

	cfg.EngineConfig.RegisterFlags(f)
}

// Validate the config
func (cfg *Config) Validate(limits validation.Limits) error {
	// Ensure the config wont create a situation where no queriers are returned.
	if limits.QueryIngestersWithin != 0 && cfg.QueryStoreAfter != 0 {
		if cfg.QueryStoreAfter >= time.Duration(limits.QueryIngestersWithin) {
			return errBadLookbackConfigs
		}
	}

	return nil
}

func getChunksIteratorFunction(cfg Config) chunkIteratorFunc {
	if cfg.BatchIterators {
		return batch.NewChunkMergeIterator
	} else if cfg.Iterators {
		return iterators.NewChunkMergeIterator
	}
	return mergeChunks
}

// New builds a queryable and promql engine.
func New(cfg Config, limits *validation.Overrides, distributor Distributor, stores []QueryableWithFilter, reg prometheus.Registerer, logger log.Logger, tracker *activitytracker.ActivityTracker) (storage.SampleAndChunkQueryable, storage.ExemplarQueryable, *promql.Engine) {
	iteratorFunc := getChunksIteratorFunction(cfg)

	distributorQueryable := newDistributorQueryable(distributor, iteratorFunc, limits, logger)

	ns := make([]QueryableWithFilter, len(stores))
	for ix, s := range stores {
		ns[ix] = storeQueryable{
			QueryableWithFilter: s,
			QueryStoreAfter:     cfg.QueryStoreAfter,
		}
	}
	queryable := NewQueryable(distributorQueryable, ns, iteratorFunc, cfg, limits, logger)
	exemplarQueryable := newDistributorExemplarQueryable(distributor, logger)

	lazyQueryable := storage.QueryableFunc(func(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
		querier, err := queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		return lazyquery.NewLazyQuerier(querier), nil
	})

	engine := promql.NewEngine(engine.NewPromQLEngineOptions(cfg.EngineConfig, tracker, logger, reg))
	return NewSampleAndChunkQueryable(lazyQueryable), exemplarQueryable, engine
}

// NewSampleAndChunkQueryable creates a SampleAndChunkQueryable from a Queryable.
func NewSampleAndChunkQueryable(q storage.Queryable) storage.SampleAndChunkQueryable {
	return &sampleAndChunkQueryable{q}
}

type sampleAndChunkQueryable struct {
	storage.Queryable
}

func (q *sampleAndChunkQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	qr, err := q.Queryable.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &chunkQuerier{qr}, nil
}

type chunkQuerier struct {
	storage.Querier
}

func (q *chunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return storage.NewSeriesSetToChunkSet(q.Querier.Select(sortSeries, hints, matchers...))
}

// QueryableWithFilter extends Queryable interface with `UseQueryable` filtering function.
type QueryableWithFilter interface {
	storage.Queryable

	// UseQueryable returns true if this queryable should be used to satisfy the query for given time range.
	// Query min and max time are in milliseconds since epoch.
	UseQueryable(now time.Time, queryMinT, queryMaxT int64, userID string) bool
}

// NewQueryable creates a new Queryable for mimir.
func NewQueryable(distributor QueryableWithFilter, stores []QueryableWithFilter, chunkIterFn chunkIteratorFunc, cfg Config, limits *validation.Overrides, logger log.Logger) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		now := time.Now()

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(limits.MaxFetchedSeriesPerQuery(userID), limits.MaxFetchedChunkBytesPerQuery(userID), limits.MaxChunksPerQuery(userID)))

		mint, maxt, err = validateQueryTimeRange(ctx, userID, mint, maxt, limits, cfg.MaxQueryIntoFuture, logger)
		if errors.Is(err, errEmptyTimeRange) {
			return storage.NoopQuerier(), nil
		} else if err != nil {
			return nil, err
		}

		q := querier{
			ctx:                ctx,
			mint:               mint,
			maxt:               maxt,
			chunkIterFn:        chunkIterFn,
			limits:             limits,
			maxQueryIntoFuture: cfg.MaxQueryIntoFuture,
			logger:             logger,
		}

		if distributor.UseQueryable(now, mint, maxt, userID) {
			dqr, err := distributor.Querier(ctx, mint, maxt)
			if err != nil {
				return nil, err
			}
			q.queriers = append(q.queriers, dqr)
		}

		for _, s := range stores {
			if !s.UseQueryable(now, mint, maxt, userID) {
				continue
			}

			cqr, err := s.Querier(ctx, mint, maxt)
			if err != nil {
				return nil, err
			}

			q.queriers = append(q.queriers, cqr)
		}

		return q, nil
	})
}

// querier implements storage.Querier, running requests across a set of queriers.
type querier struct {
	queriers []storage.Querier

	chunkIterFn chunkIteratorFunc
	ctx         context.Context
	mint, maxt  int64

	limits             *validation.Overrides
	maxQueryIntoFuture time.Duration
	logger             log.Logger
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q querier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "querier.Select")
	defer log.Span.Finish()

	if sp == nil {
		sp = &storage.SelectHints{
			Start: q.mint,
			End:   q.maxt,
		}
	}

	level.Debug(log).Log("hint.func", sp.Func, "start", util.TimeFromMillis(sp.Start).UTC().String(), "end",
		util.TimeFromMillis(sp.End).UTC().String(), "step", sp.Step, "matchers", util.MatchersStringer(matchers))

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Validate query time range. Even if the time range has already been validated when we created
	// the querier, we need to check it again here because the time range specified in hints may be
	// different.
	startMs, endMs, err := validateQueryTimeRange(ctx, userID, sp.Start, sp.End, q.limits, q.maxQueryIntoFuture, q.logger)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.NoopSeriesSet()
	} else if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if sp.Func == "series" { // Clamp max time range for series-only queries, before we check max length.
		maxQueryLength := q.limits.MaxLabelsQueryLength(userID)
		startMs = int64(clampTime(ctx, model.Time(startMs), maxQueryLength, model.Time(endMs).Add(-maxQueryLength), true, "start", "max label query length", log))
	}

	// The time range may have been manipulated during the validation,
	// so we make sure changes are reflected back to hints.
	sp.Start = startMs
	sp.End = endMs

	startTime := model.Time(startMs)
	endTime := model.Time(endMs)

	// Validate query time range.
	if maxQueryLength := q.limits.MaxPartialQueryLength(userID); maxQueryLength > 0 && endTime.Sub(startTime) > maxQueryLength {
		return storage.ErrSeriesSet(validation.NewMaxQueryLengthError(endTime.Sub(startTime), maxQueryLength))
	}

	if len(q.queriers) == 1 {
		return q.queriers[0].Select(true, sp, matchers...)
	}

	sets := make(chan storage.SeriesSet, len(q.queriers))
	for _, querier := range q.queriers {
		go func(querier storage.Querier) {
			sets <- querier.Select(true, sp, matchers...)
		}(querier)
	}

	var result []storage.SeriesSet
	for range q.queriers {
		select {
		case set := <-sets:
			result = append(result, set)
		case <-ctx.Done():
			return storage.ErrSeriesSet(ctx.Err())
		}
	}

	// we have all the sets from different sources (chunk from store, chunks from ingesters,
	// time series from store and time series from ingesters).
	// mergeSeriesSets will return sorted set.
	return q.mergeSeriesSets(result)
}

// LabelValues implements storage.Querier.
func (q querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(q.queriers) == 1 {
		return q.queriers[0].LabelValues(name, matchers...)
	}

	var (
		g, _     = errgroup.WithContext(q.ctx)
		sets     = [][]string{}
		warnings = storage.Warnings(nil)

		resMtx sync.Mutex
	)

	for _, querier := range q.queriers {
		// Need to reassign as the original variable will change and can't be relied on in a goroutine.
		querier := querier
		g.Go(func() error {
			// NB: Values are sorted in Mimir already.
			myValues, myWarnings, err := querier.LabelValues(name, matchers...)
			if err != nil {
				return err
			}

			resMtx.Lock()
			sets = append(sets, myValues)
			warnings = append(warnings, myWarnings...)
			resMtx.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(sets...), warnings, nil
}

func (q querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(q.queriers) == 1 {
		return q.queriers[0].LabelNames(matchers...)
	}

	var (
		g, _     = errgroup.WithContext(q.ctx)
		sets     = [][]string{}
		warnings = storage.Warnings(nil)

		resMtx sync.Mutex
	)

	for _, querier := range q.queriers {
		// Need to reassign as the original variable will change and can't be relied on in a goroutine.
		querier := querier
		g.Go(func() error {
			// NB: Names are sorted in Mimir already.
			myNames, myWarnings, err := querier.LabelNames(matchers...)
			if err != nil {
				return err
			}

			resMtx.Lock()
			sets = append(sets, myNames)
			warnings = append(warnings, myWarnings...)
			resMtx.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(sets...), warnings, nil
}

func (querier) Close() error {
	return nil
}

func (q querier) mergeSeriesSets(sets []storage.SeriesSet) storage.SeriesSet {
	// Here we deal with sets that are based on chunks and build single set from them.
	// Remaining sets are merged with chunks-based one using storage.NewMergeSeriesSet

	otherSets := []storage.SeriesSet(nil)
	chunks := []chunk.Chunk(nil)

	for _, set := range sets {
		nonChunkSeries := []storage.Series(nil)

		// SeriesSet may have some series backed up by chunks, and some not.
		for set.Next() {
			s := set.At()

			if sc, ok := s.(SeriesWithChunks); ok {
				chunks = append(chunks, sc.Chunks()...)
			} else {
				nonChunkSeries = append(nonChunkSeries, s)
			}
		}

		if err := set.Err(); err != nil {
			otherSets = append(otherSets, storage.ErrSeriesSet(err))
		} else if len(nonChunkSeries) > 0 {
			otherSets = append(otherSets, &sliceSeriesSet{series: nonChunkSeries, ix: -1})
		}
	}

	if len(chunks) == 0 {
		return storage.NewMergeSeriesSet(otherSets, storage.ChainedSeriesMerge)
	}

	// partitionChunks returns set with sorted series, so it can be used by NewMergeSeriesSet
	chunksSet := partitionChunks(chunks, q.mint, q.maxt, q.chunkIterFn)

	if len(otherSets) == 0 {
		return chunksSet
	}

	otherSets = append(otherSets, chunksSet)
	return storage.NewMergeSeriesSet(otherSets, storage.ChainedSeriesMerge)
}

type sliceSeriesSet struct {
	series []storage.Series
	ix     int
}

func (s *sliceSeriesSet) Next() bool {
	s.ix++
	return s.ix < len(s.series)
}

func (s *sliceSeriesSet) At() storage.Series {
	if s.ix < 0 || s.ix >= len(s.series) {
		return nil
	}
	return s.series[s.ix]
}

func (s *sliceSeriesSet) Err() error {
	return nil
}

func (s *sliceSeriesSet) Warnings() storage.Warnings {
	return nil
}

type storeQueryable struct {
	QueryableWithFilter
	QueryStoreAfter time.Duration
}

func (s storeQueryable) UseQueryable(now time.Time, queryMinT, queryMaxT int64, userID string) bool {
	// Include this store only if mint is within QueryStoreAfter w.r.t current time.
	if s.QueryStoreAfter != 0 && queryMinT > util.TimeToMillis(now.Add(-s.QueryStoreAfter)) {
		return false
	}
	return s.QueryableWithFilter.UseQueryable(now, queryMinT, queryMaxT, userID)
}

type alwaysTrueFilterQueryable struct {
	storage.Queryable
}

func (alwaysTrueFilterQueryable) UseQueryable(_ time.Time, _, _ int64, _ string) bool {
	return true
}

// UseAlwaysQueryable wraps storage.Queryable into QueryableWithFilter, with no query filtering.
func UseAlwaysQueryable(q storage.Queryable) QueryableWithFilter {
	return alwaysTrueFilterQueryable{Queryable: q}
}

type useBeforeTimestampQueryable struct {
	storage.Queryable
	ts int64 // Timestamp in milliseconds
}

func (u useBeforeTimestampQueryable) UseQueryable(_ time.Time, queryMinT, _ int64, _ string) bool {
	if u.ts == 0 {
		return true
	}
	return queryMinT < u.ts
}

// UseBeforeTimestampQueryable returns QueryableWithFilter, that is used only if query starts before given timestamp.
// If timestamp is zero (time.IsZero), queryable is always used.
func UseBeforeTimestampQueryable(queryable storage.Queryable, ts time.Time) QueryableWithFilter {
	t := int64(0)
	if !ts.IsZero() {
		t = util.TimeToMillis(ts)
	}
	return useBeforeTimestampQueryable{
		Queryable: queryable,
		ts:        t,
	}
}

func validateQueryTimeRange(ctx context.Context, userID string, startMs, endMs int64, limits *validation.Overrides, maxQueryIntoFuture time.Duration, logger log.Logger) (int64, int64, error) {
	now := model.Now()
	startTime := model.Time(startMs)
	endTime := model.Time(endMs)

	endTime = clampTime(ctx, endTime, maxQueryIntoFuture, now.Add(maxQueryIntoFuture), false, "end", "max query into future", logger)

	maxQueryLookback := limits.MaxQueryLookback(userID)
	startTime = clampTime(ctx, startTime, maxQueryLookback, now.Add(-maxQueryLookback), true, "start", "max query lookback", logger)

	if endTime.Before(startTime) {
		return 0, 0, errEmptyTimeRange
	}

	return int64(startTime), int64(endTime), nil
}

// Ensure a time is within bounds, and log in traces to ease debugging.
func clampTime(ctx context.Context, t model.Time, limit time.Duration, clamp model.Time, before bool, kind, name string, logger log.Logger) model.Time {
	if limit > 0 && ((before && t.Before(clamp)) || (!before && t.After(clamp))) {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log(
			"msg", "the "+kind+" time of the query has been manipulated because of the '"+name+"' setting",
			"original", util.FormatTimeModel(t),
			"updated", util.FormatTimeModel(clamp))
		t = clamp
	}
	return t
}
