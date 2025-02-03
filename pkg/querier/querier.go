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
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/querier/engine"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Config contains the configuration require to create a querier
type Config struct {
	// QueryStoreAfter the time after which queries should also be sent to the store and not just ingesters.
	QueryStoreAfter time.Duration `yaml:"query_store_after" category:"advanced"`

	StoreGatewayClient ClientConfig `yaml:"store_gateway_client"`

	ShuffleShardingIngestersEnabled bool `yaml:"shuffle_sharding_ingesters_enabled" category:"advanced"`

	PreferAvailabilityZone                         string        `yaml:"prefer_availability_zone" category:"experimental" doc:"hidden"`
	StreamingChunksPerIngesterSeriesBufferSize     uint64        `yaml:"streaming_chunks_per_ingester_series_buffer_size" category:"advanced"`
	StreamingChunksPerStoreGatewaySeriesBufferSize uint64        `yaml:"streaming_chunks_per_store_gateway_series_buffer_size" category:"advanced"`
	MinimizeIngesterRequests                       bool          `yaml:"minimize_ingester_requests" category:"advanced"`
	MinimiseIngesterRequestsHedgingDelay           time.Duration `yaml:"minimize_ingester_requests_hedging_delay" category:"advanced"`

	QueryEngine               string `yaml:"query_engine" category:"experimental"`
	EnableQueryEngineFallback bool   `yaml:"enable_query_engine_fallback" category:"experimental"`

	FilterQueryablesEnabled bool `yaml:"filter_queryables_enabled" category:"advanced"`

	// PromQL engine config.
	EngineConfig engine.Config `yaml:",inline"`
}

const (
	queryStoreAfterFlag = "querier.query-store-after"
	prometheusEngine    = "prometheus"
	mimirEngine         = "mimir"
)

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.StoreGatewayClient.RegisterFlagsWithPrefix("querier.store-gateway-client", f)

	f.DurationVar(&cfg.QueryStoreAfter, queryStoreAfterFlag, 12*time.Hour, "The time after which a metric should be queried from storage and not just ingesters. 0 means all queries are sent to store. If this option is enabled, the time range of the query sent to the store-gateway will be manipulated to ensure the query end is not more recent than 'now - query-store-after'.")
	f.BoolVar(&cfg.ShuffleShardingIngestersEnabled, "querier.shuffle-sharding-ingesters-enabled", true, fmt.Sprintf("Fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since -%s. If this setting is false or -%s is '0', queriers always query all ingesters (ingesters shuffle sharding on read path is disabled).", validation.QueryIngestersWithinFlag, validation.QueryIngestersWithinFlag))
	f.StringVar(&cfg.PreferAvailabilityZone, "querier.prefer-availability-zone", "", "Preferred availability zone to query ingesters from when using the ingest storage.")

	const minimiseIngesterRequestsFlagName = "querier.minimize-ingester-requests"
	f.BoolVar(&cfg.MinimizeIngesterRequests, minimiseIngesterRequestsFlagName, true, "If true, when querying ingesters, only the minimum required ingesters required to reach quorum will be queried initially, with other ingesters queried only if needed due to failures from the initial set of ingesters. Enabling this option reduces resource consumption for the happy path at the cost of increased latency for the unhappy path.")
	f.DurationVar(&cfg.MinimiseIngesterRequestsHedgingDelay, minimiseIngesterRequestsFlagName+"-hedging-delay", 3*time.Second, "Delay before initiating requests to further ingesters when request minimization is enabled and the initially selected set of ingesters have not all responded. Ignored if -"+minimiseIngesterRequestsFlagName+" is not enabled.")

	// Why 256 series / ingester/store-gateway?
	// Based on our testing, 256 series / ingester was a good balance between memory consumption and the CPU overhead of managing a batch of series.
	f.Uint64Var(&cfg.StreamingChunksPerIngesterSeriesBufferSize, "querier.streaming-chunks-per-ingester-buffer-size", 256, "Number of series to buffer per ingester when streaming chunks from ingesters.")
	f.Uint64Var(&cfg.StreamingChunksPerStoreGatewaySeriesBufferSize, "querier.streaming-chunks-per-store-gateway-buffer-size", 256, "Number of series to buffer per store-gateway when streaming chunks from store-gateways.")

	f.StringVar(&cfg.QueryEngine, "querier.query-engine", prometheusEngine, fmt.Sprintf("Query engine to use, either '%v' or '%v'", prometheusEngine, mimirEngine))
	f.BoolVar(&cfg.EnableQueryEngineFallback, "querier.enable-query-engine-fallback", true, "If set to true and the Mimir query engine is in use, fall back to using the Prometheus query engine for any queries not supported by the Mimir query engine.")

	f.BoolVar(&cfg.FilterQueryablesEnabled, "querier.filter-queryables-enabled", false, "If set to true, the header 'X-Filter-Queryables' can be used to filter down the list of queryables that shall be used. This is useful to test and monitor single queryables in isolation.")

	cfg.EngineConfig.RegisterFlags(f)
}

func (cfg *Config) Validate() error {
	if cfg.QueryEngine != prometheusEngine && cfg.QueryEngine != mimirEngine {
		return fmt.Errorf("unknown PromQL engine '%s'", cfg.QueryEngine)
	}

	return nil
}

func (cfg *Config) ValidateLimits(limits validation.Limits) error {
	// Ensure the config wont create a situation where no queriers are returned.
	if limits.QueryIngestersWithin != 0 && cfg.QueryStoreAfter != 0 {
		if cfg.QueryStoreAfter >= time.Duration(limits.QueryIngestersWithin) {
			return errBadLookbackConfigs
		}
	}

	return nil
}

// ShouldQueryIngesters provides a check for whether the ingesters will be used for a given query.
func ShouldQueryIngesters(queryIngestersWithin time.Duration, now time.Time, queryMaxT int64) bool {
	if queryIngestersWithin != 0 {
		queryIngestersMinT := util.TimeToMillis(now.Add(-queryIngestersWithin))
		if queryIngestersMinT >= queryMaxT {
			return false
		}
	}
	return true
}

// ShouldQueryBlockStore provides a check for whether the block store will be used for a given query.
func ShouldQueryBlockStore(queryStoreAfter time.Duration, now time.Time, queryMinT int64) bool {
	if queryStoreAfter != 0 {
		queryStoreMaxT := util.TimeToMillis(now.Add(-queryStoreAfter))
		if queryMinT >= queryStoreMaxT {
			return false
		}
	}
	return true
}

// New builds a queryable and promql engine.
func New(cfg Config, limits *validation.Overrides, distributor Distributor, queryables []TimeRangeQueryable, reg prometheus.Registerer, logger log.Logger, tracker *activitytracker.ActivityTracker) (storage.SampleAndChunkQueryable, storage.ExemplarQueryable, promql.QueryEngine, error) {
	queryMetrics := stats.NewQueryMetrics(reg)

	queryables = append(queryables, TimeRangeQueryable{
		Queryable:   NewDistributorQueryable(distributor, limits, queryMetrics, logger),
		StorageName: "ingester",
		IsApplicable: func(_ context.Context, tenantID string, now time.Time, _, queryMaxT int64, _ log.Logger, _ ...*labels.Matcher) bool {
			return ShouldQueryIngesters(limits.QueryIngestersWithin(tenantID), now, queryMaxT)
		},
	})

	queryable := newQueryable(queryables, cfg, limits, queryMetrics, logger)
	exemplarQueryable := newDistributorExemplarQueryable(distributor, logger)

	lazyQueryable := storage.QueryableFunc(func(minT int64, maxT int64) (storage.Querier, error) {
		querier, err := queryable.Querier(minT, maxT)
		if err != nil {
			return nil, err
		}
		return lazyquery.NewLazyQuerier(querier), nil
	})

	opts, mqeOpts, engineExperimentalFunctionsEnabled := engine.NewPromQLEngineOptions(cfg.EngineConfig, tracker, logger, reg)

	// Experimental functions can only be enabled globally, and not on a per-engine basis.
	parser.EnableExperimentalFunctions = engineExperimentalFunctionsEnabled

	var eng promql.QueryEngine

	switch cfg.QueryEngine {
	case prometheusEngine:
		eng = promql.NewEngine(opts)
	case mimirEngine:
		limitsProvider := &tenantQueryLimitsProvider{limits: limits}
		streamingEngine, err := streamingpromql.NewEngine(mqeOpts, limitsProvider, queryMetrics, logger)
		if err != nil {
			return nil, nil, nil, err
		}

		if cfg.EnableQueryEngineFallback {
			prometheusEngine := promql.NewEngine(opts)
			eng = compat.NewEngineWithFallback(streamingEngine, prometheusEngine, reg, logger)
		} else {
			eng = streamingEngine
		}
	default:
		panic(fmt.Sprintf("invalid config not caught by validation: unknown PromQL engine '%s'", cfg.QueryEngine))
	}

	return NewSampleAndChunkQueryable(lazyQueryable), exemplarQueryable, eng, nil
}

// NewSampleAndChunkQueryable creates a SampleAndChunkQueryable from a Queryable.
func NewSampleAndChunkQueryable(q storage.Queryable) storage.SampleAndChunkQueryable {
	return &sampleAndChunkQueryable{q}
}

type sampleAndChunkQueryable struct {
	storage.Queryable
}

func (q *sampleAndChunkQueryable) ChunkQuerier(minT, maxT int64) (storage.ChunkQuerier, error) {
	qr, err := q.Queryable.Querier(minT, maxT)
	if err != nil {
		return nil, err
	}
	return &chunkQuerier{qr}, nil
}

type chunkQuerier struct {
	storage.Querier
}

func (q *chunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return storage.NewSeriesSetToChunkSet(q.Querier.Select(ctx, sortSeries, hints, matchers...))
}

// newQueryable creates a new Queryable for Mimir.
func newQueryable(
	queryables []TimeRangeQueryable,
	cfg Config,
	limits *validation.Overrides,
	queryMetrics *stats.QueryMetrics,
	logger log.Logger,
) storage.Queryable {
	return storage.QueryableFunc(func(minT, maxT int64) (storage.Querier, error) {
		return multiQuerier{
			queryables:   queryables,
			queryMetrics: queryMetrics,
			cfg:          cfg,
			minT:         minT,
			maxT:         maxT,
			limits:       limits,
			logger:       logger,
		}, nil

	})
}

// TimeRangeQueryable is a Queryable that is aware of when it is applicable.
type TimeRangeQueryable struct {
	storage.Queryable
	IsApplicable func(ctx context.Context, tenantID string, now time.Time, queryMinT, queryMaxT int64, logger log.Logger, matchers ...*labels.Matcher) bool
	StorageName  string
}

func NewStoreGatewayTimeRangeQueryable(q storage.Queryable, querierConfig Config) TimeRangeQueryable {
	return TimeRangeQueryable{
		Queryable:   q,
		StorageName: "store-gateway",
		IsApplicable: func(_ context.Context, _ string, now time.Time, queryMinT, _ int64, _ log.Logger, _ ...*labels.Matcher) bool {
			return ShouldQueryBlockStore(querierConfig.QueryStoreAfter, now, queryMinT)
		},
	}
}

// multiQuerier implements storage.Querier, orchestrating requests across a set of queriers.
type multiQuerier struct {
	queryables   []TimeRangeQueryable
	queryMetrics *stats.QueryMetrics
	cfg          Config
	minT, maxT   int64

	limits *validation.Overrides

	logger log.Logger
}

func (mq multiQuerier) getQueriers(ctx context.Context, matchers ...*labels.Matcher) (context.Context, []storage.Querier, error) {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, mq.logger, "multiQuerier.getQueriers")
	defer spanLog.Span.Finish()

	now := time.Now()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, nil, err
	}

	ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(
		mq.limits.MaxFetchedSeriesPerQuery(tenantID),
		mq.limits.MaxFetchedChunkBytesPerQuery(tenantID),
		mq.limits.MaxChunksPerQuery(tenantID),
		mq.limits.MaxEstimatedChunksPerQuery(tenantID),
		mq.queryMetrics,
	))

	mq.minT, mq.maxT, err = validateQueryTimeRange(tenantID, mq.minT, mq.maxT, now.UnixMilli(), mq.limits, spanlogger.FromContext(ctx, mq.logger))
	if err != nil {
		return nil, nil, err
	}

	var queriers []storage.Querier
	useQueryables, filterUsedQueryables := getFilterQueryablesFromContext(ctx)
	for _, queryable := range mq.queryables {
		if filterUsedQueryables {
			if !useQueryables.use(queryable.StorageName) {
				level.Debug(spanLog).Log("queryable_name", queryable.StorageName, "use_queryable", false)
				// Skip this queryable if it's not in the list of queryables to use.
				continue
			}
		}

		isApplicable := queryable.IsApplicable(ctx, tenantID, now, mq.minT, mq.maxT, mq.logger, matchers...)
		level.Debug(spanLog).Log("queryable_name", queryable.StorageName, "use_queryable", true, "is_applicable", isApplicable)
		if isApplicable {
			q, err := queryable.Querier(mq.minT, mq.maxT)
			if err != nil {
				return nil, nil, err
			}

			queriers = append(queriers, q)
			mq.queryMetrics.QueriesExecutedTotal.WithLabelValues(queryable.StorageName).Inc()
		}
	}

	return ctx, queriers, nil
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (mq multiQuerier) Select(ctx context.Context, _ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, mq.logger, "querier.Select")
	defer spanLog.Span.Finish()

	ctx, queriers, err := mq.getQueriers(ctx, matchers...)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.EmptySeriesSet()
	}
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if sp == nil {
		sp = &storage.SelectHints{
			Start: mq.minT,
			End:   mq.maxT,
		}
	} else {
		// Make a copy, to avoid changing shared SelectHints.
		scp := *sp
		sp = &scp
	}
	now := time.Now()

	level.Debug(spanLog).Log("hint.func", sp.Func, "start", util.TimeFromMillis(sp.Start).UTC().String(), "end",
		util.TimeFromMillis(sp.End).UTC().String(), "step", sp.Step, "matchers", util.MatchersStringer(matchers))

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Validate query time range. Even if the time range has already been validated when we created
	// the querier, we need to check it again here because the time range specified in hints may be
	// different.
	startMs, endMs, err := validateQueryTimeRange(userID, sp.Start, sp.End, now.UnixMilli(), mq.limits, spanLog)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.NoopSeriesSet()
	} else if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if sp.Func == "series" { // Clamp max time range for series-only queries, before we check max length.
		startMs = clampToMaxLabelQueryLength(spanLog, startMs, endMs, now.UnixMilli(), mq.limits.MaxLabelsQueryLength(userID).Milliseconds())
	}

	// The time range may have been manipulated during the validation,
	// so we make sure changes are reflected back to hints.
	sp.Start = startMs
	sp.End = endMs

	startTime := model.Time(startMs)
	endTime := model.Time(endMs)

	// Validate query time range.
	if maxQueryLength := mq.limits.MaxPartialQueryLength(userID); maxQueryLength > 0 && endTime.Sub(startTime) > maxQueryLength {
		return storage.ErrSeriesSet(NewMaxQueryLengthError(endTime.Sub(startTime), maxQueryLength))
	}

	if len(queriers) == 1 {
		return queriers[0].Select(ctx, true, sp, matchers...)
	}

	sets := make(chan storage.SeriesSet, len(queriers))
	for _, querier := range queriers {
		go func(querier storage.Querier) {
			sets <- querier.Select(ctx, true, sp, matchers...)
		}(querier)
	}

	var result []storage.SeriesSet
	for range queriers {
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
	return mq.mergeSeriesSets(result)
}

func clampToMaxLabelQueryLength(spanLog *spanlogger.SpanLogger, startMs, endMs, nowMs, maxLabelQueryLengthMs int64) int64 {
	if maxLabelQueryLengthMs == 0 {
		// It's unlimited.
		return startMs
	}
	unsetStartTime := startMs == v1.MinTime.UnixMilli()
	unsetEndTime := endMs == v1.MaxTime.UnixMilli()

	switch {
	case unsetStartTime && unsetEndTime:
		// The user asked for "everything", but that's too expensive.
		// We clamp the start, since the past likely has more data.
		// Allow querying into the future because that will likely have much less data.
		// Leaving end unchanged also allows to query the future for samples with timestamps in the future.
		earliestAllowedStart := nowMs - maxLabelQueryLengthMs
		logClampEvent(spanLog, startMs, earliestAllowedStart, "min", "max label query length")
		startMs = earliestAllowedStart
	case unsetStartTime:
		// We can't provide all data since the beginning of time.
		// But end was provided, so we use the end as the anchor.
		earliestAllowedStart := endMs - maxLabelQueryLengthMs
		logClampEvent(spanLog, startMs, earliestAllowedStart, "min", "max label query length")
		startMs = earliestAllowedStart
	case unsetEndTime:
		// Start was provided, but not end.
		// We clamp the start relative to now so that we don't query a lot of data.
		if earliestAllowedStart := nowMs - maxLabelQueryLengthMs; earliestAllowedStart > startMs {
			logClampEvent(spanLog, startMs, earliestAllowedStart, "min", "max label query length")
			startMs = earliestAllowedStart
		}
	default:
		// Both start and end were provided. We clamp the start.
		// There's no strong reason to do this vs clamping end.
		if earliestAllowedStart := endMs - maxLabelQueryLengthMs; earliestAllowedStart > startMs {
			logClampEvent(spanLog, startMs, earliestAllowedStart, "min", "max label query length")
			startMs = earliestAllowedStart
		}
	}
	return startMs
}

// LabelValues implements storage.Querier.
func (mq multiQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, queriers, err := mq.getQueriers(ctx, matchers...)
	if errors.Is(err, errEmptyTimeRange) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if len(queriers) == 1 {
		return queriers[0].LabelValues(ctx, name, hints, matchers...)
	}

	var (
		g, _     = errgroup.WithContext(ctx)
		sets     = [][]string{}
		warnings annotations.Annotations

		resMtx sync.Mutex
	)

	for _, querier := range queriers {
		g.Go(func() error {
			// NB: Values are sorted in Mimir already.
			myValues, myWarnings, err := querier.LabelValues(ctx, name, hints, matchers...)
			if err != nil {
				return err
			}

			resMtx.Lock()
			sets = append(sets, myValues)
			warnings.Merge(myWarnings)
			resMtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(sets...), warnings, nil
}

func (mq multiQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, queriers, err := mq.getQueriers(ctx, matchers...)
	if errors.Is(err, errEmptyTimeRange) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if len(queriers) == 1 {
		return queriers[0].LabelNames(ctx, hints, matchers...)
	}

	var (
		g, _     = errgroup.WithContext(ctx)
		sets     = [][]string{}
		warnings annotations.Annotations

		resMtx sync.Mutex
	)

	for _, querier := range queriers {
		g.Go(func() error {
			// NB: Names are sorted in Mimir already.
			myNames, myWarnings, err := querier.LabelNames(ctx, hints, matchers...)
			if err != nil {
				return err
			}

			resMtx.Lock()
			sets = append(sets, myNames)
			warnings.Merge(myWarnings)
			resMtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(sets...), warnings, nil
}

func (multiQuerier) Close() error {
	return nil
}

func (mq multiQuerier) mergeSeriesSets(sets []storage.SeriesSet) storage.SeriesSet {
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
		return storage.NewMergeSeriesSet(otherSets, 0, storage.ChainedSeriesMerge)
	}

	// partitionChunks returns set with sorted series, so it can be used by NewMergeSeriesSet
	chunksSet := partitionChunks(chunks)

	if len(otherSets) == 0 {
		return chunksSet
	}

	otherSets = append(otherSets, chunksSet)
	return storage.NewMergeSeriesSet(otherSets, 0, storage.ChainedSeriesMerge)
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

func (s *sliceSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func validateQueryTimeRange(userID string, startMs, endMs, now int64, limits *validation.Overrides, spanLog *spanlogger.SpanLogger) (int64, int64, error) {
	maxQueryLookback := limits.MaxQueryLookback(userID)
	startMs = clampMinTime(spanLog, startMs, now, -maxQueryLookback, "max query lookback")

	if endMs < startMs {
		return 0, 0, errEmptyTimeRange
	}

	return startMs, endMs, nil
}

// clampMaxTime allows for time-limiting query minT and maxT based on configured limits.
//
// maxT is the original timestamp to be clamped based on other supplied parameters.
//
// refT is the reference time from which to apply limitDelta; generally now() for clamping maxT
//
// limitDelta should be negative if the limit is looking back in time from the reference time;
// Ex:
//   - query-store-after: refT is now(), limitDelta is negative. maxT is now or in the past,
//     and may need to be clamped further into the past
//   - max-query-into-future: refT is now(), limitDelta is positive. maxT is now or in the future,
//     and may need to be clamped to be less far into the future
func clampMaxTime(spanLog *spanlogger.SpanLogger, maxT int64, refT int64, limitDelta time.Duration, limitName string) int64 {
	if limitDelta == 0 {
		// limits equal to 0 are considered to not be enabled
		return maxT
	}
	clampedT := min(maxT, refT+limitDelta.Milliseconds())

	if clampedT != maxT {
		logClampEvent(spanLog, maxT, clampedT, "max", limitName)
	}

	return clampedT
}

// clampMinTime allows for time-limiting query minT based on configured limits.
//
// minT is the original timestamp to be clamped based on other supplied parameters.
//
// refT is the reference time from which to apply limitDelta; generally now() or query maxT because
// when we truncate on the basis of overall query length, we leave maxT as-is and bring minT forward.
//
// limitDelta should be negative for all existing use cases for clamping minT,
// as we look backwards from the reference time to apply the limit.
func clampMinTime(spanLog *spanlogger.SpanLogger, minT int64, refT int64, limitDelta time.Duration, limitName string) int64 {
	if limitDelta == 0 {
		// limits equal to 0 are considered to not be enabled
		return minT
	}
	clampedT := max(minT, refT+limitDelta.Milliseconds())

	if clampedT != minT {
		logClampEvent(spanLog, minT, clampedT, "min", limitName)
	}

	return clampedT
}

func logClampEvent(spanLog *spanlogger.SpanLogger, originalT, clampedT int64, minOrMax, settingName string) {
	msg := fmt.Sprintf(
		"the %s time of the query has been manipulated because of the '%s' setting",
		minOrMax, settingName,
	)
	spanLog.DebugLog(
		"msg", msg,
		"original", util.TimeFromMillis(originalT).String(),
		"updated", util.TimeFromMillis(clampedT).String(),
	)
}

type tenantQueryLimitsProvider struct {
	limits *validation.Overrides
}

func (p *tenantQueryLimitsProvider) GetMaxEstimatedMemoryConsumptionPerQuery(ctx context.Context) (uint64, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return 0, err
	}

	totalLimit := uint64(0)

	for _, tenantID := range tenantIDs {
		tenantLimit := p.limits.MaxEstimatedMemoryConsumptionPerQuery(tenantID)

		if tenantLimit == 0 {
			// If any tenant is unlimited, then treat whole query as unlimited.
			return 0, nil
		}

		// Given we'll enforce limits like the max chunks limit on a per-tenant basis (and therefore effectively allow the
		// query to consume the sum of all tenants' limits), emulate equivalent behaviour with the memory consumption limit.
		totalLimit += tenantLimit
	}

	return totalLimit, nil
}
