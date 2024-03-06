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
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/querier/engine"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Config contains the configuration require to create a querier
type Config struct {
	// QueryStoreAfter the time after which queries should also be sent to the store and not just ingesters.
	QueryStoreAfter time.Duration `yaml:"query_store_after" category:"advanced"`
	// Deprecated in Mimir 2.12, remove in Mimir 2.14
	MaxQueryIntoFuture time.Duration `yaml:"max_query_into_future" category:"deprecated"`

	StoreGatewayClient ClientConfig `yaml:"store_gateway_client"`

	ShuffleShardingIngestersEnabled bool `yaml:"shuffle_sharding_ingesters_enabled" category:"advanced"`

	PreferStreamingChunksFromIngesters             bool          `yaml:"prefer_streaming_chunks_from_ingesters" category:"experimental"` // Enabled by default as of Mimir 2.11, remove altogether in 2.12.
	PreferStreamingChunksFromStoreGateways         bool          `yaml:"prefer_streaming_chunks_from_store_gateways" category:"experimental"`
	PreferAvailabilityZone                         string        `yaml:"prefer_availability_zone" category:"experimental" doc:"hidden"`
	StreamingChunksPerIngesterSeriesBufferSize     uint64        `yaml:"streaming_chunks_per_ingester_series_buffer_size" category:"advanced"`
	StreamingChunksPerStoreGatewaySeriesBufferSize uint64        `yaml:"streaming_chunks_per_store_gateway_series_buffer_size" category:"experimental"`
	MinimizeIngesterRequests                       bool          `yaml:"minimize_ingester_requests" category:"experimental"` // Enabled by default as of Mimir 2.11, remove altogether in 2.12.
	MinimiseIngesterRequestsHedgingDelay           time.Duration `yaml:"minimize_ingester_requests_hedging_delay" category:"advanced"`

	// PromQL engine config.
	EngineConfig engine.Config `yaml:",inline"`
}

const (
	queryStoreAfterFlag = "querier.query-store-after"
)

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.StoreGatewayClient.RegisterFlagsWithPrefix("querier.store-gateway-client", f)

	f.DurationVar(&cfg.MaxQueryIntoFuture, "querier.max-query-into-future", 10*time.Minute, "Maximum duration into the future you can query. 0 to disable.")
	f.DurationVar(&cfg.QueryStoreAfter, queryStoreAfterFlag, 12*time.Hour, "The time after which a metric should be queried from storage and not just ingesters. 0 means all queries are sent to store. If this option is enabled, the time range of the query sent to the store-gateway will be manipulated to ensure the query end is not more recent than 'now - query-store-after'.")
	f.BoolVar(&cfg.ShuffleShardingIngestersEnabled, "querier.shuffle-sharding-ingesters-enabled", true, fmt.Sprintf("Fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since -%s. If this setting is false or -%s is '0', queriers always query all ingesters (ingesters shuffle sharding on read path is disabled).", validation.QueryIngestersWithinFlag, validation.QueryIngestersWithinFlag))
	f.BoolVar(&cfg.PreferStreamingChunksFromIngesters, "querier.prefer-streaming-chunks-from-ingesters", true, "Request ingesters stream chunks. Ingesters will only respond with a stream of chunks if the target ingester supports this, and this preference will be ignored by ingesters that do not support this.")
	f.BoolVar(&cfg.PreferStreamingChunksFromStoreGateways, "querier.prefer-streaming-chunks-from-store-gateways", false, "Request store-gateways stream chunks. Store-gateways will only respond with a stream of chunks if the target store-gateway supports this, and this preference will be ignored by store-gateways that do not support this.")
	f.StringVar(&cfg.PreferAvailabilityZone, "querier.prefer-availability-zone", "", "Preferred availability zone to query ingesters from when using the ingest storage.")

	const minimiseIngesterRequestsFlagName = "querier.minimize-ingester-requests"
	f.BoolVar(&cfg.MinimizeIngesterRequests, minimiseIngesterRequestsFlagName, true, "If true, when querying ingesters, only the minimum required ingesters required to reach quorum will be queried initially, with other ingesters queried only if needed due to failures from the initial set of ingesters. Enabling this option reduces resource consumption for the happy path at the cost of increased latency for the unhappy path.")
	f.DurationVar(&cfg.MinimiseIngesterRequestsHedgingDelay, minimiseIngesterRequestsFlagName+"-hedging-delay", 3*time.Second, "Delay before initiating requests to further ingesters when request minimization is enabled and the initially selected set of ingesters have not all responded. Ignored if -"+minimiseIngesterRequestsFlagName+" is not enabled.")

	// Why 256 series / ingester/store-gateway?
	// Based on our testing, 256 series / ingester was a good balance between memory consumption and the CPU overhead of managing a batch of series.
	f.Uint64Var(&cfg.StreamingChunksPerIngesterSeriesBufferSize, "querier.streaming-chunks-per-ingester-buffer-size", 256, "Number of series to buffer per ingester when streaming chunks from ingesters.")
	f.Uint64Var(&cfg.StreamingChunksPerStoreGatewaySeriesBufferSize, "querier.streaming-chunks-per-store-gateway-buffer-size", 256, "Number of series to buffer per store-gateway when streaming chunks from store-gateways.")

	cfg.EngineConfig.RegisterFlags(f)
}

func (cfg *Config) Validate() error {
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
func New(cfg Config, limits *validation.Overrides, distributor Distributor, storeQueryable storage.Queryable, reg prometheus.Registerer, logger log.Logger, tracker *activitytracker.ActivityTracker) (storage.SampleAndChunkQueryable, storage.ExemplarQueryable, *promql.Engine) {
	queryMetrics := stats.NewQueryMetrics(reg)

	distributorQueryable := newDistributorQueryable(distributor, limits, queryMetrics, logger)

	queryable := newQueryable(distributorQueryable, storeQueryable, cfg, limits, queryMetrics, logger)
	exemplarQueryable := newDistributorExemplarQueryable(distributor, logger)

	lazyQueryable := storage.QueryableFunc(func(minT int64, maxT int64) (storage.Querier, error) {
		querier, err := queryable.Querier(minT, maxT)
		if err != nil {
			return nil, err
		}
		return lazyquery.NewLazyQuerier(querier), nil
	})

	engineOpts, engineExperimentalFunctionsEnabled := engine.NewPromQLEngineOptions(cfg.EngineConfig, tracker, logger, reg)
	engine := promql.NewEngine(engineOpts)

	// Experimental functions can only be enabled globally, and not on a per-engine basis.
	parser.EnableExperimentalFunctions = engineExperimentalFunctionsEnabled

	return NewSampleAndChunkQueryable(lazyQueryable), exemplarQueryable, engine
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
	distributor storage.Queryable,
	blockStore storage.Queryable,
	cfg Config,
	limits *validation.Overrides,
	queryMetrics *stats.QueryMetrics,
	logger log.Logger,
) storage.Queryable {
	return storage.QueryableFunc(func(minT, maxT int64) (storage.Querier, error) {
		return multiQuerier{
			distributor:        distributor,
			blockStore:         blockStore,
			queryMetrics:       queryMetrics,
			cfg:                cfg,
			minT:               minT,
			maxT:               maxT,
			maxQueryIntoFuture: cfg.MaxQueryIntoFuture,
			limits:             limits,
			logger:             logger,
		}, nil

	})
}

// multiQuerier implements storage.Querier, orchestrating requests across a set of queriers.
type multiQuerier struct {
	distributor  storage.Queryable
	blockStore   storage.Queryable
	queryMetrics *stats.QueryMetrics
	cfg          Config
	minT, maxT   int64

	maxQueryIntoFuture time.Duration
	limits             *validation.Overrides

	logger log.Logger
}

func (mq multiQuerier) getQueriers(ctx context.Context) (context.Context, []storage.Querier, error) {
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

	mq.minT, mq.maxT, err = validateQueryTimeRange(tenantID, mq.minT, mq.maxT, now.UnixMilli(), mq.limits, mq.cfg.MaxQueryIntoFuture, spanlogger.FromContext(ctx, mq.logger))
	if err != nil {
		return nil, nil, err
	}

	var queriers []storage.Querier
	// distributor or blockStore queryables passed into newQueryable should only be nil in tests;
	// the decision of whether to construct the ingesters or block store queryables
	// should be made here, not by the caller of newQueryable
	//
	// queriers may further apply stricter internal logic and decide no-op for a given query

	if mq.distributor != nil && ShouldQueryIngesters(mq.limits.QueryIngestersWithin(tenantID), now, mq.maxT) {
		q, err := mq.distributor.Querier(mq.minT, mq.maxT)
		if err != nil {
			return nil, nil, err
		}
		queriers = append(queriers, q)
		mq.queryMetrics.QueriesExecutedTotal.WithLabelValues("ingester").Inc()
	}

	if mq.blockStore != nil && ShouldQueryBlockStore(mq.cfg.QueryStoreAfter, now, mq.minT) {
		q, err := mq.blockStore.Querier(mq.minT, mq.maxT)
		if err != nil {
			return nil, nil, err
		}
		queriers = append(queriers, q)
		mq.queryMetrics.QueriesExecutedTotal.WithLabelValues("store-gateway").Inc()
	}

	return ctx, queriers, nil
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (mq multiQuerier) Select(ctx context.Context, _ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, mq.logger, "querier.Select")
	defer spanLog.Span.Finish()

	ctx, queriers, err := mq.getQueriers(ctx)
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
	startMs, endMs, err := validateQueryTimeRange(userID, sp.Start, sp.End, now.UnixMilli(), mq.limits, mq.maxQueryIntoFuture, spanLog)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.NoopSeriesSet()
	} else if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if sp.Func == "series" { // Clamp max time range for series-only queries, before we check max length.
		maxQueryLength := mq.limits.MaxLabelsQueryLength(userID)
		startMs = clampMinTime(spanLog, startMs, endMs, -maxQueryLength, "max label query length")
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

// LabelValues implements storage.Querier.
func (mq multiQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, queriers, err := mq.getQueriers(ctx)
	if errors.Is(err, errEmptyTimeRange) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if len(queriers) == 1 {
		return queriers[0].LabelValues(ctx, name, matchers...)
	}

	var (
		g, _     = errgroup.WithContext(ctx)
		sets     = [][]string{}
		warnings annotations.Annotations

		resMtx sync.Mutex
	)

	for _, querier := range queriers {
		// Need to reassign as the original variable will change and can't be relied on in a goroutine.
		querier := querier
		g.Go(func() error {
			// NB: Values are sorted in Mimir already.
			myValues, myWarnings, err := querier.LabelValues(ctx, name, matchers...)
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

func (mq multiQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx, queriers, err := mq.getQueriers(ctx)
	if errors.Is(err, errEmptyTimeRange) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if len(queriers) == 1 {
		return queriers[0].LabelNames(ctx, matchers...)
	}

	var (
		g, _     = errgroup.WithContext(ctx)
		sets     = [][]string{}
		warnings annotations.Annotations

		resMtx sync.Mutex
	)

	for _, querier := range queriers {
		// Need to reassign as the original variable will change and can't be relied on in a goroutine.
		querier := querier
		g.Go(func() error {
			// NB: Names are sorted in Mimir already.
			myNames, myWarnings, err := querier.LabelNames(ctx, matchers...)
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
		return storage.NewMergeSeriesSet(otherSets, storage.ChainedSeriesMerge)
	}

	// partitionChunks returns set with sorted series, so it can be used by NewMergeSeriesSet
	chunksSet := partitionChunks(chunks, mq.minT, mq.maxT)

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

func (s *sliceSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func validateQueryTimeRange(userID string, startMs, endMs, now int64, limits *validation.Overrides, maxQueryIntoFuture time.Duration, spanLog *spanlogger.SpanLogger) (int64, int64, error) {
	endMs = clampMaxTime(spanLog, endMs, now, maxQueryIntoFuture, "max query into future")

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
	clampedT := math.Min(maxT, refT+limitDelta.Milliseconds())

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
	clampedT := math.Max(minT, refT+limitDelta.Milliseconds())

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
