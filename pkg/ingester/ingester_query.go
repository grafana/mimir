// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// StartReadRequest implements ingesterReceiver and is called by a gRPC tap Handle when a request is first received to
// determine if a request should be permitted. When permitted, StartReadRequest returns a context with a function that
// should be called to finish the started read request once the request is completed. If it wasn't successful, the
// causing error is returned and a nil context is returned.
func (i *Ingester) StartReadRequest(ctx context.Context) (resultCtx context.Context, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	if err := i.checkAvailableForRead(); err != nil {
		return nil, err
	}

	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	// When this is not the only replica handling a request, limit reads for overloads
	if !client.IsOnlyReplicaContext(ctx) {
		// Check that a permit can be later acquired
		if i.reactiveLimiter.read != nil && !i.reactiveLimiter.read.CanAcquirePermit() {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightReadRequests).Inc()
			return nil, newReactiveLimiterExceededError(adaptivelimiter.ErrExceeded)
		}
		finish, err := i.circuitBreaker.tryAcquireReadPermit()
		if err != nil {
			return nil, err
		}
		start := time.Now()
		ctx = context.WithValue(ctx, readReqCtxKey, &readRequestState{
			requestFinish: func(err error) {
				finish(time.Since(start), err)
			},
		})
	}

	return ctx, nil
}

// PrepareReadRequest implements ingesterReceiver and is called by a gRPC interceptor when a request is in progress.
// When successful, PrepareReadRequest returns a function that should be called once the request is completed.
func (i *Ingester) PrepareReadRequest(ctx context.Context) (finishFn func(error), err error) {
	cbFinish := func(error) {}
	if st := getReadRequestState(ctx); st != nil {
		cbFinish = st.requestFinish
	}

	// When this is not the only replica handling a request, limit reads for overloads
	if !client.IsOnlyReplicaContext(ctx) && i.reactiveLimiter.read != nil {
		// Acquire a permit, blocking if needed
		permit, err := i.reactiveLimiter.read.AcquirePermit(ctx)
		if err != nil {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightReadRequests).Inc()
			return nil, newReactiveLimiterExceededError(err)
		}
		return func(err error) {
			cbFinish(err)
			if errors.Is(err, context.Canceled) {
				permit.Drop()
			} else {
				permit.Record()
			}
		}, nil
	}

	return cbFinish, nil
}

func (i *Ingester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (resp *client.ExemplarQueryResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(ctx, i.logger, tracer, "Ingester.QueryExemplars")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromExemplarQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.ExemplarQueryResponse{}, nil
	}

	q, err := db.ExemplarQuerier(ctx)
	if err != nil {
		return nil, err
	}

	// It's not required to sort series from a single ingester because series are sorted by the Exemplar Storage before returning from Select.
	res, err := q.Select(from, through, matchers...)
	if err != nil {
		return nil, err
	}

	numExemplars := 0

	result := &client.ExemplarQueryResponse{}
	for _, es := range res {
		ts := mimirpb.TimeSeries{
			Labels:    mimirpb.FromLabelsToLabelAdapters(es.SeriesLabels),
			Exemplars: mimirpb.FromExemplarsToExemplarProtos(es.Exemplars),
		}

		numExemplars += len(ts.Exemplars)
		result.Timeseries = append(result.Timeseries, ts)
	}

	i.metrics.queriedExemplars.Observe(float64(numExemplars))

	return result, nil
}

func (i *Ingester) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (resp *client.LabelValuesResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	labelName, startTimestampMs, endTimestampMs, hints, matchers, err := client.FromLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelValuesResponse{}, nil
	}

	q, err := db.Querier(startTimestampMs, endTimestampMs)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, _, err := q.LabelValues(ctx, labelName, hints, matchers...)
	if err != nil {
		return nil, err
	}

	// Besides we are passing the hints to q.LabelValues, we also limit the number of returned values here
	// because LabelQuerier can resolve the labelValues using different instance and then joining the results,
	// so we want to apply the limit at the end.
	if hints != nil && hints.Limit > 0 && len(vals) > hints.Limit {
		vals = vals[:hints.Limit]
	}

	// The label value strings are sometimes pointing to memory mapped file
	// regions that may become unmapped anytime after Querier.Close is called.
	// So we copy those strings.
	for i, s := range vals {
		vals[i] = strings.Clone(s)
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, nil
}

func (i *Ingester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (resp *client.LabelNamesResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	spanlog, ctx := spanlogger.New(ctx, i.logger, tracer, "Ingester.LabelNames")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelNamesResponse{}, nil
	}

	mint, maxt, hints, matchers, err := client.FromLabelNamesRequest(req)
	if err != nil {
		return nil, err
	}

	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// Log the actual matchers passed down to TSDB. This can be useful for troubleshooting purposes.
	spanlog.DebugLog("num_matchers", len(matchers), "matchers", util.LabelMatchersToString(matchers))

	names, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, err
	}

	// Besides we are passing the hints to q.LabelNames, we also limit the number of returned values here
	// because LabelQuerier can resolve the labelNames using different instance and then joining the results,
	// so we want to apply the limit at the end.
	if hints != nil && hints.Limit > 0 && len(names) > hints.Limit {
		names = names[:hints.Limit]
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, nil
}

// MetricsForLabelMatchers implements IngesterServer.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (resp *client.MetricsForLabelMatchersResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.MetricsForLabelMatchersResponse{}, nil
	}

	// Parse the request
	hints, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	mint, maxt := req.StartTimestampMs, req.EndTimestampMs
	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// Run a query for each matchers set and collect all the results.
	var sets []storage.SeriesSet

	for _, matchers := range matchersSet {
		// Interrupt if the context has been canceled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		hints := &storage.SelectHints{
			Start: mint,
			End:   maxt,
			Limit: hints.Limit,
			Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
		}

		seriesSet := q.Select(ctx, true, hints, matchers...)
		sets = append(sets, seriesSet)
	}

	// Generate the response merging all series sets.
	result := &client.MetricsForLabelMatchersResponse{
		Metric: make([]*mimirpb.Metric, 0),
	}

	mergedSet := storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
	for mergedSet.Next() {
		// Interrupt if the context has been canceled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Besides we are passing the hints to q.Select, we also limit the number of returned series here,
		// to account for cases when series were resolved via different instances and joined into a single seriesSet,
		// which are not limited by the MergeSeriesSet.
		if hints.Limit > 0 && len(result.Metric) >= hints.Limit {
			break
		}

		result.Metric = append(result.Metric, &mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(mergedSet.At().Labels()),
		})
	}

	return result, nil
}

func (i *Ingester) UserStats(ctx context.Context, req *client.UserStatsRequest) (resp *client.UserStatsResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.UserStatsResponse{}, nil
	}

	return createUserStats(db, req)
}

// AllUserStats returns some per-tenant statistics about the data ingested in this ingester.
//
// When using the experimental ingest storage, this function doesn't support the read consistency setting
// because the purpose of this function is to show a snapshot of the live ingester's state.
func (i *Ingester) AllUserStats(_ context.Context, req *client.UserStatsRequest) (resp *client.UsersStatsResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	users := i.tsdbs

	response := &client.UsersStatsResponse{
		Stats: make([]*client.UserIDStatsResponse, 0, len(users)),
	}
	for userID, db := range users {
		userStats, err := createUserStats(db, req)
		if err != nil {
			return nil, err
		}
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userID,
			Data:   userStats,
		})
	}
	return response, nil
}

// we defined to use the limit of 1 MB because we have default limit for the GRPC message that is 4 MB.
// So, 1 MB limit will prevent reaching the limit and won't affect performance significantly.
const labelNamesAndValuesTargetSizeBytes = 1 * 1024 * 1024

func (i *Ingester) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, stream client.Ingester_LabelNamesAndValuesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(stream.Context())
	if err != nil {
		return err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(stream.Context(), userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	index, err := db.Head().Index()
	if err != nil {
		return err
	}
	defer index.Close()
	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return err
	}

	var valueFilter func(name, value string) (bool, error)
	switch request.GetCountMethod() {
	case client.IN_MEMORY:
		valueFilter = func(string, string) (bool, error) {
			return true, nil
		}
	case client.ACTIVE:
		valueFilter = func(name, value string) (bool, error) {
			return activeseries.IsLabelValueActive(stream.Context(), index, db.activeSeries, name, value)
		}
	default:
		return fmt.Errorf("unknown count method %q", request.GetCountMethod())
	}

	return labelNamesAndValues(index, matchers, labelNamesAndValuesTargetSizeBytes, stream, valueFilter)
}

// labelValuesCardinalityTargetSizeBytes is the maximum allowed size in bytes for label cardinality response.
// We arbitrarily set it to 1mb to avoid reaching the actual gRPC default limit (4mb).
const labelValuesCardinalityTargetSizeBytes = 1 * 1024 * 1024

func (i *Ingester) LabelValuesCardinality(req *client.LabelValuesCardinalityRequest, srv client.Ingester_LabelValuesCardinalityServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(srv.Context())
	if err != nil {
		return err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(srv.Context(), userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	idx, err := db.Head().Index()
	if err != nil {
		return err
	}
	defer idx.Close()

	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return err
	}

	var postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error)
	switch req.GetCountMethod() {
	case client.IN_MEMORY:
		postingsForMatchersFn = tsdb.PostingsForMatchers
	case client.ACTIVE:
		postingsForMatchersFn = func(ctx context.Context, ix tsdb.IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			postings, err := tsdb.PostingsForMatchers(ctx, ix, ms...)
			if err != nil {
				return nil, err
			}
			return activeseries.NewPostings(db.activeSeries, postings), nil
		}
	default:
		return fmt.Errorf("unknown count method %q", req.GetCountMethod())
	}

	return labelValuesCardinality(
		req.GetLabelNames(),
		matchers,
		idx,
		postingsForMatchersFn,
		labelValuesCardinalityTargetSizeBytes,
		srv,
	)
}

func createUserStats(db *userTSDB, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	apiRate := db.ingestedAPISamples.Rate()
	ruleRate := db.ingestedRuleSamples.Rate()

	var series uint64
	switch req.GetCountMethod() {
	case client.IN_MEMORY:
		series = db.Head().NumSeries()
	case client.ACTIVE:
		activeSeries, _, _, _ := db.activeSeries.Active()
		series = uint64(activeSeries)
	default:
		return nil, fmt.Errorf("unknown count method %q", req.GetCountMethod())
	}

	return &client.UserStatsResponse{
		IngestionRate:     apiRate + ruleRate,
		ApiIngestionRate:  apiRate,
		RuleIngestionRate: ruleRate,
		NumSeries:         series,
	}, nil
}

const queryStreamBatchMessageSize = 1 * 1024 * 1024

// QueryStream streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	ctx := stream.Context()
	spanlog := spanlogger.FromContext(ctx, i.logger)

	// Previously a chunk batch size of 0 was used to request a non-streaming response.
	// Streaming series and chunks has been the default for some time and we no longer
	// support non-streaming responses.
	if req.StreamingChunksBatchSize == 0 {
		req.StreamingChunksBatchSize = fallbackChunkStreamBatchSize
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	// Check if query sharding is enabled for this query. If so, we need to remove the
	// query sharding label from matchers and pass the shard info down the query execution path.
	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return err
	}

	i.metrics.queries.Inc()

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}

	selectHints := initSelectHints(int64(from), int64(through))
	selectHints = configSelectHintsWithShard(selectHints, shard)
	// Disable chunks trimming, so that we don't have to rewrite chunks which have samples outside
	// the requested from/through range. PromQL engine can handle it.
	selectHints = configSelectHintsWithDisabledTrimming(selectHints)

	numSamples := 0
	numSeries := 0

	spanlog.DebugLog("msg", "using executeStreamingQuery")
	numSeries, numSamples, err = i.executeStreamingQuery(ctx, db, selectHints, matchers, stream, req.StreamingChunksBatchSize, spanlog)
	if err != nil {
		return err
	}

	i.metrics.queriedSeries.WithLabelValues("merged_blocks").Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	spanlog.DebugLog("series", numSeries, "samples", numSamples)
	return nil
}

func (i *Ingester) executeStreamingQuery(ctx context.Context, db *userTSDB, hints *storage.SelectHints, matchers []*labels.Matcher, stream client.Ingester_QueryStreamServer, batchSize uint64, spanlog *spanlogger.SpanLogger) (numSeries, numSamples int, _ error) {
	var q storage.ChunkQuerier
	var err error
	if i.limits.OutOfOrderTimeWindow(db.userID) > 0 {
		q, err = db.UnorderedChunkQuerier(hints.Start, hints.End)
	} else {
		q, err = db.ChunkQuerier(hints.Start, hints.End)
	}
	if err != nil {
		return 0, 0, err
	}

	// The querier must remain open until we've finished streaming chunks.
	defer q.Close()

	allSeries, numSeries, err := i.sendStreamingQuerySeries(ctx, q, hints, matchers, stream)
	if err != nil {
		return 0, 0, err
	}

	spanlog.DebugLog("msg", "finished sending series", "series", numSeries)

	numSamples, numChunks, numBatches, err := i.sendStreamingQueryChunks(allSeries, stream, batchSize)
	if err != nil {
		return 0, 0, err
	}

	spanlog.DebugLog("msg", "finished sending chunks", "chunks", numChunks, "batches", numBatches)

	return numSeries, numSamples, nil
}

// chunkSeriesNode is used a build a linked list of slices of series.
// This is useful to avoid lots of allocation when you don't know the
// total number of series upfront.
//
// NOTE: Do not use this struct directly. Get it from getChunkSeriesNode()
// and put it back using putChunkSeriesNode() when you are done using it.
type chunkSeriesNode struct {
	series []storage.ChunkIterable
	next   *chunkSeriesNode
}

// Capacity of the slice in chunkSeriesNode.
const chunkSeriesNodeSize = 1024

// chunkSeriesNodePool is used during streaming queries.
var chunkSeriesNodePool zeropool.Pool[*chunkSeriesNode]

func getChunkSeriesNode() *chunkSeriesNode {
	sn := chunkSeriesNodePool.Get()
	if sn == nil {
		sn = &chunkSeriesNode{
			series: make([]storage.ChunkIterable, 0, chunkSeriesNodeSize),
		}
	}
	return sn
}

func putChunkSeriesNode(sn *chunkSeriesNode) {
	sn.series = sn.series[:0]
	sn.next = nil
	chunkSeriesNodePool.Put(sn)
}

func (i *Ingester) sendStreamingQuerySeries(ctx context.Context, q storage.ChunkQuerier, hints *storage.SelectHints, matchers []*labels.Matcher, stream client.Ingester_QueryStreamServer) (*chunkSeriesNode, int, error) {
	// Series must be sorted so that they can be read by the querier in the order the PromQL engine expects.
	ss := q.Select(ctx, true, hints, matchers...)
	if ss.Err() != nil {
		return nil, 0, errors.Wrap(ss.Err(), "selecting series from ChunkQuerier")
	}

	seriesInBatch := make([]client.QueryStreamSeries, 0, queryStreamBatchSize)

	// We retain the iterator factory returned by IteratorFactory() rather than the full storage.ChunkSeries,
	// so that we don't hold references to labels or other series data longer than necessary.
	// The factory retains only the minimum data required to create the chunk iterator, and still supports
	// iterator reuse in sendStreamingQueryChunks().
	//
	// It is non-trivial to know the total number of series here, so we use a linked list of slices
	// of series and re-use them for future use as well. Even if we did know total number of series
	// here, maybe re-use of slices is a good idea.
	allSeriesList := getChunkSeriesNode()
	lastSeriesNode := allSeriesList
	seriesCount := 0

	for ss.Next() {
		cs := ss.At()

		if len(lastSeriesNode.series) == chunkSeriesNodeSize {
			newNode := getChunkSeriesNode()
			lastSeriesNode.next = newNode
			lastSeriesNode = newNode
		}
		lastSeriesNode.series = append(lastSeriesNode.series, cs.IteratorFactory())
		seriesCount++

		chunkCount, err := cs.ChunkCount()
		if err != nil {
			return nil, 0, errors.Wrap(err, "getting ChunkSeries chunk count")
		}

		lbls := cs.Labels()
		seriesInBatch = append(seriesInBatch, client.QueryStreamSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(lbls),
			ChunkCount: int64(chunkCount),
		})

		if len(seriesInBatch) >= queryStreamBatchSize {
			err := client.SendQueryStream(stream, &client.QueryStreamResponse{
				StreamingSeries: seriesInBatch,
			})
			if err != nil {
				return nil, 0, err
			}

			seriesInBatch = seriesInBatch[:0]
		}
	}

	// Send any remaining series, and signal that there are no more.
	err := client.SendQueryStream(stream, &client.QueryStreamResponse{
		StreamingSeries:     seriesInBatch,
		IsEndOfSeriesStream: true,
	})
	if err != nil {
		return nil, 0, err
	}

	// Ensure no error occurred while iterating the series set.
	if err := ss.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "iterating ChunkSeriesSet")
	}

	return allSeriesList, seriesCount, nil
}

func (i *Ingester) sendStreamingQueryChunks(allSeries *chunkSeriesNode, stream client.Ingester_QueryStreamServer, batchSize uint64) (int, int, int, error) {
	var (
		it             chunks.Iterator
		seriesIdx      = -1
		currNode       = allSeries
		numSamples     = 0
		numChunks      = 0
		numBatches     = 0
		seriesInBatch  = make([]client.QueryStreamSeriesChunks, 0, batchSize)
		batchSizeBytes = 0
	)

	for currNode != nil {
		for _, series := range currNode.series {
			seriesIdx++
			seriesChunks := client.QueryStreamSeriesChunks{
				SeriesIndex: uint64(seriesIdx),
			}

			it = series.Iterator(it)

			for it.Next() {
				meta := it.At()

				// It is not guaranteed that chunk returned by iterator is populated.
				// For now just return error. We could also try to figure out how to read the chunk.
				if meta.Chunk == nil {
					return 0, 0, 0, errors.Errorf("unfilled chunk returned from TSDB chunk querier")
				}

				ch, err := client.ChunkFromMeta(meta)
				if err != nil {
					return 0, 0, 0, err
				}

				seriesChunks.Chunks = append(seriesChunks.Chunks, ch)
				numSamples += meta.Chunk.NumSamples()
			}

			if err := it.Err(); err != nil {
				return 0, 0, 0, err
			}

			numChunks += len(seriesChunks.Chunks)
			msgSize := seriesChunks.Size()

			if (batchSizeBytes > 0 && batchSizeBytes+msgSize > queryStreamBatchMessageSize) || len(seriesInBatch) >= int(batchSize) {
				// Adding this series to the batch would make it too big, flush the data and add it to new batch instead.
				err := client.SendQueryStream(stream, &client.QueryStreamResponse{
					StreamingSeriesChunks: seriesInBatch,
				})
				if err != nil {
					return 0, 0, 0, err
				}

				seriesInBatch = seriesInBatch[:0]
				batchSizeBytes = 0
				numBatches++
			}

			seriesInBatch = append(seriesInBatch, seriesChunks)
			batchSizeBytes += msgSize
		}

		toBePutInPool := currNode
		currNode = currNode.next
		putChunkSeriesNode(toBePutInPool)
	}

	// Send any remaining series.
	if batchSizeBytes != 0 {
		err := client.SendQueryStream(stream, &client.QueryStreamResponse{
			StreamingSeriesChunks: seriesInBatch,
		})
		if err != nil {
			return 0, 0, 0, err
		}
		numBatches++
	}

	return numSamples, numChunks, numBatches, nil
}

func (i *Ingester) mapReadErrorToErrorWithStatus(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	return mapReadErrorToErrorWithStatus(err)
}

// MetricsMetadata returns all the metrics metadata of a user.
func (i *Ingester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (resp *client.MetricsMetadataResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	userMetadata := i.getUserMetadata(userID)

	if userMetadata == nil {
		return &client.MetricsMetadataResponse{}, nil
	}

	return &client.MetricsMetadataResponse{Metadata: userMetadata.toClientMetadata(req)}, nil
}

func initSelectHints(start, end int64) *storage.SelectHints {
	return &storage.SelectHints{
		Start: start,
		End:   end,
	}
}

func configSelectHintsWithShard(hints *storage.SelectHints, shard *sharding.ShardSelector) *storage.SelectHints {
	if shard != nil {
		// If query sharding is enabled, we need to pass it along with hints.
		hints.ShardIndex = shard.ShardIndex
		hints.ShardCount = shard.ShardCount
	}
	return hints
}

func configSelectHintsWithDisabledTrimming(hints *storage.SelectHints) *storage.SelectHints {
	hints.DisableTrimming = true
	return hints
}

func (i *Ingester) UserRegistryHandler(w http.ResponseWriter, r *http.Request) {
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	reg := i.tsdbMetrics.RegistryForTenant(userID)
	if reg == nil {
		http.Error(w, "user registry not found", http.StatusNotFound)
		return
	}

	promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		DisableCompression: true,
		ErrorHandling:      promhttp.HTTPErrorOnError,
		Timeout:            10 * time.Second,
	}).ServeHTTP(w, r)
}

// checkReadOverloaded checks whether the ingester read path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkReadOverloaded() error {
	if i.utilizationBasedLimiter == nil {
		return nil
	}

	reason := i.utilizationBasedLimiter.LimitingReason()
	if reason == "" {
		return nil
	}

	i.metrics.utilizationLimitedRequests.WithLabelValues(reason).Inc()
	return errTooBusy
}

func (i *Ingester) enforceReadConsistency(ctx context.Context, tenantID string) error {
	// Read consistency is enforced by design in Mimir, unless using the ingest storage.
	if i.ingestReader == nil {
		return nil
	}

	var level string
	if c, ok := api.ReadConsistencyLevelFromContext(ctx); ok {
		level = c
	} else {
		level = i.limits.IngestStorageReadConsistency(tenantID)
	}

	spanLog := spanlogger.FromContext(ctx, i.logger)
	spanLog.DebugLog("msg", "checked read consistency", "level", level)

	if level == api.ReadConsistencyEventual {
		if maxDelay, ok := api.ReadConsistencyMaxDelayFromContext(ctx); ok {
			spanLog.DebugLog("msg", "enforcing read consistency max delay", "max_delay", maxDelay)
			return errors.Wrap(i.ingestReader.EnforceReadMaxDelay(maxDelay), "enforce read consistency max delay")
		}
	}

	if level == api.ReadConsistencyStrong {
		// Check if request already contains the minimum offset we have to guarantee being queried
		// for our partition.
		if offsets, ok := api.ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
			if offset, ok := offsets.Lookup(i.ingestPartitionID); ok {
				spanLog.DebugLog("msg", "enforcing strong read consistency", "offset", offset)
				return errors.Wrap(i.ingestReader.WaitReadConsistencyUntilOffset(ctx, offset), "wait for read consistency")
			}
		}

		spanLog.DebugLog("msg", "enforcing strong read consistency", "offset", "last produced")
		return errors.Wrap(i.ingestReader.WaitReadConsistencyUntilLastProducedOffset(ctx), "wait for read consistency")
	}

	return nil
}
