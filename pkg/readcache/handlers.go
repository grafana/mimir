// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"sort"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// The handlers below are the read-RPC subset of the ingester gRPC
// surface, ported to operate against per-(tenant, partition) TSDBs
// instead of a single per-tenant TSDB.
//
// The Phase 2A implementation is deliberately a simpler port of
// pkg/ingester/ingester_query.go than the ingester's full streaming
// pipeline. Notably:
//   - No per-tenant ActiveSeries tracker (readcache is read-only).
//   - No projection-label optimisation (the readcache port can opt in
//     in a follow-up patch once Phase 2A is observed end-to-end).
//   - No read-consistency enforcement against Kafka offsets (the Kafka
//     reader in Phase 2A is a follow-up patch; once it lands, this
//     file gets the equivalent of enforceReadConsistency).
//
// The contract on the wire (the messages emitted on the stream) is
// preserved so distributor reads can target a readcache pod
// interchangeably with an ingester pod once Phase 2C routes traffic.

const queryStreamBatchSize = 128

// queryStream streams series + chunks for the given matchers across
// the partitions this readcache instance owns for the requested
// tenant.
func (r *Readcache) queryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	ctx := stream.Context()
	spanlog, ctx := spanlogger.New(ctx, r.logger, tracer, "Readcache.QueryStream")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	dbs, err := r.listTSDBsForTenant(userID, req.QueryAttributionHint)
	if err != nil {
		return err
	}
	if len(dbs) == 0 {
		// No owned partition for this tenant: send EOS and return.
		return client.SendQueryStream(stream, &client.QueryStreamResponse{IsEndOfSeriesStream: true})
	}

	type countedChunk struct {
		chunk   client.Chunk
		samples int
	}
	appendUniqueChunk := func(chunks []countedChunk, chunk client.Chunk, samples int) ([]countedChunk, bool) {
		for _, existing := range chunks {
			if existing.chunk.Equal(chunk) {
				return chunks, false
			}
		}
		return append(chunks, countedChunk{chunk: chunk, samples: samples}), true
	}

	type seriesItem struct {
		labels labels.Labels
		chunks []countedChunk
	}
	var items []seriesItem
	var closers []storage.ChunkQuerier
	defer func() {
		for _, q := range closers {
			_ = q.Close()
		}
	}()

	// Phase 1: across all owned partitions, gather every (labels,
	// chunks) pair, then stream globally sorted and deduplicated
	// labels. We materialize chunks before emitting labels because the
	// stream protocol requires the labels phase to finish before the
	// chunks phase starts, and every advertised series must later have
	// at least one chunk.
	// The wire protocol requires every StreamingSeries message to land
	// before the IsEndOfSeriesStream marker; interleaving series and
	// chunk messages breaks the receiver's series-count accounting.
	for _, db := range dbs {
		q, err := db.ChunkQuerier(int64(from), int64(through))
		if err != nil {
			return err
		}
		closers = append(closers, q)

		hints := &storage.SelectHints{Start: int64(from), End: int64(through)}
		ss := q.Select(ctx, true, hints, matchers...)
		if ss.Err() != nil {
			return errors.Wrap(ss.Err(), "selecting series from partition TSDB")
		}

		for ss.Next() {
			cs := ss.At()
			lbls := cs.Labels()

			var itemChunks []countedChunk
			it := cs.IteratorFactory().Iterator(nil)
			for it.Next() {
				meta := it.At()
				if meta.Chunk == nil {
					return errors.Errorf("unfilled chunk returned from TSDB chunk querier")
				}
				ch, err := client.ChunkFromMeta(meta)
				if err != nil {
					return err
				}
				itemChunks, _ = appendUniqueChunk(itemChunks, ch, meta.Chunk.NumSamples())
			}
			if err := it.Err(); err != nil {
				return err
			}

			// Never advertise a series with no in-range chunks: the
			// querier's batch merge iterator assumes every streamed
			// series carries at least one chunk and panics otherwise.
			// Multi-epoch TSDBs make this case real: a frozen epoch
			// can index a labelset while owning no chunk inside
			// [from, through].
			if len(itemChunks) == 0 {
				continue
			}

			items = append(items, seriesItem{
				labels: lbls,
				chunks: itemChunks,
			})
		}
		if err := ss.Err(); err != nil {
			return errors.Wrap(err, "iterating chunk series set")
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return labels.Compare(items[i].labels, items[j].labels) < 0
	})
	coalesced := items[:0]
	for _, item := range items {
		if len(coalesced) > 0 && labels.Equal(coalesced[len(coalesced)-1].labels, item.labels) {
			for _, chunk := range item.chunks {
				coalesced[len(coalesced)-1].chunks, _ = appendUniqueChunk(coalesced[len(coalesced)-1].chunks, chunk.chunk, chunk.samples)
			}
			continue
		}
		coalesced = append(coalesced, item)
	}
	items = coalesced

	seriesBatch := make([]client.QueryStreamSeries, 0, queryStreamBatchSize)
	for _, item := range items {
		seriesBatch = append(seriesBatch, client.QueryStreamSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(item.labels),
			ChunkCount: int64(len(item.chunks)),
		})

		if len(seriesBatch) >= queryStreamBatchSize {
			if err := client.SendQueryStream(stream, &client.QueryStreamResponse{
				StreamingSeries: seriesBatch,
			}); err != nil {
				return err
			}
			seriesBatch = seriesBatch[:0]
		}
	}
	if len(seriesBatch) > 0 {
		if err := client.SendQueryStream(stream, &client.QueryStreamResponse{
			StreamingSeries: seriesBatch,
		}); err != nil {
			return err
		}
	}

	// EOS marker separates the labels phase from the chunks phase.
	if err := client.SendQueryStream(stream, &client.QueryStreamResponse{IsEndOfSeriesStream: true}); err != nil {
		return err
	}

	// Phase 2: stream chunks for each series in the same order the
	// labels were emitted.
	chunksBatch := make([]client.QueryStreamSeriesChunks, 0, queryStreamBatchSize)
	var samples int
	for idx, item := range items {
		seriesChunks := client.QueryStreamSeriesChunks{SeriesIndex: uint64(idx)}

		sort.Slice(item.chunks, func(i, j int) bool {
			if item.chunks[i].chunk.StartTimestampMs == item.chunks[j].chunk.StartTimestampMs {
				return item.chunks[i].chunk.EndTimestampMs < item.chunks[j].chunk.EndTimestampMs
			}
			return item.chunks[i].chunk.StartTimestampMs < item.chunks[j].chunk.StartTimestampMs
		})
		for _, ch := range item.chunks {
			seriesChunks.Chunks = append(seriesChunks.Chunks, ch.chunk)
			samples += ch.samples
		}
		chunksBatch = append(chunksBatch, seriesChunks)
		if len(chunksBatch) >= queryStreamBatchSize {
			if err := client.SendQueryStream(stream, &client.QueryStreamResponse{
				StreamingSeriesChunks: chunksBatch,
			}); err != nil {
				return err
			}
			chunksBatch = chunksBatch[:0]
		}
	}
	if len(chunksBatch) > 0 {
		if err := client.SendQueryStream(stream, &client.QueryStreamResponse{
			StreamingSeriesChunks: chunksBatch,
		}); err != nil {
			return err
		}
	}

	r.queryLoad.Attribute(req.QueryAttributionHint, int64(samples))
	// Mirror of cortex_ingester_queried_samples: one observation per
	// QueryStream with the total samples carried by the streamed
	// chunks, so old-world and readcache query volume line up.
	if r.queriedSamples != nil {
		r.queriedSamples.Observe(float64(samples))
	}
	spanlog.DebugLog("series", len(items), "samples", samples)
	return nil
}

func (r *Readcache) queryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matcherSet, err := client.FromExemplarQueryRequest(req)
	if err != nil {
		return nil, err
	}

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return &client.ExemplarQueryResponse{}, nil
	}

	merged := map[string]mimirpb.TimeSeries{}
	keys := []string{}
	for _, db := range dbs {
		q, err := db.ExemplarQuerier(ctx)
		if err != nil {
			return nil, err
		}
		res, err := q.Select(from, through, matcherSet...)
		if err != nil {
			return nil, err
		}
		for _, exemplarSet := range res {
			lbls := mimirpb.FromLabelsToLabelAdapters(exemplarSet.SeriesLabels)
			key := mimirpb.FromLabelAdaptersToKeyString(lbls)
			exemplars := make([]mimirpb.Exemplar, 0, len(exemplarSet.Exemplars))
			for _, e := range exemplarSet.Exemplars {
				exemplars = append(exemplars, mimirpb.Exemplar{
					Labels:      mimirpb.FromLabelsToLabelAdapters(e.Labels),
					Value:       e.Value,
					TimestampMs: e.Ts,
				})
			}
			if existing, ok := merged[key]; ok {
				existing.Exemplars = append(existing.Exemplars, exemplars...)
				merged[key] = existing
			} else {
				keys = append(keys, key)
				merged[key] = mimirpb.TimeSeries{Labels: lbls, Exemplars: exemplars}
			}
		}
	}

	sort.Strings(keys)
	out := &client.ExemplarQueryResponse{Timeseries: make([]mimirpb.TimeSeries, 0, len(keys))}
	for _, k := range keys {
		out.Timeseries = append(out.Timeseries, merged[k])
	}
	return out, nil
}

func (r *Readcache) labelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	labelName, mint, maxt, hints, matchers, err := client.FromLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return &client.LabelValuesResponse{}, nil
	}

	seen := map[string]struct{}{}
	for _, db := range dbs {
		q, err := db.Querier(mint, maxt)
		if err != nil {
			return nil, err
		}
		vals, _, err := q.LabelValues(ctx, labelName, hints, matchers...)
		q.Close()
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			seen[strings.Clone(v)] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	if hints != nil && hints.Limit > 0 && len(out) > hints.Limit {
		out = out[:hints.Limit]
	}
	return &client.LabelValuesResponse{LabelValues: out}, nil
}

func (r *Readcache) labelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	mint, maxt, hints, matchers, err := client.FromLabelNamesRequest(req)
	if err != nil {
		return nil, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	spanlog, ctx := spanlogger.New(ctx, r.logger, tracer, "Readcache.LabelNames")
	defer spanlog.Finish()

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return &client.LabelNamesResponse{}, nil
	}

	seen := map[string]struct{}{}
	for _, db := range dbs {
		q, err := db.Querier(mint, maxt)
		if err != nil {
			return nil, err
		}
		names, _, err := q.LabelNames(ctx, hints, matchers...)
		q.Close()
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			seen[strings.Clone(n)] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	if hints != nil && hints.Limit > 0 && len(out) > hints.Limit {
		out = out[:hints.Limit]
	}
	spanlog.DebugLog("num_matchers", len(matchers), "matchers", util.LabelMatchersToString(matchers))
	return &client.LabelNamesResponse{LabelNames: out}, nil
}

func (r *Readcache) metricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	hints, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return &client.MetricsForLabelMatchersResponse{Metric: []*mimirpb.Metric{}}, nil
	}

	seen := map[string]struct{}{}
	mint, maxt := req.StartTimestampMs, req.EndTimestampMs
	for _, db := range dbs {
		q, err := db.Querier(mint, maxt)
		if err != nil {
			return nil, err
		}
		for _, matchers := range matchersSet {
			if ctx.Err() != nil {
				q.Close()
				return nil, ctx.Err()
			}
			ss := q.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt, Limit: hints.Limit, Func: "series"}, matchers...)
			for ss.Next() {
				key := ss.At().Labels().String()
				seen[key] = struct{}{}
				if hints.Limit > 0 && len(seen) >= hints.Limit {
					break
				}
			}
		}
		q.Close()
	}

	// We collect the actual labels.Labels values during iteration in
	// the loop above. Reuse a separate pass over each TSDB to gather
	// them. (Simpler than the ingester's MergeSeriesSet because read
	// across heads is rare in the readcache phase 2A bring-up; this
	// implementation can be refactored toward MergeSeriesSet once
	// production readcache traffic exists.)
	result := &client.MetricsForLabelMatchersResponse{Metric: make([]*mimirpb.Metric, 0, len(seen))}
	for _, db := range dbs {
		q, err := db.Querier(mint, maxt)
		if err != nil {
			return nil, err
		}
		for _, matchers := range matchersSet {
			ss := q.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt, Limit: hints.Limit, Func: "series"}, matchers...)
			for ss.Next() {
				lbls := ss.At().Labels()
				key := lbls.String()
				if _, ok := seen[key]; !ok {
					continue
				}
				delete(seen, key)
				result.Metric = append(result.Metric, &mimirpb.Metric{Labels: mimirpb.FromLabelsToLabelAdapters(lbls)})
				if hints.Limit > 0 && len(result.Metric) >= hints.Limit {
					break
				}
			}
			if hints.Limit > 0 && len(result.Metric) >= hints.Limit {
				break
			}
		}
		q.Close()
		if hints.Limit > 0 && len(result.Metric) >= hints.Limit {
			break
		}
	}
	return result, nil
}

func (r *Readcache) userStats(ctx context.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return &client.UserStatsResponse{}, nil
	}
	var numSeries uint64
	for _, db := range dbs {
		numSeries += db.Head().NumSeries()
	}
	return &client.UserStatsResponse{
		NumSeries: numSeries,
		// Rate stats aren't tracked on readcache; surface zero so the
		// callers can treat readcache the same as a cold ingester.
	}, nil
}

func (r *Readcache) allUserStats(_ context.Context, _ *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	r.partitionMu.RLock()
	defer r.partitionMu.RUnlock()

	// Aggregate per tenant across all owned partitions.
	perTenant := map[string]uint64{}
	for _, p := range r.partitions {
		p.tenantsMu.RLock()
		for tenant, db := range p.tenants {
			perTenant[tenant] += db.Head().NumSeries()
		}
		p.tenantsMu.RUnlock()
	}
	out := &client.UsersStatsResponse{Stats: make([]*client.UserIDStatsResponse, 0, len(perTenant))}
	for tenant, n := range perTenant {
		out.Stats = append(out.Stats, &client.UserIDStatsResponse{
			UserId: tenant,
			Data:   &client.UserStatsResponse{NumSeries: n},
		})
	}
	return out, nil
}

// labelNamesAndValues streams label-name → values pairs over the gRPC
// stream. For Phase 2A this is a simpler implementation than the
// ingester's (no active-series filter, no cost-attribution); the wire
// shape is preserved.
func (r *Readcache) labelNamesAndValues(req *client.LabelNamesAndValuesRequest, srv client.Ingester_LabelNamesAndValuesServer) error {
	ctx := srv.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return err
	}

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return err
	}
	if len(dbs) == 0 {
		return nil
	}

	// Aggregate label name -> set of values across all heads, then
	// stream the response in one shot. For very large fan-outs this
	// would need the ingester's chunked streamer; revisit before
	// production.
	perName := map[string]map[string]struct{}{}
	for _, db := range dbs {
		idx, err := db.Head().Index()
		if err != nil {
			return err
		}
		names, err := idx.LabelNames(ctx, matchers...)
		if err != nil {
			idx.Close()
			return err
		}
		for _, name := range names {
			vals, err := idx.SortedLabelValues(ctx, name, nil, matchers...)
			if err != nil {
				idx.Close()
				return err
			}
			set := perName[name]
			if set == nil {
				set = map[string]struct{}{}
				perName[name] = set
			}
			for _, v := range vals {
				set[strings.Clone(v)] = struct{}{}
			}
		}
		idx.Close()
	}

	resp := &client.LabelNamesAndValuesResponse{}
	for name, vals := range perName {
		v := make([]string, 0, len(vals))
		for s := range vals {
			v = append(v, s)
		}
		sort.Strings(v)
		resp.Items = append(resp.Items, &client.LabelValues{LabelName: name, Values: v})
	}
	return srv.Send(resp)
}

// labelValuesCardinality streams label-name → (value, count) pairs.
// Simplified Phase 2A port.
func (r *Readcache) labelValuesCardinality(req *client.LabelValuesCardinalityRequest, srv client.Ingester_LabelValuesCardinalityServer) error {
	ctx := srv.Context()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	labelNames := req.GetLabelNames()
	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return err
	}

	dbs, err := r.listTSDBsForTenant(userID, nil)
	if err != nil {
		return err
	}
	if len(dbs) == 0 {
		return nil
	}

	// Aggregate across heads. For each label name, count distinct
	// (value -> series count). Series counts are summed across
	// partitions (treating each partition as disjoint, which it is
	// under hash routing).
	type valueCount struct {
		count uint64
	}
	type nameAgg map[string]*valueCount
	agg := map[string]nameAgg{}

	for _, db := range dbs {
		idx, err := db.Head().Index()
		if err != nil {
			return err
		}
		for _, name := range labelNames {
			vals, err := idx.SortedLabelValues(ctx, name, nil, matchers...)
			if err != nil {
				idx.Close()
				return err
			}
			a := agg[name]
			if a == nil {
				a = nameAgg{}
				agg[name] = a
			}
			for _, v := range vals {
				postings, err := idx.Postings(ctx, name, v)
				if err != nil {
					idx.Close()
					return err
				}
				var count uint64
				for postings.Next() {
					count++
				}
				if err := postings.Err(); err != nil {
					idx.Close()
					return err
				}
				if existing, ok := a[v]; ok {
					existing.count += count
				} else {
					a[strings.Clone(v)] = &valueCount{count: count}
				}
			}
		}
		idx.Close()
	}

	resp := &client.LabelValuesCardinalityResponse{}
	for name, vals := range agg {
		item := &client.LabelValueSeriesCount{LabelName: name, LabelValueSeries: make(map[string]uint64, len(vals))}
		for v, c := range vals {
			item.LabelValueSeries[v] = c.count
		}
		resp.Items = append(resp.Items, item)
	}
	return srv.Send(resp)
}

// activeSeries is a no-op in Phase 2A: readcache does not track
// active series. The response is shaped so callers get a clean empty
// stream rather than an Unimplemented error.
func (r *Readcache) activeSeries(_ *client.ActiveSeriesRequest, srv client.Ingester_ActiveSeriesServer) error {
	return srv.Send(&client.ActiveSeriesResponse{})
}

// hashRangeStats serves the rebalancer's view of work done by this
// readcache pod: per-(partition, hash range) active-series counts plus
// per-partition totals and query-load EWMAs.
//
// Each partition's currentRanges are always emitted, even at zero, so
// the rebalancer sees the claimed footprint. Historical ranges are
// emitted only when they still have residue series in the head; once
// the head compacts and the count drops to zero, the walker GCs the
// historical entry on the next tick.
func (r *Readcache) hashRangeStats(_ context.Context, _ *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	partSnap := r.partitionSeries.Snapshot()

	partitionSeries := make([]client.PartitionActiveSeries, len(partSnap.Partitions))
	for i, e := range partSnap.Partitions {
		partitionSeries[i] = client.PartitionActiveSeries{
			PartitionId:  e.PartitionID,
			ActiveSeries: e.ActiveSeries,
		}
	}

	resp := &client.HashRangeStatsResponse{
		TotalActiveSeries:     partSnap.Total,
		PartitionActiveSeries: partitionSeries,
	}

	for _, p := range parts {
		for _, hrc := range p.ranges.snapshotCounts() {
			resp.Rates = append(resp.Rates, client.HashRangeRate{
				Lo:           hrc.Range.Lo,
				Hi:           hrc.Range.Hi,
				ActiveSeries: hrc.Count,
				SampleRate:   hrc.SampleRate,
				PartitionId:  p.partitionID,
			})
		}
	}

	ql := r.queryLoad.Snapshot()
	resp.UnnamedQuerySamplesEwma = ql.Unnamed
	resp.PartitionQueryLoads = make([]client.PartitionQueryLoad, len(ql.PerPartition))
	for j, p := range ql.PerPartition {
		resp.PartitionQueryLoads[j] = client.PartitionQueryLoad{
			PartitionId: p.PartitionID,
			SamplesEwma: p.SamplesEWMA,
		}
	}
	return resp, nil
}

// setHashRanges installs the latest per-partition hash range assignment
// from the rebalancer. The request is a flat list of
// (partition_id, lo, hi) entries; we group by partition and call
// setRanges on each partition's bookkeeping in turn. Partitions owned
// by this readcache that don't appear in the request have their
// currentRanges cleared (any residue moves to historicalRanges).
// Entries naming an unowned partition are dropped with a debug log —
// the next assignment reconcile from the rebalancer will straighten it
// out.
func (r *Readcache) setHashRanges(_ context.Context, req *client.SetHashRangesRequest) (*client.SetHashRangesResponse, error) {
	byPartition := make(map[int32][]assignment.HashRange)
	for _, rng := range req.Ranges {
		byPartition[rng.PartitionId] = append(byPartition[rng.PartitionId], assignment.HashRange{Lo: rng.Lo, Hi: rng.Hi})
	}

	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	owned := make(map[int32]struct{}, len(parts))
	var updatedPartitions, clearedPartitions []int32
	var ignoredUnowned []int32
	for _, p := range parts {
		owned[p.partitionID] = struct{}{}
		ranges := byPartition[p.partitionID]
		p.ranges.setRanges(ranges)
		if len(ranges) > 0 {
			updatedPartitions = append(updatedPartitions, p.partitionID)
		} else {
			clearedPartitions = append(clearedPartitions, p.partitionID)
		}
	}

	// Surface entries the rebalancer routed to us but for partitions
	// we don't own. Reaching here usually means a partition just
	// finished moving off this readcache and the next round will fix
	// it up.
	for pid := range byPartition {
		if _, ok := owned[pid]; !ok {
			ignoredUnowned = append(ignoredUnowned, pid)
			level.Warn(r.logger).Log("msg", "SetHashRanges entry for unowned partition; ignoring", "partition", pid, "instance_id", r.cfg.InstanceID)
		}
	}

	sort.Slice(updatedPartitions, func(i, j int) bool { return updatedPartitions[i] < updatedPartitions[j] })
	sort.Slice(clearedPartitions, func(i, j int) bool { return clearedPartitions[i] < clearedPartitions[j] })
	sort.Slice(ignoredUnowned, func(i, j int) bool { return ignoredUnowned[i] < ignoredUnowned[j] })

	level.Info(r.logger).Log(
		"msg", "SetHashRanges applied",
		"instance_id", r.cfg.InstanceID,
		"owned_partitions", len(parts),
		"updated_partitions", len(updatedPartitions),
		"cleared_partitions", len(clearedPartitions),
		"ignored_unowned", len(ignoredUnowned),
		"total_ranges", len(req.Ranges),
		"updated_partition_ids", formatPartitionIDs(updatedPartitions, 20),
		"cleared_partition_ids", formatPartitionIDs(clearedPartitions, 20),
		"ignored_unowned_ids", formatPartitionIDs(ignoredUnowned, 20),
	)
	return &client.SetHashRangesResponse{}, nil
}

// getHashRanges returns the currently-claimed hash ranges across every
// owned partition. Each entry carries the partition_id so the
// rebalancer can rebuild a (partition, range) view from this response.
func (r *Readcache) getHashRanges(_ context.Context, _ *client.GetHashRangesRequest) (*client.GetHashRangesResponse, error) {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	resp := &client.GetHashRangesResponse{}
	for _, p := range parts {
		for _, rng := range p.ranges.currentRangesCopy() {
			resp.Ranges = append(resp.Ranges, client.HashRangeEntry{
				Lo:          rng.Lo,
				Hi:          rng.Hi,
				PartitionId: p.partitionID,
			})
		}
	}
	return resp, nil
}

// silence unused-import lints when only some files in the package
// reference these helpers — these come into play once the Kafka
// reader is wired in pkg/readcache/kafka.go.
var (
	_ = tsdb.NewRangeHead
	_ = index.AllPostingsKey
)
