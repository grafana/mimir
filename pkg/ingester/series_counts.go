// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// SeriesCounts counts the head's series that match the request's matchers
// and have a chunk in its window, grouped by one label, from the series'
// chunk metas. No sample is read, except that the open head chunk's last
// sample is read when the chunk starts before the window or the count is per
// bucket, since its metas don't say where it ends. The matching postings are
// resolved first and priced at ingesterSeriesCostBytes per series, or
// ingesterMultiSourceSeriesCostBytes over several sources, so a request over
// max_index_bytes, bounded by the tenant's
// -querier.cardinality-estimate-max-index-bytes, is refused before any series
// is walked. A request for hashes is refused over max_hashes, and any request
// over max_groups, before counting where a local block gives a lower bound
// and otherwise while counting.
//
// The head, its out-of-order chunks included, and the ingester's compacted
// local blocks that overlap the window are counted; a series in several of
// them counts once. The response's min_time_ms is the oldest time the
// ingester holds for the tenant, whether or not it is in the window, so a
// caller can tell an ingester that holds nothing in the window from one
// that holds nothing at all, and shard_size_unchanged_since_ms is since when the
// ownedSeries service has seen the tenant's shard size unchanged. A request
// with shard_size_unchanged_before_ms is answered with shard_size_changed and
// nothing counted when that time is not before it, or not known. Experimental.
func (i *Ingester) SeriesCounts(req *client.SeriesCountsRequest, stream client.Ingester_SeriesCountsServer) error {
	resp, err := i.seriesCountsWithStatus(stream.Context(), req)
	if err != nil {
		return err
	}
	return client.SendSeriesCountsResponse(stream, resp, seriesCountsMessageSize)
}

// seriesCountsMetrics are the ingester's cardinality estimate count metrics.
type seriesCountsMetrics struct {
	seriesWalked prometheus.Counter
	walkDuration *prometheus.HistogramVec
	refusals     *prometheus.CounterVec
}

// Refusal reasons of the ingester's count refusals counter: the limits a
// refusal names, and the concurrency gate.
const (
	seriesCountsRefusedByMaxIndexBytes = cardinality.LimitMaxIndexBytes
	seriesCountsRefusedByMaxHashes     = cardinality.LimitMaxHashes
	seriesCountsRefusedByMaxGroups     = cardinality.LimitMaxGroups
	seriesCountsRefusedByGate          = "gate"
)

func newSeriesCountsMetrics(reg prometheus.Registerer) *seriesCountsMetrics {
	return &seriesCountsMetrics{
		seriesWalked: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_cardinality_estimate_series_walked_total",
			Help: "Series the ingester walked to answer cardinality estimate count requests.",
		}),
		walkDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingester_cardinality_estimate_walk_duration_seconds",
			Help:                            "Time the ingester spent resolving and walking one cardinality estimate count request's series, by sources: head, blocks (local blocks only) or head_and_blocks.",
			Buckets:                         prometheus.DefBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"sources"}),
		refusals: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_cardinality_estimate_refusals_total",
			Help: "Cardinality estimate count requests the ingester refused, by limit: max_index_bytes, max_hashes, max_groups, or gate when -ingester.cardinality-estimate-max-concurrent walks were running.",
		}, []string{"limit"}),
	}
}

// seriesCountsSources is the sources label of a walk over sources.
func seriesCountsSources(sources []*countSource) string {
	head, blocks := false, false
	for _, src := range sources {
		if src.block {
			blocks = true
		} else {
			head = true
		}
	}
	switch {
	case head && blocks:
		return "head_and_blocks"
	case blocks:
		return "blocks"
	}
	return "head"
}

// newSeriesCountsGate returns the gate on count walks running at once, nil
// for no limit. It refuses without waiting, and exports the in-flight and
// wait metrics of the query gates, labelled gate="cardinality_estimate".
func newSeriesCountsGate(reg prometheus.Registerer, maxConcurrent int) gate.Gate {
	if maxConcurrent <= 0 {
		return nil
	}
	gateReg := prometheus.WrapRegistererWith(prometheus.Labels{"gate": "cardinality_estimate"}, prometheus.WrapRegistererWithPrefix("cortex_ingester_", reg))
	return gate.NewInstrumented(gateReg, maxConcurrent, gate.NewRejecting(maxConcurrent))
}

// startSeriesCounts takes a slot for a count walk, which the returned
// function frees. An ingester already running
// -ingester.cardinality-estimate-max-concurrent of them answers Unavailable
// without waiting, so the distributor asks another zone when there is one.
// A count walk holds the request's series in memory, so the read limits,
// which bound requests by how many run and not by what they hold, aren't
// enough on their own.
func (i *Ingester) startSeriesCounts(ctx context.Context) (func(), error) {
	if i.seriesCountsGate == nil {
		return func() {}, nil
	}
	if err := i.seriesCountsGate.Start(ctx); err != nil {
		if errors.Is(err, gate.ErrMaxConcurrent) {
			i.seriesCountsMetrics.refusals.WithLabelValues(seriesCountsRefusedByGate).Inc()
			return nil, status.Errorf(codes.Unavailable, "too many cardinality estimate count walks running, more than -ingester.cardinality-estimate-max-concurrent %d", i.cfg.CardinalityEstimateMaxConcurrent)
		}
		// A cancelled request, as the walk returns it.
		return nil, err
	}
	return i.seriesCountsGate.Done, nil
}

// ingesterSeriesCostBytes is what a series counts for against max_index_bytes
// in the ingester. The head and the local blocks are read from memory, so no
// index bytes are read and the price stands for the work of walking one
// series: its labels and chunk metas, measured at about 170 nanoseconds, and
// 8 bytes of heap for its ref. It is 8 times lower than the 512 bytes the
// store-gateway prices a series at when it knows nothing about the block
// (tsdb.EstimatedSeriesP99Size), since a store-gateway reads those bytes from
// object storage. With the default limit of 2 GiB it allows about 33M series.
const ingesterSeriesCostBytes = 64

// ingesterMultiSourceSeriesCostBytes is what a series counts for against
// max_index_bytes when the walk covers more than one source, such as the head
// and a local block. Each series is then held before it is counted, to count
// it once across the sources: one gathered value, its map entry and the
// buckets its chunks touch (one word with no step), about 100 bytes of heap
// estimated from the structs, not measured.
// That is more than the walk price, so it replaces it. The price is charged
// per series ref of each source, which covers the 8-byte ref held for the
// walk, so a series in two sources pays twice. With the default limit of
// 2 GiB it allows about 21M refs. The real cost is to be measured on a large
// tenant (task M1) and this constant updated.
const ingesterMultiSourceSeriesCostBytes = 100

// seriesCountsMessageSize is about how large each message of a streamed
// SeriesCounts answer is.
const seriesCountsMessageSize = queryStreamBatchMessageSize

// seriesCountsWithStatus answers a SeriesCounts request as one response, the
// errors carrying their gRPC status.
func (i *Ingester) seriesCountsWithStatus(ctx context.Context, req *client.SeriesCountsRequest) (*client.SeriesCountsResponse, error) {
	resp, _, err := i.seriesCountsWalked(ctx, req)
	return resp, err
}

// seriesCountsWalked is seriesCountsWithStatus that also returns the series
// walked, the series the matchers selected, counted or not, which is logged
// and not sent.
func (i *Ingester) seriesCountsWalked(ctx context.Context, req *client.SeriesCountsRequest) (*client.SeriesCountsResponse, int64, error) {
	resp, walked, err := i.seriesCounts(ctx, req)
	if err == nil {
		return resp, walked, nil
	}
	// The request's own errors, InvalidArgument and the max_index_bytes
	// refusal, keep their codes; the caller maps them to 400 and 422.
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return nil, 0, err
	}
	return nil, 0, i.mapReadErrorToErrorWithStatus(err)
}

func (i *Ingester) seriesCounts(ctx context.Context, req *client.SeriesCountsRequest) (resp *client.SeriesCountsResponse, walked int64, err error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, 0, err
	}
	counter, err := cardinality.NewSeriesCounter(cardinality.CounterOptions{
		MinT:        req.StartTimestampMs,
		MaxT:        req.EndTimestampMs,
		Step:        req.StepMs,
		Window:      req.WindowMs,
		GroupBy:     req.GroupBy,
		Hashes:      req.Hashes,
		SketchAbove: req.SketchAbove,
	})
	if err != nil {
		return nil, 0, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.MaxIndexBytes < 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "max_index_bytes can't be negative")
	}
	if req.MaxHashes < 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "max_hashes can't be negative")
	}
	if req.MaxGroups < 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "max_groups can't be negative")
	}
	// The tenant's limit also bounds a call that sets no max_index_bytes,
	// or a larger one, whoever sends it; the hashes its memory limit lets
	// one estimate hold bound max_hashes the same way.
	maxIndexBytes := cardinality.BoundMaxIndexBytes(req.MaxIndexBytes, i.limits.CardinalityEstimateMaxIndexBytes(userID))
	req.MaxHashes = cardinality.BoundMaxHashes(req.MaxHashes, cardinality.HashBudget(i.limits.CardinalityEstimateMaxMemoryBytes(userID)))
	matchers, err := client.FromLabelMatchers(req.Matchers)
	if err != nil {
		return nil, 0, status.Error(codes.InvalidArgument, err.Error())
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, 0, err
	}
	db := i.getTSDB(userID)
	if db == nil {
		return &client.SeriesCountsResponse{WindowMs: req.WindowMs}, 0, nil
	}
	head := db.Head()
	resp = &client.SeriesCountsResponse{MinTimeMs: oldestHeld(db), ShardSizeUnchangedSinceMs: db.shardSizeUnchangedSince(), WindowMs: req.WindowMs}
	if before := req.ShardSizeUnchangedBeforeMs; before > 0 && (resp.ShardSizeUnchangedSinceMs == 0 || resp.ShardSizeUnchangedSinceMs >= before) {
		resp.ShardSizeChanged = true
		return resp, walked, nil
	}
	sources, err := countSources(db, req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		for _, src := range sources {
			src.close()
		}
	}()
	if len(sources) == 0 {
		return resp, walked, nil
	}

	// Only counting takes a slot: waiting for read consistency, the answers
	// above and sending the response hold no series.
	done, err := i.startSeriesCounts(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer done()
	// What the walk did, for the metrics and, when it was refused, an info
	// log line; an answered walk logs at debug level only.
	walkStart := time.Now()
	var (
		seriesWalked int64
		// walking is set once the limits checked before the walk passed,
		// so the walk duration is only of walks.
		walking bool
	)
	defer func() {
		m := i.seriesCountsMetrics
		m.seriesWalked.Add(float64(seriesWalked))
		if walking {
			m.walkDuration.WithLabelValues(seriesCountsSources(sources)).Observe(time.Since(walkStart).Seconds())
		}
		if err == nil {
			return
		}
		msg := status.Convert(err).Message()
		// A refusal names its limit in its status detail.
		limit := cardinality.RefusedBy(err)
		if limit != "" {
			m.refusals.WithLabelValues(limit).Inc()
			level.Info(util_log.WithContext(ctx, i.logger)).Log("msg", "series counts refused", "limit", limit, "sources", seriesCountsSources(sources), "series_walked", seriesWalked, "err", msg)
		}
	}()

	// Every series of a local block has a chunk in the block, so when the
	// block is the only source and sits inside the window, and nothing else
	// is asked for, each group's count is its postings list's length and no
	// series is walked. The head is always walked: it can hold series with
	// no chunk, left by a rolled-back append until the next garbage
	// collection or kept for an exemplar, which the walk doesn't count. Two
	// sources can hold the same series, so they are walked and counted by
	// hash.
	if src := sources[0]; len(sources) == 1 && src.block && len(matchers) == 0 && req.StepMs == 0 && !req.Hashes &&
		src.blockMinT >= req.StartTimestampMs && src.blockMaxT < req.EndTimestampMs {
		if err := countFromPostings(ctx, src.idx, counter.GroupBy(), req.MaxGroups, resp); err != nil {
			return nil, 0, err
		}
		return resp, walked, nil
	}

	// Resolve every source's postings first, so max_index_bytes is checked
	// against the whole request before any series is walked. The series are
	// priced at ingesterSeriesCostBytes each, or
	// ingesterMultiSourceSeriesCostBytes when several sources are gathered,
	// so the limit allows maxSeries of them. With no matchers every series of
	// a source matches, and the count is known without reading its postings.
	// Otherwise the postings are expanded only up to what the limit allows,
	// so a refused request holds at most maxSeries refs instead of every
	// matching series.
	seriesCost := int64(ingesterSeriesCostBytes)
	if len(sources) > 1 {
		seriesCost = ingesterMultiSourceSeriesCostBytes
	}
	maxSeries := int64(math.MaxInt64)
	if maxIndexBytes > 0 {
		maxSeries = maxIndexBytes / seriesCost
	}
	if maxIndexBytes > 0 && len(matchers) == 0 {
		var all int64
		for _, src := range sources {
			all += src.numSeries
		}
		if all > maxSeries {
			return nil, 0, cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would walk %d series in %d head and local blocks, about %d bytes at %d bytes each, more than max_index_bytes %d", all, len(sources), all*seriesCost, seriesCost, maxIndexBytes)
		}
	}
	// With no matchers, every series of a local block inside the window is
	// counted, and every value of the group-by label in a block's index has
	// a series, so the block's values are a lower bound on the groups. The
	// head's values are not: a head series may have no chunk in the window.
	if req.MaxGroups > 0 && len(matchers) == 0 {
		for _, src := range sources {
			if !src.block || src.blockMinT < req.StartTimestampMs || src.blockMaxT >= req.EndTimestampMs {
				continue
			}
			values, err := src.idx.LabelValues(ctx, counter.GroupBy(), nil)
			if err != nil {
				return nil, 0, err
			}
			if err := checkGroupCount(req.MaxGroups, counter.GroupBy(), len(values)); err != nil {
				return nil, 0, err
			}
		}
	}
	// One span for resolving the sources' postings and one for the walk.
	// Ending a span twice is a no-op, so the deferred End covers every
	// return, and the span a refusal or failure ended in records it.
	pctx, postingsSpan := tracer.Start(ctx, "Ingester.SeriesCounts postings")
	defer postingsSpan.End()
	current := postingsSpan
	defer func() {
		if err != nil {
			current.RecordError(err)
		}
	}()
	postingsSpan.SetAttributes(attribute.Int("sources", len(sources)))
	var total int64
	for _, src := range sources {
		var postings index.Postings
		if len(matchers) == 0 {
			name, value := index.AllPostingsKey()
			postings, err = src.idx.Postings(pctx, name, value)
		} else {
			postings, err = tsdb.PostingsForMatchers(pctx, src.idx, matchers...)
		}
		if err != nil {
			return nil, 0, err
		}
		refs, exceeded, err := expandPostingsUpTo(pctx, postings, maxSeries-total)
		if err != nil {
			return nil, 0, err
		}
		if exceeded {
			return nil, 0, cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would walk more than %d series in %d head and local blocks, at %d bytes each more than max_index_bytes %d", maxSeries, len(sources), seriesCost, maxIndexBytes)
		}
		src.refs = refs
		total += int64(len(refs))
	}
	postingsSpan.SetAttributes(attribute.Int64("series_matched_summed_over_sources", total))
	// Asked for hashes, the ingester holds one per distinct matching series
	// with a chunk in the window until it answers. Before the walk it refuses
	// a request when one local block inside the window already matches more
	// series than max_hashes: every series of such a block has a chunk in the
	// window, and a series can be in the head and in every block, so the
	// largest is a lower bound on the distinct series. The head is left out,
	// since it can hold series with no chunk in the window. The walk then
	// refuses once the series counted pass max_hashes.
	if req.Hashes && req.MaxHashes > 0 {
		var largest int64
		for _, src := range sources {
			if src.block && src.blockMinT >= req.StartTimestampMs && src.blockMaxT < req.EndTimestampMs {
				largest = max(largest, int64(len(src.refs)))
			}
		}
		if largest > req.MaxHashes {
			return nil, 0, cardinality.MaxHashesRefusal(largest, req.MaxHashes)
		}
	}

	postingsSpan.End()
	walking = true
	wctx, walkSpan := tracer.Start(ctx, "Ingester.SeriesCounts walk")
	defer walkSpan.End()
	current = walkSpan
	ctx = wctx

	// One source is counted as it is walked. Several hold the same series
	// over different times, so the buckets each series' chunks touch are
	// gathered across them first, by label-set hash, and the series is
	// counted once. A series with no chunk in the window is not gathered.
	type gathered struct {
		value   string
		buckets cardinality.SeriesBuckets
	}
	var (
		byHash     map[uint64]*gathered
		builder    labels.ScratchBuilder
		metas      []chunks.Meta
		ranges     []cardinality.ChunkRange
		oooBuilder labels.ScratchBuilder
		oooMetas   []chunks.Meta
	)
	values := map[string]string{}
	if len(sources) > 1 {
		byHash = map[uint64]*gathered{}
	}
	for _, src := range sources {
		for n, ref := range src.refs {
			if n%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, 0, err
				}
			}
			var found bool
			ranges, found, err = appendSeriesRanges(ranges[:0], src, ref, req, &builder, &metas, &oooBuilder, &oooMetas)
			if err != nil {
				return nil, 0, err
			}
			if !found {
				// Removed by a head compaction since the postings were read.
				continue
			}
			walked++
			seriesWalked++
			if byHash == nil {
				counter.Add(builder.Labels(), ranges)
				if err := checkCountedHashes(req, counter); err != nil {
					return nil, 0, err
				}
				if err := checkGroupCount(req.MaxGroups, counter.GroupBy(), counter.GroupCount()); err != nil {
					return nil, 0, err
				}
				continue
			}
			if !counter.InWindow(ranges) {
				continue
			}
			lset := builder.Labels()
			h := labels.StableHash(lset)
			g := byHash[h]
			if g == nil {
				// The value is a substring of the series' labels, which a
				// block's reader allocates for each series, so it is
				// interned instead of holding those labels for the walk.
				v := lset.Get(counter.GroupBy())
				if interned, ok := values[v]; ok {
					v = interned
				} else {
					v = strings.Clone(v)
					values[v] = v
					// Each value gathered becomes a group.
					if err := checkGroupCount(req.MaxGroups, counter.GroupBy(), len(values)); err != nil {
						return nil, 0, err
					}
				}
				g = &gathered{value: v}
				byHash[h] = g
			}
			g.buckets = counter.MarkBuckets(g.buckets, ranges)
		}
	}
	for h, g := range byHash {
		counter.AddGroupBuckets(g.value, h, g.buckets)
		if err := checkCountedHashes(req, counter); err != nil {
			return nil, 0, err
		}
	}

	groups, err := counter.Groups()
	if err != nil {
		return nil, 0, err
	}
	resp.SeriesCounted = counter.Counted()
	walkSpan.SetAttributes(attribute.Int("sources", len(sources)), attribute.Int64("series_walked", walked), attribute.Int64("series_counted", resp.SeriesCounted))
	level.Debug(util_log.WithContext(ctx, i.logger)).Log("msg", "series counts", "series_walked", walked, "series_counted", resp.SeriesCounted, "head_min_time_ms", head.MinTime(), "head_max_time_ms", head.MaxTime())
	resp.Groups = make([]*client.SeriesCountGroup, 0, len(groups))
	for _, g := range groups {
		resp.Groups = append(resp.Groups, &client.SeriesCountGroup{Value: g.Value, Counts: g.Counts, Hashes: g.Hashes, Sketch: g.Sketch})
	}
	return resp, walked, nil
}

// checkCountedHashes refuses a request for hashes once the series counter
// holds more than its max_hashes: each series it counted is a distinct hash,
// since a source holds a series once and several sources are counted by
// hash.
func checkCountedHashes(req *client.SeriesCountsRequest, counter *cardinality.SeriesCounter) error {
	if !req.Hashes || req.MaxHashes <= 0 || counter.Counted() <= req.MaxHashes {
		return nil
	}
	return cardinality.MaxHashesRefusal(counter.Counted(), req.MaxHashes)
}

// checkGroupCount refuses a request once groups, the distinct values of
// groupBy counted or known to be counted, are more than maxGroups. The
// refusal names max_groups, which the querier reads the limit from, as the
// store-gateway's does.
func checkGroupCount(maxGroups int64, groupBy string, groups int) error {
	if maxGroups <= 0 || int64(groups) <= maxGroups {
		return nil
	}
	return cardinality.Refusalf(cardinality.LimitMaxGroups, "the head and local blocks hold at least %d distinct values of %s, more than max_groups %d", groups, groupBy, maxGroups)
}

// appendSeriesRanges appends the time ranges of the chunks of the series
// ref in src: its out-of-order chunks when src reads them, then its chunks
// in src's index, and sets builder to its labels. The open head chunk has
// no end yet, its meta says MaxInt64, so its last sample is read when the
// end matters, which is when the chunk starts before the window or the
// count is per bucket. A series removed by a head compaction since the
// postings were read is reported as not found, whether its index entry or
// its open chunk is gone, and is not an error.
func appendSeriesRanges(ranges []cardinality.ChunkRange, src *countSource, ref storage.SeriesRef, req *client.SeriesCountsRequest, builder *labels.ScratchBuilder, metas *[]chunks.Meta, oooBuilder *labels.ScratchBuilder, oooMetas *[]chunks.Meta) (_ []cardinality.ChunkRange, found bool, err error) {
	if err := src.idx.Series(ref, builder, metas); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return ranges, false, nil
		}
		return ranges, false, err
	}
	if src.ooo != nil {
		if ranges, err = appendOutOfOrderRanges(ranges, src.ooo, ref, oooBuilder, oooMetas); err != nil {
			return ranges, false, err
		}
	}
	for _, m := range *metas {
		maxT := m.MaxTime
		if maxT == math.MaxInt64 {
			if m.MinTime < req.StartTimestampMs || req.StepMs > 0 {
				if maxT, err = lastSampleTime(src.chunkr, m); err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						return ranges, false, nil
					}
					return ranges, false, err
				}
			} else {
				maxT = req.EndTimestampMs - 1
			}
		}
		ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: maxT})
	}
	return ranges, true, nil
}

// appendOutOfOrderRanges appends the time ranges of the head series ref's
// out-of-order chunks in the window, read from ooo. Their metas carry their
// real end, merged where they overlap. A series removed by a head
// compaction since its in-order chunks were read has none: the reader
// returns ErrNotFound without touching metas, so they are cleared first and
// the previous series' chunks are not taken for this one's.
func appendOutOfOrderRanges(ranges []cardinality.ChunkRange, ooo tsdb.IndexReader, ref storage.SeriesRef, builder *labels.ScratchBuilder, metas *[]chunks.Meta) ([]cardinality.ChunkRange, error) {
	*metas = (*metas)[:0]
	if err := ooo.Series(ref, builder, metas); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return ranges, nil
		}
		return ranges, err
	}
	for _, m := range *metas {
		ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: m.MaxTime})
	}
	return ranges, nil
}

// expandPostingsUpTo returns the series refs of p, and stops without
// returning them once more than max would be held, which it reports as
// exceeded. So a request the limit refuses holds at most max refs.
func expandPostingsUpTo(ctx context.Context, p index.Postings, max int64) (refs []storage.SeriesRef, exceeded bool, err error) {
	for p.Next() {
		if int64(len(refs)) >= max {
			return nil, true, nil
		}
		refs = append(refs, p.At())
		if len(refs)%(1<<16) == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
	}
	return refs, false, p.Err()
}

// countFromPostings sets resp's groups to one count per value of groupBy,
// the length of its postings list in idx, and counts the series without the
// label under the empty value. It reads postings only, no series, so idx
// must not change while it reads: a block's index, not the head's. A
// positive maxGroups refuses the request before any postings list is read
// when the label has more values than that, and after counting when the
// series without the label make one group too many.
func countFromPostings(ctx context.Context, idx tsdb.IndexReader, groupBy string, maxGroups int64, resp *client.SeriesCountsResponse) error {
	countPostings := func(name, value string) (int64, error) {
		p, err := idx.Postings(ctx, name, value)
		if err != nil {
			return 0, err
		}
		var n int64
		for p.Next() {
			n++
			if n%(1<<16) == 0 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
		}
		return n, p.Err()
	}
	allName, allValue := index.AllPostingsKey()
	all, err := countPostings(allName, allValue)
	if err != nil {
		return err
	}
	values, err := idx.SortedLabelValues(ctx, groupBy, nil)
	if err != nil {
		return err
	}
	if err := checkGroupCount(maxGroups, groupBy, len(values)); err != nil {
		return err
	}
	// The first slot is for the series without the label.
	groups := make([]*client.SeriesCountGroup, 1, len(values)+1)
	withLabel := int64(0)
	for j, v := range values {
		if j%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		n, err := countPostings(groupBy, v)
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		withLabel += n
		groups = append(groups, &client.SeriesCountGroup{Value: v, Counts: []int64{n}})
	}
	if without := all - withLabel; without > 0 {
		groups[0] = &client.SeriesCountGroup{Value: "", Counts: []int64{without}}
	} else {
		groups = groups[1:]
	}
	if err := checkGroupCount(maxGroups, groupBy, len(groups)); err != nil {
		return err
	}
	resp.Groups = groups
	resp.SeriesCounted = all
	return nil
}

// lastSampleTime is the timestamp of the last sample in the head chunk
// meta points to. An empty chunk reports its start.
func lastSampleTime(chunkr tsdb.ChunkReader, meta chunks.Meta) (int64, error) {
	chk, iterable, err := chunkr.ChunkOrIterable(meta)
	if err != nil {
		return 0, err
	}
	var it chunkenc.Iterator
	if chk != nil {
		it = chk.Iterator(nil)
	} else {
		it = iterable.Iterator(nil)
	}
	last := meta.MinTime
	for it.Next() != chunkenc.ValNone {
		last = it.AtT()
	}
	return last, it.Err()
}

// oldestHeld is the oldest time db holds for its tenant, the start of its
// head or of its oldest local block, or 0 when it holds no series and no
// block.
func oldestHeld(db *userTSDB) int64 {
	oldest := int64(math.MaxInt64)
	if head := db.Head(); head.NumSeries() > 0 {
		oldest = head.MinTime()
	}
	for _, b := range db.Blocks() {
		oldest = min(oldest, b.MinTime())
	}
	if oldest == math.MaxInt64 {
		return 0
	}
	return oldest
}

// countSource is one place an ingester holds a tenant's series: its head or
// one of its compacted local blocks.
type countSource struct {
	// numSeries is how many series the source holds, whatever the window: a
	// request with no matchers walks all of them.
	numSeries int64
	// block is set for a local block, whose bounds are blockMinT and
	// blockMaxT, both included.
	block                bool
	blockMinT, blockMaxT int64
	idx                  tsdb.IndexReader
	chunkr               tsdb.ChunkReader // head only, to find the open chunk's end
	// ooo reads the head's out-of-order chunks, head only, when the
	// out-of-order head overlaps the window. Its refs are the head's.
	ooo   tsdb.IndexReader
	refs  []storage.SeriesRef
	close func()
}

// countSourcesBetweenReadsHook, if set, runs after countSources opens the
// head and before it lists the blocks. Tests use it to run a head
// compaction between the two reads.
var countSourcesBetweenReadsHook func()

// countSources opens the index of every local block and of the head that
// overlaps [minT, maxT), oldest first. The head's index keeps only the
// chunks inside the window; a block's index returns all of a series'
// chunks, which the counter filters.
func countSources(db *userTSDB, minT, maxT int64) (out []*countSource, err error) {
	defer func() {
		if err != nil {
			for _, src := range out {
				src.close()
			}
			out = nil
		}
	}()
	// Read the head first. Opening its chunk reader registers a read in the
	// head's isolation state, and every head truncation or series eviction
	// waits for overlapping reads before it removes data
	// (WaitForPendingReadersInTimeRange). Any compaction whose block is not
	// in the list read below therefore can't remove its data from the head
	// until this request is done; one whose block is listed may already
	// have, and the block holds it. A series in both counts once.
	var headSrc *countSource
	head := db.Head()
	inOrder := head.MaxTime() >= minT && head.MinTime() < maxT
	outOfOrder := head.MinOOOTime() < maxT && head.MaxOOOTime() >= minT
	if head.NumSeries() > 0 && (inOrder || outOfOrder) {
		// The window end is exclusive and the range head's is not.
		rh := tsdb.NewRangeHead(head, minT, maxT-1)
		chunkr, err := rh.Chunks()
		if err != nil {
			return out, err
		}
		idx, err := rh.Index()
		if err != nil {
			_ = chunkr.Close()
			return out, err
		}
		headSrc = &countSource{numSeries: int64(head.NumSeries()), idx: idx, chunkr: chunkr, close: func() { _ = idx.Close(); _ = chunkr.Close() }}
		if outOfOrder {
			// In-order chunks are left out: an in-order start of MaxInt64
			// selects none, so only out-of-order chunks come back. Only
			// chunk metas are read, not chunk data, so no out-of-order
			// isolation is taken; a chunk already compacted into a local
			// block is listed in both and the series counts once.
			headSrc.ooo = tsdb.NewHeadAndOOOIndexReader(head, math.MaxInt64, minT, maxT-1, 0)
		}
		out = append(out, headSrc)
	}
	if countSourcesBetweenReadsHook != nil {
		countSourcesBetweenReadsHook()
	}
	for _, b := range db.Blocks() {
		if b.MaxTime() <= minT || b.MinTime() >= maxT {
			continue
		}
		idx, err := b.Index()
		if err != nil {
			return out, err
		}
		// A block's max time is exclusive.
		out = append(out, &countSource{numSeries: int64(b.Meta().Stats.NumSeries), block: true, blockMinT: b.MinTime(), blockMaxT: b.MaxTime() - 1, idx: idx, close: func() { _ = idx.Close() }})
	}
	return out, nil
}
