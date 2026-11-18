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
	"github.com/grafana/dskit/grpcutil"
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
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/ingester/client"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// SeriesCounts counts the series matching the request's matchers that have a
// chunk in its window, grouped by one label, in the head (out-of-order chunks
// included) and the local blocks overlapping the window. A series in several
// sources counts once. A request over max_groups, or that would hold more than
// the tenant's cardinality estimate memory limit, is refused. Experimental.
func (i *Ingester) SeriesCounts(req *client.SeriesCountsRequest, stream client.Ingester_SeriesCountsServer) error {
	if err := i.checkSeriesCountsEnabled(stream.Context()); err != nil {
		return err
	}
	resp, _, err := i.seriesCounts(stream.Context(), req)
	if err != nil {
		return err
	}
	return client.SendSeriesCountsResponse(stream, resp)
}

// checkSeriesCountsEnabled refuses a tenant that has not enabled the cardinality estimate.
func (i *Ingester) checkSeriesCountsEnabled(ctx context.Context) error {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	if !i.limits.CardinalityEstimateEnabled(userID) {
		return status.Error(codes.FailedPrecondition, cardinality.DisabledError(userID))
	}
	return nil
}

type seriesCountsMetrics struct {
	seriesWalked prometheus.Counter
	walkDuration *prometheus.HistogramVec
	refusals     *prometheus.CounterVec
}

// Refusal reasons of the count refusals counter.
const (
	seriesCountsRefusedByMaxGroups = cardinality.LimitMaxGroups
	seriesCountsRefusedByMemory    = cardinality.LimitMemory
	seriesCountsRefusedByGate      = "gate"
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
			Help: "Cardinality estimate count requests the ingester refused, by limit: max_groups, memory, or gate when -ingester.cardinality-estimate-max-concurrent walks were running.",
		}, []string{"limit"}),
	}
}

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

// newSeriesCountsGate returns the gate on concurrent count walks, nil for no limit. It refuses without waiting.
func newSeriesCountsGate(reg prometheus.Registerer, maxConcurrent int) gate.Gate {
	if maxConcurrent <= 0 {
		return nil
	}
	gateReg := prometheus.WrapRegistererWith(prometheus.Labels{"gate": "cardinality_estimate"}, prometheus.WrapRegistererWithPrefix("cortex_ingester_", reg))
	return gate.NewInstrumented(gateReg, maxConcurrent, gate.NewRejecting(maxConcurrent))
}

// startSeriesCounts takes a slot for a count walk, which the returned function frees. It returns
// Unavailable without waiting when all slots are taken, so the distributor can ask another zone.
func (i *Ingester) startSeriesCounts(ctx context.Context) (func(), error) {
	if i.seriesCountsGate == nil {
		return func() {}, nil
	}
	if err := i.seriesCountsGate.Start(ctx); err != nil {
		if errors.Is(err, gate.ErrMaxConcurrent) {
			i.seriesCountsMetrics.refusals.WithLabelValues(seriesCountsRefusedByGate).Inc()
			return nil, cardinality.Busyf("too many cardinality estimate count walks running, more than -ingester.cardinality-estimate-max-concurrent %d", i.cfg.CardinalityEstimateMaxConcurrent)
		}
		// A cancelled request, as the walk returns it.
		return nil, err
	}
	return i.seriesCountsGate.Done, nil
}

// What a walk holds for each series, measured by BenchmarkSeriesCountsWalk_HeldBytes.
const (
	// seriesRefBytes is one resolved series ref.
	seriesRefBytes = 8
	// gatheredSeriesBytes is one series gathered by hash, map entry included. Measured at 56.
	gatheredSeriesBytes = 56
	// countedGroupBytes is one group of counts, before its value.
	countedGroupBytes = 64
)

// seriesCounts answers a SeriesCounts request, with errors carrying their gRPC status. It also returns
// the series walked, which is logged and not sent.
func (i *Ingester) seriesCounts(ctx context.Context, req *client.SeriesCountsRequest) (resp *client.SeriesCountsResponse, walked int64, err error) {
	// The request's own errors keep their codes.
	defer func() {
		if err == nil {
			return
		}
		walked = 0
		if st, ok := grpcutil.ErrorToStatus(err); !ok || st.Code() == codes.Unknown {
			err = i.mapReadErrorToErrorWithStatus(err)
		}
	}()
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, 0, err
	}
	w, err := i.newSeriesCountsWalk(userID, req)
	if err != nil {
		return nil, 0, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, 0, err
	}
	db := i.getTSDB(userID)
	if db == nil {
		return &client.SeriesCountsResponse{}, 0, nil
	}
	head := db.db.Head()
	resp = &client.SeriesCountsResponse{MinTimeMs: oldestHeld(db)}
	sources, err := i.countSources(db, req.StartMs, req.EndMs)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		for _, src := range sources {
			src.close()
		}
	}()
	if len(sources) == 0 {
		return resp, 0, nil
	}

	// Only counting takes a slot.
	done, err := i.startSeriesCounts(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer done()
	walkStart := time.Now()
	// walking is set once the limits checked before the walk passed.
	var walking bool
	defer func() {
		m := i.seriesCountsMetrics
		m.seriesWalked.Add(float64(w.walked))
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
			level.Info(util_log.WithContext(ctx, i.logger)).Log("msg", "series counts refused", "limit", limit, "sources", seriesCountsSources(sources), "series_walked", w.walked, "err", msg)
		}
	}()

	if ok, err := w.countFromPostingsOnly(ctx, sources, resp); ok || err != nil {
		if err != nil {
			return nil, 0, err
		}
		return resp, 0, nil
	}
	if err := w.boundGroups(ctx, sources); err != nil {
		return nil, 0, err
	}
	// One span for resolving the postings and one for the walk. Ending a span twice is a no-op.
	pctx, postingsSpan := tracer.Start(ctx, "Ingester.SeriesCounts postings")
	defer postingsSpan.End()
	current := postingsSpan
	defer func() {
		if err != nil {
			current.RecordError(err)
		}
	}()
	postingsSpan.SetAttributes(attribute.Int("sources", len(sources)))
	total, err := w.resolvePostings(pctx, sources)
	if err != nil {
		return nil, 0, err
	}
	postingsSpan.SetAttributes(attribute.Int64("series_matched_summed_over_sources", total))
	postingsSpan.End()

	walking = true
	wctx, walkSpan := tracer.Start(ctx, "Ingester.SeriesCounts walk")
	defer walkSpan.End()
	current = walkSpan
	if err := w.walk(wctx, sources); err != nil {
		return nil, 0, err
	}
	if err := w.finish(); err != nil {
		return nil, 0, err
	}
	resp.Groups = w.counter.Groups()
	resp.SeriesCounted = w.counter.Counted()
	walkSpan.SetAttributes(attribute.Int("sources", len(sources)), attribute.Int64("series_walked", w.walked), attribute.Int64("series_counted", resp.SeriesCounted))
	level.Debug(util_log.WithContext(wctx, i.logger)).Log("msg", "series counts", "series_walked", w.walked, "series_counted", resp.SeriesCounted, "head_min_time_ms", head.MinTime(), "head_max_time_ms", head.MaxTime())
	return resp, w.walked, nil
}

// seriesCountsWalk is one SeriesCounts request's walk over the head and local blocks.
type seriesCountsWalk struct {
	req         *client.SeriesCountsRequest
	counterOpts cardinality.CounterOptions
	counter     *cardinality.SeriesCounter
	matchers    []*labels.Matcher
	// walked is the series walked so far, counted or not.
	walked int64
	// byHash holds the group-by value of each series in the window by label-set hash until finish
	// counts them. It is set when the walk covers several sources, so a series in more than one counts once.
	byHash map[uint64]string
	// values interns the group-by values of byHash.
	values map[string]string
	// maxMemoryBytes is the tenant's memory limit, 0 for none. It bounds heldBytes: the series refs,
	// the series gathered by hash, and the groups and hashes counted that the walk holds.
	maxMemoryBytes uint64
	heldBytes      uint64
}

// hold adds b bytes to what the walk holds and refuses the request once that is over the tenant's memory limit.
func (w *seriesCountsWalk) hold(b uint64) error {
	w.heldBytes += b
	if w.maxMemoryBytes > 0 && w.heldBytes > w.maxMemoryBytes {
		return cardinality.Refusalf(cardinality.LimitMemory, "the request would hold more than %d bytes of series, groups and hashes in the ingester while counting, the tenant's cardinality estimate memory limit", w.maxMemoryBytes)
	}
	return nil
}

// newSeriesCountsWalk validates req. max_groups is the caller's to set: the querier bounds it by the tenant's.
func (i *Ingester) newSeriesCountsWalk(userID string, req *client.SeriesCountsRequest) (*seriesCountsWalk, error) {
	counterOpts, counter, matchers, err := cardinality.CounterForQuery(&req.CountQuery, req.MaxGroups)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &seriesCountsWalk{
		req:            req,
		counterOpts:    counterOpts,
		counter:        counter,
		matchers:       matchers,
		maxMemoryBytes: i.limits.CardinalityEstimateMaxMemoryBytes(userID),
	}, nil
}

// countFromPostingsOnly counts a request that needs no walk and reports whether it did. That is
// a single local block inside the window, with no matchers or hashes. The head is always
// walked, since it can hold series with no chunk.
func (w *seriesCountsWalk) countFromPostingsOnly(ctx context.Context, sources []*countSource, resp *client.SeriesCountsResponse) (bool, error) {
	req := w.req
	src := sources[0]
	if len(sources) != 1 || !src.block || len(w.matchers) > 0 || req.Hashes ||
		src.blockMinT < req.StartMs || src.blockMaxT >= req.EndMs {
		return false, nil
	}
	return true, countFromPostings(ctx, src.idx, w.counter.GroupBy(), req.MaxGroups, w.hold, resp)
}

// boundGroups refuses a request whose local blocks inside the window already hold more groups than
// max_groups. The head is left out, since a head series may have no chunk in the window.
func (w *seriesCountsWalk) boundGroups(ctx context.Context, sources []*countSource) error {
	req := w.req
	if req.MaxGroups <= 0 || len(w.matchers) > 0 {
		return nil
	}
	for _, src := range sources {
		if !src.block || src.blockMinT < req.StartMs || src.blockMaxT >= req.EndMs {
			continue
		}
		values, err := src.idx.LabelValues(ctx, w.counter.GroupBy(), nil)
		if err != nil {
			return err
		}
		if err := checkGroupCount(req.MaxGroups, w.counter.GroupBy(), len(values)); err != nil {
			return err
		}
	}
	return nil
}

// resolvePostings resolves every source's postings before any series is walked, so the memory limit
// is checked against the whole request. It returns the series matched, summed over the sources.
func (w *seriesCountsWalk) resolvePostings(ctx context.Context, sources []*countSource) (int64, error) {
	var total int64
	for _, src := range sources {
		var (
			postings index.Postings
			err      error
		)
		if len(w.matchers) == 0 {
			name, value := index.AllPostingsKey()
			postings, err = src.idx.Postings(ctx, name, value)
		} else {
			postings, err = tsdb.PostingsForMatchers(ctx, src.idx, w.matchers...)
		}
		if err != nil {
			return 0, err
		}
		// The refs are bounded by the memory limit before they are read.
		allowed := int64(math.MaxInt64)
		if w.maxMemoryBytes > 0 {
			allowed = int64(max(w.maxMemoryBytes, w.heldBytes)-w.heldBytes) / seriesRefBytes
		}
		refs, exceeded, err := expandPostingsUpTo(ctx, postings, allowed)
		if err != nil {
			return 0, err
		}
		if exceeded {
			return 0, w.hold(uint64(allowed+1) * seriesRefBytes)
		}
		if err := w.hold(uint64(len(refs)) * seriesRefBytes); err != nil {
			return 0, err
		}
		src.refs = refs
		total += int64(len(refs))
	}
	return total, nil
}

// walk counts the sources' resolved series. With several sources each series is gathered by
// label-set hash first, so it counts once. finish must follow.
func (w *seriesCountsWalk) walk(ctx context.Context, sources []*countSource) error {
	if len(sources) > 1 {
		w.gatherByHash()
	}
	return w.walkSources(ctx, sources)
}

func (w *seriesCountsWalk) gatherByHash() {
	w.byHash = map[uint64]string{}
	w.values = map[string]string{}
}

func (w *seriesCountsWalk) walkSources(ctx context.Context, sources []*countSource) error {
	req, counter, byHash := w.req, w.counter, w.byHash
	var (
		builder    labels.ScratchBuilder
		metas      []chunks.Meta
		ranges     []cardinality.ChunkRange
		oooBuilder labels.ScratchBuilder
		oooMetas   []chunks.Meta
		err        error
	)
	for _, src := range sources {
		for n, ref := range src.refs {
			if n%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			var found bool
			ranges, found, err = appendSeriesRanges(ranges[:0], src, ref, req, &builder, &metas, &oooBuilder, &oooMetas)
			if err != nil {
				return err
			}
			if !found {
				// Removed by a head compaction.
				continue
			}
			w.walked++
			if byHash == nil {
				lset := builder.Labels()
				groups, hashes := counter.GroupCount(), counter.HashesHeld()
				counter.Add(lset, ranges)
				if err := w.holdCounted(groups, hashes, lset.Get(counter.GroupBy())); err != nil {
					return err
				}
				continue
			}
			if !counter.InWindow(ranges) {
				continue
			}
			lset := builder.Labels()
			h := labels.StableHash(lset)
			if _, ok := byHash[h]; !ok {
				// The value is a substring of the series' labels, so it is cloned and interned.
				v := lset.Get(counter.GroupBy())
				if interned, ok := w.values[v]; ok {
					v = interned
				} else {
					v = strings.Clone(v)
					w.values[v] = v
					if err := checkGroupCount(req.MaxGroups, counter.GroupBy(), len(w.values)); err != nil {
						return err
					}
				}
				if err := w.hold(gatheredSeriesBytes); err != nil {
					return err
				}
				byHash[h] = v
			}
		}
	}
	return nil
}

// finish counts the series gathered by hash.
func (w *seriesCountsWalk) finish() error {
	for h, v := range w.byHash {
		groups, hashes := w.counter.GroupCount(), w.counter.HashesHeld()
		w.counter.AddGroupInWindow(v, h)
		if err := w.holdCounted(groups, hashes, v); err != nil {
			return err
		}
	}
	return nil
}

// holdCounted checks max_groups and holds what the counter added since it held groupsBefore groups
// and hashesBefore hashes, value being the last group-by value added.
func (w *seriesCountsWalk) holdCounted(groupsBefore int, hashesBefore int64, value string) error {
	groups := w.counter.GroupCount()
	if err := checkGroupCount(w.req.MaxGroups, w.counter.GroupBy(), groups); err != nil {
		return err
	}
	var b uint64
	if groups > groupsBefore {
		if w.req.Hashes {
			b += uint64(cardinality.GroupBytes + len(value))
		} else {
			b += uint64(countedGroupBytes + len(value))
		}
	}
	b += uint64(w.counter.HashesHeld()-hashesBefore) * cardinality.HashBytes
	if b == 0 {
		return nil
	}
	return w.hold(b)
}

// checkGroupCount refuses a request once groups, the distinct values of groupBy, are more than maxGroups.
func checkGroupCount(maxGroups int64, groupBy string, groups int) error {
	if maxGroups <= 0 || int64(groups) <= maxGroups {
		return nil
	}
	return cardinality.MaxGroupsRefusal("the head and local blocks hold at least", int64(groups), groupBy, maxGroups)
}

// appendSeriesRanges appends the time ranges of the chunks of series ref in src, out-of-order
// chunks first, and sets builder to its labels. The open head chunk's meta says MaxInt64, so its
// last sample is read when the end matters. A series removed by a head compaction, or left with no
// chunk once its open chunk is gone, is reported as not found, not as an error.
func appendSeriesRanges(ranges []cardinality.ChunkRange, src *countSource, ref storage.SeriesRef, req *client.SeriesCountsRequest, builder *labels.ScratchBuilder, metas *[]chunks.Meta, oooBuilder *labels.ScratchBuilder, oooMetas *[]chunks.Meta) (_ []cardinality.ChunkRange, found bool, err error) {
	if err := src.idx.Series(ref, builder, metas); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return ranges, false, nil
		}
		return ranges, false, err
	}
	held, openGone := len(ranges), false
	if src.ooo != nil {
		if ranges, err = appendOutOfOrderRanges(ranges, src.ooo, ref, oooBuilder, oooMetas); err != nil {
			return ranges, false, err
		}
	}
	for _, m := range *metas {
		maxT := m.MaxTime
		if maxT == math.MaxInt64 {
			if m.MinTime < req.StartMs {
				if maxT, err = lastSampleTime(src.chunkr, m); err != nil {
					if errors.Is(err, storage.ErrNotFound) {
						// Only the open chunk is gone: the chunks collected so far still count.
						openGone = true
						continue
					}
					return ranges, false, err
				}
			} else {
				maxT = req.EndMs - 1
			}
		}
		ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: maxT})
	}
	return ranges, !openGone || len(ranges) > held, nil
}

// appendOutOfOrderRanges appends the time ranges of the head series ref's out-of-order chunks in the
// window, read from ooo. The metas are cleared first, since the reader leaves them untouched on ErrNotFound.
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

// expandPostingsUpTo returns the series refs of p, or reports exceeded without them once more than max.
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

// countFromPostings sets resp's groups to one count per value of groupBy, the length of its
// postings list in idx, and counts the series without the label under the empty value. Each group is
// held with hold before it is kept. idx must
// not change while it reads: a block's index, not the head's.
func countFromPostings(ctx context.Context, idx tsdb.IndexReader, groupBy string, maxGroups int64, hold func(uint64) error, resp *client.SeriesCountsResponse) error {
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
	groups := make([]*cardinalitypb.SeriesCountGroup, 1, len(values)+1)
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
		if err := hold(uint64(countedGroupBytes + len(v))); err != nil {
			return err
		}
		withLabel += n
		groups = append(groups, &cardinalitypb.SeriesCountGroup{Value: v, Counts: []int64{n}})
	}
	if without := all - withLabel; without > 0 {
		groups[0] = &cardinalitypb.SeriesCountGroup{Value: "", Counts: []int64{without}}
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

// lastSampleTime is the timestamp of the last sample in the open head chunk meta points to. The
// head's chunk reader returns the chunk's max time with a copy of its bytes, so no sample is
// decoded. That time can be of a sample appended after the reader was opened.
func lastSampleTime(chunkr tsdb.ChunkReader, meta chunks.Meta) (int64, error) {
	if withCopy, ok := chunkr.(tsdb.ChunkReaderWithCopy); ok {
		chk, _, maxT, err := withCopy.ChunkOrIterableWithCopy(meta)
		if err != nil {
			return 0, err
		}
		if chk == nil {
			// The series was evicted from the head: no samples.
			return meta.MinTime, nil
		}
		return maxT, nil
	}
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

// oldestHeld is the oldest time db holds for its tenant, or 0 when it holds nothing.
func oldestHeld(db *userTSDB) int64 {
	oldest := int64(math.MaxInt64)
	if head := db.db.Head(); head.NumSeries() > 0 {
		oldest = min(head.MinTime(), head.MinOOOTime())
	}
	for _, b := range db.Blocks() {
		oldest = min(oldest, b.MinTime())
	}
	if oldest == math.MaxInt64 {
		return 0
	}
	return oldest
}

// countSource is the head or one of the local blocks.
type countSource struct {
	// numSeries is how many series the source holds, whatever the window.
	numSeries int64
	// block is set for a local block, whose bounds are blockMinT and blockMaxT, both included.
	block                bool
	blockMinT, blockMaxT int64
	idx                  tsdb.IndexReader
	chunkr               tsdb.ChunkReader // head only, to find the open chunk's end
	// ooo reads the head's out-of-order chunks, head only, when they overlap the window.
	ooo tsdb.IndexReader
	// refs are the series to walk, once resolved.
	refs  []storage.SeriesRef
	close func()
}

// countSources opens the index of the head and of every local block that overlaps [minT, maxT).
// The head's index keeps only the chunks inside the window; a block's returns all of them.
func (i *Ingester) countSources(db *userTSDB, minT, maxT int64) (out []*countSource, err error) {
	defer func() {
		if err != nil {
			for _, src := range out {
				src.close()
			}
			out = nil
		}
	}()
	// Read the head first: opening its chunk reader holds back head truncation, so data compacted
	// into a block not listed below stays in the head until this request is done.
	var headSrc *countSource
	head := db.db.Head()
	inOrder := head.MaxTime() >= minT && head.MinTime() < maxT
	outOfOrder := head.MinOOOTime() < maxT && head.MaxOOOTime() >= minT
	if head.NumSeries() > 0 && (inOrder || outOfOrder) {
		// The range head's end is inclusive.
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
			// An in-order start of MaxInt64 selects no in-order chunks. No out-of-order isolation
			// is taken, since tsdb does not export it, so a block an out-of-order head compaction
			// writes during the walk is not counted by it.
			headSrc.ooo = tsdb.NewHeadAndOOOIndexReader(head, math.MaxInt64, minT, maxT-1, 0)
		}
		out = append(out, headSrc)
	}
	if i.countSourcesBetweenReadsHook != nil {
		i.countSourcesBetweenReadsHook()
	}
	if out, err = appendBlockSources(out, db, minT, maxT); err != nil {
		return out, err
	}
	if i.countSourcesAfterReadsHook != nil {
		i.countSourcesAfterReadsHook()
	}
	return out, nil
}

// appendBlockSources appends a source for every local block that overlaps [minT, maxT).
func appendBlockSources(out []*countSource, db *userTSDB, minT, maxT int64) ([]*countSource, error) {
	for _, b := range db.Blocks() {
		if b.MaxTime() <= minT || b.MinTime() >= maxT {
			continue
		}
		idx, err := b.Index()
		if err != nil {
			return out, err
		}
		out = append(out, &countSource{numSeries: int64(b.Meta().Stats.NumSeries), block: true, blockMinT: b.MinTime(), blockMaxT: b.MaxTime() - 1, idx: idx, close: func() { _ = idx.Close() }})
	}
	return out, nil
}
