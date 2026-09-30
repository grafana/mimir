// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdbchunks "github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// headUnderTest is what the differential tests drive on the Prometheus TSDB and on the engine: the
// per-tenant TSDB methods Mimir's ingester uses.
type headUnderTest interface {
	Appender(context.Context) EngineAppender
	ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	Querier(mint, maxt int64) (storage.Querier, error)
	ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error)
	Index() (tsdb.IndexReader, error)
	NumSeries() uint64
	MinTime() int64
	MaxTime() int64
	MinOOOTime() int64
	MaxOOOTime() int64
	AppendableMinValidTime() (int64, bool)
	ForEachSecondaryHash(func([]tsdbchunks.HeadSeriesRef, []uint32))
	ForEachShardHash(func([]storage.SeriesRef, []uint64))
	Compact(context.Context) error
	CompactHead(mint, maxt int64) error
	CompactOOOHead(context.Context) error
	CompactSelectedSeries([]storage.SeriesRef) error
	Close() error
}

type prometheusHead struct{ db *tsdb.DB }

func (p prometheusHead) Appender(ctx context.Context) EngineAppender {
	return p.db.Appender(ctx).(EngineAppender)
}
func (p prometheusHead) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return p.db.UnorderedChunkQuerier(mint, maxt)
}
func (p prometheusHead) Querier(mint, maxt int64) (storage.Querier, error) {
	return p.db.Querier(mint, maxt)
}
func (p prometheusHead) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return p.db.ExemplarQuerier(ctx)
}
func (p prometheusHead) Index() (tsdb.IndexReader, error) { return p.db.Head().Index() }
func (p prometheusHead) NumSeries() uint64                { return p.db.Head().NumSeries() }
func (p prometheusHead) MinTime() int64                   { return p.db.Head().MinTime() }
func (p prometheusHead) MaxTime() int64                   { return p.db.Head().MaxTime() }
func (p prometheusHead) MinOOOTime() int64                { return p.db.Head().MinOOOTime() }
func (p prometheusHead) MaxOOOTime() int64                { return p.db.Head().MaxOOOTime() }
func (p prometheusHead) AppendableMinValidTime() (int64, bool) {
	return p.db.Head().AppendableMinValidTime()
}
func (p prometheusHead) ForEachSecondaryHash(fn func([]tsdbchunks.HeadSeriesRef, []uint32)) {
	p.db.Head().ForEachSecondaryHash(fn)
}
func (p prometheusHead) ForEachShardHash(fn func([]storage.SeriesRef, []uint64)) {
	p.db.Head().ForEachShardHash(fn)
}
func (p prometheusHead) Compact(ctx context.Context) error { return p.db.Compact(ctx) }
func (p prometheusHead) CompactHead(mint, maxt int64) error {
	return p.db.CompactHead(tsdb.NewRangeHead(p.db.Head(), mint, maxt))
}
func (p prometheusHead) CompactOOOHead(ctx context.Context) error { return p.db.CompactOOOHead(ctx) }
func (p prometheusHead) CompactSelectedSeries(refs []storage.SeriesRef) error {
	return p.db.CompactSelectedSeries(refs)
}
func (p prometheusHead) Close() error { return p.db.Close() }

// recordingCallback records series creations and deletions, and can refuse creations like the
// ingester's series limits.
type recordingCallback struct {
	mu      sync.Mutex
	limit   int
	live    map[string]int
	created int
	deleted int
}

var errSeriesLimit = fmt.Errorf("per-user series limit reached")

func newRecordingCallback(limit int) *recordingCallback {
	return &recordingCallback{limit: limit, live: map[string]int{}}
}

func (c *recordingCallback) PreCreation(promlabels.Labels) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limit > 0 && c.created-c.deleted >= c.limit {
		return errSeriesLimit
	}
	return nil
}

func (c *recordingCallback) PostCreation(lset promlabels.Labels) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created++
	c.live[lset.String()]++
}

func (c *recordingCallback) PostDeletion(deleted map[tsdbchunks.HeadSeriesRef]promlabels.Labels) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lset := range deleted {
		c.deleted++
		c.live[lset.String()]--
		if c.live[lset.String()] == 0 {
			delete(c.live, lset.String())
		}
	}
}

func (c *recordingCallback) liveSeries() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.live))
	for name := range c.live {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// traceSeries logs the calls made for the series whose labels contain it.
var traceSeries = os.Getenv("ENGINE_TRACE_SERIES")

// conflicts are the differential workload's timestamps appended again with another value or type,
// by series: the in-order and out-of-order copies are both stored, and which one a query reads is
// up to how each TSDB orders the chunks of its head and blocks (see
// TestEngineConflictingDuplicates), so queried samples are compared without them.
var conflicts *appendHistory

type appendHistory struct {
	appended  map[string]map[int64]string
	conflicts map[string]map[int64]bool
}

func newAppendHistory() *appendHistory {
	return &appendHistory{appended: map[string]map[int64]string{}, conflicts: map[string]map[int64]bool{}}
}

func (h *appendHistory) record(lset promlabels.Labels, ts int64, content string) {
	series := lset.String()
	if h.appended[series] == nil {
		h.appended[series] = map[int64]string{}
	}
	if previous, ok := h.appended[series][ts]; ok && previous != content {
		if h.conflicts[series] == nil {
			h.conflicts[series] = map[int64]bool{}
		}
		h.conflicts[series][ts] = true
	}
	h.appended[series][ts] = content
}

type differentialOptions struct {
	oooWindowMs  int64
	maxExemplars int64
	timely       bool
	seriesLimit  int
}

func secondaryHash(lset promlabels.Labels) uint32 {
	return mimirpb.ShardByAllLabels("tenant", lset)
}

// openPrometheus opens the reference TSDB in dir, replaying its WAL.
func openPrometheus(t testing.TB, dir string, opts differentialOptions, callback tsdb.SeriesLifecycleCallback) prometheusHead {
	promOpts := tsdb.DefaultOptions()
	promOpts.MinBlockDuration = chunkRangeMs
	promOpts.MaxBlockDuration = chunkRangeMs
	promOpts.NoLockfile = true
	promOpts.IsolationDisabled = true
	promOpts.EnableExemplarStorage = true
	promOpts.MaxExemplars = opts.maxExemplars
	promOpts.EnableSharding = true
	promOpts.OutOfOrderTimeWindow = opts.oooWindowMs
	promOpts.TimelyCompaction = opts.timely
	promOpts.SeriesLifecycleCallback = callback
	promOpts.SecondaryHashFunction = secondaryHash
	promOpts.EnableOverlappingCompaction = false
	// Without the ingester invalidating it on series creation, a cached postings list outlives
	// the series appended since: the reference doesn't cache.
	uncached := tsdb.DefaultPostingsForMatchersCacheConfig
	uncached.TTL = 0
	promOpts.HeadPostingsForMatchersCacheFactory = tsdb.NewPostingsForMatchersCacheFactory(uncached)
	promOpts.BlockPostingsForMatchersCacheFactory = tsdb.NewPostingsForMatchersCacheFactory(uncached)
	db, err := tsdb.Open(dir, nil, nil, promOpts, nil)
	require.NoError(t, err)
	db.DisableCompactions()
	return prometheusHead{db}
}

func openEngine(t testing.TB, dir string, opts differentialOptions, callback tsdb.SeriesLifecycleCallback) *Engine {
	engine, err := OpenEngine(dir, "tenant", EngineOptions{
		Shards:                  4,
		OutOfOrderTimeWindowMs:  opts.oooWindowMs,
		MaxExemplars:            opts.maxExemplars,
		TimelyCompaction:        opts.timely,
		SeriesLifecycleCallback: callback,
		SecondaryHashFunction:   secondaryHash,
	})
	require.NoError(t, err)
	return engine
}

type workloadSeries struct {
	lset      promlabels.Labels
	histogram bool
	float     bool
}

func workloadSeriesSet(count int) []workloadSeries {
	series := make([]workloadSeries, 0, count)
	for n := range count {
		lset := promlabels.FromStrings(
			"__name__", fmt.Sprintf("metric_%d", n%6),
			"job", fmt.Sprintf("job-%d", n%4),
			"n", fmt.Sprint(n),
		)
		if n%9 == 0 {
			lset = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%6), "n", fmt.Sprint(n), "sparse", "yes")
		}
		series = append(series, workloadSeries{lset: lset, histogram: n%7 == 3, float: n%11 == 3})
	}
	return series
}

func testHistogram(r *rand.Rand, t int64) *histogram.Histogram {
	buckets := 1 + r.IntN(4)
	deltas := make([]int64, buckets)
	count := uint64(0)
	current := int64(0)
	for index := range deltas {
		next := int64(1 + r.IntN(5))
		deltas[index] = next - current
		current = next
		count += uint64(next)
	}
	return &histogram.Histogram{
		Count:           count,
		Sum:             float64(t%1000) / 3,
		Schema:          0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: uint32(buckets)}},
		PositiveBuckets: deltas,
	}
}

// step is one call made on both appenders, with what each returned.
type appendOutcome struct {
	hasRef bool
	err    string
}

func outcomeOf(ref storage.SeriesRef, err error) appendOutcome {
	o := appendOutcome{hasRef: ref != 0}
	if err != nil {
		o.err = err.Error()
	}
	return o
}

// runRound appends one random batch through an appender of each head, checking every call returns
// the same, and commits or rolls back both.
func runRound(t *testing.T, r *rand.Rand, heads [2]headUnderTest, series []workloadSeries, cursor *int64, oooWindow int64) {
	ctx := context.Background()
	appenders := [2]EngineAppender{heads[0].Appender(ctx), heads[1].Appender(ctx)}
	refs := [2]map[int]storage.SeriesRef{{}, {}}
	for range 20 + r.IntN(60) {
		index := r.IntN(len(series))
		s := series[index]
		ts := *cursor + int64(r.IntN(30_000))
		switch roll := r.IntN(100); {
		case roll < 8:
			ts = *cursor - int64(r.IntN(int(max(oooWindow, 60_000)*2)))
		case roll < 12:
			ts = *cursor - 3*chunkRangeMs
		case roll < 16:
			ts = *cursor
		}
		kind := r.IntN(100)
		v := float64(r.IntN(10))
		if kind < 3 {
			v = math.Float64frombits(value.StaleNaN)
		}
		// Drawn once, so both sides get the same call.
		st := ts - int64(1+r.IntN(5000))
		if conflicts != nil {
			switch {
			case kind >= 3 && kind < 8:
				conflicts.record(s.lset, st, "float 0")
				conflicts.record(s.lset, ts, "float 0")
			case s.histogram && kind < 65:
				h := testHistogram(rand.New(rand.NewPCG(uint64(ts), uint64(index))), ts)
				if kind >= 60 {
					conflicts.record(s.lset, ts-1000, "histogram zero")
				}
				conflicts.record(s.lset, ts, "histogram "+h.String())
			default:
				conflicts.record(s.lset, ts, fmt.Sprint("float ", math.Float64bits(v)))
			}
		}
		var outcomes [2]appendOutcome
		for side := range appenders {
			app := appenders[side]
			ref, ok := refs[side][index]
			if !ok {
				found, _ := app.GetRef(s.lset, s.lset.Hash())
				ref = found
			}
			var (
				got storage.SeriesRef
				err error
			)
			switch {
			case kind >= 3 && kind < 8:
				got, err = app.AppendSTZeroSample(ref, s.lset, ts, st)
			case s.histogram && kind < 60:
				h := testHistogram(rand.New(rand.NewPCG(uint64(ts), uint64(index))), ts)
				if s.float {
					got, err = app.AppendHistogram(ref, s.lset, ts, nil, h.ToFloat(nil))
				} else {
					got, err = app.AppendHistogram(ref, s.lset, ts, h, nil)
				}
			case s.histogram && kind < 65:
				h := testHistogram(rand.New(rand.NewPCG(uint64(ts), uint64(index))), ts)
				got, err = app.AppendHistogramSTZeroSample(ref, s.lset, ts, ts-1000, h, nil)
			default:
				got, err = app.Append(ref, s.lset, ts, v)
			}
			if err == nil && got != 0 {
				refs[side][index] = got
				if kind%10 == 0 {
					e := exemplar.Exemplar{Labels: promlabels.FromStrings("trace_id", fmt.Sprint(ts%97)), Value: v, Ts: ts, HasTs: true}
					_, _ = app.AppendExemplar(got, s.lset, e)
				}
			}
			outcomes[side] = outcomeOf(got, err)
			if traceSeries != "" && strings.Contains(s.lset.String(), traceSeries) {
				t.Logf("side %d kind %d series %s ts %d -> %v %v", side, kind, s.lset, ts, got, err)
			}
		}
		require.Equal(t, outcomes[0], outcomes[1], "series %s at %d", s.lset, ts)
		// Seen from both appenders in the same round, a series keeps a ref on both or neither.
	}
	if r.IntN(10) == 0 {
		for _, app := range appenders {
			require.NoError(t, app.Rollback())
		}
	} else {
		for _, app := range appenders {
			require.NoError(t, app.Commit())
		}
	}
	*cursor += 60_000 + int64(r.IntN(120_000))
}

// decodedSample is a decoded sample, float or histogram, for comparing what queriers read.
type decodedSample struct {
	t   int64
	f   uint64
	h   string
	typ chunkenc.ValueType
}

// mergedSamples decodes a chunk series like a querier: overlapping chunks merge by timestamp.
func mergedSamples(t *testing.T, series storage.ChunkSeries) []decodedSample {
	var all []decodedSample
	it := series.Iterator(nil)
	for it.Next() {
		meta := it.At()
		values := meta.Chunk.Iterator(nil)
		for typ := values.Next(); typ != chunkenc.ValNone; typ = values.Next() {
			switch typ {
			case chunkenc.ValFloat:
				ts, v := values.At()
				all = append(all, decodedSample{t: ts, f: math.Float64bits(v), typ: typ})
			case chunkenc.ValHistogram:
				ts, h := values.AtHistogram(nil)
				h.CounterResetHint = histogram.UnknownCounterReset
				all = append(all, decodedSample{t: ts, h: h.String(), typ: typ})
			case chunkenc.ValFloatHistogram:
				ts, h := values.AtFloatHistogram(nil)
				h.CounterResetHint = histogram.UnknownCounterReset
				all = append(all, decodedSample{t: ts, h: h.String(), typ: typ})
			}
		}
		require.NoError(t, values.Err())
	}
	require.NoError(t, it.Err())
	sort.SliceStable(all, func(x, y int) bool { return all[x].t < all[y].t })
	return slices.CompactFunc(all, func(x, y decodedSample) bool { return x.t == y.t })
}

type queriedSeries struct {
	labels  string
	samples []decodedSample
}

func querySamples(t *testing.T, head headUnderTest, mint, maxt int64, hints *storage.SelectHints, matchers ...*promlabels.Matcher) []queriedSeries {
	q, err := head.ChunkQuerier(mint, maxt)
	require.NoError(t, err)
	defer q.Close()
	set := q.Select(context.Background(), true, hints, matchers...)
	var out []queriedSeries
	for set.Next() {
		series := set.At()
		samples := mergedSamples(t, series)
		// Chunks overlap the range; only samples in it count.
		var conflicting map[int64]bool
		if conflicts != nil {
			conflicting = conflicts.conflicts[series.Labels().String()]
		}
		samples = slices.DeleteFunc(samples, func(s decodedSample) bool { return s.t < mint || s.t > maxt || conflicting[s.t] })
		if len(samples) > 0 {
			out = append(out, queriedSeries{series.Labels().String(), samples})
		}
	}
	require.NoError(t, set.Err())
	return out
}

func refLabels(t *testing.T, head headUnderTest, postings interface {
	Next() bool
	At() storage.SeriesRef
	Err() error
}) []string {
	ix, err := head.Index()
	require.NoError(t, err)
	defer ix.Close()
	var (
		builder promlabels.ScratchBuilder
		out     []string
		last    storage.SeriesRef
		first   = true
	)
	for postings.Next() {
		ref := postings.At()
		require.True(t, first || ref > last, "postings sorted by ref")
		first, last = false, ref
		require.NoError(t, ix.Series(ref, &builder, nil))
		out = append(out, builder.Labels().String())
	}
	require.NoError(t, postings.Err())
	slices.Sort(out)
	return out
}

// nonNil treats nothing found as empty, which Prometheus returns either way.
func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// compareHeads checks both heads read the same: stats, queried samples, label lookups, postings,
// series, hashes and exemplars.
func compareHeads(t *testing.T, heads [2]headUnderTest, callbacks [2]*recordingCallback, series []workloadSeries, round int) {
	ctx := context.Background()
	msg := fmt.Sprintf("round %d", round)
	if heads[0].NumSeries() != heads[1].NumSeries() {
		var held [2][]string
		for side, head := range heads {
			ix, err := head.Index()
			require.NoError(t, err)
			p, err := ix.Postings(ctx, "", "")
			require.NoError(t, err)
			held[side] = refLabels(t, head, p)
		}
		require.Equal(t, held[0], held[1], "head series, %s", msg)
	}
	require.Equal(t, heads[0].NumSeries(), heads[1].NumSeries(), "series count, %s", msg)
	require.Equal(t, heads[0].MinTime(), heads[1].MinTime(), "min time, %s", msg)
	require.Equal(t, heads[0].MaxTime(), heads[1].MaxTime(), "max time, %s", msg)
	for _, head := range heads[1:] {
		minValid, ok := heads[0].AppendableMinValidTime()
		engineMinValid, engineOK := head.AppendableMinValidTime()
		require.Equal(t, ok, engineOK, msg)
		require.Equal(t, minValid, engineMinValid, "appendable min valid time, %s", msg)
	}
	require.Equal(t, callbacks[0].liveSeries(), callbacks[1].liveSeries(), "created minus deleted series, %s", msg)

	maxTime := heads[0].MaxTime()
	ranges := [][2]int64{{math.MinInt64, math.MaxInt64}, {maxTime - chunkRangeMs, maxTime}, {maxTime - 3*chunkRangeMs, maxTime - chunkRangeMs}, {maxTime - 20*60_000, maxTime - 5*60_000}}
	matcherSets := [][]*promlabels.Matcher{
		{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_3")},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-1"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "__name__", "metric_1")},
		{promlabels.MustNewMatcher(promlabels.MatchRegexp, "n", "1.*"), promlabels.MustNewMatcher(promlabels.MatchNotRegexp, "job", "job-2|job-3")},
		{promlabels.MustNewMatcher(promlabels.MatchEqual, "sparse", "yes")},
		{promlabels.MustNewMatcher(promlabels.MatchNotEqual, "sparse", "yes"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_(1|2)")},
	}
	for _, r := range ranges {
		for _, matchers := range matcherSets {
			for _, shard := range []*storage.SelectHints{nil, {Start: r[0], End: r[1], ShardIndex: 1, ShardCount: 3}} {
				hints := &storage.SelectHints{Start: r[0], End: r[1], DisableTrimming: true}
				if shard != nil {
					hints = shard
					hints.DisableTrimming = true
				}
				require.Equal(t, querySamples(t, heads[0], r[0], r[1], hints, matchers...), querySamples(t, heads[1], r[0], r[1], hints, matchers...),
					"samples of %v in [%d, %d] shard %v, %s", matchers, r[0], r[1], shard != nil, msg)
			}
			queriers := [2]storage.Querier{}
			for side, head := range heads {
				q, err := head.Querier(r[0], r[1])
				require.NoError(t, err)
				queriers[side] = q
			}
			names := [2][]string{}
			values := [2][]string{}
			selected := [2][]string{}
			for side, q := range queriers {
				var err error
				names[side], _, err = q.LabelNames(ctx, nil, matchers...)
				require.NoError(t, err)
				values[side], _, err = q.LabelValues(ctx, "job", nil, matchers...)
				require.NoError(t, err)
				set := q.Select(ctx, true, &storage.SelectHints{Start: r[0], End: r[1], Func: "series"}, matchers...)
				for set.Next() {
					selected[side] = append(selected[side], set.At().Labels().String())
				}
				require.NoError(t, set.Err())
				require.NoError(t, q.Close())
			}
			if !slices.Equal(nonNil(names[0]), nonNil(names[1])) || !slices.Equal(nonNil(values[0]), nonNil(values[1])) {
				var blocks []string
				for _, b := range heads[0].(prometheusHead).db.Blocks() {
					meta := b.Meta()
					blocks = append(blocks, fmt.Sprintf("[%d,%d)#%d%s", b.Meta().MinTime, b.Meta().MaxTime, meta.Stats.NumSeries, map[bool]string{true: "ooo", false: ""}[meta.Compaction.FromOutOfOrder()]))
				}
				engine := heads[1].(*Engine)
				view := engine.store.headView(engine.tenantID)
				t.Logf("prometheus: head [%d,%d] ooo [%d,%d] blocks %v; engine: head [%d,%d] ooo [%d,%d] headMin %d blocks %v; names %v vs %v",
					heads[0].MinTime(), heads[0].MaxTime(), heads[0].MinOOOTime(), heads[0].MaxOOOTime(), blocks,
					view.minTime, view.maxTime, view.minOOOTime, view.maxOOOTime, view.headMin, blockRanges(view.blocks), names[0], names[1])
			}
			require.Equal(t, nonNil(names[0]), nonNil(names[1]), "label names of %v in [%d, %d], %s", matchers, r[0], r[1], msg)
			require.Equal(t, nonNil(values[0]), nonNil(values[1]), "job values of %v in [%d, %d], %s", matchers, r[0], r[1], msg)
			if !slices.Equal(selected[0], selected[1]) {
				for side, head := range heads {
					q, _ := head.ChunkQuerier(math.MinInt64, math.MaxInt64)
					set := q.Select(ctx, true, nil, matchers...)
					for set.Next() {
						it := set.At().Iterator(nil)
						var metas []string
						for it.Next() {
							metas = append(metas, fmt.Sprintf("[%d,%d]", it.At().MinTime, it.At().MaxTime))
						}
						if traceSeries != "" && strings.Contains(set.At().Labels().String(), traceSeries) {
							t.Logf("side %d chunks of %s: %v", side, set.At().Labels(), metas)
						}
					}
					_ = q.Close()
				}
			}
			if !slices.Equal(selected[0], selected[1]) && traceSeries != "" {
				engine := heads[1].(*Engine)
				for _, shard := range engine.store.shards {
					if tt, ok := shard.tenants[engine.tenantID]; ok {
						tt.series.forEach(func(entry *seriesEntry) {
							if !strings.Contains(toPromLabels(entry.labels, &promlabels.ScratchBuilder{}).String(), traceSeries) {
								return
							}
							sr := &entry.series
							it := sr.chunks.iter()
							var metas []string
							for c, more := it.next(); more; c, more = it.next() {
								metas = append(metas, fmt.Sprintf("[%d,%d,ooo=%v]", c.MinTime, c.MaxTime, c.OutOfOrder))
							}
							fh := "nil"
							if sr.floatHead != nil {
								fh = fmt.Sprintf("[%d,%d]", sr.floatHead.minTime, sr.floatHead.lastTimestamp())
							}
							hh := "nil"
							if sr.histogramHead != nil {
								hh = fmt.Sprintf("[%d,%d]", sr.histogramHead.FirstTimestamp(), sr.histogramHead.Last().Timestamp)
							}
							t.Logf("engine state: chunks %v float head %s histogram head %s ooo %v", metas, fh, hh, sr.outOfOrder)
						})
					}
				}
			}
			require.Equal(t, nonNil(selected[0]), nonNil(selected[1]), "series of %v in [%d, %d], %s", matchers, r[0], r[1], msg)
		}
	}

	// The head index.
	indexes := [2]tsdb.IndexReader{}
	for side, head := range heads {
		ix, err := head.Index()
		require.NoError(t, err)
		indexes[side] = ix
	}
	for _, name := range []string{"__name__", "job", "n", "sparse", "missing"} {
		var values [2][]string
		for side, ix := range indexes {
			// The head's LabelValues is unsorted.
			v, err := ix.LabelValues(ctx, name, nil)
			require.NoError(t, err)
			slices.Sort(v)
			values[side] = v
		}
		require.Equal(t, nonNil(values[0]), nonNil(values[1]), "head values of %s, %s", name, msg)
		for _, v := range values[0] {
			var lists [2][]string
			for side, ix := range indexes {
				p, err := ix.Postings(ctx, name, v)
				require.NoError(t, err)
				lists[side] = refLabels(t, heads[side], p)
			}
			require.Equal(t, nonNil(lists[0]), nonNil(lists[1]), "postings of %s=%s, %s", name, v, msg)
		}
		var all [2][]string
		for side, ix := range indexes {
			all[side] = refLabels(t, heads[side], ix.PostingsForAllLabelValues(ctx, name))
		}
		require.Equal(t, nonNil(all[0]), nonNil(all[1]), "postings of every %s, %s", name, msg)
	}
	var names [2][]string
	for side, ix := range indexes {
		n, err := ix.LabelNames(ctx)
		require.NoError(t, err)
		names[side] = n
	}
	require.Equal(t, nonNil(names[0]), nonNil(names[1]), "head label names, %s", msg)
	for _, matchers := range matcherSets {
		var lists, sharded [2][]string
		for side, ix := range indexes {
			p, err := tsdb.PostingsForMatchers(ctx, ix, matchers...)
			require.NoError(t, err)
			lists[side] = refLabels(t, heads[side], p)
			p, err = tsdb.PostingsForMatchers(ctx, ix, matchers...)
			require.NoError(t, err)
			sharded[side] = refLabels(t, heads[side], ix.ShardedPostings(p, 2, 3))
		}
		require.Equal(t, nonNil(lists[0]), nonNil(lists[1]), "postings for %v, %s", matchers, msg)
		require.Equal(t, nonNil(sharded[0]), nonNil(sharded[1]), "sharded postings for %v, %s", matchers, msg)
	}
	allName, allValue := "", ""
	var every [2][]string
	for side, ix := range indexes {
		p, err := ix.Postings(ctx, allName, allValue)
		require.NoError(t, err)
		every[side] = refLabels(t, heads[side], p)
	}
	require.Equal(t, nonNil(every[0]), nonNil(every[1]), "all postings, %s", msg)
	for _, ix := range indexes {
		require.NoError(t, ix.Close())
	}

	// Hashes by series.
	var secondary [2]map[string]uint32
	var shards [2]map[string]uint64
	for side, head := range heads {
		ix, err := head.Index()
		require.NoError(t, err)
		secondary[side] = map[string]uint32{}
		shards[side] = map[string]uint64{}
		var builder promlabels.ScratchBuilder
		head.ForEachSecondaryHash(func(refs []tsdbchunks.HeadSeriesRef, hashes []uint32) {
			for index, ref := range refs {
				require.NoError(t, ix.Series(storage.SeriesRef(ref), &builder, nil))
				secondary[side][builder.Labels().String()] = hashes[index]
			}
		})
		head.ForEachShardHash(func(refs []storage.SeriesRef, hashes []uint64) {
			for index, ref := range refs {
				require.NoError(t, ix.Series(ref, &builder, nil))
				shards[side][builder.Labels().String()] = hashes[index]
			}
		})
		require.NoError(t, ix.Close())
	}
	require.Equal(t, secondary[0], secondary[1], "secondary hashes, %s", msg)
	require.Equal(t, shards[0], shards[1], "shard hashes, %s", msg)

	// Exemplars.
	var found [2][]string
	for side, head := range heads {
		q, err := head.ExemplarQuerier(ctx)
		require.NoError(t, err)
		results, err := q.Select(math.MinInt64, math.MaxInt64, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")})
		require.NoError(t, err)
		for _, result := range results {
			for _, e := range result.Exemplars {
				found[side] = append(found[side], fmt.Sprintf("%s %s %v %d", result.SeriesLabels, e.Labels, e.Value, e.Ts))
			}
		}
		slices.Sort(found[side])
	}
	require.Equal(t, found[0], found[1], "exemplars, %s", msg)
}

func TestEngineMatchesPrometheusHead(t *testing.T) {
	for _, opts := range []differentialOptions{
		{},
		{oooWindowMs: 30 * 60_000},
		{oooWindowMs: 2 * chunkRangeMs / 2, maxExemplars: 50},
		{timely: true, maxExemplars: 1000},
		{timely: true, oooWindowMs: 20 * 60_000, seriesLimit: 40},
	} {
		t.Run(fmt.Sprintf("%+v", opts), func(t *testing.T) {
			for seed := range engineSeeds() {
				t.Run(fmt.Sprint(seed), func(t *testing.T) {
					dirs := [2]string{t.TempDir(), t.TempDir()}
					var heads [2]headUnderTest
					var callbacks [2]*recordingCallback
					callbacks = [2]*recordingCallback{newRecordingCallback(opts.seriesLimit), newRecordingCallback(opts.seriesLimit)}
					heads = [2]headUnderTest{openPrometheus(t, dirs[0], opts, callbacks[0]), openEngine(t, dirs[1], opts, callbacks[1])}
					t.Cleanup(func() {
						for _, head := range heads {
							_ = head.Close()
						}
					})
					r := rand.New(rand.NewPCG(seed, 42))
					conflicts = newAppendHistory()
					t.Cleanup(func() { conflicts = nil })
					series := workloadSeriesSet(70)
					cursor := int64(10 * chunkRangeMs)
					for round := range 90 {
						runRound(t, r, heads, series, &cursor, opts.oooWindowMs)
						if traceSeries != "" {
							t.Logf("round %d: min %d/%d max %d/%d", round, heads[0].MinTime(), heads[1].MinTime(), heads[0].MaxTime(), heads[1].MaxTime())
						}
						roll := r.IntN(20)
						if traceSeries != "" {
							t.Logf("round %d: compaction roll %d", round, roll)
							for side, head := range heads {
								q, _ := head.ChunkQuerier(math.MinInt64, math.MaxInt64)
								set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+"))
								for set.Next() {
									if !strings.Contains(set.At().Labels().String(), traceSeries) {
										continue
									}
									it := set.At().Iterator(nil)
									for it.Next() {
										t.Logf("round %d side %d chunk [%d,%d] n=%d", round, side, it.At().MinTime, it.At().MaxTime, it.At().Chunk.NumSamples())
									}
								}
								_ = q.Close()
							}
						}
						switch {
						case roll < 3:
							for _, head := range heads {
								require.NoError(t, head.Compact(context.Background()))
							}
						case roll < 4:
							maxt := heads[0].MaxTime() - 30*60_000
							mint := heads[0].MinTime()
							if maxt > mint {
								for _, head := range heads {
									require.NoError(t, head.CompactHead(mint, maxt))
									require.NoError(t, head.CompactOOOHead(context.Background()))
								}
							}
						case roll < 5:
							// Evict a few series, found by their labels on each side.
							picked := []workloadSeries{series[r.IntN(len(series))], series[r.IntN(len(series))]}
							for _, head := range heads {
								require.NoError(t, head.CompactOOOHead(context.Background()))
								app := head.Appender(context.Background())
								var refs []storage.SeriesRef
								for _, s := range picked {
									if ref, _ := app.GetRef(s.lset, s.lset.Hash()); ref != 0 {
										refs = append(refs, ref)
									}
								}
								require.NoError(t, app.Rollback())
								if traceSeries != "" {
									t.Logf("round %d: evicting %v as %v", round, picked, refs)
								}
								require.NoError(t, head.CompactSelectedSeries(refs))
							}
						case roll == 5:
							// A graceful restart of the engine must be invisible: it goes on matching the
							// Prometheus head that kept running. A restarted Prometheus head is a different
							// reference, since its WAL replay forgets rejected samples' times and resurrects
							// collected series. The restored series are reported created again, like the
							// ingester's replay: series limits don't apply to them.
							require.NoError(t, heads[1].Close())
							callbacks[1] = newRecordingCallback(0)
							heads[1] = openEngine(t, dirs[1], opts, callbacks[1])
							callbacks[1].limit = opts.seriesLimit
							require.True(t, heads[1].(*Engine).Restored())
						}
						if traceSeries != "" {
							t.Logf("round %d: blocks %d/%d", round, len(heads[0].(prometheusHead).db.Blocks()), len(heads[1].(*Engine).store.headView("tenant").blocks))
						}
						compareHeads(t, heads, callbacks, series, round)
					}
				})
			}
		})
	}
}

func blockRanges(blocks []emulatedBlock) []string {
	var ranges []string
	for _, b := range blocks {
		ranges = append(ranges, fmt.Sprintf("[%d,%d)#%d%s", b.minTime, b.maxTime, len(b.series), map[bool]string{true: "ooo", false: ""}[b.outOfOrder]))
	}
	return ranges
}

// engineSeeds are the differential test's seeds: ENGINE_SEEDS runs more of them.
func engineSeeds() uint64 {
	if n, err := strconv.ParseUint(os.Getenv("ENGINE_SEEDS"), 10, 64); err == nil {
		return n
	}
	return 4
}

func TestEngineStartsEmptyAfterCrash(t *testing.T) {
	dir := t.TempDir()
	lset := promlabels.FromStrings("__name__", "up", "job", "a")
	appendOne := func(engine *Engine, ts int64) {
		app := engine.Appender(context.Background())
		_, err := app.Append(0, lset, ts, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}

	engine := openEngine(t, dir, differentialOptions{}, nil)
	require.True(t, engine.Restored(), "a new directory has nothing to lose")
	appendOne(engine, 1000)
	require.NoError(t, engine.Close())

	engine = openEngine(t, dir, differentialOptions{}, nil)
	require.True(t, engine.Restored())
	require.Equal(t, uint64(1), engine.NumSeries())
	appendOne(engine, 2000)

	// Without Close, the snapshot the last Open consumed is gone: the next Open must not resume
	// from the chunk files alone.
	crashed := openEngine(t, dir, differentialOptions{}, nil)
	t.Cleanup(func() { _ = engine.Close(); _ = crashed.Close() })
	require.False(t, crashed.Restored())
	require.Equal(t, uint64(0), crashed.NumSeries())
	require.Equal(t, int64(math.MaxInt64), crashed.MinTime())
}

// The ingester appends from a goroutine per request while queries and compactions run.
func TestEngineConcurrentUse(t *testing.T) {
	engine := openEngine(t, t.TempDir(), differentialOptions{oooWindowMs: 30 * 60_000, maxExemplars: 100}, newRecordingCallback(0))
	t.Cleanup(func() { _ = engine.Close() })
	// Under 3h of samples, so no head compaction makes a lagging writer's samples too old.
	const writers, rounds, perWriter = 4, 150, 50
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Go(func() {
			refs := make([]storage.SeriesRef, perWriter)
			for round := range rounds {
				app := engine.Appender(context.Background())
				for n := range perWriter {
					lset := promlabels.FromStrings("__name__", "m", "writer", fmt.Sprint(writer), "n", fmt.Sprint(n))
					ref, err := app.Append(refs[n], lset, int64(round)*60_000, float64(round))
					assert.NoError(t, err)
					refs[n] = ref
					_, err = app.AppendExemplar(ref, lset, exemplar.Exemplar{Labels: promlabels.FromStrings("trace_id", fmt.Sprint(round)), Value: 1, Ts: int64(round) * 60_000, HasTs: true})
					assert.NoError(t, err)
				}
				assert.NoError(t, app.Commit())
			}
		})
	}
	done := make(chan struct{})
	var readers sync.WaitGroup
	readers.Go(func() {
		matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "m")
		for {
			select {
			case <-done:
				return
			default:
			}
			q, err := engine.ChunkQuerier(math.MinInt64, math.MaxInt64)
			assert.NoError(t, err)
			set := q.Select(context.Background(), true, nil, matcher)
			for set.Next() {
				it := set.At().Iterator(nil)
				for it.Next() {
				}
			}
			assert.NoError(t, set.Err())
			assert.NoError(t, q.Close())
			index, err := engine.Index()
			assert.NoError(t, err)
			_, err = index.LabelValues(context.Background(), "n", nil)
			assert.NoError(t, err)
			assert.NoError(t, index.Close())
		}
	})
	readers.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			assert.NoError(t, engine.Compact(context.Background()))
			assert.NoError(t, engine.CompactOOOHead(context.Background()))
		}
	})
	wg.Wait()
	close(done)
	readers.Wait()
	require.Equal(t, uint64(writers*perWriter), engine.NumSeries())
	q, err := engine.ChunkQuerier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	defer q.Close()
	set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "m"))
	samples := 0
	for set.Next() {
		samples += len(mergedSamples(t, set.At()))
	}
	require.Equal(t, writers*perWriter*rounds, samples)
}

// A timestamp appended again with another value after newer samples goes out of order, next to
// its in-order copy: while both are in the head, the out-of-order one is read, and once the
// out-of-order head is compacted, the in-order one, whose chunk comes first.
func TestEngineConflictingDuplicates(t *testing.T) {
	opts := differentialOptions{oooWindowMs: 60 * 60_000}
	heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), opts, nil), openEngine(t, t.TempDir(), opts, nil)}
	t.Cleanup(func() {
		for _, head := range heads {
			_ = head.Close()
		}
	})
	lset := promlabels.FromStrings("__name__", "x")
	matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "x")
	for _, head := range heads {
		app := head.Appender(context.Background())
		for _, sample := range []struct {
			t int64
			v float64
		}{{72_000_000, 1}, {72_010_000, 2}, {72_000_000, 3}} {
			_, err := app.Append(0, lset, sample.t, sample.v)
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	read := func(head headUnderTest) float64 {
		series := querySamples(t, head, math.MinInt64, math.MaxInt64, nil, matcher)
		require.Len(t, series, 1)
		return math.Float64frombits(series[0].samples[0].f)
	}
	for _, head := range heads {
		require.Equal(t, 3.0, read(head))
	}
	for _, head := range heads {
		require.NoError(t, head.CompactOOOHead(context.Background()))
		require.Equal(t, 1.0, read(head))
	}
}

// A histogram repeated at the head's last timestamp is a no-op when it's the same and a
// duplicate otherwise, for float histograms as for integer ones.
func TestEngineRepeatedHistogramsAtTheLastTimestamp(t *testing.T) {
	opts := differentialOptions{}
	heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), opts, nil), openEngine(t, t.TempDir(), opts, nil)}
	t.Cleanup(func() {
		for _, head := range heads {
			_ = head.Close()
		}
	})
	integer := func(count uint64) *histogram.Histogram {
		return &histogram.Histogram{Count: count, Sum: float64(count), Schema: 0, ZeroThreshold: 0.001,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{int64(count)}}
	}
	float := func(count float64) *histogram.FloatHistogram {
		return &histogram.FloatHistogram{Count: count, Sum: count, Schema: 0, ZeroThreshold: 0.001,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []float64{count}}
	}
	var errs [2][]error
	for index, head := range heads {
		for _, sample := range []struct {
			name string
			t    int64
			h    *histogram.Histogram
			fh   *histogram.FloatHistogram
		}{
			{"floats", 1_000, nil, float(1)}, {"floats", 1_000, nil, float(1)}, {"floats", 1_000, nil, float(2)},
			{"floats", 1_000, integer(1), nil},
			{"integers", 1_000, integer(1), nil}, {"integers", 1_000, integer(1), nil}, {"integers", 1_000, integer(2), nil},
			{"integers", 1_000, nil, float(1)},
		} {
			app := head.Appender(context.Background())
			_, err := app.AppendHistogram(0, promlabels.FromStrings("__name__", sample.name), sample.t, sample.h, sample.fh)
			errs[index] = append(errs[index], err)
			require.NoError(t, app.Commit())
		}
	}
	require.Equal(t, errs[0], errs[1])
	require.ErrorIs(t, errs[1][2], storage.ErrDuplicateSampleForTimestamp)
	require.NoError(t, errs[1][1])
}

// Like Mimir's ingesters without shipping, retention counts from when a block was written, not from
// its samples' times: old samples stay until their block is older than the retention period.
func TestEngineRetentionCountsFromBlockCreation(t *testing.T) {
	samples := func(e *Engine) []int64 {
		q, err := e.ChunkQuerier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		defer q.Close()
		set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "m"))
		var got []int64
		for set.Next() {
			chunkIt := set.At().Iterator(nil)
			for chunkIt.Next() {
				it := chunkIt.At().Chunk.Iterator(nil)
				for it.Next() == chunkenc.ValFloat {
					ts, _ := it.At()
					got = append(got, ts)
				}
			}
		}
		return got
	}
	for name, retention := range map[string]int64{"kept within the retention": time.Hour.Milliseconds(), "expired after it": 1} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			e, err := OpenEngine(dir, "user", EngineOptions{Shards: 2, TimelyCompaction: true, RetentionMs: retention})
			require.NoError(t, err)
			app := e.Appender(context.Background())
			lset := promlabels.FromStrings("__name__", "m")
			ref, err := app.Append(0, lset, 0, 1)
			require.NoError(t, err)
			_, err = app.Append(ref, lset, 1, 2)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
			// Across a restart too, which restores the blocks' creation times.
			require.NoError(t, e.Close())
			e, err = OpenEngine(dir, "user", EngineOptions{Shards: 2, TimelyCompaction: true, RetentionMs: retention})
			require.NoError(t, err)
			defer e.Close()
			// Retention doesn't touch the head, only blocks.
			require.NoError(t, e.Compact(context.Background()))
			require.Equal(t, []int64{0, 1}, samples(e))
			// Like the ingester's forced compaction.
			require.NoError(t, e.CompactHead(0, 1))
			require.NoError(t, e.Compact(context.Background()))
			if retention > 5 {
				require.Equal(t, []int64{0, 1}, samples(e), "a block just written keeps its samples")
			}
			time.Sleep(5 * time.Millisecond)
			require.NoError(t, e.Compact(context.Background()))
			if retention > 5 {
				require.Equal(t, []int64{0, 1}, samples(e))
			} else {
				require.Empty(t, samples(e), "the block is older than the retention")
			}
		})
	}
}
