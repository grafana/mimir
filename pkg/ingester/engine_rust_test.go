// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/ingest"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// rustDifferential is an ingester with the Prometheus engine and one with the Rust engine, pushed the same records.
type rustDifferential struct {
	t          *testing.T
	limits     validation.Limits
	prometheus *Ingester
	rust       *Ingester
	regs       [2]*prometheus.Registry
	rustDir    string
	ctx        context.Context
}

const rustTestTenant = "tenant"

func startRustTestIngester(t *testing.T, engine, dir string, limits validation.Limits) *Ingester {
	ing, _ := startRustTestIngesterWithRegistry(t, engine, dir, limits)
	return ing
}

func startRustTestIngesterWithRegistry(t *testing.T, engine, dir string, limits validation.Limits) (*Ingester, *prometheus.Registry) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = engine
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	// Compaction runs when the test says.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	reg := prometheus.NewPedanticRegistry()
	ing, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dir, reg)
	require.NoError(t, err)
	startAndWaitHealthy(t, ing, r)
	return ing, reg
}

func newRustDifferential(t *testing.T, oooWindow time.Duration) *rustDifferential {
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(oooWindow)
	limits.NativeHistogramsIngestionEnabled = true
	limits.MaxGlobalExemplarsPerUser = 50
	d := &rustDifferential{t: t, limits: limits, rustDir: t.TempDir(), ctx: user.InjectOrgID(context.Background(), rustTestTenant)}
	d.prometheus, d.regs[0] = startRustTestIngesterWithRegistry(t, "prometheus", t.TempDir(), limits)
	d.rust, d.regs[1] = startRustTestIngesterWithRegistry(t, "rust", d.rustDir, limits)
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), d.prometheus)
		_ = services.StopAndAwaitTerminated(context.Background(), d.rust)
	})
	return d
}

// push pushes the request as a version 2 record to both, which must fail alike.
func (d *rustDifferential) push(build func() *mimirpb.WriteRequest) {
	serializer := ingest.RecordSerializerFromVersion(2)
	var errs [2]error
	for i, ing := range []*Ingester{d.prometheus, d.rust} {
		records, _, err := serializer.ToRecords("ingest", 0, rustTestTenant, build(), 64<<20)
		require.NoError(d.t, err)
		for _, rec := range records {
			if err := ing.PushRecord(d.ctx, rec.Value, 2); err != nil && errs[i] == nil {
				errs[i] = err
			}
		}
	}
	// Which error a push reports first depends on the order the engine finds them in: the discarded samples, which
	// count them all by reason, are compared instead.
	require.Equal(d.t, errs[0] == nil, errs[1] == nil, "prometheus: %v, rust: %v", errs[0], errs[1])
}

// discarded returns the samples and exemplars each ingester discarded, by reason.
func (d *rustDifferential) discarded() [2]map[string]float64 {
	var out [2]map[string]float64
	for i, reg := range d.regs {
		out[i] = map[string]float64{}
		families, err := reg.Gather()
		require.NoError(d.t, err)
		for _, f := range families {
			if !strings.HasPrefix(f.GetName(), "cortex_discarded_") && !strings.HasPrefix(f.GetName(), "cortex_ingester_active_") && f.GetName() != "cortex_ingester_ingested_samples_total" && f.GetName() != "cortex_ingester_ingested_exemplars_total" && f.GetName() != "cortex_ingester_ingested_samples_failures_total" {
				continue
			}
			for _, m := range f.GetMetric() {
				key := f.GetName()
				for _, l := range m.GetLabel() {
					key += "," + l.GetName() + "=" + l.GetValue()
				}
				out[i][key] = m.GetCounter().GetValue() + m.GetGauge().GetValue()
			}
		}
	}
	return out
}

func (d *rustDifferential) compact() {
	for _, ing := range []*Ingester{d.prometheus, d.rust} {
		ing.compactBlocks(context.Background(), false, 0, nil)
	}
}

// read returns the series of the select as text, a line per series with its samples.
func rustTestRead(t *testing.T, ing *Ingester, mint, maxt int64, ms ...*labels.Matcher) []string {
	db := ing.getTSDB(rustTestTenant)
	require.NotNil(t, db)
	q, err := db.Querier(mint, maxt)
	require.NoError(t, err)
	defer q.Close()
	set := q.Select(context.Background(), true, nil, ms...)
	var out []string
	var it chunkenc.Iterator
	for set.Next() {
		var b strings.Builder
		b.WriteString(set.At().Labels().String())
		it = set.At().Iterator(it)
		for {
			typ := it.Next()
			if typ == chunkenc.ValNone {
				break
			}
			switch typ {
			case chunkenc.ValFloat:
				ts, v := it.At()
				fmt.Fprintf(&b, " %d:%x", ts, math.Float64bits(v))
			case chunkenc.ValHistogram:
				ts, h := it.AtHistogram(nil)
				if value.IsStaleNaN(h.Sum) {
					fmt.Fprintf(&b, " %d:stale-histogram", ts)
				} else {
					fmt.Fprintf(&b, " %d:%s", ts, h.String())
				}
			case chunkenc.ValFloatHistogram:
				ts, h := it.AtFloatHistogram(nil)
				if value.IsStaleNaN(h.Sum) {
					fmt.Fprintf(&b, " %d:stale-float-histogram", ts)
				} else {
					fmt.Fprintf(&b, " %d:%s", ts, h.String())
				}
			}
		}
		require.NoError(t, it.Err())
		out = append(out, b.String())
	}
	require.NoError(t, set.Err())
	return out
}

func rustTestLabels(t *testing.T, ing *Ingester, mint, maxt int64, ms ...*labels.Matcher) (names []string, values map[string][]string) {
	db := ing.getTSDB(rustTestTenant)
	q, err := db.Querier(mint, maxt)
	require.NoError(t, err)
	defer q.Close()
	names, _, err = q.LabelNames(context.Background(), nil, ms...)
	require.NoError(t, err)
	values = map[string][]string{}
	for _, n := range names {
		v, _, err := q.LabelValues(context.Background(), n, nil, ms...)
		require.NoError(t, err)
		slices.Sort(v)
		values[n] = v
	}
	slices.Sort(names)
	return names, values
}

// rustCollectStream keeps what a streamed query sends, to decode it.
type rustCollectStream struct {
	grpc.ServerStream
	ctx    context.Context
	series []client.QueryStreamSeries
	chunks map[uint64][]client.Chunk
}

func (s *rustCollectStream) Context() context.Context { return s.ctx }

func (s *rustCollectStream) Send(resp *client.QueryStreamResponse) error {
	// Responses refer to memory that is reused once sent: what is kept is copied.
	for _, series := range resp.StreamingSeries {
		ls := make([]mimirpb.LabelAdapter, len(series.Labels))
		for i, l := range series.Labels {
			ls[i] = mimirpb.LabelAdapter{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)}
		}
		s.series = append(s.series, client.QueryStreamSeries{Labels: ls, ChunkCount: series.ChunkCount})
	}
	for _, c := range resp.StreamingSeriesChunks {
		for _, ch := range c.Chunks {
			ch.Data = slices.Clone(ch.Data)
			s.chunks[c.SeriesIndex] = append(s.chunks[c.SeriesIndex], ch)
		}
	}
	return nil
}

// rustTestStream returns what a streamed query sends, a line per series with its samples in [mint, maxt].
func rustTestStream(t *testing.T, ing *Ingester, mint, maxt int64, ms ...*labels.Matcher) []string {
	req, err := client.ToQueryRequest(model.Time(mint), model.Time(maxt), ms)
	require.NoError(t, err)
	req.StreamingChunksBatchSize = 7
	stream := &rustCollectStream{ctx: user.InjectOrgID(context.Background(), rustTestTenant), chunks: map[uint64][]client.Chunk{}}
	require.NoError(t, ing.QueryStream(req, stream))
	var out []string
	for i, series := range stream.series {
		chunks := stream.chunks[uint64(i)]
		// Prometheus counts a series' out-of-order chunks before merging them; the Rust engine counts what it sends.
		if ing.cfg.BlocksStorageConfig.TSDB.Engine == "rust" {
			require.Len(t, chunks, int(series.ChunkCount), "series %s", series.Labels)
		}
		samples := map[int64]string{}
		for _, c := range chunks {
			enc := map[chunk.Encoding]chunkenc.Encoding{chunk.PrometheusXorChunk: chunkenc.EncXOR, chunk.PrometheusHistogramChunk: chunkenc.EncHistogram, chunk.PrometheusFloatHistogramChunk: chunkenc.EncFloatHistogram, chunk.PrometheusXor2Chunk: chunkenc.EncXOR2}[chunk.Encoding(c.Encoding)]
			ch, err := chunkenc.FromData(enc, c.Data)
			require.NoError(t, err)
			it := ch.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				ts := it.AtT()
				if ts < mint || ts > maxt {
					continue
				}
				switch typ {
				case chunkenc.ValFloat:
					_, v := it.At()
					samples[ts] = fmt.Sprintf("%x", math.Float64bits(v))
				case chunkenc.ValHistogram:
					_, h := it.AtHistogram(nil)
					samples[ts] = h.String()
				case chunkenc.ValFloatHistogram:
					_, h := it.AtFloatHistogram(nil)
					samples[ts] = h.String()
				}
			}
			require.NoError(t, it.Err())
		}
		var b strings.Builder
		b.WriteString(mimirpb.FromLabelAdaptersToLabels(series.Labels).String())
		for _, ts := range slices.Sorted(maps.Keys(samples)) {
			fmt.Fprintf(&b, " %d:%s", ts, samples[ts])
		}
		out = append(out, b.String())
	}
	return out
}

// compare checks that both read the same, for queries over various ranges and matchers.
func (d *rustDifferential) compare(from, to int64) {
	eq := func(n, v string) *labels.Matcher { return labels.MustNewMatcher(labels.MatchEqual, n, v) }
	re := func(n, v string) *labels.Matcher { return labels.MustNewMatcher(labels.MatchRegexp, n, v) }
	selectors := [][]*labels.Matcher{
		{re("__name__", ".+")},
		{eq("job", "job-1")},
		{eq("__name__", "requests_total"), re("pod", "pod-1.*")},
		{re("__name__", "requests_total|latency"), labels.MustNewMatcher(labels.MatchNotEqual, "job", "job-2")},
		{eq("__name__", "latency_histogram")},
		{labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "pod-[0-4].*"), eq("job", "job-0")},
		{eq("missing", ""), eq("job", "job-3")},
		{re("le", ".+")},
	}
	span := to - from
	ranges := [][2]int64{{from, to}, {from, from + span/3}, {from + span/2, to}, {to - 10*60_000, to}, {from + span/4, from + span/4 + 30*60_000}}
	for _, r := range ranges {
		for _, ms := range selectors {
			want := rustTestRead(d.t, d.prometheus, r[0], r[1], ms...)
			got := rustTestRead(d.t, d.rust, r[0], r[1], ms...)
			requireSameSeries(d.t, want, got, fmt.Sprintf("range %v, matchers %v", r, ms))
			requireSameSeries(d.t, rustTestStream(d.t, d.prometheus, r[0], r[1], ms...), rustTestStream(d.t, d.rust, r[0], r[1], ms...), fmt.Sprintf("streamed, range %v, matchers %v", r, ms))
			if len(want) == 0 {
				// What label queries see of a range without samples is how each engine tracks its time bounds.
				continue
			}
			wantNames, wantValues := rustTestLabels(d.t, d.prometheus, r[0], r[1], ms...)
			gotNames, gotValues := rustTestLabels(d.t, d.rust, r[0], r[1], ms...)
			if os.Getenv("RUST_TEST_DEBUG") != "" && !slices.Equal(wantNames, gotNames) {
				st, _ := d.rust.getTSDB(rustTestTenant).db.(*rustEngine).native.Stats()
				d.t.Logf("names differ in %v: rust head %+v, prometheus head %+v, %d series read", r, st, d.prometheus.getTSDB(rustTestTenant).Head().TimeBounds(), len(got))
			}
			require.Equal(d.t, wantNames, gotNames, "range %v, matchers %v", r, ms)
			if os.Getenv("RUST_TEST_DEBUG") != "" && !slices.Equal(wantValues["pod"], gotValues["pod"]) {
				for _, v := range wantValues["pod"] {
					if !slices.Contains(gotValues["pod"], v) {
						m := labels.MustNewMatcher(labels.MatchEqual, "pod", v)
						p := rustTestRead(d.t, d.prometheus, math.MinInt64, math.MaxInt64, m)
						rr := rustTestRead(d.t, d.rust, math.MinInt64, math.MaxInt64, m)
						first := func(x []string) string {
							if len(x) == 0 {
								return "none"
							}
							f := strings.Fields(x[0])
							return f[1] + " .. " + f[len(f)-1]
						}
						st, _ := d.rust.getTSDB(rustTestTenant).db.(*rustEngine).native.Stats()
						d.t.Logf("missing %s in range %v: prometheus %s, rust %s; rust head %+v", v, r, first(p), first(rr), st)
						break
					}
				}
			}
			require.Equal(d.t, wantValues, gotValues, "range %v, matchers %v", r, ms)
		}
	}
}

// rustTestWorkload pushes rounds of samples with what a real tenant sends now and then: series that come and go,
// late and duplicate samples, staleness markers, histograms and exemplars.
func rustTestWorkload(d *rustDifferential, base int64, rounds int, seed uint64, compactEvery int) {
	const interval = 15_000
	for round := range rounds {
		build := func() *mimirpb.WriteRequest {
			rng := rand.New(rand.NewPCG(seed, uint64(round)))
			now := base + int64(round)*interval
			req := &mimirpb.WriteRequest{Source: mimirpb.API}
			for n := range 120 {
				// Series n is around for a part of the rounds, and some come back.
				first, last := (n*7)%(rounds/2), rounds/2+(n*13)%(rounds/2)
				if n%5 == 0 {
					first, last = 0, rounds
				}
				if round < first || round > last+1 || (n%11 == 0 && round%200 < 20) {
					continue
				}
				name := "requests_total"
				if n%3 == 1 {
					name = "latency"
				}
				lbls := []mimirpb.LabelAdapter{{Name: "__name__", Value: name}, {Name: "job", Value: fmt.Sprintf("job-%d", n%4)}, {Name: "pod", Value: fmt.Sprintf("pod-%d", n)}}
				if n%7 == 0 {
					lbls = append(lbls, mimirpb.LabelAdapter{Name: "le", Value: fmt.Sprint(n % 3)})
					slices.SortFunc(lbls, func(a, b mimirpb.LabelAdapter) int { return strings.Compare(a.Name, b.Name) })
				}
				t := now + int64(rng.IntN(20))
				v := float64(round*(1+n%5)) + float64(rng.IntN(100))/100
				if round == last+1 {
					v = math.Float64frombits(value.StaleNaN)
				}
				ts := &mimirpb.TimeSeries{Labels: lbls, Samples: []mimirpb.Sample{{TimestampMs: t, Value: v}}}
				switch r := rng.IntN(100); {
				case r < 4 && round > 10:
					// Late, by up to 40m.
					ts.Samples = []mimirpb.Sample{{TimestampMs: t - 1 - int64(rng.IntN(40*60_000)), Value: v + 0.5}}
				case r < 5:
					// The same twice, or with another value.
					ts.Samples = append(ts.Samples, mimirpb.Sample{TimestampMs: t, Value: v + float64(rng.IntN(2))})
				case r < 6:
					ts.Exemplars = []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: fmt.Sprint(rng.IntN(1000))}}, Value: v, TimestampMs: t}}
				}
				req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
			}
			for n := range 6 {
				lbls := []mimirpb.LabelAdapter{{Name: "__name__", Value: "latency_histogram"}, {Name: "job", Value: fmt.Sprintf("job-%d", n%4)}, {Name: "pod", Value: fmt.Sprintf("hist-%d", n)}}
				t := now + int64(rng.IntN(20))
				var h mimirpb.Histogram
				if n%2 == 0 {
					h = mimirpb.FromHistogramToHistogramProto(t, util_test.GenerateTestHistogram(round+n))
				} else {
					h = mimirpb.FromFloatHistogramToHistogramProto(t, util_test.GenerateTestFloatHistogram(round+n))
				}
				req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: lbls, Histograms: []mimirpb.Histogram{h}}})
			}
			return req
		}
		d.push(build)
		if compactEvery > 0 && round%compactEvery == compactEvery-1 {
			d.compact()
		}
	}
}

func TestRustMatchesPrometheus(t *testing.T) {
	for _, window := range []time.Duration{0, 30 * time.Minute} {
		t.Run(fmt.Sprintf("ooo window %s", window), func(t *testing.T) {
			d := newRustDifferential(t, window)
			const rounds = 6 * 240
			base := time.Now().Add(-6*time.Hour).UnixMilli() / 15_000 * 15_000
			rustTestWorkload(d, base, rounds, 1, 120)
			end := base + rounds*15_000 + 60_000
			d.compare(base-time.Hour.Milliseconds(), end)
			// The active series are counted alike, the engine keeping them or the ingester's tracker.
			now := time.Now()
			d.prometheus.updateActiveSeries(now)
			d.rust.updateActiveSeries(now)
			discarded := d.discarded()
			require.Equal(t, discarded[0], discarded[1])

			// What a clean restart restores reads the same.
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), d.rust))
			d.rust = startRustTestIngester(t, "rust", d.rustDir, d.limits)
			d.compare(base-time.Hour.Milliseconds(), end)

			// And ingestion goes on from it.
			rustTestWorkload(d, base+rounds*15_000, 120, 2, 0)
			d.compare(base-time.Hour.Milliseconds(), base+(rounds+120)*15_000)
		})
	}
}

func TestRustSeriesCountsFollowTheHead(t *testing.T) {
	limits := defaultLimitsTestConfig()
	ing := startRustTestIngester(t, "rust", t.TempDir(), limits)
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })
	ctx := user.InjectOrgID(context.Background(), rustTestTenant)
	now := time.Now().UnixMilli()
	push := func(n int, ts int64) {
		req := &mimirpb.WriteRequest{}
		for i := range n {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "m"}, {Name: "i", Value: fmt.Sprint(i)}},
				Samples: []mimirpb.Sample{{TimestampMs: ts, Value: 1}},
			}})
		}
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, rustTestTenant, req, 64<<20)
		require.NoError(t, err)
		for _, rec := range records {
			require.NoError(t, ing.PushRecord(ctx, rec.Value, 2))
		}
	}
	push(10, now-time.Hour.Milliseconds())
	push(10, now-time.Hour.Milliseconds()+15_000)
	db := ing.getTSDB(rustTestTenant)
	require.Equal(t, uint64(10), db.Head().NumSeries())
	require.Equal(t, int64(10), ing.seriesCount.Load())
	// Evicting a series takes it out of the head, and keeps it queryable.
	require.NoError(t, db.db.Evict(context.Background(), []storage.SeriesRef{1, 2}))
	require.Equal(t, uint64(8), db.Head().NumSeries())
	require.Equal(t, int64(8), ing.seriesCount.Load())
	require.Len(t, rustTestRead(t, ing, now-2*time.Hour.Milliseconds(), now, labels.MustNewMatcher(labels.MatchEqual, "__name__", "m")), 10)
	// A sample brings it back.
	push(10, now-time.Hour.Milliseconds()+30_000)
	require.Equal(t, uint64(10), db.Head().NumSeries())
	require.Equal(t, int64(10), ing.seriesCount.Load())
}

// requireSameSeries fails at the first series or sample that differs, which a whole diff would be too long to show.
func requireSameSeries(t *testing.T, want, got []string, msg string) {
	t.Helper()
	for i := 0; i < len(want) || i < len(got); i++ {
		if i >= len(want) || i >= len(got) {
			extra := got
			if len(want) > len(got) {
				extra = want
			}
			t.Fatalf("%s: %d series expected, %d got; first extra: %.300s", msg, len(want), len(got), extra[min(len(want), len(got))])
		}
		if want[i] == got[i] {
			continue
		}
		w, g := strings.Fields(want[i]), strings.Fields(got[i])
		for j := 0; j < len(w) || j < len(g); j++ {
			if j >= len(w) || j >= len(g) || w[j] != g[j] {
				at := func(f []string) string {
					if j < len(f) {
						return strings.Join(f[max(0, j-2):min(len(f), j+3)], " ")
					}
					return "<end>"
				}
				t.Fatalf("%s: series %d differs at sample %d (%d expected, %d got)\nexpected: %.300s\ngot:      %.300s\nlabels: %.200s", msg, i, j, len(w), len(g), at(w), at(g), w[0])
			}
		}
	}
}

// BenchmarkRustQueryStream streams the series of one pod from cold files and from the head, the cost of a small query.
func BenchmarkRustQueryStream(b *testing.B) {
	ing := startRustTestIngester(&testing.T{}, "rust", b.TempDir(), defaultLimitsTestConfig())
	b.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ing) })
	ctx := user.InjectOrgID(context.Background(), rustTestTenant)
	const series, rounds, interval = 20_000, 240, 60_000
	start := time.Now().Add(-4*time.Hour).UnixMilli() / interval * interval
	serializer := ingest.RecordSerializerFromVersion(2)
	for round := range rounds {
		for from := 0; from < series; from += 1000 {
			req := &mimirpb.WriteRequest{}
			for n := from; n < from+1000; n++ {
				req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
					Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", n%300)}, {Name: "job", Value: fmt.Sprintf("job-%d", n%20)}, {Name: "pod", Value: fmt.Sprintf("pod-%d", n/20)}},
					Samples: []mimirpb.Sample{{TimestampMs: start + int64(round)*interval + int64(n%25), Value: float64(round*n%977) / 7}},
				}})
			}
			records, _, err := serializer.ToRecords("ingest", 0, rustTestTenant, req, 64<<20)
			require.NoError(b, err)
			for _, rec := range records {
				require.NoError(b, ing.PushRecord(ctx, rec.Value, 2))
			}
		}
		if round%30 == 29 {
			ing.compactBlocks(context.Background(), false, 0, nil)
		}
	}
	db := ing.getTSDB(rustTestTenant)
	end := start + rounds*interval
	for _, r := range []struct {
		name       string
		mint, maxt int64
	}{{"cold", start + interval*30, start + interval*90}, {"head", end - 60*interval, end}, {"many", end - 10*interval, end}} {
		b.Run(r.name, func(b *testing.B) {
			q, err := db.db.ChunkQuerier(r.mint, r.maxt, false)
			require.NoError(b, err)
			defer q.Close()
			ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "pod", "pod-500")}
			want := 20
			if r.name == "many" {
				// A twentieth of the series, like a query of a metric every pod has.
				ms, want = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "job-3")}, series/20
			}
			batching := streamBatching{SeriesBatchSize: 128, ChunksBatchSize: 256, ChunksBatchMessageBytes: 1 << 20}
			b.ReportAllocs()
			for b.Loop() {
				stats, err := q.(streamingQuerier).QueryStream(ctx, &storage.SelectHints{Start: r.mint, End: r.maxt}, ms, batching, func(*client.QueryStreamResponse) error { return nil })
				if err != nil || stats.Series != want {
					b.Fatal(err, stats.Series)
				}
			}
		})
	}
}
