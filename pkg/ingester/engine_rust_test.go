// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/rustengine"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

type rustLifecycle struct {
	count int
	deny  error
}

func (l *rustLifecycle) PreCreation(labels.Labels) error { return l.deny }
func (l *rustLifecycle) PostCreation(labels.Labels)      { l.count++ }
func (l *rustLifecycle) PostDeletion(removed map[chunks.HeadSeriesRef]labels.Labels) {
	l.count -= len(removed)
}
func rustTestEngine(t *testing.T, dir string, callback *rustLifecycle) *rustEngine {
	t.Helper()
	opts := tsdb.DefaultOptions()
	opts.SeriesLifecycleCallback = callback
	opts.MaxExemplars = 10
	e, err := openRustEngine(dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	return e
}
func rustTestAppender(t *testing.T, e *rustEngine) *rustAppender {
	t.Helper()
	_, err := e.client.Call(rustengine.Request{Op: "begin"})
	require.NoError(t, err)
	return &rustAppender{engine: e, ctx: t.Context(), created: map[storage.SeriesRef]labels.Labels{}, inactive: map[storage.SeriesRef]bool{}}
}
func rustTestAppend(t *testing.T, a *rustAppender, ls labels.Labels, ts int64, v float64) storage.SeriesRef {
	t.Helper()
	ref, copied := a.GetRef(ls, ls.Hash())
	ref, err := a.Append(ref, copied, 0, ts, v, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	return ref
}
func rustTestRead(t *testing.T, e *rustEngine, mint, maxt int64, ms ...*labels.Matcher) []storage.Series {
	t.Helper()
	q, err := e.Querier(mint, maxt)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), true, nil, ms...)
	var series []storage.Series
	for set.Next() {
		series = append(series, set.At())
	}
	require.NoError(t, set.Err())
	return series
}
func TestRustEnginePersistenceFlushAndEviction(t *testing.T) {
	dir := t.TempDir()
	hooks := &rustLifecycle{}
	e := rustTestEngine(t, dir, hooks)
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "requests", "zone", "a")
	ref := rustTestAppend(t, a, ls, 10, 1)
	rustTestAppend(t, a, ls, 20, math.Float64frombits(0x7ff0000000000002))
	rustTestAppend(t, a, ls, 30, 3)
	_, err := a.AppendExemplars(ref, ls, []exemplar.Exemplar{{Labels: labels.FromStrings("trace_id", "synthetic"), Ts: 10, Value: 2, HasTs: true}})
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	require.Equal(t, 1, hooks.count)
	require.NoError(t, e.Flush(t.Context(), 20))
	require.Equal(t, uint64(1), e.NumSeries())
	require.Equal(t, int64(30), e.TimeBounds().MinTime)
	require.NoError(t, e.Evict(t.Context(), []storage.SeriesRef{ref}))
	require.Zero(t, e.NumSeries())
	require.Zero(t, hooks.count)
	require.Len(t, rustTestRead(t, e, 0, 40), 1)
	require.NoError(t, e.Close())
	hooks = &rustLifecycle{}
	e = rustTestEngine(t, dir, hooks)
	require.Zero(t, e.NumSeries())
	series := rustTestRead(t, e, 0, 40)
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, int64(10), ts)
	require.Equal(t, 1.0, v)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	_, v = it.At()
	require.Equal(t, uint64(0x7ff0000000000002), math.Float64bits(v))
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v = it.At()
	require.Equal(t, int64(30), ts)
	require.Equal(t, 3.0, v)
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.NoError(t, it.Err())
	a = rustTestAppender(t, e)
	require.Equal(t, ref, rustTestAppend(t, a, ls, 40, 4))
	require.NoError(t, a.Commit())
	require.Equal(t, 1, hooks.count)
	eq, err := e.ExemplarQuerier(t.Context())
	require.NoError(t, err)
	ex, err := eq.Select(0, 100)
	require.NoError(t, err)
	require.Len(t, ex, 1)
	require.Equal(t, "synthetic", ex[0].Exemplars[0].Labels.Get("trace_id"))
}
func TestRustEngineRollbackSoftErrorsAndLimits(t *testing.T) {
	hooks := &rustLifecycle{}
	e := rustTestEngine(t, t.TempDir(), hooks)
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "test")
	ref := rustTestAppend(t, a, ls, 10, 1)
	_, err := a.Append(ref, ls, 0, 10, 2, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrDuplicateSampleForTimestamp)
	_, err = a.Append(ref, ls, 0, 9, 1, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrOutOfOrderSample)
	require.NoError(t, a.Rollback())
	require.Zero(t, e.NumSeries())
	require.Zero(t, hooks.count)
	denied := errors.New("synthetic series limit")
	hooks.deny = denied
	a = rustTestAppender(t, e)
	_, err = a.Append(0, ls, 0, 10, 1, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, denied)
	require.NoError(t, a.Rollback())
	require.Zero(t, hooks.count)
	hooks.deny = nil
	a = rustTestAppender(t, e)
	rustTestAppend(t, a, ls, 10, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	a.ctx = ctx
	require.ErrorIs(t, a.Commit(), context.Canceled)
	require.Zero(t, e.NumSeries())
	require.Zero(t, hooks.count)
}
func TestRustEngineHistogramsAndChunkQueries(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	a := rustTestAppender(t, e)
	h := util_test.GenerateTestHistogram(1)
	fh := h.ToFloat(nil)
	for _, test := range []struct {
		name string
		h    *histogram.Histogram
		fh   *histogram.FloatHistogram
	}{{"integer", h, nil}, {"float", nil, fh}} {
		_, err := a.Append(0, labels.FromStrings("__name__", test.name), 0, 10, 0, test.h, test.fh, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, a.Commit())
	series := rustTestRead(t, e, 0, 20)
	require.Len(t, series, 2)
	for _, s := range series {
		it := s.Iterator(nil)
		if s.Labels().Get("__name__") == "integer" {
			require.Equal(t, chunkenc.ValHistogram, it.Next())
			_, got := it.AtHistogram(nil)
			require.Equal(t, h, got)
		} else {
			require.Equal(t, chunkenc.ValFloatHistogram, it.Next())
			_, got := it.AtFloatHistogram(nil)
			require.Equal(t, fh, got)
		}
	}
	q, err := e.ChunkQuerier(0, 20, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), true, nil)
	count := 0
	for set.Next() {
		it := set.At().Iterator(nil)
		require.True(t, it.Next())
		require.Equal(t, int64(10), it.At().MinTime)
		require.NoError(t, it.Err())
		count++
	}
	require.NoError(t, set.Err())
	require.Equal(t, 2, count)
}
func TestRustEngineLabelIndexesSurviveEviction(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	a := rustTestAppender(t, e)
	cold := rustTestAppend(t, a, labels.FromStrings("__name__", "metric", "zone", "a"), 10, 1)
	rustTestAppend(t, a, labels.FromStrings("__name__", "metric", "zone", "b"), 10, 1)
	rustTestAppend(t, a, labels.FromStrings("__name__", "other"), 10, 1)
	require.NoError(t, a.Commit())
	require.NoError(t, e.Evict(t.Context(), []storage.SeriesRef{cold}))
	head, err := e.Index()
	require.NoError(t, err)
	values, err := head.LabelValues(t.Context(), "zone", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, values)
	q, err := e.Querier(0, 20)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	for _, test := range []struct {
		matcher *labels.Matcher
		want    []string
	}{{labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric"), []string{"a", "b"}}, {labels.MustNewMatcher(labels.MatchRegexp, "zone", "a|b"), []string{"a", "b"}}, {labels.MustNewMatcher(labels.MatchEqual, "zone", ""), []string{}}, {labels.MustNewMatcher(labels.MatchNotEqual, "zone", "a"), []string{"b"}}} {
		values, _, err := q.LabelValues(t.Context(), "zone", nil, test.matcher)
		require.NoError(t, err)
		require.Equal(t, test.want, values)
	}
	require.Len(t, rustTestRead(t, e, 0, 20, labels.MustNewMatcher(labels.MatchEqual, "zone", "")), 1)
}
func TestRustEngineColdTimeFilterAndRetention(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "test")
	rustTestAppend(t, a, ls, 10, 1)
	require.NoError(t, a.Commit())
	require.NoError(t, e.Flush(t.Context(), 10))
	a = rustTestAppender(t, e)
	rustTestAppend(t, a, ls, 30, 3)
	require.NoError(t, a.Commit())
	files, err := filepath.Glob(filepath.Join(dir, "rust-engine", "cold-*.json"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	original, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(files[0], []byte("broken"), 0600))
	require.Len(t, rustTestRead(t, e, 20, 40), 1)
	q, err := e.Querier(0, 15)
	require.NoError(t, err)
	set := q.Select(t.Context(), false, nil)
	require.False(t, set.Next())
	require.EqualError(t, set.Err(), "expected value at line 1 column 1")
	require.NoError(t, q.Close())
	require.NoError(t, os.WriteFile(files[0], original, 0600))
	_, err = e.client.Call(rustengine.Request{Op: "retain", UpTo: math.MaxInt64})
	require.NoError(t, err)
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	require.Empty(t, rustTestRead(t, e, 0, 15))
	require.Len(t, rustTestRead(t, e, 20, 40), 1)
	a = rustTestAppender(t, e)
	_, err = a.Append(0, ls, 0, 10, 1, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
	require.NoError(t, a.Rollback())
}
func TestRustEngineWALTailAndDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	_, err := openRustEngine(dir, tsdb.DefaultOptions())
	require.Error(t, err)
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "test")
	rustTestAppend(t, a, ls, 10, 1)
	require.NoError(t, a.Commit())
	a = rustTestAppender(t, e)
	rustTestAppend(t, a, ls, 20, 2)
	require.NoError(t, a.Commit())
	require.NoError(t, e.Close())
	wal, err := os.OpenFile(filepath.Join(dir, "rust-engine", "wal.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = wal.WriteString(`{"seq":3`)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	series := rustTestRead(t, e, 0, 30)
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	require.Equal(t, chunkenc.ValFloat, it.Next())
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.NoError(t, it.Err())
}
func TestRustGoIngesterIntegration(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	cfg.BlocksStorageConfig.TSDB.Engine = "rust"
	ing, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, t.TempDir(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, ing, r)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing)) })
	now := time.Now().UnixMilli()
	ctx := user.InjectOrgID(t.Context(), "synthetic")
	write := func(timestamp int64, value float64) error {
		return ing.PushToStorageAndReleaseRequest(ctx, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test"}}, Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: value}}}}}})
	}
	require.NoError(t, write(now, 1))
	require.NoError(t, write(now+1, 2))
	require.ErrorContains(t, write(now, 2), "duplicate")
	db := ing.getTSDB("synthetic")
	require.IsType(t, &rustEngine{}, db.db)
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Equal(t, int64(1), ing.seriesCount.Load())
	require.NoError(t, ing.NotifyPreCommit(ctx))
	require.Len(t, rustTestRead(t, db.db.(*rustEngine), now, now+1), 1)
}
func TestRustEngineConcurrentTenantIsolation(t *testing.T) {
	first := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	second := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, e := range []*rustEngine{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.client.Call(rustengine.Request{Op: "begin"})
			if err != nil {
				errs <- err
				return
			}
			_, err = e.client.Call(rustengine.Request{Op: "append", Labels: []rustengine.Label{{Name: "__name__", Value: "same"}}, Sample: &rustengine.Sample{Timestamp: 1, Kind: 1, Bits: math.Float64bits(float64(i))}})
			if err == nil {
				_, err = e.client.Call(rustengine.Request{Op: "commit"})
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for i, e := range []*rustEngine{first, second} {
		series := rustTestRead(t, e, 0, 2)
		require.Len(t, series, 1)
		it := series[0].Iterator(nil)
		require.Equal(t, chunkenc.ValFloat, it.Next())
		_, v := it.At()
		require.Equal(t, float64(i), v)
	}
}

func TestRustEngineCompressedChunksOOOAndRestart(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 1000, MaxExemplars: 10}))
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "compressed")
	var ref storage.SeriesRef
	for n := range 360 {
		ref = rustTestAppend(t, a, ls, int64(n*2), float64(n))
	}
	require.NoError(t, a.Commit())
	a = rustTestAppender(t, e)
	_, err := a.Append(ref, ls, 0, 20, 99, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrDuplicateSampleForTimestamp)
	_, err = a.Append(ref, ls, 0, 21, 10.5, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	require.NoError(t, e.Flush(t.Context(), 350))
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	require.Equal(t, int64(352), e.TimeBounds().MinTime)
	series := rustTestRead(t, e, 0, 1000)
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	count := 0
	previous := int64(-1)
	for it.Next() != chunkenc.ValNone {
		ts, v := it.At()
		require.Greater(t, ts, previous)
		require.Equal(t, float64(ts)/2, v)
		previous = ts
		count++
	}
	require.NoError(t, it.Err())
	require.Equal(t, 361, count)
}
func TestRustEngineTypedStalenessAndXOR2(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	a := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "histogram")
	ref, err := a.Append(0, ls, 0, 1, 0, util_test.GenerateTestHistogram(1), nil, storage.AppendV2Options{})
	require.NoError(t, err)
	_, err = a.Append(ref, ls, 0, 2, math.Float64frombits(0x7ff0000000000002), nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	series := rustTestRead(t, e, 0, 3)
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	_, h := it.AtHistogram(nil)
	require.Equal(t, uint64(0x7ff0000000000002), math.Float64bits(h.Sum))
	require.NoError(t, it.Err())
	require.NoError(t, e.Configure(engineSettings{FloatChunkEncoding: "xor2"}))
	a = rustTestAppender(t, e)
	rustTestAppend(t, a, labels.FromStrings("__name__", "float"), 1, 1)
	require.NoError(t, a.Commit())
	q, err := e.ChunkQuerier(0, 3, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "float"))
	require.True(t, set.Next())
	chunks := set.At().Iterator(nil)
	require.True(t, chunks.Next())
	require.Equal(t, chunkenc.EncXOR2, chunks.At().Chunk.Encoding())
	require.NoError(t, chunks.Err())
	require.NoError(t, set.Err())
}

func TestRustIndexSeriesWithoutChunkMetadata(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	want := labels.FromStrings("__name__", "synthetic")
	ref := rustTestAppend(t, app, want, 1, 1)
	require.NoError(t, app.Commit())
	idx, err := e.Index()
	require.NoError(t, err)
	defer func() { require.NoError(t, idx.Close()) }()
	builder := labels.NewScratchBuilder(1)
	require.NoError(t, idx.Series(ref, &builder, nil))
	require.True(t, labels.Equal(want, builder.Labels()))
}
