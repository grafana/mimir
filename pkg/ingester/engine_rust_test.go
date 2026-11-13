// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
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
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
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
	files, err := filepath.Glob(filepath.Join(dir, "rust-engine", "cold-*.bin"))
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
	require.EqualError(t, set.Err(), "truncated cold series")
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
	wal, err := os.OpenFile(filepath.Join(dir, "rust-engine", "wal.bin"), os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = wal.Write([]byte{255, 255, 255})
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
	require.NoError(t, write(now+10, 10))
	match := &client.LabelMatchers{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "test"}}}
	for _, matchers := range [][]*client.LabelMatchers{{match}, {match, match}} {
		response, err := ing.MetricsForLabelMatchers(ctx, &client.MetricsForLabelMatchersRequest{StartTimestampMs: now, EndTimestampMs: now + 10, MatchersSet: matchers, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []*mimirpb.Metric{{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test"}}}}, response.Metric)
	}
	require.NoError(t, db.db.Flush(ctx, now+10))
	files, err := filepath.Glob(filepath.Join(ing.cfg.BlocksStorageConfig.TSDB.Dir, "synthetic", "rust-engine", "cold-*.bin"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		require.NoError(t, os.Remove(file))
	}
	_, err = ing.MetricsForLabelMatchers(ctx, &client.MetricsForLabelMatchersRequest{StartTimestampMs: now + 5, EndTimestampMs: now + 6, MatchersSet: []*client.LabelMatchers{match}})
	require.ErrorContains(t, err, "os error 2")
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

func TestRustStreamingChunksSnapshotPaginationAndClose(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{FloatChunkEncoding: "xor"}))
	app := rustTestAppender(t, e)
	for _, name := range []string{"b", "a"} {
		ls := labels.FromStrings("__name__", name)
		for n := range 5000 {
			rustTestAppend(t, app, ls, int64(n*17), float64(n)*1.25)
		}
	}
	require.NoError(t, app.Commit())
	q, err := e.ChunkQuerier(17, 4998*17, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), true, nil)
	cursor := set.(*rustStreamSet).cursor
	// The cursor must retain the selected revision when storage files disappear.
	require.NoError(t, e.Flush(t.Context(), 5000*17))
	_, err = e.client.Call(rustengine.Request{Op: "retain", UpTo: math.MaxInt64})
	require.NoError(t, err)
	var first storage.ChunkSeries
	for _, name := range []string{"a", "b"} {
		require.True(t, set.Next())
		series := set.At()
		if first == nil {
			first = series
		}
		require.Equal(t, name, series.Labels().Get("__name__"))
		count, err := series.ChunkCount()
		require.NoError(t, err)
		require.Greater(t, count, 32)
		it := series.Iterator(nil).(*rustStreamChunks)
		n := 0
		for it.Next() {
			require.Less(t, len(it.page), 16)
			meta := it.At()
			require.Equal(t, chunkenc.EncXOR, meta.Chunk.Encoding())
			samples := meta.Chunk.Iterator(nil)
			for samples.Next() != chunkenc.ValNone {
				ts, v := samples.At()
				require.Equal(t, int64(n*17), ts)
				require.Equal(t, float64(n)*1.25, v)
				n++
			}
			require.NoError(t, samples.Err())
		}
		require.NoError(t, it.Err())
		require.Equal(t, 5000, n)
	}
	require.False(t, set.Next())
	require.NoError(t, set.Err())
	// Iterators remain reusable after the set advances to another series.
	again := first.Iterator(nil)
	require.True(t, again.Next())
	require.NoError(t, again.Err())
	require.NoError(t, q.Close())
	_, err = e.client.Call(rustengine.Request{Op: "cursor_series", Cursor: cursor.id})
	require.EqualError(t, err, "query cursor is closed")
	require.Empty(t, cursor.q.cursors)
}

func TestRustStreamingCancellation(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "test")
	for n := range 3000 {
		rustTestAppend(t, app, ls, int64(n), float64(n))
	}
	require.NoError(t, app.Commit())
	q, err := e.ChunkQuerier(0, 3000, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	set := q.Select(ctx, false, nil)
	require.True(t, set.Next())
	it := set.At().Iterator(nil)
	require.True(t, it.Next())
	cancel()
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	cursor := set.(*rustStreamSet).cursor
	require.Eventually(t, func() bool {
		cursor.q.mu.Lock()
		defer cursor.q.mu.Unlock()
		return len(cursor.q.cursors) == 0
	}, time.Second, time.Millisecond)
	_, err = e.client.Call(rustengine.Request{Op: "cursor_series", Cursor: cursor.id})
	require.EqualError(t, err, "query cursor is closed")
}

type rustCancelAfterFrame struct {
	context.Context
	calls int
}

func (c *rustCancelAfterFrame) Err() error {
	c.calls++
	if c.calls >= 3 {
		return context.Canceled
	}
	return nil
}
func TestRustStreamingIngestRollbackAcrossFrames(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	var series []mimirpb.PreallocTimeseries
	for n := range 600 {
		name := fmt.Sprintf("series_%04d", n)
		rustTestAppend(t, app, labels.FromStrings("__name__", name), 1, 1)
		series = append(series, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: name}}, Samples: []mimirpb.Sample{{TimestampMs: 2, Value: 2}}}})
	}
	require.NoError(t, app.Commit())
	batch := ingestBatch{Series: series, MinTimestampMs: 0, MaxTimestampMs: 10}
	request := &pushRequest{req: &mimirpb.WriteRequest{Timeseries: series}}
	ctx := &ingestContext{Context: &rustCancelAfterFrame{Context: t.Context()}, request: request, stats: &pushStats{}}
	outcome, err := e.Ingest(ctx, batch)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 256, outcome.Samples)
	q, err := e.Querier(2, 2)
	require.NoError(t, err)
	set := q.Select(t.Context(), false, nil)
	require.False(t, set.Next())
	require.NoError(t, set.Err())
	require.NoError(t, q.Close())
	ctx.Context = t.Context()
	outcome, err = e.Ingest(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, 600, outcome.Samples)
	require.Len(t, rustTestRead(t, e, 2, 2), 600)
}

func TestRustStreamingFloatEncodingCompatibility(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{FloatChunkEncoding: "xor"}))
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "bits")
	var ts int64 = -10000000
	var times []int64
	var bits []uint64
	deltas := []int64{1, 15, 8192, 65536, 1048576, 4294967296}
	values := []uint64{0, 1 << 63, math.Float64bits(math.Inf(1)), 0x7ff0000000000002, math.Float64bits(math.Inf(-1)), 0x7ff8000000000017, math.Float64bits(1.23456789)}
	for n := range 600 {
		ts += deltas[n%len(deltas)]
		b := values[n%len(values)]
		rustTestAppend(t, app, ls, ts, math.Float64frombits(b))
		times = append(times, ts)
		bits = append(bits, b)
	}
	require.NoError(t, app.Commit())
	series := rustTestRead(t, e, times[0], times[len(times)-1])
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	for i := range times {
		require.Equal(t, chunkenc.ValFloat, it.Next())
		ts, value := it.At()
		require.Equal(t, times[i], ts)
		require.Equal(t, bits[i], math.Float64bits(value))
	}
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.NoError(t, it.Err())
}

func TestRustColdIndexSkipsUnselectedSeries(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	app := rustTestAppender(t, e)
	good := labels.FromStrings("__name__", "good")
	bad := labels.FromStrings("__name__", "bad")
	rustTestAppend(t, app, good, 1, 1)
	badRef := rustTestAppend(t, app, bad, 1, 2)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 1))
	checkpoint, err := os.ReadFile(filepath.Join(dir, "rust-engine", "head.json"))
	require.NoError(t, err)
	var head struct {
		Cold []struct {
			File      string
			Locations map[string][2]uint64
		}
	}
	require.NoError(t, json.Unmarshal(checkpoint, &head))
	require.Len(t, head.Cold, 1)
	location := head.Cold[0].Locations[strconv.FormatUint(uint64(badRef), 10)]
	f, err := os.OpenFile(filepath.Join(dir, "rust-engine", head.Cold[0].File), os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("bad!"), int64(location[0]))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Len(t, rustTestRead(t, e, 0, 2, labels.MustNewMatcher(labels.MatchEqual, "__name__", "good")), 1)
	q, err := e.Querier(0, 2)
	require.NoError(t, err)
	set := q.Select(t.Context(), false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "bad"))
	require.False(t, set.Next())
	require.EqualError(t, set.Err(), "invalid cold file version")
	require.NoError(t, q.Close())
}

func TestRustEngineOOOWindowSurvivesFlush(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 100, MaxExemplars: 10}))
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "late")
	ref := rustTestAppend(t, app, ls, 10, 10)
	rustTestAppend(t, app, ls, 20, 20)
	rustTestAppend(t, app, ls, 100, 100)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 20))
	app = rustTestAppender(t, e)
	_, err := app.Append(ref, ls, 0, 15, 15, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	_, err = app.Append(ref, ls, 0, 10, 999, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrDuplicateSampleForTimestamp)
	_, err = app.Append(ref, ls, 0, -100, 1, nil, nil, storage.AppendV2Options{})
	require.ErrorIs(t, err, storage.ErrOutOfBounds)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	series := rustTestRead(t, e, 0, 100)
	require.Len(t, series, 1)
	it := series[0].Iterator(nil)
	for _, want := range []int64{10, 15, 20, 100} {
		require.Equal(t, chunkenc.ValFloat, it.Next())
		ts, v := it.At()
		require.Equal(t, want, ts)
		require.Equal(t, float64(want), v)
	}
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.NoError(t, it.Err())
}

func TestRustUnorderedChunksPreserveSealedBytes(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 1000, FloatChunkEncoding: "xor"}))
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "unordered")
	var ref storage.SeriesRef
	expected := chunkenc.NewXORChunk()
	expectedAppender, err := expected.Appender()
	require.NoError(t, err)
	for n := range 300 {
		ref = rustTestAppend(t, app, ls, int64(n*2), float64(n))
		if n < 120 {
			expectedAppender.Append(0, int64(n*2), float64(n))
		}
	}
	require.NoError(t, app.Commit())
	app = rustTestAppender(t, e)
	_, err = app.Append(ref, ls, 0, 21, 10.5, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	q, err := e.ChunkQuerier(0, 600, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), false, nil)
	require.True(t, set.Next())
	count, err := set.At().ChunkCount()
	require.NoError(t, err)
	require.Equal(t, 4, count)
	it := set.At().Iterator(nil)
	require.True(t, it.Next())
	require.Equal(t, expected.Bytes(), it.At().Chunk.Bytes())
	points := map[int64]float64{}
	for {
		values := it.At().Chunk.Iterator(nil)
		for values.Next() != chunkenc.ValNone {
			ts, v := values.At()
			points[ts] = v
		}
		require.NoError(t, values.Err())
		if !it.Next() {
			break
		}
	}
	require.NoError(t, it.Err())
	require.Len(t, points, 301)
	require.Equal(t, 10.5, points[21])
	require.False(t, set.Next())
	require.NoError(t, set.Err())
}

func TestRustIndexBatchedFilteredLabelMetadata(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "x", "team", "red", "zone", "a"), 1, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "x", "extra", "u", "zone", "b"), 1, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "y", "zone", "c"), 1, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "z", "zone", ""), 1, 1)
	require.NoError(t, app.Commit())
	ix, err := e.Index()
	require.NoError(t, err)
	defer func() { require.NoError(t, ix.Close()) }()
	match := labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")
	names, err := ix.LabelNames(t.Context(), match)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "extra", "team", "zone"}, names)
	values, err := ix.LabelValues(t.Context(), "zone", nil, match)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, values)
	values, err = ix.LabelValues(t.Context(), "zone", &storage.LabelHints{Limit: 1}, match)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, values)
	values, err = ix.LabelValues(t.Context(), "zone", nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "z"))
	require.NoError(t, err)
	require.Equal(t, []string{""}, values)
	names, err = ix.LabelNames(t.Context(), labels.MustNewMatcher(labels.MatchEqual, "__name__", "absent"))
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestRustExemplarFilteringUsesSeriesAndTime(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	for _, name := range []string{"a", "b", "empty"} {
		ls := labels.FromStrings("__name__", name)
		ref := rustTestAppend(t, app, ls, 1, 1)
		if name == "empty" {
			continue
		}
		_, err := app.AppendExemplars(ref, ls, []exemplar.Exemplar{{Ts: 2, Value: 2, HasTs: true, Labels: labels.FromStrings("trace_id", name)}, {Ts: 4, Value: 4, HasTs: true, Labels: labels.FromStrings("trace_id", name)}})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	q, err := e.ExemplarQuerier(t.Context())
	require.NoError(t, err)
	result, err := q.Select(0, 3, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "a")})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "a", result[0].SeriesLabels.Get("__name__"))
	require.Len(t, result[0].Exemplars, 1)
	result, err = q.Select(3, 5, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "a")}, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "b")})
	require.NoError(t, err)
	require.Len(t, result, 2)
	for _, s := range result {
		require.Len(t, s.Exemplars, 1)
		require.Equal(t, int64(4), s.Exemplars[0].Ts)
	}
	result, err = q.Select(0, 10, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "empty")})
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRustIngestFramesDoNotRetainGoRequestSlices(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "borrowed"), 1, 1)
	require.NoError(t, app.Commit())
	series := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "borrowed"}}, Samples: []mimirpb.Sample{{TimestampMs: 2, Value: 2}}}}}
	ctx := &ingestContext{Context: t.Context(), request: &pushRequest{req: &mimirpb.WriteRequest{Timeseries: series}}, stats: &pushStats{}}
	outcome, err := e.Ingest(ctx, ingestBatch{Series: series, MinTimestampMs: 0, MaxTimestampMs: 10})
	require.NoError(t, err)
	require.Equal(t, 1, outcome.Samples)
	series[0].Labels[0].Value = "reused"
	series[0].Samples[0] = mimirpb.Sample{TimestampMs: 999, Value: 999}
	got := rustTestRead(t, e, 2, 2)
	require.Len(t, got, 1)
	require.Equal(t, "borrowed", got[0].Labels().Get("__name__"))
	it := got[0].Iterator(nil)
	require.Equal(t, chunkenc.ValFloat, it.Next())
	ts, v := it.At()
	require.Equal(t, int64(2), ts)
	require.Equal(t, 2.0, v)
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.NoError(t, it.Err())
}

func TestRustKafkaIngestBuffersUntilOffsetSync(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "buffered"), 1, 1)
	require.NoError(t, app.Commit())
	path := filepath.Join(dir, "rust-engine", "wal.bin")
	before, err := os.Stat(path)
	require.NoError(t, err)
	ingester := &Ingester{}
	ingester.cfg.IngestStorageConfig.Enabled = true
	series := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "buffered"}}, Samples: []mimirpb.Sample{{TimestampMs: 2, Value: 2}}}}}
	ctx := &ingestContext{Context: t.Context(), i: ingester, request: &pushRequest{req: &mimirpb.WriteRequest{Timeseries: series}}, stats: &pushStats{}}
	outcome, err := e.Ingest(ctx, ingestBatch{Series: series, MinTimestampMs: 0, MaxTimestampMs: 10})
	require.NoError(t, err)
	require.Equal(t, 1, outcome.Samples)
	require.True(t, e.walBuffered)
	buffered, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.Size(), buffered.Size())
	require.NoError(t, e.Sync())
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, after.Size(), before.Size())
}

func TestRustLabelMetadataIncludesWholeOverlappingHead(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "early", "old", "present"), 10, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "late", "new", "present"), 100, 1)
	require.NoError(t, app.Commit())
	for _, bounds := range [][2]int64{{10, 10}, {50, 50}, {100, 100}} {
		q, err := e.Querier(bounds[0], bounds[1])
		require.NoError(t, err)
		values, _, err := q.LabelValues(t.Context(), "__name__", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"early", "late"}, values)
		names, _, err := q.LabelNames(t.Context(), nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "early"))
		require.NoError(t, err)
		require.Equal(t, []string{"__name__", "old"}, names)
		require.NoError(t, q.Close())
	}
	q, err := e.Querier(101, 200)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	values, _, err := q.LabelValues(t.Context(), "__name__", nil)
	require.NoError(t, err)
	require.Empty(t, values)
}

func TestRustColdLabelMetadataUsesBlockRanges(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	block := e.blockDuration
	require.Positive(t, block)
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "early"), 10, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "middle"), block+10, 1)
	gap := labels.FromStrings("__name__", "gap")
	rustTestAppend(t, app, gap, 10, 1)
	rustTestAppend(t, app, gap, 2*block+10, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 3*block))
	for _, restart := range []bool{false, true} {
		if restart {
			require.NoError(t, e.Close())
			e = rustTestEngine(t, dir, &rustLifecycle{})
		}
		for _, tc := range []struct {
			start int64
			want  []string
		}{
			{50, []string{"early", "gap"}},
			{block + 50, []string{"middle"}},
			{2*block + 50, []string{}},
			{5, []string{}},
			{block + 5, []string{}},
			{2*block + 5, []string{"gap"}},
			{3*block + 50, []string{}},
		} {
			q, err := e.Querier(tc.start, tc.start)
			require.NoError(t, err)
			values, _, err := q.LabelValues(t.Context(), "__name__", nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, values)
			require.NoError(t, q.Close())
		}
	}
}

func TestRustIdleEvictedLabelsRemainInOverlappingHeadWindow(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	block := e.blockDuration
	app := rustTestAppender(t, e)
	rustTestAppend(t, app, labels.FromStrings("__name__", "old"), block-block/6, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "current"), block+block/12, 1)
	require.NoError(t, app.Commit())
	e.idleEviction = (25 * time.Minute).Milliseconds()
	require.NoError(t, e.Compact(t.Context()))
	check := func(want []string) {
		q, err := e.Querier(block+block/12, block+block/12)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		values, _, err := q.LabelValues(t.Context(), "__name__", nil)
		require.NoError(t, err)
		require.Equal(t, want, values)
	}
	check([]string{"current", "old"})
	require.NoError(t, e.Flush(t.Context(), block+block/12))
	check([]string{"current"})
}

func TestRustIdleMetadataIncludesColdWithoutChangingPhysicalPostings(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	minute := time.Minute.Milliseconds()
	app := rustTestAppender(t, e)
	old := labels.FromStrings("__name__", "x", "zone", "idle", "idle_only", "yes")
	rustTestAppend(t, app, old, 10*minute, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "x", "zone", "live"), 50*minute, 1)
	require.NoError(t, app.Commit())
	e.idleEviction = 25 * minute
	require.NoError(t, e.Compact(t.Context()))
	check := func(wantIdle bool) {
		ix := &rustIndex{engine: e}
		match := labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")
		p, err := ix.PostingsForMatchers(t.Context(), false, match)
		require.NoError(t, err)
		refs, err := index.ExpandPostings(p)
		require.NoError(t, err)
		require.Len(t, refs, 1)
		for _, ms := range [][]*labels.Matcher{nil, {match}} {
			values, err := ix.LabelValues(t.Context(), "zone", nil, ms...)
			require.NoError(t, err)
			names, err := ix.LabelNames(t.Context(), ms...)
			require.NoError(t, err)
			counts, err := ix.LabelValuesCounts(t.Context(), []string{"zone", "missing"}, ms, math.MinInt64)
			require.NoError(t, err)
			require.Empty(t, counts["missing"])
			allValues, err := ix.LabelNamesAndValues(t.Context(), ms)
			require.NoError(t, err)
			require.Equal(t, values, allValues["zone"])
			require.Len(t, allValues, len(names))
			if wantIdle {
				require.Equal(t, []string{"idle", "live"}, values)
				require.Equal(t, []string{"__name__", "idle_only", "zone"}, names)
				require.Equal(t, map[string]uint64{"idle": 1, "live": 1}, counts["zone"])
			} else {
				require.Equal(t, []string{"live"}, values)
				require.Equal(t, []string{"__name__", "zone"}, names)
				require.Equal(t, map[string]uint64{"live": 1}, counts["zone"])
			}
		}
	}
	check(true)
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	check(true)
	app = rustTestAppender(t, e)
	rustTestAppend(t, app, old, 50*minute+1, 2)
	require.NoError(t, app.Commit())
	ix := &rustIndex{engine: e}
	counts, err := ix.LabelValuesCounts(t.Context(), []string{"zone"}, nil, math.MinInt64)
	require.NoError(t, err)
	require.Equal(t, uint64(1), counts["zone"]["idle"])
	ref, _ := app.GetRef(old, old.Hash())
	require.NoError(t, e.Evict(t.Context(), []storage.SeriesRef{ref}))
	require.NoError(t, e.Flush(t.Context(), 30*minute))
	check(false)
}

func TestRustIdleMetadataAtHeadBoundary(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	floor := time.Hour.Milliseconds() + 200
	app := rustTestAppender(t, e)
	idle := labels.FromStrings("__name__", "idle")
	rustTestAppend(t, app, labels.FromStrings("__name__", "compacted"), floor-1, 1)
	rustTestAppend(t, app, idle, floor, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "live"), 2*time.Hour.Milliseconds()+200, 1)
	require.NoError(t, app.Commit())
	ref, _ := app.GetRef(idle, idle.Hash())
	require.NoError(t, e.move(t.Context(), rustengine.Request{Op: "evict", Refs: []uint64{uint64(ref)}, Filter: true}))
	require.NoError(t, e.Flush(t.Context(), floor-1))
	ix := &rustIndex{engine: e}
	values, err := ix.LabelValues(t.Context(), "__name__", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"idle", "live"}, values)
	q, err := e.Querier(2*time.Hour.Milliseconds()+200, math.MaxInt64)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	values, _, err = q.LabelValues(t.Context(), "__name__", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"idle", "live"}, values)
}

func TestRustTimedLabelsAcrossSelectionSizes(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	var rows []labels.Labels
	for n := range 64 {
		ls := labels.FromStrings("__name__", "x", "group", strconv.Itoa(n%8), "id", fmt.Sprintf("%02d", n), fmt.Sprintf("extra_%d", n%4), fmt.Sprintf("value_%d", n%3))
		rows = append(rows, ls)
		rustTestAppend(t, app, ls, int64(10+n), 1)
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 31))
	for _, headOnly := range []bool{false, true} {
		mint, maxt, visible := int64(0), int64(1000), rows
		if headOnly {
			mint, maxt, visible = 73, 73, rows[22:]
		}
		q, err := e.Querier(mint, maxt)
		require.NoError(t, err)
		for _, matchers := range [][]*labels.Matcher{nil, {labels.MustNewMatcher(labels.MatchEqual, "group", "2")}, {labels.MustNewMatcher(labels.MatchEqual, "id", "11")}} {
			wanted := map[string]map[string]bool{}
			for _, row := range visible {
				matches := true
				for _, m := range matchers {
					matches = matches && m.Matches(row.Get(m.Name))
				}
				if matches {
					row.Range(func(l labels.Label) {
						if wanted[l.Name] == nil {
							wanted[l.Name] = map[string]bool{}
						}
						wanted[l.Name][l.Value] = true
					})
				}
			}
			for _, limit := range []int{0, 1, 3} {
				hints := &storage.LabelHints{Limit: limit}
				wantNames := make([]string, 0, len(wanted))
				for name := range wanted {
					wantNames = append(wantNames, name)
				}
				slices.Sort(wantNames)
				if limit > 0 {
					wantNames = wantNames[:min(limit, len(wantNames))]
				}
				names, _, err := q.LabelNames(t.Context(), hints, matchers...)
				require.NoError(t, err)
				require.Equal(t, wantNames, names)
				for _, name := range []string{"group", "id", "missing"} {
					wantValues := make([]string, 0, len(wanted[name]))
					for value := range wanted[name] {
						wantValues = append(wantValues, value)
					}
					slices.Sort(wantValues)
					if limit > 0 {
						wantValues = wantValues[:min(limit, len(wantValues))]
					}
					values, _, err := q.LabelValues(t.Context(), name, hints, matchers...)
					require.NoError(t, err)
					require.Equal(t, wantValues, values)
				}
			}
		}
		require.NoError(t, q.Close())
	}
}

func TestRustCompactionPreservesGoHeadHorizon(t *testing.T) {
	hour := time.Hour.Milliseconds()
	for _, timely := range []bool{false, true} {
		t.Run(fmt.Sprintf("timely=%v", timely), func(t *testing.T) {
			opts := tsdb.DefaultOptions()
			opts.MinBlockDuration = 2 * hour
			opts.TimelyCompaction = timely
			e, err := openRustEngine(t.TempDir(), opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, e.Close()) })
			app := rustTestAppender(t, e)
			rustTestAppend(t, app, labels.FromStrings("__name__", "ancient"), 9*hour, 1)
			rustTestAppend(t, app, labels.FromStrings("__name__", "young"), 20*hour+hour/2, 1)
			current := labels.FromStrings("__name__", "current")
			rustTestAppend(t, app, current, 22*hour+hour/20, 1)
			require.NoError(t, app.Commit())
			require.NoError(t, e.Compact(t.Context()))
			require.Equal(t, uint64(2), e.NumSeries())
			state, err := e.client.Call(rustengine.Request{Op: "stats"})
			require.NoError(t, err)
			require.Equal(t, 20*hour, state.MinValid)
			lastKept := 23 * hour
			if !timely {
				lastKept = 23*hour + hour/2
			}
			app = rustTestAppender(t, e)
			rustTestAppend(t, app, current, lastKept, 2)
			require.NoError(t, app.Commit())
			require.NoError(t, e.Compact(t.Context()))
			require.Equal(t, uint64(2), e.NumSeries())
			app = rustTestAppender(t, e)
			rustTestAppend(t, app, current, lastKept+1, 3)
			require.NoError(t, app.Commit())
			require.NoError(t, e.Compact(t.Context()))
			require.Equal(t, uint64(1), e.NumSeries())
			require.Len(t, rustTestRead(t, e, 0, 24*hour), 3)
		})
	}
}

func TestRustSelectorCacheTracksIndexChangesAndTenantIsolation(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	head := &rustIndex{engine: e}
	cold := &rustIndex{engine: e, cold: true}
	match := labels.MustNewMatcher(labels.MatchEqual, "__name__", "x")
	read := func(ix *rustIndex) []storage.SeriesRef {
		p, err := ix.PostingsForMatchers(t.Context(), false, match)
		require.NoError(t, err)
		refs, err := index.ExpandPostings(p)
		require.NoError(t, err)
		return refs
	}
	app := rustTestAppender(t, e)
	first := labels.FromStrings("__name__", "x", "zone", "a")
	one := rustTestAppend(t, app, first, 1, 1)
	require.NoError(t, app.Commit())
	require.Equal(t, []storage.SeriesRef{one}, read(head))
	old, err := head.PostingsForMatchers(t.Context(), false, match)
	require.NoError(t, err)
	generation := e.selectors[0].generation
	app = rustTestAppender(t, e)
	rustTestAppend(t, app, first, 2, 2)
	require.NoError(t, app.Commit())
	require.Equal(t, []storage.SeriesRef{one}, read(head))
	require.Equal(t, generation, e.selectors[0].generation)
	app = rustTestAppender(t, e)
	two := rustTestAppend(t, app, labels.FromStrings("__name__", "x", "zone", "b"), 2, 3)
	require.NoError(t, app.Commit())
	require.Equal(t, []storage.SeriesRef{one, two}, read(head))
	snapshot, err := index.ExpandPostings(old)
	require.NoError(t, err)
	require.Equal(t, []storage.SeriesRef{one}, snapshot)
	require.Equal(t, []storage.SeriesRef{one, two}, read(cold))
	require.NoError(t, e.Flush(t.Context(), 2))
	require.Empty(t, read(head))
	require.Equal(t, []storage.SeriesRef{one, two}, read(cold))
	_, err = e.client.Call(rustengine.Request{Op: "retain", UpTo: math.MaxInt64})
	require.NoError(t, err)
	require.Empty(t, read(cold))
	other := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	require.Empty(t, read(&rustIndex{engine: other}))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = head.PostingsForMatchers(canceled, false, match)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, e.Close())
	_, err = head.PostingsForMatchers(t.Context(), false, match)
	require.EqualError(t, err, "Rust engine is closed")
}

func TestRustSelectorCacheBoundedConcurrentReadsAndWrites(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	ix := &rustIndex{engine: e}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Go(func() {
		for n := range 100 {
			series := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "x"}, {Name: "series", Value: fmt.Sprint(n)}}, Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 1}}}}}
			ctx := &ingestContext{Context: t.Context(), request: &pushRequest{req: &mimirpb.WriteRequest{Timeseries: series}}, stats: &pushStats{}}
			if _, err := e.Ingest(ctx, ingestBatch{Series: series, MinTimestampMs: 0, MaxTimestampMs: 10}); err != nil {
				errs <- err
				return
			}
		}
	})
	for range 2 {
		wg.Go(func() {
			previous := 0
			for range 200 {
				p, err := ix.PostingsForMatchers(t.Context(), false, labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))
				if err != nil {
					errs <- err
					return
				}
				refs, err := index.ExpandPostings(p)
				if err != nil {
					errs <- err
					return
				}
				if len(refs) < previous {
					errs <- fmt.Errorf("cached series count regressed from %d to %d", previous, len(refs))
					return
				}
				previous = len(refs)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for n := range 200 {
		p, err := ix.PostingsForMatchers(t.Context(), false, labels.MustNewMatcher(labels.MatchEqual, "series", fmt.Sprint(n)))
		require.NoError(t, err)
		refs, err := index.ExpandPostings(p)
		require.NoError(t, err)
		if n < 100 {
			require.Len(t, refs, 1)
		} else {
			require.Empty(t, refs)
		}
	}
	require.LessOrEqual(t, e.selectors[0].bytes, 64<<10)
	require.LessOrEqual(t, len(e.selectors[0].postings), 128)
}

func TestRustSelectorCacheKeepsHotResultsDuringEviction(t *testing.T) {
	for _, keyBytes := range []int{0, 300} {
		t.Run(fmt.Sprint(keyBytes), func(t *testing.T) {
			e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
			app := rustTestAppender(t, e)
			for n := range 5000 {
				rustTestAppend(t, app, labels.FromStrings("__name__", "hot", "row", fmt.Sprint(n)), 1, 1)
			}
			require.NoError(t, app.Commit())
			ix := &rustIndex{engine: e}
			match := labels.MustNewMatcher(labels.MatchEqual, "__name__", "hot")
			retained, err := ix.PostingsForMatchers(t.Context(), false, match)
			require.NoError(t, err)
			hot := e.selectors[0].lru.Front()
			for n := range 256 {
				p, err := ix.PostingsForMatchers(t.Context(), false, match)
				require.NoError(t, err)
				refs, err := index.ExpandPostings(p)
				require.NoError(t, err)
				require.Len(t, refs, 5000)
				_, err = ix.PostingsForMatchers(t.Context(), false, labels.MustNewMatcher(labels.MatchEqual, "row", strings.Repeat("z", keyBytes)+fmt.Sprint(n)))
				require.NoError(t, err)
			}
			value := hot.Value.(*rustCachedSelector)
			require.Same(t, hot, e.selectors[0].postings[value.key])
			require.LessOrEqual(t, e.selectors[0].bytes, 64<<10)
			require.LessOrEqual(t, e.selectors[0].lru.Len(), 128)
			for n := range 512 {
				_, err = ix.PostingsForMatchers(t.Context(), false, labels.MustNewMatcher(labels.MatchEqual, "row", fmt.Sprint(n)))
				require.NoError(t, err)
			}
			require.NotContains(t, e.selectors[0].postings, value.key)
			refs, err := index.ExpandPostings(retained)
			require.NoError(t, err)
			require.Len(t, refs, 5000)
		})
	}
}

func TestRustStreamingLabelAdaptersOwnTheirResponse(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "owned", "zone", "a")
	rustTestAppend(t, app, ls, 1, 1)
	require.NoError(t, app.Commit())
	q, err := e.ChunkQuerier(0, 2, true)
	require.NoError(t, err)
	set := q.Select(t.Context(), true, nil)
	require.True(t, set.Next())
	streamed := set.At().(*rustStreamSeries)
	adapters := streamed.LabelAdapters()
	require.Equal(t, mimirpb.FromLabelsToLabelAdapters(ls), adapters)
	require.True(t, streamed.lset.IsEmpty())
	require.NoError(t, q.Close())
	require.NoError(t, e.Close())
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if !labels.Equal(ls, streamed.Labels()) {
					t.Error("streamed labels changed")
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, mimirpb.FromLabelsToLabelAdapters(ls), adapters)
}

func TestRustIdleEvictionPreservesOOOAndColdReads(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 2 * time.Hour.Milliseconds(), MaxExemplars: 10}))
	minute := time.Minute.Milliseconds()
	app := rustTestAppender(t, e)
	oldLabels := labels.FromStrings("__name__", "old")
	old := rustTestAppend(t, app, oldLabels, 10*minute, 1)
	rustTestAppend(t, app, oldLabels, 11*minute, 2)
	protected := labels.FromStrings("__name__", "protected")
	rustTestAppend(t, app, protected, 20*minute, 1)
	rustTestAppend(t, app, protected, 19*minute, 2)
	rustTestAppend(t, app, labels.FromStrings("__name__", "border"), 35*minute, 1)
	current := labels.FromStrings("__name__", "current")
	rustTestAppend(t, app, current, 60*minute, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Sync())
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 2 * time.Hour.Milliseconds(), MaxExemplars: 10}))
	e.idleEviction = 25 * minute
	require.NoError(t, e.Compact(t.Context()))
	require.Equal(t, uint64(3), e.NumSeries())
	require.NoError(t, e.Close())
	e = rustTestEngine(t, dir, &rustLifecycle{})
	require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: 2 * time.Hour.Milliseconds(), MaxExemplars: 10}))
	e.idleEviction = 25 * minute
	q, err := e.Querier(5*minute, 5*minute)
	require.NoError(t, err)
	values, _, err := q.LabelValues(t.Context(), "__name__", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"border", "current", "old", "protected"}, values)
	require.NoError(t, q.Close())

	app = rustTestAppender(t, e)
	rustTestAppend(t, app, current, 60*minute+1, 2)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Compact(t.Context()))
	require.Equal(t, uint64(2), e.NumSeries())
	app = rustTestAppender(t, e)
	_, err = app.Append(old, oldLabels, 0, 10*minute, 99, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Rollback())
	require.Len(t, rustTestRead(t, e, 0, 61*minute), 4)
	require.NoError(t, e.Flush(t.Context(), 30*minute))
	require.Equal(t, uint64(1), e.NumSeries())
}

func TestRustFactoryUsesActiveSeriesIdleTimeout(t *testing.T) {
	opts := tsdb.DefaultOptions()
	engine, err := openTenantEngine("rust", t.TempDir(), nil, nil, opts, false, 20*time.Minute, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, engine.Close()) }()
	require.Equal(t, (25 * time.Minute).Milliseconds(), engine.(*rustEngine).idleEviction)
}

func TestRustDirectStreamMatchesChunkIteratorOutput(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	for n := range 19 {
		ls := labels.FromStrings("__name__", "x", "id", fmt.Sprintf("%02d", n))
		for timestamp := range 37 {
			rustTestAppend(t, app, ls, int64(timestamp), float64(n*100+timestamp))
		}
	}
	_, err := app.Append(0, labels.FromStrings("__name__", "h"), 0, 10, 0, &histogram.Histogram{Count: 1, ZeroCount: 1, Sum: 1, ZeroThreshold: 0.001}, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("__name__", "fh"), 0, 10, 0, nil, &histogram.FloatHistogram{Count: 1.25, ZeroCount: 1.25, Sum: 2.5, ZeroThreshold: 0.001}, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 30))
	for _, encoding := range []chunkenc.Encoding{chunkenc.EncXOR, chunkenc.EncXOR2} {
		t.Run(encoding.String(), func(t *testing.T) {
			e.floatEncoding.Store(uint32(encoding))
			q, err := e.ChunkQuerier(0, 1000, true)
			require.NoError(t, err)
			defer func() { require.NoError(t, q.Close()) }()
			set := q.Select(t.Context(), true, nil)
			var wantSeries []client.QueryStreamSeries
			var wantChunks []client.QueryStreamSeriesChunks
			wantSamples := 0
			for set.Next() {
				series := set.At()
				count, err := series.ChunkCount()
				require.NoError(t, err)
				wantSeries = append(wantSeries, client.QueryStreamSeries{Labels: mimirpb.FromLabelsToLabelAdapters(series.Labels()), ChunkCount: int64(count)})
				row := client.QueryStreamSeriesChunks{SeriesIndex: uint64(len(wantChunks))}
				it := series.Iterator(nil)
				for it.Next() {
					converted, err := client.ChunkFromMeta(it.At())
					require.NoError(t, err)
					row.Chunks = append(row.Chunks, converted)
					wantSamples += it.At().Chunk.NumSamples()
				}
				require.NoError(t, it.Err())
				wantChunks = append(wantChunks, row)
			}
			require.NoError(t, set.Err())
			stream := q.(streamingQuerier)
			var gotSeries []client.QueryStreamSeries
			var gotChunks []client.QueryStreamSeriesChunks
			endMarkers, chunkMessages := 0, 0
			stats, err := stream.QueryStream(t.Context(), nil, nil, streamBatching{SeriesBatchSize: 2, ChunksBatchSize: 3, ChunksBatchMessageBytes: 100}, func(message *client.QueryStreamResponse) error {
				if message.IsEndOfSeriesStream {
					endMarkers++
				}
				for _, series := range message.StreamingSeries {
					series.Labels = slices.Clone(series.Labels)
					gotSeries = append(gotSeries, series)
				}
				if len(message.StreamingSeriesChunks) > 0 {
					chunkMessages++
				}
				for _, row := range message.StreamingSeriesChunks {
					row.Chunks = slices.Clone(row.Chunks)
					gotChunks = append(gotChunks, row)
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, wantSeries, gotSeries)
			require.Equal(t, wantChunks, gotChunks)
			require.Equal(t, 1, endMarkers)
			require.Equal(t, len(wantSeries), stats.Series)
			require.Equal(t, wantSamples, stats.Samples)
			require.Equal(t, chunkMessages, stats.Batches)
			sendError := errors.New("send failed")
			_, err = stream.QueryStream(t.Context(), nil, nil, streamBatching{SeriesBatchSize: 2, ChunksBatchSize: 3, ChunksBatchMessageBytes: 100}, func(*client.QueryStreamResponse) error { return sendError })
			require.ErrorIs(t, err, sendError)
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = stream.QueryStream(cancelled, nil, nil, streamBatching{SeriesBatchSize: 2, ChunksBatchSize: 3, ChunksBatchMessageBytes: 100}, func(*client.QueryStreamResponse) error { t.Fatal("sent a cancelled query"); return nil })
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestRustForcedFlushDoesNotRejectFutureWrites(t *testing.T) {
	for _, cutoff := range []int64{1000, math.MaxInt64} {
		t.Run(strconv.FormatInt(cutoff, 10), func(t *testing.T) {
			e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
			ls := labels.FromStrings("__name__", "x")
			app := rustTestAppender(t, e)
			rustTestAppend(t, app, ls, 1, 1)
			require.NoError(t, app.Commit())
			require.NoError(t, e.Flush(t.Context(), cutoff))
			oldest, ok := e.OldestAppendableTime()
			require.True(t, ok)
			require.Equal(t, int64(2), oldest)
			require.NoError(t, e.Flush(t.Context(), cutoff))
			app = rustTestAppender(t, e)
			rustTestAppend(t, app, ls, 3, 3)
			require.NoError(t, app.Commit())
			require.Len(t, rustTestRead(t, e, 0, 5), 1)
		})
	}
}

func TestRustIdleSeriesRevivesAtItsColdTimestamp(t *testing.T) {
	for _, bits := range []uint64{math.Float64bits(1), 0x7ff0000000000002} {
		t.Run(fmt.Sprint(bits), func(t *testing.T) {
			e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
			e.timelyCompaction = true
			e.idleEviction = (25 * time.Minute).Milliseconds()
			require.NoError(t, e.Configure(engineSettings{OutOfOrderTimeWindow: (2 * time.Hour).Milliseconds()}))
			base := int64(20 * time.Hour / time.Millisecond)
			old := labels.FromStrings("__name__", "old")
			app := rustTestAppender(t, e)
			rustTestAppend(t, app, old, base+1000, 1)
			rustTestAppend(t, app, labels.FromStrings("__name__", "live"), base+time.Hour.Milliseconds()+1000, 1)
			require.NoError(t, app.Commit())
			require.NoError(t, e.Compact(t.Context()))
			require.Equal(t, uint64(1), e.NumSeries())
			app = rustTestAppender(t, e)
			ref := rustTestAppend(t, app, old, base+1000, math.Float64frombits(bits))
			require.NoError(t, app.Commit())
			require.Equal(t, uint64(2), e.NumSeries())
			last, err := e.client.Call(rustengine.Request{Op: "last", Ref: uint64(ref)})
			require.NoError(t, err)
			require.NotNil(t, last.Sample)
			require.Equal(t, bits, last.Sample.Bits)
		})
	}
}

func TestRustEqualityCursorsMatchLabelSemantics(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	var rows []labels.Labels
	var evicted []storage.SeriesRef
	for n := range 257 {
		ls := labels.FromStrings("__name__", "métric", "group", fmt.Sprint(n%3), "row", fmt.Sprint(n))
		rows = append(rows, ls)
		var ref storage.SeriesRef
		for timestamp := range 5 {
			ref = rustTestAppend(t, app, ls, int64(timestamp), float64(n))
		}
		if n%2 == 0 {
			evicted = append(evicted, ref)
		}
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 2))
	require.NoError(t, e.Evict(t.Context(), evicted))
	match := func(kind labels.MatchType, name, value string) *labels.Matcher {
		return labels.MustNewMatcher(kind, name, value)
	}
	cases := [][]*labels.Matcher{
		nil,
		{match(labels.MatchEqual, "__name__", "métric")},
		{match(labels.MatchEqual, "group", "1"), match(labels.MatchEqual, "__name__", "métric")},
		{match(labels.MatchEqual, "group", "1"), match(labels.MatchEqual, "group", "1")},
		{match(labels.MatchEqual, "group", "1"), match(labels.MatchEqual, "group", "2")},
		{match(labels.MatchEqual, "missing", "value")},
		{match(labels.MatchEqual, "missing", "")},
		{match(labels.MatchRegexp, "group", "1|2")},
		{match(labels.MatchNotEqual, "group", "1")},
	}
	for _, ms := range cases {
		for _, shardCount := range []uint64{0, 3} {
			var want []string
			for _, ls := range rows {
				matches := shardCount == 0 || labels.StableHash(ls)%shardCount == 1
				for _, m := range ms {
					matches = matches && m.Matches(ls.Get(m.Name))
				}
				if matches {
					want = append(want, ls.String())
				}
			}
			slices.Sort(want)
			for _, function := range []string{"", "series"} {
				q, err := e.Querier(0, 10)
				require.NoError(t, err)
				hints := &storage.SelectHints{Start: 0, End: 10, Func: function, ShardIndex: 1, ShardCount: shardCount}
				set := q.Select(t.Context(), true, hints, ms...)
				var got []string
				for set.Next() {
					got = append(got, set.At().Labels().String())
				}
				require.NoError(t, set.Err())
				slices.Sort(got)
				require.Equal(t, want, got, "%v shard count %d function %s", ms, shardCount, function)
				require.NoError(t, q.Close())
			}
		}
	}
	for _, op := range []string{"cursor_open_equal", "cursor_open_labels_equal"} {
		_, err := e.client.Call(rustengine.Request{Op: op})
		require.EqualError(t, err, "nonempty equality selectors required")
		_, err = e.client.Call(rustengine.Request{Op: op, Labels: []rustengine.Label{{Name: "group", Value: ""}}})
		require.EqualError(t, err, "nonempty equality selectors required")
	}
}

func TestRustAllCursorMatchesExplicitReferences(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	var refs []uint64
	for n := range 19 {
		ls := labels.FromStrings("__name__", fmt.Sprintf("s%02d", n))
		var ref storage.SeriesRef
		for timestamp := range 250 {
			ref = rustTestAppend(t, app, ls, int64(timestamp), float64(n*1000+timestamp))
		}
		refs = append(refs, uint64(ref))
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 100))
	for _, op := range []string{"cursor_open", "cursor_open_labels"} {
		request := rustengine.Request{Op: op, Mint: 0, Maxt: 300, Sorted: true}
		empty, err := e.client.Call(request)
		require.NoError(t, err)
		require.Zero(t, empty.Count)
		_, err = e.client.Call(rustengine.Request{Op: "cursor_close", Cursor: empty.Cursor})
		require.NoError(t, err)
		request.Refs = refs
		explicit, err := e.client.Call(request)
		require.NoError(t, err)
		request.Op, request.Refs = op+"_all", nil
		all, err := e.client.Call(request)
		require.NoError(t, err)
		require.Equal(t, uint64(len(refs)), all.Count)
		require.Equal(t, explicit.Count, all.Count)
		for offset := uint64(0); offset < all.Count; offset += 16 {
			page := rustengine.Request{Op: "cursor_series_page", Ref: offset, Limit: 16, Cursor: explicit.Cursor}
			want, err := e.client.Call(page)
			require.NoError(t, err)
			page.Cursor = all.Cursor
			got, err := e.client.Call(page)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		for _, cursor := range []uint64{explicit.Cursor, all.Cursor} {
			_, err = e.client.Call(rustengine.Request{Op: "cursor_close", Cursor: cursor})
			require.NoError(t, err)
		}
	}
	q, err := e.Querier(0, 300)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), true, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "absent"))
	require.False(t, set.Next())
	require.NoError(t, set.Err())
}

func TestRustSeriesPagesPreserveIndicesAndReusableFirstChunks(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	for n := range 19 {
		ls := labels.FromStrings("__name__", fmt.Sprintf("s%02d", n))
		for timestamp := range 250 {
			rustTestAppend(t, app, ls, int64(timestamp), float64(n*1000+timestamp))
		}
	}
	require.NoError(t, app.Commit())
	q, err := e.ChunkQuerier(0, 300, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(t.Context(), true, nil)
	var retained []storage.ChunkSeries
	for n := 0; set.Next(); n++ {
		series := set.At()
		retained = append(retained, series)
		require.Equal(t, fmt.Sprintf("s%02d", n), series.Labels().Get("__name__"))
		count, err := series.ChunkCount()
		require.NoError(t, err)
		it := series.Iterator(nil)
		chunks, samples := 0, 0
		for it.Next() {
			chunks++
			values := it.At().Chunk.Iterator(nil)
			for values.Next() != chunkenc.ValNone {
				timestamp, value := values.At()
				require.Equal(t, int64(samples), timestamp)
				require.Equal(t, float64(n*1000+samples), value)
				samples++
			}
			require.NoError(t, values.Err())
		}
		require.NoError(t, it.Err())
		require.Equal(t, count, chunks)
		require.Equal(t, 250, samples)
	}
	require.NoError(t, set.Err())
	require.Len(t, retained, 19)
	for n, series := range retained {
		it := series.Iterator(nil)
		require.True(t, it.Next())
		values := it.At().Chunk.Iterator(nil)
		require.Equal(t, chunkenc.ValFloat, values.Next())
		_, value := values.At()
		require.Equal(t, float64(n*1000), value)
	}
}

func TestRustKafkaRecoveryPreservesCleanHeadsAndRewindsCrashOffsets(t *testing.T) {
	dir := t.TempDir()
	open := func() *rustEngine {
		e, err := openRustEngineWithKafkaReplay(dir, tsdb.DefaultOptions(), true)
		require.NoError(t, err)
		return e
	}
	e := open()
	app := rustTestAppender(t, e)
	ls := labels.FromStrings("__name__", "x")
	rustTestAppend(t, app, ls, 1, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Sync())
	wal, err := os.Stat(filepath.Join(dir, "rust-engine", "wal.bin"))
	require.NoError(t, err)
	require.Zero(t, wal.Size())
	// Bypass the engine's clean-close checkpoint to model process death.
	e.client.Close()
	e.closed = true
	e = open()
	require.True(t, e.NeedsKafkaReplay())
	// Failed ingester startup closes tenants before offsets have been rewound.
	require.NoError(t, e.Close())
	e = open()
	require.True(t, e.NeedsKafkaReplay())
	require.Zero(t, e.NumSeries())
	i := &Ingester{logger: log.NewNopLogger()}
	i.cfg.BlocksStorageConfig.TSDB.Dir = dir
	i.cfg.IngestStorageConfig.Enabled = true
	i.cfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = true
	offset := filepath.Join(dir, "kafka-offset.json")
	other := filepath.Join(dir, "runtime.json")
	require.NoError(t, os.WriteFile(offset, []byte("committed"), 0600))
	require.NoError(t, os.WriteFile(other, []byte("keep"), 0600))
	require.NoError(t, i.resetKafkaOffsetsAfterEngineRecovery([]string{"tenant"}))
	e.KafkaOffsetsRewound()
	require.NoFileExists(t, offset)
	require.FileExists(t, other)
	app = rustTestAppender(t, e)
	rustTestAppend(t, app, ls, 1, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Close())
	_, err = openRustEngine(dir, tsdb.DefaultOptions())
	require.EqualError(t, err, "Kafka replay storage requires Kafka-only ingestion with file-stored offsets")
	e = open()
	require.False(t, e.NeedsKafkaReplay())
	require.Len(t, rustTestRead(t, e, 0, 2), 1)
	require.NoError(t, os.WriteFile(offset, []byte("committed"), 0600))
	require.NoError(t, i.resetKafkaOffsetsAfterEngineRecovery(nil))
	require.FileExists(t, offset)
	require.NoError(t, e.Close())
	for _, change := range []func(*Ingester){
		func(i *Ingester) { i.cfg.IngestStorageConfig.Enabled = false },
		func(i *Ingester) { i.cfg.PushGrpcMethodEnabled = true },
		func(i *Ingester) { i.cfg.IngestStorageConfig.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced = false },
		func(i *Ingester) { i.cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled = true },
	} {
		candidate := &Ingester{cfg: i.cfg, logger: i.logger}
		change(candidate)
		require.False(t, candidate.kafkaReplayEnabled())
		require.EqualError(t, candidate.resetKafkaOffsetsAfterEngineRecovery([]string{"tenant"}), "unclean engine recovery requires Kafka-only ingestion with file-stored offsets")
		require.FileExists(t, offset)
	}
}

func TestRustHeadSnapshotRanges(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	for n := range 80 {
		ls := labels.FromStrings("__name__", "x", "id", fmt.Sprintf("%02d", n))
		var ref storage.SeriesRef
		for timestamp := range 1000 {
			var err error
			if n%20 == 0 {
				h := &histogram.Histogram{Count: uint64(timestamp + 1), ZeroCount: uint64(timestamp + 1), Sum: float64(timestamp + 1), ZeroThreshold: 0.001}
				ref, err = app.Append(ref, ls, 0, int64(timestamp), 0, h, nil, storage.AppendV2Options{})
			} else {
				ref, err = app.Append(ref, ls, 0, int64(timestamp), float64(n*1000+timestamp), nil, nil, storage.AppendV2Options{})
			}
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 399))
	for _, bounds := range [][2]int64{{0, 999}, {100, 110}, {395, 405}, {450, 460}, {900, 999}} {
		q, err := e.Querier(bounds[0], bounds[1])
		require.NoError(t, err)
		set := q.Select(t.Context(), true, nil)
		seriesCount := 0
		for set.Next() {
			series := set.At()
			n, err := strconv.Atoi(series.Labels().Get("id"))
			require.NoError(t, err)
			it := series.Iterator(nil)
			expected := bounds[0]
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				require.Equal(t, expected, it.AtT())
				if n%20 == 0 {
					require.Equal(t, chunkenc.ValHistogram, typ)
					_, h := it.AtHistogram(nil)
					require.Equal(t, uint64(expected+1), h.Count)
					require.Equal(t, float64(expected+1), h.Sum)
				} else {
					require.Equal(t, chunkenc.ValFloat, typ)
					_, v := it.At()
					require.Equal(t, float64(int64(n*1000)+expected), v)
				}
				expected++
			}
			require.NoError(t, it.Err())
			require.Equal(t, bounds[1]+1, expected)
			seriesCount++
		}
		require.NoError(t, set.Err())
		require.Equal(t, 80, seriesCount)
		require.NoError(t, q.Close())
	}
}

func TestRustBatchedPostingsKeepStableShardsAndLabelOrder(t *testing.T) {
	e := rustTestEngine(t, t.TempDir(), &rustLifecycle{})
	app := rustTestAppender(t, e)
	var refs []storage.SeriesRef
	sets := map[storage.SeriesRef]labels.Labels{}
	for n := range 273 {
		long := strings.Repeat("é", 1+n%17)
		if n%19 == 0 {
			long = strings.Repeat("é", 800)
		}
		ls := labels.FromStrings("__name__", "x", "id", fmt.Sprintf("%03d", 272-n), "long", long, "étiquette", "Québec ☀️")
		ref := rustTestAppend(t, app, ls, int64(10+n), 1)
		refs = append(refs, ref)
		sets[ref] = ls
	}
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 199))
	ix := &rustIndex{engine: e, cold: true}
	for _, count := range []uint64{1, 3, 16, math.MaxUint64} {
		shards := []uint64{0, count - 1}
		if count <= 16 {
			shards = nil
			for shard := range count {
				shards = append(shards, shard)
			}
		}
		for _, shard := range shards {
			var want []storage.SeriesRef
			for _, ref := range refs {
				if labels.StableHash(sets[ref])%count == shard {
					want = append(want, ref)
				}
			}
			got, err := index.ExpandPostings(ix.ShardedPostings(index.NewListPostings(refs), shard, count))
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	}
	sorted, err := index.ExpandPostings(ix.SortedPostings(index.NewListPostings(refs)))
	require.NoError(t, err)
	want := slices.Clone(refs)
	slices.Reverse(want)
	require.Equal(t, want, sorted)
	_, err = index.ExpandPostings(ix.ShardedPostings(index.NewListPostings(refs), 1, 1))
	require.EqualError(t, err, "invalid shard")
	_, err = index.ExpandPostings(ix.ShardedPostings(index.NewListPostings([]storage.SeriesRef{999999}), 0, 1))
	require.ErrorIs(t, err, storage.ErrNotFound)
	readError := errors.New("postings failed")
	_, err = index.ExpandPostings(ix.ShardedPostings(index.ErrPostings(readError), 0, 1))
	require.ErrorIs(t, err, readError)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = index.ExpandPostings(ix.shardedPostings(cancelled, index.NewListPostings(refs), 0, 1))
	require.ErrorIs(t, err, context.Canceled)
}

func TestRustLabelsOnlySelectionMatchesSampleSelection(t *testing.T) {
	dir := t.TempDir()
	e := rustTestEngine(t, dir, &rustLifecycle{})
	app := rustTestAppender(t, e)
	for n := range 35 {
		ls := labels.FromStrings("__name__", "dense", "id", fmt.Sprintf("%02d", n))
		for timestamp := range 20 {
			rustTestAppend(t, app, ls, int64(timestamp*10), float64(n*100+timestamp))
		}
	}
	for _, name := range []string{"float_gap", "histogram_gap"} {
		ls := labels.FromStrings("__name__", name)
		ref := storage.SeriesRef(0)
		for _, timestamp := range []int64{10, 100} {
			var err error
			if name == "histogram_gap" {
				ref, err = app.Append(ref, ls, 0, timestamp, 0, &histogram.Histogram{Count: 1, ZeroCount: 1, Sum: 1, ZeroThreshold: 0.001}, nil, storage.AppendV2Options{})
			} else {
				ref, err = app.Append(ref, ls, 0, timestamp, 1, nil, nil, storage.AppendV2Options{})
			}
			require.NoError(t, err)
		}
	}
	rustTestAppend(t, app, labels.FromStrings("__name__", "cold_only"), 10, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Flush(t.Context(), 99))
	collect := func(set storage.SeriesSet, noSamples bool) []string {
		out := []string{}
		for set.Next() {
			series := set.At()
			out = append(out, series.Labels().String())
			if noSamples {
				it := series.Iterator(nil)
				require.Equal(t, chunkenc.ValNone, it.Next())
				require.NoError(t, it.Err())
			}
		}
		require.NoError(t, set.Err())
		return out
	}
	for _, bounds := range [][2]int64{{0, 1000}, {50, 60}, {55, 59}, {95, 105}, {110, 190}, {200, 300}, {100, 50}} {
		for _, limit := range []int{0, 1, 17} {
			for _, shards := range [][2]uint64{{0, 0}, {1, 3}} {
				q, err := e.Querier(bounds[0], bounds[1])
				require.NoError(t, err)
				hints := &storage.SelectHints{Start: bounds[0], End: bounds[1], Limit: limit, ShardIndex: shards[0], ShardCount: shards[1]}
				want := collect(q.Select(t.Context(), true, hints), false)
				hints.Func = "series"
				got := collect(q.Select(t.Context(), true, hints), true)
				require.Equal(t, want, got, "bounds=%v limit=%d shards=%v", bounds, limit, shards)
				require.NoError(t, q.Close())
			}
		}
	}
	q, err := e.Querier(0, 1000)
	require.NoError(t, err)
	want := collect(q.Select(t.Context(), true, nil), false)
	require.NoError(t, q.Close())
	files, err := filepath.Glob(filepath.Join(dir, "rust-engine", "cold-*.bin"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		require.NoError(t, os.Remove(file))
	}
	q, err = e.Querier(0, 1000)
	require.NoError(t, err)
	set := q.Select(t.Context(), true, &storage.SelectHints{Start: 0, End: 1000, Func: "series"})
	require.Equal(t, want, collect(set, true))
	require.NoError(t, q.Close())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	q, err = e.Querier(0, 1000)
	require.NoError(t, err)
	set = q.Select(cancelled, true, &storage.SelectHints{Start: 0, End: 1000, Func: "series"})
	require.False(t, set.Next())
	require.ErrorIs(t, set.Err(), context.Canceled)
	require.NoError(t, q.Close())
}
