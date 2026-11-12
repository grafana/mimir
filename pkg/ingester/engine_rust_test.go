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
	"strconv"
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
			{2*block + 50, []string{"gap"}},
			{3*block + 50, []string{}},
		} {
			q, err := e.Querier(tc.start, tc.start+10)
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
	old := rustTestAppend(t, app, labels.FromStrings("__name__", "old"), 10, 1)
	rustTestAppend(t, app, labels.FromStrings("__name__", "current"), block+100, 1)
	require.NoError(t, app.Commit())
	require.NoError(t, e.Evict(t.Context(), []storage.SeriesRef{old}))
	check := func(want []string) {
		q, err := e.Querier(block+100, block+100)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		values, _, err := q.LabelValues(t.Context(), "__name__", nil)
		require.NoError(t, err)
		require.Equal(t, want, values)
	}
	check([]string{"current", "old"})
	require.NoError(t, e.Flush(t.Context(), block+100))
	check([]string{"current"})
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
