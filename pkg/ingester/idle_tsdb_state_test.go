// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

func newIdleStateTestTracker(t testing.TB, dir string) *idleTSDBStateTracker {
	t.Helper()
	return newIdleTSDBStateTracker(dir, log.NewNopLogger(), newIdleTSDBStateMetrics(prometheus.NewRegistry()))
}

func openIdleStateTestDB(t testing.TB, dir string) *tsdb.DB {
	t.Helper()
	opts := tsdb.DefaultOptions()
	opts.MinBlockDuration = time.Hour.Milliseconds()
	opts.MaxBlockDuration = time.Hour.Milliseconds()
	db, err := tsdb.Open(dir, nil, prometheus.NewRegistry(), opts, nil)
	require.NoError(t, err)
	db.DisableCompactions()
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func appendIdleStateTestSample(t testing.TB, db *tsdb.DB, ts int64) {
	t.Helper()
	app := db.Head().AppenderV2(t.Context())
	_, err := app.Append(0, labels.FromStrings("__name__", "test"), 0, ts, 1, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
}

func idleStateTestConfig(t testing.TB) Config {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.PersistIdleStateEnabled = true
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = time.Hour
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = time.Hour
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBWhenShippingDisabled = true
	return cfg
}

func TestIdleTSDBStateRead(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	for name, data := range map[string]string{
		"corrupt":            "{",
		"unsupported":        `{"version":2,"state":"empty","empty_since_ms":1}`,
		"unknown state":      `{"version":1,"state":"other"}`,
		"missing timestamp":  `{"version":1,"state":"empty"}`,
		"negative timestamp": `{"version":1,"state":"empty","empty_since_ms":-1}`,
		"future timestamp":   fmt.Sprintf(`{"version":1,"state":"empty","empty_since_ms":%d}`, now.Add(time.Hour).UnixMilli()),
		"invalid block ID":   `{"version":1,"state":"empty","empty_since_ms":1,"block_ids":["invalid"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, idleTSDBStateFilename), []byte(data), 0600))
			s := newIdleStateTestTracker(t, dir)
			s.now = func() time.Time { return now }
			s.recover(openIdleStateTestDB(t, dir))
			require.False(t, s.isIdle(time.Hour))
			require.Equal(t, float64(1), testutil.ToFloat64(s.metrics.failures.WithLabelValues("read")))
			require.NoError(t, s.observeEmpty(nil))
			record, err := readIdleTSDBState(dir, now)
			require.NoError(t, err)
			require.Equal(t, now.UnixMilli(), record.EmptySinceMs)
		})
	}
	t.Run("missing", func(t *testing.T) {
		dir := t.TempDir()
		s := newIdleStateTestTracker(t, dir)
		s.recover(openIdleStateTestDB(t, dir))
		require.False(t, s.isIdle(time.Hour))
		require.Zero(t, testutil.ToFloat64(s.metrics.failures.WithLabelValues("read")))
	})
}

func TestIdleTSDBStateRecovery(t *testing.T) {
	for _, mode := range []string{"empty", "retention removed blocks", "legacy head", "legacy block", "active"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			old := time.Now().Add(-2 * time.Hour).UnixMilli()
			record := idleTSDBState{Version: 1, State: idleTSDBEmpty, EmptySinceMs: old}
			if mode == "retention removed blocks" {
				record.BlockIDs = []ulid.ULID{ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV")}
			}
			if mode == "active" {
				record.State = idleTSDBActive
			}
			require.NoError(t, writeIdleTSDBState(dir, record))
			if mode == "legacy head" || mode == "legacy block" {
				t.Run("legacy writer", func(t *testing.T) {
					db := openIdleStateTestDB(t, dir)
					appendIdleStateTestSample(t, db, 1)
					if mode == "legacy block" {
						require.NoError(t, db.CompactHead(tsdb.NewRangeHead(db.Head(), 1, 1)))
						require.Zero(t, db.Head().NumSeries())
					}
				})
			}
			db := openIdleStateTestDB(t, dir)
			s := newIdleStateTestTracker(t, dir)
			s.recover(db)
			trusted := mode == "empty" || mode == "retention removed blocks"
			require.Equal(t, trusted, s.isIdle(time.Hour))
			if trusted {
				require.Equal(t, float64(1), testutil.ToFloat64(s.metrics.restored))
				require.NoError(t, s.observeEmpty(idleTSDBBlockIDs(db)))
				require.Equal(t, old, s.record.EmptySinceMs)
			} else if db.Head().NumSeries() == 0 {
				require.NoError(t, s.observeEmpty(idleTSDBBlockIDs(db)))
				require.Greater(t, s.record.EmptySinceMs, old)
			}
		})
	}
}

func TestIdleTSDBStatePersistenceFailures(t *testing.T) {
	for _, failure := range []string{"create", "rename", "after directory sync"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			s := newIdleStateTestTracker(t, dir)
			s.now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
			path := filepath.Join(dir, idleTSDBStateFilename)
			switch failure {
			case "create":
				require.NoError(t, os.Mkdir(path+".tmp", 0700))
			case "rename":
				require.NoError(t, os.Mkdir(path, 0700))
			case "after directory sync":
				s.persist = func(dir string, state idleTSDBState) error {
					if err := writeIdleTSDBState(dir, state); err != nil {
						return err
					}
					return os.ErrInvalid
				}
			}
			require.Error(t, s.observeEmpty(nil))
			since := s.record.EmptySinceMs
			require.False(t, s.isIdle(time.Hour), "a failed write must never authorize deletion")
			require.Equal(t, float64(1), testutil.ToFloat64(s.metrics.failures.WithLabelValues("write")))
			if failure == "create" {
				require.NoError(t, os.Remove(path+".tmp"))
			}
			if failure == "rename" {
				require.NoError(t, os.Remove(path))
			}
			s.persist = writeIdleTSDBState
			require.NoError(t, s.observeEmpty(nil))
			require.Equal(t, since, s.record.EmptySinceMs)
			s.now = time.Now
			require.True(t, s.isIdle(time.Hour))
		})
	}
}

func TestIdleTSDBStateActivation(t *testing.T) {
	dir := t.TempDir()
	s := newIdleStateTestTracker(t, dir)
	require.NoError(t, s.observeEmpty(nil))
	old := s.record
	failure := errors.New("injected storage failure")
	s.persist = func(string, idleTSDBState) error { return failure }
	require.ErrorIs(t, s.activate(), failure)
	require.Equal(t, old, s.record)
	var writes atomic.Int64
	s.persist = func(dir string, state idleTSDBState) error {
		writes.Add(1)
		return writeIdleTSDBState(dir, state)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Go(func() { errs <- s.activate() })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), writes.Load())
	record, err := readIdleTSDBState(dir, time.Now())
	require.NoError(t, err)
	require.Equal(t, idleTSDBActive, record.State)
	s.persist = func(string, idleTSDBState) error {
		t.Error("active appends must not perform metadata I/O")
		return failure
	}
	require.NoError(t, s.activate())
}

func TestIdleTSDBStateAppendGate(t *testing.T) {
	dir := t.TempDir()
	db := openIdleStateTestDB(t, dir)
	u := &userTSDB{db: db, idleState: newIdleStateTestTracker(t, dir), state: active}
	state, err := u.acquireAppendLock(1)
	require.NoError(t, err)
	started, done := make(chan struct{}), make(chan error, 1)
	go func() { close(started); done <- u.observeEmptyTSDB() }()
	<-started
	// Observation must wait for the admitted append, without blocking the state transition.
	require.Eventually(t, func() bool {
		if u.idleState.gate.TryRLock() {
			u.idleState.gate.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	require.True(t, u.stateMtx.TryLock())
	u.stateMtx.Unlock()
	select {
	case err := <-done:
		t.Fatalf("observation passed an admitted append: %v", err)
	default:
	}
	require.NoError(t, u.idleState.activate())
	appendIdleStateTestSample(t, db, 1)
	u.releaseAppendLock(state)
	require.NoError(t, <-done)
	require.Equal(t, idleTSDBActive, u.idleState.record.State)
	// Rejected admission must release the gate too.
	require.True(t, func() bool { ok, _ := u.changeState(active, closing); return ok }())
	_, err = u.acquireAppendLock(1)
	require.ErrorIs(t, err, errTSDBClosing)
	require.True(t, u.idleState.gate.TryLock())
	u.idleState.gate.Unlock()
}

func TestIngesterPersistIdleStateStartupCleanup(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	for restart := range 3 {
		t.Run(fmt.Sprintf("restart %d", restart), func(t *testing.T) {
			cfg := idleStateTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 3 * time.Hour
			i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
			require.NoError(t, err)
			t.Cleanup(i.lifecycler.StopAsync)
			u, err := i.getOrCreateTSDB(userID)
			require.NoError(t, err)
			if restart == 0 {
				u.idleState.record.EmptySinceMs = old
				require.NoError(t, writeIdleTSDBState(u.db.Dir(), u.idleState.record))
			} else {
				require.Equal(t, old, u.idleState.record.EmptySinceMs)
			}
			require.Equal(t, tsdbNotIdle, i.closeAndDeleteUserTSDBIfIdle(userID))
			i.closeAllTSDB()
		})
	}
	cfg := idleStateTestConfig(t)
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(i.lifecycler.StopAsync)
	startAndWaitHealthy(t, i, r)
	require.Nil(t, i.getTSDB(userID), "cleanup must finish during startup, before the first periodic sweep")
	_, err = os.Stat(filepath.Join(dir, userID))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, float64(1), testutil.ToFloat64(i.metrics.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))))
}

func TestIngesterPersistIdleStatePush(t *testing.T) {
	for _, kind := range []string{"sample", "histogram", "float histogram", "exemplar", "out of order"} {
		t.Run(kind, func(t *testing.T) {
			cfg := idleStateTestConfig(t)
			limits := defaultLimitsTestConfig()
			limits.NativeHistogramsIngestionEnabled = true
			limits.MaxGlobalExemplarsPerUser = 100
			limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
			i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", prometheus.NewRegistry())
			require.NoError(t, err)
			t.Cleanup(i.lifecycler.StopAsync)
			startAndWaitHealthy(t, i, r)
			ctx := user.InjectOrgID(t.Context(), userID)
			u, err := i.getOrCreateTSDB(userID)
			require.NoError(t, err)
			series := mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test"}}}
			ts := time.Now().UnixMilli()
			if kind == "exemplar" || kind == "out of order" {
				pushSingleSampleAtTime(t, i, ts+1)
				// Force reactivation even with an existing series to exercise these data paths.
				u.idleState.mu.Lock()
				u.idleState.record.State = idleTSDBEmpty
				u.idleState.mu.Unlock()
			}
			switch kind {
			case "sample", "out of order":
				series.Samples = []mimirpb.Sample{{TimestampMs: ts, Value: 1}}
			case "histogram":
				series.Histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(ts, util_test.GenerateTestHistogram(1))}
			case "float histogram":
				series.Histograms = []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(ts, util_test.GenerateTestFloatHistogram(1))}
			case "exemplar":
				series.Exemplars = []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "test"}}, TimestampMs: ts, Value: 1}}
			}
			request := func() *mimirpb.WriteRequest {
				return &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &series}}}
			}
			persist := u.idleState.persist
			failure := errors.New("injected metadata write failure")
			u.idleState.persist = func(string, idleTSDBState) error { return failure }
			err = i.PushWithCleanup(ctx, request(), func() {})
			require.EqualError(t, err, "user=1: persist active TSDB idle state: injected metadata write failure")
			require.Equal(t, codes.Internal, status.Code(mapPushErrorToErrorWithStatus(err)))
			require.Equal(t, idleTSDBEmpty, u.idleState.record.State)
			if kind != "exemplar" {
				q, err := u.Querier(ts, ts)
				require.NoError(t, err)
				set := q.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "test"))
				require.False(t, set.Next(), "metadata failure must roll back the accepted sample")
				require.NoError(t, set.Err())
				require.NoError(t, q.Close())
			}
			u.idleState.persist = persist
			require.NoError(t, i.PushWithCleanup(ctx, request(), func() {}))
			record, err := readIdleTSDBState(u.db.Dir(), time.Now())
			require.NoError(t, err)
			require.Equal(t, idleTSDBActive, record.State)
			u.idleState.persist = func(string, idleTSDBState) error {
				t.Error("steady-state write performed metadata I/O")
				return failure
			}
			if kind != "exemplar" {
				// The exact duplicate is still an accepted append and requires no metadata update.
				require.NoError(t, i.PushWithCleanup(ctx, request(), func() {}))
			}
		})
	}
}

func TestIngesterPersistIdleStateRejectedRequest(t *testing.T) {
	cfg := idleStateTestConfig(t)
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(i.lifecycler.StopAsync)
	startAndWaitHealthy(t, i, r)
	u, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	before := u.idleState.record
	u.idleState.persist = func(string, idleTSDBState) error {
		t.Error("rejected request performed metadata I/O")
		return os.ErrInvalid
	}
	series := &mimirpb.TimeSeries{
		Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: "test"}},
		Exemplars: []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "test"}}, TimestampMs: 1, Value: 1}},
	}
	err = i.PushWithCleanup(user.InjectOrgID(t.Context(), userID), &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: series}}}, func() {})
	// Exemplars ingestion is disabled, so there is no accepted sample or exemplar.
	require.NoError(t, err)
	require.Equal(t, before, u.idleState.record)
}

func TestIngesterPersistIdleStateDisabled(t *testing.T) {
	for _, mode := range []string{"flag", "timeout", "shipping"} {
		t.Run(mode, func(t *testing.T) {
			cfg := idleStateTestConfig(t)
			switch mode {
			case "flag":
				cfg.BlocksStorageConfig.TSDB.PersistIdleStateEnabled = false
			case "timeout":
				cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0
			case "shipping":
				cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBWhenShippingDisabled = false
			}
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, userID), 0700))
			path := filepath.Join(dir, userID, idleTSDBStateFilename)
			require.NoError(t, os.WriteFile(path, []byte("ignored"), 0600))
			i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
			require.NoError(t, err)
			t.Cleanup(i.lifecycler.StopAsync)
			u, err := i.getOrCreateTSDB(userID)
			require.NoError(t, err)
			require.Nil(t, u.idleState)
			require.Nil(t, i.idleTSDBStateMetrics)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "ignored", string(data))
			i.closeAllTSDB()
		})
	}
}

// This helper deliberately exits without closing the TSDB or flushing deferred cleanup.
func TestIdleTSDBStateCrashProcess(t *testing.T) {
	mode := os.Getenv("MIMIR_IDLE_STATE_CRASH")
	if mode == "" {
		return
	}
	dir := os.Getenv("MIMIR_IDLE_STATE_DIR")
	db := openIdleStateTestDB(t, dir)
	s := newIdleStateTestTracker(t, dir)
	s.recover(db)
	require.NoError(t, s.observeEmpty(nil))
	switch mode {
	case "temporary file":
		f, err := atomicfs.Create(filepath.Join(dir, idleTSDBStateFilename))
		require.NoError(t, err)
		_, err = f.WriteString(`{"version":1,"state":"active"}`)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
	case "active before commit":
		require.NoError(t, s.activate())
	case "committed":
		require.NoError(t, s.activate())
		appendIdleStateTestSample(t, db, 1)
		require.NoError(t, db.Head().FsyncWLSegments())
	case "legacy committed":
		appendIdleStateTestSample(t, db, 1)
		require.NoError(t, db.Head().FsyncWLSegments())
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
	os.Exit(0)
}

func TestIdleTSDBStateAbruptRestart(t *testing.T) {
	for _, mode := range []string{"temporary file", "active before commit", "committed", "legacy committed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			old := time.Now().Add(-2 * time.Hour).UnixMilli()
			require.NoError(t, writeIdleTSDBState(dir, idleTSDBState{Version: 1, State: idleTSDBEmpty, EmptySinceMs: old}))
			exe, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestIdleTSDBStateCrashProcess$")
			cmd.Env = append(os.Environ(), "MIMIR_IDLE_STATE_CRASH="+mode, "MIMIR_IDLE_STATE_DIR="+dir)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			db := openIdleStateTestDB(t, dir)
			s := newIdleStateTestTracker(t, dir)
			s.recover(db)
			require.Equal(t, mode == "temporary file", s.isIdle(time.Hour))
			if mode == "committed" || mode == "legacy committed" {
				require.Equal(t, uint64(1), db.Head().NumSeries())
			}
		})
	}
}

func BenchmarkIdleTSDBState(b *testing.B) {
	for _, mode := range []string{"active", "first resumed write", "recover empty"} {
		b.Run(mode, func(b *testing.B) {
			dir := b.TempDir()
			s := newIdleStateTestTracker(b, dir)
			require.NoError(b, s.observeEmpty(nil))
			var writes atomic.Int64
			s.persist = func(dir string, state idleTSDBState) error { writes.Add(1); return writeIdleTSDBState(dir, state) }
			if mode == "active" {
				require.NoError(b, s.activate())
			}
			var db *tsdb.DB
			if mode == "recover empty" {
				db = openIdleStateTestDB(b, dir)
			}
			writes.Store(0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				switch mode {
				case "active":
					require.NoError(b, s.activate())
				case "first resumed write":
					b.StopTimer()
					require.NoError(b, s.observeEmpty(nil))
					b.StartTimer()
					require.NoError(b, s.activate())
				case "recover empty":
					s.recover(db)
				}
			}
			b.StopTimer()
			if mode == "active" {
				require.Zero(b, writes.Load())
			}
		})
	}
}

func TestIngesterPersistIdleStateCloseGuards(t *testing.T) {
	for _, mode := range []string{"unshipped", "tenant deleted", "metadata failure"} {
		t.Run(mode, func(t *testing.T) {
			cfg := idleStateTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.ShipInterval = time.Hour
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBWhenShippingDisabled = false
			i, _, err := prepareIngesterWithBlocksStorage(t, cfg, nil, prometheus.NewRegistry())
			require.NoError(t, err)
			t.Cleanup(i.lifecycler.StopAsync)
			t.Cleanup(i.closeAllTSDB)
			u, err := i.getOrCreateTSDB(userID)
			require.NoError(t, err)
			appendIdleStateTestSample(t, u.db, 1)
			if mode == "tenant deleted" {
				u.deletionMarkFound.Store(true)
				u.idleState.persist = func(string, idleTSDBState) error {
					t.Error("tenant deletion should bypass idle metadata")
					return os.ErrInvalid
				}
				require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
				require.Zero(t, i.seriesCount.Load())
				return
			}
			require.NoError(t, u.db.CompactHead(tsdb.NewRangeHead(u.Head(), 1, 1)))
			require.Zero(t, u.Head().NumSeries())
			// Establish an expired empty period without waiting for wall time.
			u.idleState.record = idleTSDBState{}
			u.idleState.persisted = false
			u.idleState.now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
			if mode == "metadata failure" {
				u.idleState.persist = func(string, idleTSDBState) error { return os.ErrInvalid }
				require.Equal(t, tsdbCheckFailed, i.closeAndDeleteUserTSDBIfIdle(userID))
				require.NotNil(t, i.getTSDB(userID))
				return
			}
			require.NoError(t, u.observeEmptyTSDB())
			u.idleState.now = time.Now
			require.Equal(t, tsdbNotShipped, i.closeAndDeleteUserTSDBIfIdle(userID))
			i.shipBlocks(t.Context(), nil)
			require.Zero(t, u.getOldestUnshippedBlockTime())
			require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))
		})
	}
}

func TestIdleTSDBStateRefreshBlocks(t *testing.T) {
	dir := t.TempDir()
	db := openIdleStateTestDB(t, dir)
	appendIdleStateTestSample(t, db, 1)
	require.NoError(t, db.CompactHead(tsdb.NewRangeHead(db.Head(), 1, 1)))
	s := newIdleStateTestTracker(t, dir)
	require.NoError(t, s.observeEmpty(idleTSDBBlockIDs(db)))
	since := s.record.EmptySinceMs
	// An aware process can refresh block identities after maintenance without losing age.
	id := ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	require.NoError(t, s.observeEmpty([]ulid.ULID{id}))
	require.Equal(t, since, s.record.EmptySinceMs)
	// Retention may subsequently remove any subset without requiring a metadata write.
	s.persist = func(string, idleTSDBState) error { t.Error("retention reset the empty period"); return os.ErrInvalid }
	require.NoError(t, s.observeEmpty(nil))
}

func BenchmarkIdleTSDBStateOpen(b *testing.B) {
	for _, mode := range []string{"disabled", "restore", "initialize"} {
		b.Run(mode, func(b *testing.B) {
			cfg := idleStateTestConfig(b)
			cfg.BlocksStorageConfig.TSDB.PersistIdleStateEnabled = mode != "disabled"
			i, _, err := prepareIngesterWithBlocksStorage(b, cfg, nil, prometheus.NewRegistry())
			require.NoError(b, err)
			b.Cleanup(i.lifecycler.StopAsync)
			_, err = i.getOrCreateTSDB(userID)
			require.NoError(b, err)
			i.closeAllTSDB()
			b.ResetTimer()
			for range b.N {
				if mode == "initialize" {
					b.StopTimer()
					require.NoError(b, os.Remove(filepath.Join(i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID), idleTSDBStateFilename)))
					b.StartTimer()
				}
				u, err := i.createTSDB(userID, 0)
				require.NoError(b, err)
				b.StopTimer()
				require.NoError(b, u.Close())
				i.tsdbMetrics.RemoveRegistryForTenant(userID)
				b.StartTimer()
			}
		})
	}
}

func TestIngesterPersistIdleStateStartupCompaction(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	var before []ulid.ULID
	t.Run("seed", func(t *testing.T) {
		db := openIdleStateTestDB(t, filepath.Join(dir, userID))
		for n := range 6 {
			ts := int64(n) * (2 * time.Hour).Milliseconds()
			appendIdleStateTestSample(t, db, ts)
			require.NoError(t, db.CompactHead(tsdb.NewRangeHead(db.Head(), ts, ts+(2*time.Hour).Milliseconds()-1)))
		}
		require.Zero(t, db.Head().NumSeries())
		before = idleTSDBBlockIDs(db)
		require.Len(t, before, 6)
		require.NoError(t, writeIdleTSDBState(db.Dir(), idleTSDBState{Version: 1, State: idleTSDBEmpty, EmptySinceMs: old, BlockIDs: before}))
	})
	cfg := idleStateTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{2 * time.Hour, 6 * time.Hour, 12 * time.Hour}
	i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(i.lifecycler.StopAsync)
	t.Cleanup(i.closeAllTSDB)
	u, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotEqual(t, before, idleTSDBBlockIDs(u.db), "startup compaction must actually change the block identities")
	require.Equal(t, old, u.idleState.record.EmptySinceMs)
	record, err := readIdleTSDBState(u.db.Dir(), time.Now())
	require.NoError(t, err)
	require.Equal(t, idleTSDBBlockIDs(u.db), record.BlockIDs)
}

func TestIdleTSDBStateFailedCommit(t *testing.T) {
	dir := t.TempDir()
	wal, err := wlog.New(nil, nil, filepath.Join(dir, "wal"), compression.None)
	require.NoError(t, err)
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = dir
	h, err := tsdb.NewHead(nil, nil, wal, nil, opts, tsdb.NewHeadStats())
	require.NoError(t, err)
	require.NoError(t, h.Init(0))
	t.Cleanup(func() { require.ErrorContains(t, h.Close(), "wlog already closed") })
	s := newIdleStateTestTracker(t, dir)
	require.NoError(t, s.observeEmpty(nil))
	app := h.AppenderV2(t.Context())
	_, err = app.Append(0, labels.FromStrings("__name__", "test"), 0, 1, 1, nil, nil, storage.AOptions{})
	require.NoError(t, err)
	// Closing the log injects a real commit failure after the active record is durable.
	require.NoError(t, wal.Close())
	require.NoError(t, s.activate())
	require.ErrorIs(t, app.Commit(), os.ErrClosed)
	record, err := readIdleTSDBState(dir, time.Now())
	require.NoError(t, err)
	require.Equal(t, idleTSDBActive, record.State)
	require.False(t, s.isIdle(time.Nanosecond))
}

func TestIngesterPersistIdleStateInvalidSample(t *testing.T) {
	cfg := idleStateTestConfig(t)
	i, _, err := prepareIngesterWithBlocksStorage(t, cfg, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(i.lifecycler.StopAsync)
	t.Cleanup(i.closeAllTSDB)
	u, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	appendIdleStateTestSample(t, u.db, 1)
	require.NoError(t, u.db.CompactHead(tsdb.NewRangeHead(u.Head(), 1, 1)))
	require.NoError(t, u.observeEmptyTSDB())
	before := u.idleState.record
	u.idleState.persist = func(string, idleTSDBState) error {
		t.Error("invalid sample changed idle metadata")
		return os.ErrInvalid
	}
	ctx := user.InjectOrgID(t.Context(), userID)
	err = i.PushWithCleanup(ctx, mockWriteRequest(t, labels.FromStrings("__name__", "test"), 1, 0), func() {})
	require.ErrorContains(t, err, "err-mimir-sample-timestamp-too-old")
	require.Equal(t, before, u.idleState.record)
}

func BenchmarkIdleTSDBStateAppendAdmission(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
			u := &userTSDB{state: active}
			if enabled {
				u.idleState = newIdleStateTestTracker(b, b.TempDir())
				require.NoError(b, u.idleState.activate())
				u.idleState.persist = func(string, idleTSDBState) error { b.Fatal("active append performed metadata I/O"); return nil }
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				state, err := u.acquireAppendLock(1)
				if err != nil {
					b.Fatal(err)
				}
				if enabled {
					if err := u.idleState.activate(); err != nil {
						b.Fatal(err)
					}
				}
				u.releaseAppendLock(state)
			}
		})
	}
}

func TestIngesterPersistIdleStateReenableAfterWrites(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compacted=%t", compact), func(t *testing.T) {
			dir := t.TempDir()
			old := time.Now().Add(-2 * time.Hour).UnixMilli()
			var before []byte
			t.Run("initial empty state", func(t *testing.T) {
				cfg := idleStateTestConfig(t)
				i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
				require.NoError(t, err)
				t.Cleanup(i.lifecycler.StopAsync)
				t.Cleanup(i.closeAllTSDB)
				u, err := i.getOrCreateTSDB(userID)
				require.NoError(t, err)
				record := idleTSDBState{Version: 1, State: idleTSDBEmpty, EmptySinceMs: old}
				require.NoError(t, writeIdleTSDBState(u.db.Dir(), record))
				before, err = os.ReadFile(filepath.Join(u.db.Dir(), idleTSDBStateFilename))
				require.NoError(t, err)
			})
			t.Run("disabled writer", func(t *testing.T) {
				cfg := idleStateTestConfig(t)
				cfg.BlocksStorageConfig.TSDB.PersistIdleStateEnabled = false
				i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
				require.NoError(t, err)
				t.Cleanup(i.lifecycler.StopAsync)
				t.Cleanup(i.closeAllTSDB)
				require.NoError(t, i.PushWithCleanup(user.InjectOrgID(t.Context(), userID), mockWriteRequest(t, labels.FromStrings("__name__", "test"), 1, 1), func() {}))
				u := i.getTSDB(userID)
				if compact {
					require.NoError(t, u.db.CompactHead(tsdb.NewRangeHead(u.Head(), 1, 1)))
				}
				after, err := os.ReadFile(filepath.Join(u.db.Dir(), idleTSDBStateFilename))
				require.NoError(t, err)
				require.Equal(t, before, after, "disabled writer must leave the old record untouched")
			})
			cfg := idleStateTestConfig(t)
			i, _, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dir, prometheus.NewRegistry())
			require.NoError(t, err)
			t.Cleanup(i.lifecycler.StopAsync)
			t.Cleanup(i.closeAllTSDB)
			u, err := i.getOrCreateTSDB(userID)
			require.NoError(t, err)
			require.False(t, u.idleState.isIdle(time.Hour))
			require.Zero(t, testutil.ToFloat64(i.idleTSDBStateMetrics.restored))
			if compact {
				require.Greater(t, u.idleState.record.EmptySinceMs, old)
			}
		})
	}
}
