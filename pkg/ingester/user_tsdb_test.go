// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserTSDB_acquireAppendLock(t *testing.T) {
	t.Run("should allow to acquire the lock during forced compaction if not conflicting with the compaction time range", func(t *testing.T) {
		db := &userTSDB{}

		state, err := db.acquireAppendLock(20)
		require.NoError(t, err)
		db.releaseAppendLock(state)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		_, err = db.acquireAppendLock(20)
		require.ErrorIs(t, err, errTSDBEarlyCompaction)

		state, err = db.acquireAppendLock(21)
		require.NoError(t, err)
		db.releaseAppendLock(state)
	})

	t.Run("should count all acquired locks in the inflight appends", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsDone := make(chan struct{})
		go func() {
			db.inFlightAppends.Wait()
			close(inFlightAppendsDone)
		}()

		// Releasing the lock acquired while active should not signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-inFlightAppendsDone:
			t.Fatal("in-flight appends has been signaled as done, but there's still a lock acquired while force compacting")
		case <-time.After(100 * time.Millisecond):
		}

		// Releasing the remaining lock should signal in-flight appends done.
		db.releaseAppendLock(stateForcedCompaction)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends have no been signaled as done, but all locks has been released")
		case <-inFlightAppendsDone:
		}
	})

	t.Run("should count only locks acquired while not force compacting in the inflight appends started before forced compaction", func(t *testing.T) {
		db := &userTSDB{}

		stateActive, err := db.acquireAppendLock(20)
		require.NoError(t, err)

		ok, _ := db.changeStateToForcedCompaction(active, 20)
		require.True(t, ok)

		stateForcedCompaction, err := db.acquireAppendLock(21)
		require.NoError(t, err)

		// Start a goroutine that will signal once in-flight appends are done.
		inFlightAppendsWithoutForcedCompactionDone := make(chan struct{})
		go func() {
			db.inFlightAppendsStartedBeforeForcedCompaction.Wait()
			close(inFlightAppendsWithoutForcedCompactionDone)
		}()

		// Releasing the lock acquired while active should signal in-flight appends done.
		db.releaseAppendLock(stateActive)
		select {
		case <-time.After(100 * time.Millisecond):
			t.Fatal("in-flight appends started before forced compaction has not been signaled as done")
		case <-inFlightAppendsWithoutForcedCompactionDone:
		}

		db.releaseAppendLock(stateForcedCompaction)
	})
}

func refSet(refs ...storage.SeriesRef) map[storage.SeriesRef]struct{} {
	if len(refs) == 0 {
		return nil
	}
	out := make(map[storage.SeriesRef]struct{}, len(refs))
	for _, r := range refs {
		out[r] = struct{}{}
	}
	return out
}

// asSet copies the keys of pendingNonOwnedRefs into a comparable Go map[ref]bool for assertions.
func asSet(db *userTSDB) map[storage.SeriesRef]bool {
	out := make(map[storage.SeriesRef]bool, len(db.pendingNonOwnedRefs))
	for r := range db.pendingNonOwnedRefs {
		out[r] = true
	}
	return out
}

func TestUserTSDB_addPendingNonOwnedRefs(t *testing.T) {
	t.Run("initial snapshot populates the set with current-time per-ref timestamps", func(t *testing.T) {
		db := &userTSDB{}
		before := time.Now()

		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		after := time.Now()

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 2: true, 3: true}, asSet(db))
		for r, ts := range db.pendingNonOwnedRefs {
			assert.False(t, ts.Before(before), "ref %d: timestamp must be at or after detection time", r)
			assert.False(t, ts.After(after), "ref %d: timestamp must not be in the future", r)
		}
	})

	t.Run("re-queuing the same snapshot leaves per-ref timestamps untouched", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		before := make(map[storage.SeriesRef]time.Time, len(db.pendingNonOwnedRefs))
		for r, ts := range db.pendingNonOwnedRefs {
			before[r] = ts
		}

		// Sleep just enough that a re-bump would be observable.
		time.Sleep(2 * time.Millisecond)
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 2: true, 3: true}, asSet(db))
		for r, ts := range db.pendingNonOwnedRefs {
			assert.True(t, ts.Equal(before[r]),
				"ref %d: timestamp must not move when the snapshot adds no new refs", r)
		}
	})

	t.Run("adding a new ref stamps only the new ref and leaves existing per-ref timestamps anchored", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		previousTS := db.pendingNonOwnedRefs[1]

		time.Sleep(2 * time.Millisecond)
		db.addPendingNonOwnedRefs(refSet(1, 2, 3, 4))

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 2: true, 3: true, 4: true}, asSet(db))
		for _, r := range []storage.SeriesRef{1, 2, 3} {
			assert.True(t, db.pendingNonOwnedRefs[r].Equal(previousTS),
				"ref %d: timestamp must stay anchored when a new ref is added to a non-empty set", r)
		}
		assert.True(t, db.pendingNonOwnedRefs[4].After(previousTS),
			"ref 4: timestamp must reflect the time of the second add")
	})

	t.Run("refs absent from the snapshot are dropped without retouching the survivors", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		ts1, ts3 := db.pendingNonOwnedRefs[1], db.pendingNonOwnedRefs[3]

		time.Sleep(2 * time.Millisecond)
		// Ref 2 became owned again (e.g. ring flipped). Snapshot is {1, 3}.
		db.addPendingNonOwnedRefs(refSet(1, 3))

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 3: true}, asSet(db),
			"ref 2 should have been dropped from the pending set")
		assert.True(t, db.pendingNonOwnedRefs[1].Equal(ts1),
			"ref 1: timestamp must stay anchored across a drop-only reconciliation")
		assert.True(t, db.pendingNonOwnedRefs[3].Equal(ts3),
			"ref 3: timestamp must stay anchored across a drop-only reconciliation")
	})

	t.Run("snapshot with both drops and additions reconciles correctly", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		ts1 := db.pendingNonOwnedRefs[1]

		time.Sleep(2 * time.Millisecond)
		// Ref 2 is now owned, ref 4 is newly non-owned.
		db.addPendingNonOwnedRefs(refSet(1, 3, 4))

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 3: true, 4: true}, asSet(db))
		assert.True(t, db.pendingNonOwnedRefs[1].Equal(ts1),
			"ref 1: timestamp must stay anchored across reconciliation")
		assert.True(t, db.pendingNonOwnedRefs[4].After(ts1),
			"ref 4: new arrival must carry the current-time timestamp")
	})

	t.Run("empty snapshot drops every ref", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		require.NotEmpty(t, db.pendingNonOwnedRefs)

		db.addPendingNonOwnedRefs(nil)

		assert.Empty(t, db.pendingNonOwnedRefs)
	})

	t.Run("empty snapshot on an empty set is a no-op", func(t *testing.T) {
		db := &userTSDB{}

		db.addPendingNonOwnedRefs(nil)

		assert.Empty(t, db.pendingNonOwnedRefs)
	})

	t.Run("fully replacing the snapshot drops all old refs and stamps the new ones", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		original := db.pendingNonOwnedRefs[1]

		time.Sleep(2 * time.Millisecond)
		db.addPendingNonOwnedRefs(refSet(4, 5, 6))

		assert.Equal(t, map[storage.SeriesRef]bool{4: true, 5: true, 6: true}, asSet(db))
		for _, r := range []storage.SeriesRef{4, 5, 6} {
			assert.True(t, db.pendingNonOwnedRefs[r].After(original),
				"ref %d: timestamp must reflect the time of the replacing add", r)
		}
	})

	t.Run("duplicate refs in a single snapshot are deduplicated", func(t *testing.T) {
		db := &userTSDB{}

		db.addPendingNonOwnedRefs(refSet(1, 1, 2, 2, 3))

		assert.Equal(t, map[storage.SeriesRef]bool{1: true, 2: true, 3: true}, asSet(db))
	})
}

func TestUserTSDB_takePendingNonOwnedRefs(t *testing.T) {
	// asSortedSlice copies a returned ref slice into a deterministic order so we can
	// assert membership without depending on map iteration order.
	asSortedSlice := func(refs []storage.SeriesRef) []storage.SeriesRef {
		out := append([]storage.SeriesRef(nil), refs...)
		slices.Sort(out)
		return out
	}

	t.Run("returns nil on an empty queue regardless of notAfter", func(t *testing.T) {
		db := &userTSDB{}

		assert.Nil(t, db.takePendingNonOwnedRefs(time.Now()))
		assert.Nil(t, db.takePendingNonOwnedRefs(time.Time{}))
	})

	t.Run("returns nil and leaves the queue intact when the grace period has not elapsed", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		stamped := db.pendingNonOwnedRefs[1]

		// notAfter strictly before every ref's timestamp => take returns nil and the queue is intact.
		got := db.takePendingNonOwnedRefs(stamped.Add(-time.Second))

		assert.Nil(t, got)
		assert.Len(t, db.pendingNonOwnedRefs, 3, "queue must not be cleared on a skipped take")
		for r, ts := range db.pendingNonOwnedRefs {
			assert.True(t, ts.Equal(stamped),
				"ref %d: timestamp must not change on a skipped take", r)
		}
	})

	t.Run("returns all refs and clears the queue when the grace period has elapsed", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))

		got := db.takePendingNonOwnedRefs(time.Now().Add(time.Hour))

		assert.Equal(t, []storage.SeriesRef{1, 2, 3}, asSortedSlice(got))
		assert.Empty(t, db.pendingNonOwnedRefs, "queue must be empty after a successful take")
	})

	t.Run("returns refs at the grace-period boundary (notAfter == ref timestamp)", func(t *testing.T) {
		// Boundary: takePendingNonOwnedRefs skips a ref only when its timestamp is strictly
		// after notAfter, so notAfter == ref's timestamp must succeed.
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(42))

		got := db.takePendingNonOwnedRefs(db.pendingNonOwnedRefs[42])

		assert.Equal(t, []storage.SeriesRef{42}, got)
		assert.Empty(t, db.pendingNonOwnedRefs)
	})

	t.Run("subsequent take after a successful one returns nil", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2))
		require.NotNil(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)))

		assert.Nil(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)),
			"second take with no enqueue in between must return nil")
	})

	t.Run("take then re-enqueue then take returns only the new refs", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		_ = db.takePendingNonOwnedRefs(time.Now().Add(time.Hour))

		db.addPendingNonOwnedRefs(refSet(4, 5))

		got := db.takePendingNonOwnedRefs(time.Now().Add(time.Hour))
		assert.Equal(t, []storage.SeriesRef{4, 5}, asSortedSlice(got))
	})

	t.Run("partial drain returns only refs whose grace has elapsed", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2))

		// Sleep so the second batch lands at a strictly later timestamp.
		time.Sleep(5 * time.Millisecond)
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		newestTS := db.pendingNonOwnedRefs[3]

		// notAfter falls strictly between the two add times: refs 1,2 (older) are
		// eligible, ref 3 (newer) is retained.
		got := db.takePendingNonOwnedRefs(newestTS.Add(-time.Millisecond))

		assert.Equal(t, []storage.SeriesRef{1, 2}, asSortedSlice(got))
		assert.Equal(t, map[storage.SeriesRef]bool{3: true}, asSet(db),
			"ref 3 must stay pending because its add timestamp is after the cutoff")
		assert.True(t, db.pendingNonOwnedRefs[3].Equal(newestTS),
			"ref 3's timestamp must be preserved across a partial drain")
	})

	t.Run("a later take returns refs retained by an earlier partial drain", func(t *testing.T) {
		db := &userTSDB{}
		db.addPendingNonOwnedRefs(refSet(1, 2))
		time.Sleep(5 * time.Millisecond)
		db.addPendingNonOwnedRefs(refSet(1, 2, 3))
		ref3TS := db.pendingNonOwnedRefs[3]

		// Partial drain: refs 1,2 leave; ref 3 stays.
		_ = db.takePendingNonOwnedRefs(ref3TS.Add(-time.Millisecond))
		require.Equal(t, 1, len(db.pendingNonOwnedRefs), "ref 3 should still be pending")

		// Far-future cutoff: ref 3 is now eligible.
		got := db.takePendingNonOwnedRefs(time.Now().Add(time.Hour))
		assert.Equal(t, []storage.SeriesRef{3}, got)
		assert.Empty(t, db.pendingNonOwnedRefs)
	})
}

func TestNextForcedHeadCompactionRange(t *testing.T) {
	const blockDuration = 10

	tests := map[string]struct {
		headMinTime     int64
		headMaxTime     int64
		forcedMaxTime   int64
		expectedMinTime int64
		expectedMaxTime int64
		expectedIsValid bool
		expectedIsLast  bool
	}{
		"should compact the whole head if the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     18,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 18,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should compact the 1st block range period if the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   50,
			expectedMinTime: 12,
			expectedMaxTime: 19,
			expectedIsValid: true,
			expectedIsLast:  false,
		},
		"should honor the forcedMaxTime when the range fits within the block range": {
			headMinTime:     12,
			headMaxTime:     19,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should honor the forcedMaxTime when the range doesn't fit within the block range": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   15,
			expectedMinTime: 12,
			expectedMaxTime: 15,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if forcedMaxTime is smaller than headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   11,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return a range with min time equal to max time if forcedMaxTime is equal to headMinTime": {
			headMinTime:     12,
			headMaxTime:     21,
			forcedMaxTime:   12,
			expectedMinTime: 12,
			expectedMaxTime: 12,
			expectedIsValid: true,
			expectedIsLast:  true,
		},
		"should return no range if head has min time not set": {
			headMinTime:     math.MaxInt64,
			headMaxTime:     20,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
		"should return no range if head has max time not set": {
			headMinTime:     0,
			headMaxTime:     math.MinInt64,
			forcedMaxTime:   12,
			expectedIsValid: false,
			expectedIsLast:  true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMinTime, actualMaxTime, actualIsValid, actualIsLast := nextForcedHeadCompactionRange(blockDuration, testData.headMinTime, testData.headMaxTime, testData.forcedMaxTime)
			assert.Equal(t, testData.expectedIsValid, actualIsValid)
			assert.Equal(t, testData.expectedIsLast, actualIsLast)

			if testData.expectedIsValid {
				assert.Equal(t, testData.expectedMinTime, actualMinTime)
				assert.Equal(t, testData.expectedMaxTime, actualMaxTime)
			}
		})
	}
}

func TestGetSeriesCountAndMinLocalLimit(t *testing.T) {
	tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, tsdbDB.Close())
	})

	// append some series
	app := tsdbDB.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("hello", "world"), 10, 20)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	db := userTSDB{
		db: tsdbDB,
		ownedState: ownedSeriesState{
			ownedSeriesCount: 555,
			localSeriesLimit: 10000,
		},
	}

	t.Run("using series in Head", func(t *testing.T) {
		db.useOwnedSeriesForLimits = false

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 1, cnt)
		require.Equal(t, 0, minLimit)
	})

	t.Run("using owned series", func(t *testing.T) {
		db.useOwnedSeriesForLimits = true

		cnt, minLimit := db.getSeriesCountAndMinLocalLimit()
		require.Equal(t, 555, cnt)
		require.Equal(t, 10000, minLimit)
	})
}

func TestRecomputeOwnedSeries(t *testing.T) {
	limits := validation.Limits{MaxGlobalSeriesPerUser: 0}
	overrides := validation.NewOverrides(limits, nil)
	limiter := NewLimiter(overrides, newIngesterRingLimiterStrategy(nil, 3, true, "zone", overrides.IngestionTenantShardSize))

	t.Run("happy path", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			return 10
		})
		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, but within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}
		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += recomputeOwnedSeriesMaxSeriesDiff / 2
			return 10
		})

		require.True(t, success)
		require.Equal(t, 1, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is within limit", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All but last modifications of ownedSeries during compute will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods)-1; i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}
		mods[len(mods)-1] = recomputeOwnedSeriesMaxSeriesDiff

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.True(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	t.Run("increase during computation, last increase is above limit, computation should retry", func(t *testing.T) {
		db := userTSDB{userID: "test", limiter: limiter}

		// All modifications of ownedSeries will exceed the limit.
		mods := make([]int, recomputeOwnedSeriesMaxAttempts)
		for i := 0; i < len(mods); i++ {
			mods[i] = 2 * recomputeOwnedSeriesMaxSeriesDiff
		}

		success, attempts := db.recomputeOwnedSeriesWithComputeFn(5, "test", log.NewNopLogger(), func() int {
			db.ownedState.ownedSeriesCount += mods[0]
			mods = mods[1:]
			return 10
		})
		require.False(t, success)
		require.Equal(t, recomputeOwnedSeriesMaxAttempts, attempts)
		require.Equal(t, 10, db.ownedState.ownedSeriesCount)
		require.Equal(t, 5, db.ownedState.shardSize)
		require.Equal(t, math.MaxInt32, db.ownedState.localSeriesLimit)
	})

	newActiveSeries := func() *activeseries.ActiveSeries {
		return activeseries.NewActiveSeries(asmodel.NewMatchers(asmodel.CustomTrackersConfig{}), time.Minute, nil)
	}

	t.Run("computeOwnedSeries with no token ranges queues all series for eviction", func(t *testing.T) {
		const userID = "test-user"
		opts := tsdb.DefaultOptions()
		opts.SecondaryHashFunction = secondaryTSDBHashFunctionForUser(userID)
		tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

		app := tsdbDB.Appender(context.Background())
		_, err = app.Append(0, labels.FromStrings("__name__", "metric_a"), 100, 1.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		db := &userTSDB{cfg: &Config{EarlyCompactionNonOwnedSeriesEnabled: true}, db: tsdbDB, activeSeries: newActiveSeries(), ownedTokenRanges: nil}
		count := db.computeOwnedSeries()

		require.Equal(t, 0, count)
		require.Len(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)), 1)
		require.Equal(t, uint64(1), tsdbDB.Head().NumSeries())
	})

	t.Run("computeOwnedSeries with no token ranges and empty head queues nothing", func(t *testing.T) {
		tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

		db := &userTSDB{cfg: &Config{EarlyCompactionNonOwnedSeriesEnabled: true}, db: tsdbDB, activeSeries: newActiveSeries(), ownedTokenRanges: nil}
		count := db.computeOwnedSeries()

		require.Equal(t, 0, count)
		require.Empty(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)))
	})

	t.Run("computeOwnedSeries with all series owned queues nothing", func(t *testing.T) {
		const userID = "test-user"
		opts := tsdb.DefaultOptions()
		opts.SecondaryHashFunction = secondaryTSDBHashFunctionForUser(userID)
		tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

		app := tsdbDB.Appender(context.Background())
		_, err = app.Append(0, labels.FromStrings("__name__", "metric_a"), 100, 1.0)
		require.NoError(t, err)
		_, err = app.Append(0, labels.FromStrings("__name__", "metric_b"), 200, 2.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		db := &userTSDB{cfg: &Config{EarlyCompactionNonOwnedSeriesEnabled: true}, db: tsdbDB, activeSeries: newActiveSeries(), ownedTokenRanges: ring.TokenRanges{0, math.MaxUint32}}
		count := db.computeOwnedSeries()

		require.Equal(t, 2, count)
		require.Empty(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)))
		require.Equal(t, uint64(0), tsdbDB.Head().NumStaleSeries())
	})

	t.Run("computeOwnedSeries with some series non-owned queues only those refs", func(t *testing.T) {
		const userID = "test-user"
		opts := tsdb.DefaultOptions()
		opts.SecondaryHashFunction = secondaryTSDBHashFunctionForUser(userID)
		tsdbDB, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tsdbDB.Close()) })

		labelsA := labels.FromStrings("__name__", "metric_a")
		labelsB := labels.FromStrings("__name__", "metric_b")

		app := tsdbDB.Appender(context.Background())
		_, err = app.Append(0, labelsA, 100, 1.0)
		require.NoError(t, err)
		_, err = app.Append(0, labelsB, 200, 2.0)
		require.NoError(t, err)
		require.NoError(t, app.Commit())

		hashA := mimirpb.ShardByAllLabels(userID, labelsA)
		hashB := mimirpb.ShardByAllLabels(userID, labelsB)
		require.NotEqual(t, hashA, hashB)

		// Own only the series with the lower hash; the other is non-owned.
		minHash := min(hashA, hashB)
		db := &userTSDB{cfg: &Config{EarlyCompactionNonOwnedSeriesEnabled: true}, db: tsdbDB, activeSeries: newActiveSeries(), ownedTokenRanges: ring.TokenRanges{0, minHash}}

		count := db.computeOwnedSeries()

		require.Equal(t, 1, count)
		require.Len(t, db.takePendingNonOwnedRefs(time.Now().Add(time.Hour)), 1)
		require.Equal(t, uint64(2), tsdbDB.Head().NumSeries())
	})
}

// BenchmarkUserTSDB_addPendingNonOwnedRefs measures the per-call cost of the
// reconciliation logic across the four regimes the production path exercises:
//   - "fresh": empty set, snapshot of N refs (first detection after a ring change).
//   - "re-enqueue": same N refs re-passed every recompute cycle (steady state).
//   - "full-replacement": every recompute brings a fully disjoint snapshot
//     (worst case under churn — N drops + N adds per call).
//   - "partial-drop": pending set of N refs, snapshot is a strict half-subset
//     (stale-refs scenario after another compaction path evicted some series —
//     reconciliation must drop N/2 refs with no adds).
func BenchmarkUserTSDB_addPendingNonOwnedRefs(b *testing.B) {
	b.ReportAllocs()
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		refsA := make(map[storage.SeriesRef]struct{}, size)
		refsB := make(map[storage.SeriesRef]struct{}, size)
		refsHalf := make(map[storage.SeriesRef]struct{}, size/2)
		for i := 0; i < size; i++ {
			refsA[storage.SeriesRef(i+1)] = struct{}{}
			refsB[storage.SeriesRef(i+1+size)] = struct{}{}
			if i < size/2 {
				refsHalf[storage.SeriesRef(i+1)] = struct{}{}
			}
		}

		b.Run(fmt.Sprintf("size=%d/fresh", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := &userTSDB{}
				b.StartTimer()
				db.addPendingNonOwnedRefs(refsA)
			}
		})

		b.Run(fmt.Sprintf("size=%d/re-enqueue", size), func(b *testing.B) {
			db := &userTSDB{}
			db.addPendingNonOwnedRefs(refsA)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.addPendingNonOwnedRefs(refsA)
			}
		})

		b.Run(fmt.Sprintf("size=%d/full-replacement", size), func(b *testing.B) {
			db := &userTSDB{}
			db.addPendingNonOwnedRefs(refsA)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%2 == 0 {
					db.addPendingNonOwnedRefs(refsB)
				} else {
					db.addPendingNonOwnedRefs(refsA)
				}
			}
		})

		b.Run(fmt.Sprintf("size=%d/partial-drop", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := &userTSDB{}
				db.addPendingNonOwnedRefs(refsA)
				b.StartTimer()
				db.addPendingNonOwnedRefs(refsHalf)
			}
		})
	}
}

// BenchmarkUserTSDB_takePendingNonOwnedRefs measures the per-call cost of draining
// a populated set across the three branches of takePendingNonOwnedRefs:
//   - "full-drain": every pending ref has aged past the cutoff (ranges over keys
//     only and nils the map in one shot).
//   - "partial-drain": half the refs (added at T0) are past the cutoff and the
//     other half (added at T1) are retained.
//   - "fast-skip": even the oldest pending ref is younger than the cutoff, so the
//     function returns nil without iterating.
//
// The setup (addPendingNonOwnedRefs) is excluded from the timed region; only
// the take itself is measured.
func BenchmarkUserTSDB_takePendingNonOwnedRefs(b *testing.B) {
	b.ReportAllocs()
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		refs := make(map[storage.SeriesRef]struct{}, size)
		for i := 0; i < size; i++ {
			refs[storage.SeriesRef(i+1)] = struct{}{}
		}
		refsA := make(map[storage.SeriesRef]struct{}, size/2)
		refsB := make(map[storage.SeriesRef]struct{}, size/2)
		for i := 0; i < size/2; i++ {
			refsA[storage.SeriesRef(i+1)] = struct{}{}
			refsB[storage.SeriesRef(i+1+size/2)] = struct{}{}
		}

		b.Run(fmt.Sprintf("size=%d/full-drain", size), func(b *testing.B) {
			notAfter := time.Now().Add(time.Hour)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := &userTSDB{}
				db.addPendingNonOwnedRefs(refs)
				b.StartTimer()
				db.takePendingNonOwnedRefs(notAfter)
			}
		})

		b.Run(fmt.Sprintf("size=%d/partial-drain", size), func(b *testing.B) {
			notAfter := time.Now().Add(-time.Minute)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := &userTSDB{}
				// Stage half the refs as if they had been added an hour ago, then
				// add the other half "now". The cutoff (-1 min) falls between the
				// two batches so refsA are eligible and refsB are retained.
				db.addPendingNonOwnedRefs(refsA)
				backdated := time.Now().Add(-time.Hour)
				for r := range db.pendingNonOwnedRefs {
					db.pendingNonOwnedRefs[r] = backdated
				}
				db.addPendingNonOwnedRefs(refsB)
				b.StartTimer()
				db.takePendingNonOwnedRefs(notAfter)
			}
		})

		b.Run(fmt.Sprintf("size=%d/fast-skip", size), func(b *testing.B) {
			// notAfter strictly before every ref's add timestamp => no ref is eligible and take
			// returns nil after iterating once over the map.
			notAfter := time.Now().Add(-time.Hour)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := &userTSDB{}
				db.addPendingNonOwnedRefs(refs)
				b.StartTimer()
				db.takePendingNonOwnedRefs(notAfter)
			}
		})
	}
}
