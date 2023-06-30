// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		"should return no range if head is empty": {
			headMinTime:     math.MaxInt64,
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
