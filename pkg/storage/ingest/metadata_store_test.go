// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestEnforceConsistentSegments(t *testing.T) {
	tests := []struct {
		segments         []SegmentRef
		lastOffsetID     int64
		expectedFiltered []SegmentRef
		expectedErr      error
	}{
		{
			segments:         nil,
			expectedFiltered: nil,
		}, {
			segments:         []SegmentRef{},
			expectedFiltered: []SegmentRef{},
		}, {
			lastOffsetID:     -1,
			segments:         []SegmentRef{{OffsetID: 0}, {OffsetID: 1}},
			expectedFiltered: []SegmentRef{{OffsetID: 0}, {OffsetID: 1}},
		}, {
			lastOffsetID:     0,
			segments:         []SegmentRef{{OffsetID: 1}, {OffsetID: 2}},
			expectedFiltered: []SegmentRef{{OffsetID: 1}, {OffsetID: 2}},
		}, {
			lastOffsetID:     0,
			segments:         []SegmentRef{{OffsetID: 2}, {OffsetID: 1}}, // Bad sorting.
			expectedFiltered: []SegmentRef{},
			expectedErr:      errors.New("segments inconsistency detected (expected offset ID: 1 found: 2)"),
		}, {
			lastOffsetID:     1,
			segments:         []SegmentRef{{OffsetID: 1}, {OffsetID: 2}}, // Segment with offset equal to last offset ID.
			expectedFiltered: []SegmentRef{},
			expectedErr:      errors.New("segments inconsistency detected (expected offset ID: 2 found: 1)"),
		}, {
			lastOffsetID:     1,
			segments:         []SegmentRef{{OffsetID: 3}}, // Missing segment.
			expectedFiltered: []SegmentRef{},
			expectedErr:      errors.New("segments inconsistency detected (expected offset ID: 2 found: 3)"),
		},
	}

	for testIdx, testData := range tests {
		t.Run(fmt.Sprintf("test case %d", testIdx), func(t *testing.T) {
			actualFiltered, actualErr := enforceConsistentSegments(testData.segments, testData.lastOffsetID)

			if testData.expectedErr != nil {
				require.EqualError(t, actualErr, testData.expectedErr.Error())
			} else {
				require.NoError(t, actualErr)
			}

			assert.Equal(t, testData.expectedFiltered, actualFiltered)
		})
	}
}

func TestMetadataStore_WatchSegments(t *testing.T) {
	t.Run("should wait until segments are consistent", func(t *testing.T) {
		t.Parallel()

		// Ensure the test doesn't block forever in case of issues.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		var (
			db                = newMetadataDatabaseMemory()
			logs              = &concurrency.SyncBuffer{}
			logger            = log.NewLogfmtLogger(logs)
			store             = NewMetadataStore(db, logger)
			listSegmentsCount = atomic.NewInt64(0)
		)

		// Commit few segments.
		ref0, err := store.CommitSegment(ctx, 1, ulid.MustNew(0, nil), time.Now())
		require.NoError(t, err)
		ref1, err := store.CommitSegment(ctx, 1, ulid.MustNew(1, nil), time.Now())
		require.NoError(t, err)
		ref2, err := store.CommitSegment(ctx, 1, ulid.MustNew(2, nil), time.Now())
		require.NoError(t, err)

		// Mock the database to return inconsistent segments.
		db.registerBeforeListSegmentsHook(func(_ context.Context, _ int32, _ int64) ([]SegmentRef, error, bool) {
			listSegmentsCount.Inc()

			if listSegmentsCount.Load() < 2 {
				// Do not return ref0 to simulate inconsistent segments.
				return []SegmentRef{ref1, ref2}, nil, true
			}

			return nil, nil, false
		})

		actual := store.WatchSegments(ctx, 1, -1)
		assert.Equal(t, []SegmentRef{ref0, ref1, ref2}, actual)

		// Assert on logs.
		assert.Contains(t, logs.String(), "segments inconsistency detected")
		assert.NotContains(t, logs.String(), "segments inconsistency detected since long time")
	})

	t.Run("should return consistent segments if inconsistency is detected in the middle of segments list", func(t *testing.T) {
		t.Parallel()

		// Ensure the test doesn't block forever in case of issues.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		var (
			db     = newMetadataDatabaseMemory()
			logs   = &concurrency.SyncBuffer{}
			logger = log.NewLogfmtLogger(logs)
			store  = NewMetadataStore(db, logger)
		)

		// Commit few segments.
		ref0, err := store.CommitSegment(ctx, 1, ulid.MustNew(0, nil), time.Now())
		require.NoError(t, err)
		_, err = store.CommitSegment(ctx, 1, ulid.MustNew(1, nil), time.Now())
		require.NoError(t, err)
		ref2, err := store.CommitSegment(ctx, 1, ulid.MustNew(2, nil), time.Now())
		require.NoError(t, err)

		// Mock the database to return inconsistent segments.
		db.registerBeforeListSegmentsHook(func(_ context.Context, _ int32, _ int64) ([]SegmentRef, error, bool) {
			// Do not return ref1 to simulate inconsistent segments.
			return []SegmentRef{ref0, ref2}, nil, true
		})

		actual := store.WatchSegments(ctx, 1, -1)
		assert.Equal(t, []SegmentRef{ref0}, actual)

		// Assert on logs.
		assert.Contains(t, logs.String(), "segments inconsistency detected")
		assert.NotContains(t, logs.String(), "segments inconsistency detected since long time")
	})

	t.Run("should not wait indefinitely if a segment inconsistency is not auto-recovered", func(t *testing.T) {
		t.Parallel()

		// Ensure the test doesn't block forever in case of issues.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		var (
			db     = newMetadataDatabaseMemory()
			logs   = &concurrency.SyncBuffer{}
			logger = log.NewLogfmtLogger(logs)
			store  = NewMetadataStore(db, logger)
		)

		// Commit few segments.
		_, err := store.CommitSegment(ctx, 1, ulid.MustNew(0, nil), time.Now())
		require.NoError(t, err)
		ref1, err := store.CommitSegment(ctx, 1, ulid.MustNew(1, nil), time.Now())
		require.NoError(t, err)
		ref2, err := store.CommitSegment(ctx, 1, ulid.MustNew(2, nil), time.Now())
		require.NoError(t, err)

		// Mock the database to return inconsistent segments.
		db.registerBeforeListSegmentsHook(func(_ context.Context, _ int32, _ int64) ([]SegmentRef, error, bool) {
			// Never return ref0 to simulate permanent inconsistency.
			return []SegmentRef{ref1, ref2}, nil, true

		})

		startTime := time.Now()
		actual := store.WatchSegments(ctx, 1, -1)
		elapsedTime := time.Since(startTime)

		assert.Equal(t, []SegmentRef{ref1, ref2}, actual)
		assert.InDelta(t, consistencyErrTolerance.Seconds(), elapsedTime.Seconds(), time.Second.Seconds())

		// Assert on logs.
		assert.Contains(t, logs.String(), "segments inconsistency detected")
		assert.Contains(t, logs.String(), "segments inconsistency detected since long time")
	})
}
