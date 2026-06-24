// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const cacheEntryInterval = 24 * time.Hour

func TestCacheOperator(t *testing.T) {
	timeZero := timestamp.Time(0)
	desiredStart := timeZero.Add(10 * time.Minute)
	desiredEnd := timeZero.Add(20 * time.Minute)
	step := time.Minute
	defaultTimeRange := types.NewRangeQueryTimeRange(desiredStart, desiredEnd, step)

	timeNow := timeZero.Add(10 * time.Hour)
	ttl := 180 * time.Minute
	oooTTL := 8 * time.Minute
	oooWindow := 10 * time.Minute
	withinTTL := timeNow.Add(-100 * time.Minute)
	beforeTTLThreshold := timeNow.Add(-ttl).Add(-10 * time.Minute)
	existingTraceID := "some-old-trace-id"
	newTraceID := "some-new-trace-id"

	limits := &mockLimitsProvider{
		maxFreshness: 5 * time.Minute,
		ttl:          ttl,
		// OOO TTL and window are only set for test cases where enableOOO is true.
	}

	testCases := map[string]struct {
		timeRange                   types.QueryTimeRange // If not set, uses defaultTimeRange.
		expectedAnnotationTimeRange types.QueryTimeRange // If not set, uses timeRange. This is necessary because we don't have a way to determine what timestamp an annotation applies to, so we have to return all annotations associated with an extent if any samples from that extent are used.
		existingCacheEntry          *CacheEntry          // nil if no cache entry should be populated before running the test. If non-nil, and CacheKey is nil, CacheKey will be set to the expected value in the test.
		enableOOO                   bool

		expectedFreshlyEvaluatedRanges   []types.QueryTimeRange
		expectedWrittenCacheEntry        *CacheEntry   // nil if no cache entry should be written. If non-nil, CacheKey will be set to the expected value in the test.
		expectedTTL                      time.Duration // If not set, uses limits.ttl.
		expectedAnyCachedExtentsRetained bool
	}{
		"cache miss": {
			existingCacheEntry: nil,

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{defaultTimeRange},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, timeNow, newTraceID),
				},
			},
		},

		"cache hit, but key inside entry does not match expected value": {
			existingCacheEntry: &CacheEntry{
				CacheKey: []byte("this is not the expected cache key"),
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeZero.Add(8*time.Minute), timeZero.Add(11*time.Minute), time.Minute), withinTTL, existingTraceID),
				},
			},

			// The existing cache entry should be ignored and a new one written.
			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{defaultTimeRange},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, timeNow, newTraceID),
				},
			},
		},

		"cache hit with single extent that matches desired time range exactly": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges:   nil,
			expectedWrittenCacheEntry:        nil,
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts before desired time range and finishes at end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges:   nil,
			expectedWrittenCacheEntry:        nil,
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts before desired time range and finishes one step before end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd.Add(-step), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredEnd, desiredEnd, step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts at beginning of desired time range and finishes after end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(5*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges:   nil,
			expectedWrittenCacheEntry:        nil,
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(5*time.Minute), step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts before desired time range and finishes before end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd.Add(-3*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredEnd.Add(-2*time.Minute), desiredEnd, step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts before desired time range and finishes before end of desired time range, and desired end timestamp is not (start + N×step), ie. not aligned to step": {
			timeRange: types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(20*time.Second), step),

			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd.Add(-3*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredEnd.Add(-2*time.Minute), desiredEnd, step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd, step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts before desired time range and finishes before end of desired time range, and desired end timestamp is not (start + N×step), ie. not aligned to step, and desired time range is before Unix epoch": {
			timeRange: types.NewRangeQueryTimeRange(timeZero.Add(-20*time.Minute), timeZero.Add(-10*time.Minute).Add(20*time.Second), step),

			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeZero.Add(-25*time.Minute), timeZero.Add(-15*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(timeZero.Add(-14*time.Minute), timeZero.Add(-10*time.Minute), step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeZero.Add(-25*time.Minute), timeZero.Add(-10*time.Minute), step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(timeZero.Add(-25*time.Minute), timeZero.Add(-10*time.Minute), step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts part-way through desired time range and finishes at end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(5*time.Minute), desiredEnd, step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredStart, desiredStart.Add(4*time.Minute), step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts part-way through desired time range and finishes after end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(5*time.Minute), desiredEnd.Add(3*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredStart, desiredStart.Add(4*time.Minute), step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(3*time.Minute), step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(3*time.Minute), step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts part-way through desired time range and finishes before end of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(2*time.Minute), desiredEnd.Add(-3*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(desiredStart, desiredStart.Add(time.Minute), step),
				types.NewRangeQueryTimeRange(desiredEnd.Add(-2*time.Minute), desiredEnd, step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with extents before and after desired time range but none in desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredStart.Add(-2*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(2*time.Minute), desiredEnd.Add(4*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{defaultTimeRange},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredStart.Add(-2*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd, step), timeNow, newTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(2*time.Minute), desiredEnd.Add(4*time.Minute), step), withinTTL, existingTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with single extent that starts on last step of desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredEnd, desiredEnd.Add(3*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(-step), step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(3*time.Minute), step), withinTTL, newTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart, desiredEnd.Add(3*time.Minute), step),
		},

		"cache hit with extents before and after desired time range that overlap desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredStart.Add(2*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(-2*time.Minute), desiredEnd.Add(4*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{types.NewRangeQueryTimeRange(desiredStart.Add(3*time.Minute), desiredEnd.Add(-3*time.Minute), step)},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd.Add(4*time.Minute), step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*time.Minute), desiredEnd.Add(4*time.Minute), step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit with multiple extents in desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(2*time.Minute), desiredStart.Add(4*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(8*time.Minute), desiredStart.Add(9*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(desiredStart, desiredStart.Add(time.Minute), step),
				types.NewRangeQueryTimeRange(desiredStart.Add(5*time.Minute), desiredStart.Add(7*time.Minute), step),
				types.NewRangeQueryTimeRange(desiredStart.Add(10*time.Minute), desiredStart.Add(10*time.Minute), step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart, desiredEnd, step), withinTTL, newTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit, but extents are just before and just after the desired time range": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*step), desiredStart.Add(-step), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(step), desiredEnd.Add(5*step), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{defaultTimeRange},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					// Even though the extents don't overlap the desired time range, we still expect them to be merged into a single extent,
					// to avoid fragmentation of extents.
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-5*step), desiredEnd.Add(5*step), step), withinTTL, newTraceID),
				},
			},
			expectedAnnotationTimeRange:      types.NewRangeQueryTimeRange(desiredStart.Add(-5*step), desiredEnd.Add(5*step), step),
			expectedAnyCachedExtentsRetained: true,
		},

		"cache miss, evaluated range overlaps max freshness window": {
			timeRange:          types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow, step),
			existingCacheEntry: nil,

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow, step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					withAnnotationsForTimeRange(
						extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow.Add(-limits.maxFreshness), step), timeNow, newTraceID),
						types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow, step),
					),
				},
			},
		},

		"cache hit, but extent has already expired": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, beforeTTLThreshold, existingTraceID),
				},
			},

			// The existing cache entry should be ignored and a new one written.
			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{defaultTimeRange},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, timeNow, newTraceID),
				},
			},
		},

		"cache hit, but some extents have already expired": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-10*time.Minute), desiredStart.Add(-5*time.Minute), step), beforeTTLThreshold, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-3*time.Minute), desiredStart.Add(-2*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(time.Minute), desiredStart.Add(5*time.Minute), step), beforeTTLThreshold, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(6*time.Minute), desiredStart.Add(8*time.Minute), step), withinTTL, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(2*time.Minute), desiredEnd.Add(4*time.Minute), step), beforeTTLThreshold, existingTraceID),
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(6*time.Minute), desiredEnd.Add(8*time.Minute), step), withinTTL, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(desiredStart, desiredStart.Add(5*time.Minute), step),
				types.NewRangeQueryTimeRange(desiredStart.Add(9*time.Minute), desiredEnd, step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(desiredStart.Add(-3*time.Minute), desiredStart.Add(-2*time.Minute), step), withinTTL, existingTraceID),
					extentFor(defaultTimeRange, withinTTL, newTraceID), // This should contain data from the T=6m to T=8m cached entry, plus the freshly evaluated extents either side.
					extentFor(types.NewRangeQueryTimeRange(desiredEnd.Add(6*time.Minute), desiredEnd.Add(8*time.Minute), step), withinTTL, existingTraceID),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache miss, evaluated range within OOO window": {
			timeRange:          types.NewRangeQueryTimeRange(timeNow.Add(-oooWindow).Add(-2*time.Minute), timeNow.Add(-oooWindow).Add(2*time.Minute), step),
			existingCacheEntry: nil,
			enableOOO:          true,

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeNow.Add(-oooWindow).Add(-2*time.Minute), timeNow.Add(-oooWindow).Add(2*time.Minute), step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-oooWindow).Add(-2*time.Minute), timeNow.Add(-oooWindow).Add(2*time.Minute), step), timeNow, newTraceID),
				},
			},
			expectedTTL: oooTTL,
		},

		"cache hit, extent was written with OOO data but TTL has expired": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					// Evaluated in OOO window, TTL has expired, will be dropped from new cache entry.
					extentFor(types.NewRangeQueryTimeRange(timeZero.Add(3*time.Minute), timeZero.Add(5*time.Minute), step), timeZero.Add(5*time.Minute).Add(oooWindow).Add(-time.Minute), existingTraceID),
				},
			},
			enableOOO: true,

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				defaultTimeRange,
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(defaultTimeRange, timeNow, newTraceID),
				},
			},
		},

		"cache hit, extent was written with OOO data and TTL has not expired": {
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					// Evaluated in OOO window, TTL has not expired.
					extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow.Add(-oooWindow+time.Minute), step), timeNow.Add(-oooWindow+time.Minute).Add(oooTTL-time.Second), existingTraceID),
				},
			},
			enableOOO: true,

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				defaultTimeRange,
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					// New extent: added.
					extentFor(defaultTimeRange, timeNow, newTraceID),

					// Existing extent: retained.
					extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-10*time.Minute), timeNow.Add(-oooWindow+time.Minute), step), timeNow.Add(-oooWindow+time.Minute).Add(oooTTL-time.Second), existingTraceID),
				},
			},
			expectedTTL:                      oooTTL,
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit, but extent was somehow written entirely within max freshness window": {
			timeRange: types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow, step),
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(2*time.Minute), timeNow, step), timeNow, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow.Add(-limits.maxFreshness).Add(time.Minute), step),
			},
			expectedWrittenCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					withAnnotationsForTimeRange(
						extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow.Add(-limits.maxFreshness), step), timeNow, newTraceID),
						types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow, step),
					),
				},
			},
			expectedAnyCachedExtentsRetained: true,
		},

		"cache hit, existing extent covers entire period up to max freshness window": {
			timeRange: types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow, step),
			existingCacheEntry: &CacheEntry{
				Extents: []CachedExtent{
					extentFor(types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(-10*time.Minute), timeNow.Add(-limits.maxFreshness), step), timeNow, existingTraceID),
				},
			},

			expectedFreshlyEvaluatedRanges: []types.QueryTimeRange{
				types.NewRangeQueryTimeRange(timeNow.Add(-limits.maxFreshness).Add(step), timeNow, step),
			},
			expectedWrittenCacheEntry:        nil,
			expectedAnyCachedExtentsRetained: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.timeRange.StepCount == 0 {
				testCase.timeRange = defaultTimeRange
			}

			if testCase.expectedAnnotationTimeRange.StepCount == 0 {
				testCase.expectedAnnotationTimeRange = testCase.timeRange
			}

			if testCase.expectedTTL == 0 {
				testCase.expectedTTL = limits.ttl
			}

			if testCase.enableOOO {
				limits.oooTTL = oooTTL
				limits.oooWindow = oooWindow
			} else {
				limits.oooTTL = 0
				limits.oooWindow = 0
			}

			ctx := user.InjectOrgID(context.Background(), "some-user")
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			cache := newTestCache()
			materializer := &testMaterializer{
				t:                        t,
				ctx:                      ctx,
				memoryConsumptionTracker: memoryConsumptionTracker,
			}
			params := &planning.QueryParameters{OriginalExpression: "the_original_expression{}"}

			o := newCacheOperator(cache, materializer, createTestNode(), testCase.timeRange, memoryConsumptionTracker, posrange.PositionRange{}, params, limits, log.NewNopLogger(), cacheEntryInterval)
			o.timeNow = func() time.Time { return timeNow }
			o.getCurrentTraceID = func(context.Context) (string, bool) { return newTraceID, true }

			cacheKey, err := o.computeCacheKey(ctx)
			require.NoError(t, err)
			hashedCacheKey := caching.HashCacheKey(cacheKey)

			if testCase.existingCacheEntry != nil {
				if testCase.existingCacheEntry.CacheKey == nil {
					testCase.existingCacheEntry.CacheKey = cacheKey
				}

				b, err := testCase.existingCacheEntry.Marshal()
				require.NoError(t, err)
				cache.entries = map[string]testCacheEntry{
					hashedCacheKey: {value: b},
				}
			}

			require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
			require.Equal(t, testCase.expectedFreshlyEvaluatedRanges, materializer.materializedTimeRanges, "expected materialized time ranges to match expected freshly evaluated time ranges")
			for i, o := range materializer.materializedOperators {
				require.Truef(t, o.Prepared, "expected Prepare to be called on operator at index %d", i)
			}

			if testCase.expectedAnyCachedExtentsRetained {
				require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.SeriesMetadataSlices), "expected some cached extents to be included in memory consumption estimate")
			} else {
				require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytesBySource(limiter.SeriesMetadataSlices), "expected no cached extents to be included in memory consumption estimate")
			}

			require.NoError(t, o.AfterPrepare(ctx))
			for i, o := range materializer.materializedOperators {
				require.Truef(t, o.AfterPrepareCalled, "expected AfterPrepare to be called on operator at index %d", i)
			}

			expectedResult := testDataForTimeRange(testCase.timeRange)

			series, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(expectedResult.seriesLabels()), series, "expected series metadata to match expected")

			for idx, s := range series {
				data, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "received unexpected error while trying to read series at index %d (%s)", idx, s.Labels.String())
				require.Equalf(t, expectedResult[idx].floats, data.Floats, "expected data to match expected for series at index %d (%s)", idx, s.Labels.String())
				require.Equalf(t, expectedResult[idx].histograms, data.Histograms, "expected data to match expected for series at index %d (%s)", idx, s.Labels.String())

				mangleSeriesData(data) // Mangle the data to simulate the consumer mutating the slice, to ensure the cached data is not affected.
				types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
			}

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err, "expected EOS error when reading past end of series")

			mangleSeriesMetadata(series) // Mangle the series metadata to simulate the consumer mutating the slice, to ensure the cached data is not affected.
			types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)

			require.NoError(t, o.FinishedReading(ctx))
			for i, o := range materializer.materializedOperators {
				require.Truef(t, o.FinishedReadingCalled, "expected FinishedReading to be called on operator at index %d", i)
			}

			operatorStats, actualAnnos, err := o.Finalize(ctx)
			require.NoError(t, err)
			require.Equal(t, statsForTimeRange(testCase.timeRange), operatorStats.Encode(), "expected stats to match expected")

			actualWarnings, actualInfos := actualAnnos.AsStrings("some_expression", 0, 0)
			expectedWarnings, expectedInfos := annotationsForTimeRange(testCase.expectedAnnotationTimeRange).AsStrings("some_expression", 0, 0)
			require.ElementsMatch(t, expectedWarnings, actualWarnings, "expected warnings to match expected")
			require.ElementsMatch(t, expectedInfos, actualInfos, "expected infos to match expected")
			operatorStats.Close()

			o.Close()
			for i, o := range materializer.materializedOperators {
				require.Truef(t, o.Closed, "expected Close to be called on operator at index %d", i)
			}

			require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())

			if testCase.expectedWrittenCacheEntry == nil {
				require.Equal(t, 0, cache.setCount, "expected no cache entry to be written, but at least one was")
			} else {
				require.Equal(t, 1, cache.setCount, "expected cache entry to be written, but it was not")
				require.Equal(t, []string{hashedCacheKey}, slices.Collect(maps.Keys(cache.entries)), "expected cache entry to be written with expected key")
				require.Equal(t, testCase.expectedTTL, cache.entries[hashedCacheKey].ttl, "expected cache entry to have expected TTL")

				testCase.expectedWrittenCacheEntry.CacheKey = cacheKey
				actualBytes := cache.entries[hashedCacheKey].value
				actualEntry := &CacheEntry{}
				require.NoError(t, actualEntry.Unmarshal(actualBytes))

				// Sort the annotations in each extent in both the actual and expected cache entry to make the comparison below simpler.
				sortAnnotations(actualEntry)
				sortAnnotations(testCase.expectedWrittenCacheEntry)

				require.Equal(t, testCase.expectedWrittenCacheEntry, actualEntry, "expected cache entry to be written with expected value")
			}
		})
	}
}

func TestCacheOperator_DoesNotSaveCacheEntryIfSeriesMetadataNotCalled(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "some-user")
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	cache := newTestCache()
	materializer := &testMaterializer{
		t:                        t,
		ctx:                      ctx,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
	limits := &mockLimitsProvider{}

	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)
	o := newCacheOperator(cache, materializer, createTestNode(), timeRange, memoryConsumptionTracker, posrange.PositionRange{}, &planning.QueryParameters{OriginalExpression: "the_original_expression{}"}, limits, log.NewNopLogger(), cacheEntryInterval)

	require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
	require.NoError(t, o.AfterPrepare(ctx))

	require.True(t, o.extents.shouldWriteCacheEntry, "invalid test case: no cache entry to write")

	// Finalize the operator without calling SeriesMetadata().
	_, _, err := o.Finalize(ctx)
	require.NoError(t, err)

	require.Zerof(t, cache.setCount, "expected no cache entry to be written, but at least one was: %v", cache.entries)
}

func TestCacheOperator_DoesNotSaveCacheEntryIfNotAllSeriesRead(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "some-user")
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	cache := newTestCache()
	materializer := &testMaterializer{
		t:                        t,
		ctx:                      ctx,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
	limits := &mockLimitsProvider{}

	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)
	o := newCacheOperator(cache, materializer, createTestNode(), timeRange, memoryConsumptionTracker, posrange.PositionRange{}, &planning.QueryParameters{OriginalExpression: "the_original_expression{}"}, limits, log.NewNopLogger(), cacheEntryInterval)

	require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
	require.NoError(t, o.AfterPrepare(ctx))
	series, err := o.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, series)
	require.GreaterOrEqual(t, len(series), 2, "expected at least two series")

	// Read the first series.
	_, err = o.NextSeries(ctx)
	require.NoError(t, err)

	require.True(t, o.extents.shouldWriteCacheEntry, "invalid test case: no cache entry to write")

	// Finalize the operator without reading the later series.
	_, _, err = o.Finalize(ctx)
	require.NoError(t, err)

	require.Zerof(t, cache.setCount, "expected no cache entry to be written, but at least one was: %v", cache.entries)
}

func TestCacheOperator_DoesNotSaveCacheEntryIfFinishedReadingNotCalled(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "some-user")
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	cache := newTestCache()
	materializer := &testMaterializer{
		t:                        t,
		ctx:                      ctx,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
	limits := &mockLimitsProvider{}

	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)
	o := newCacheOperator(cache, materializer, createTestNode(), timeRange, memoryConsumptionTracker, posrange.PositionRange{}, &planning.QueryParameters{OriginalExpression: "the_original_expression{}"}, limits, log.NewNopLogger(), cacheEntryInterval)

	require.NoError(t, o.Prepare(ctx, &types.PrepareParams{}))
	require.NoError(t, o.AfterPrepare(ctx))
	series, err := o.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.NotEmpty(t, series)

	// Read all the data.
	for range series {
		_, err = o.NextSeries(ctx)
		require.NoError(t, err)
	}

	require.True(t, o.extents.shouldWriteCacheEntry, "invalid test case: no cache entry to write")

	// Finalize the operator without calling FinishedReading.
	_, _, err = o.Finalize(ctx)
	require.EqualError(t, err, "CacheOperator.writeCacheEntry() called (via Finalize()) before FinishedReading(), this should never happen")

	require.Zerof(t, cache.setCount, "expected no cache entry to be written, but at least one was: %v", cache.entries)
}

func TestCacheOperator_DoesNotStoreEmptySeries(t *testing.T) {
	floatSeries := types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "with_floats")}
	histogramSeries := types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "with_histograms")}
	emptySeries := types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "empty")}

	floatData := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1000, F: 1}}}
	histogramData := types.InstantVectorSeriesData{Histograms: []promql.HPoint{{T: 2000, H: &histogram.FloatHistogram{Count: 3}}}}
	emptyData := types.InstantVectorSeriesData{}

	testCases := map[string]struct {
		seriesMetadata   []types.SeriesMetadata
		data             []types.InstantVectorSeriesData
		expectedMetadata []querierpb.SeriesMetadata
		expectedData     []querierpb.InstantVectorSeriesData
	}{
		"no series": {
			seriesMetadata:   []types.SeriesMetadata{},
			data:             []types.InstantVectorSeriesData{},
			expectedMetadata: []querierpb.SeriesMetadata{},
			expectedData:     []querierpb.InstantVectorSeriesData{},
		},
		"no empty series": {
			seriesMetadata: []types.SeriesMetadata{floatSeries, histogramSeries},
			data:           []types.InstantVectorSeriesData{floatData, histogramData},
			expectedMetadata: []querierpb.SeriesMetadata{
				querierpb.EncodeSeriesMetadata(floatSeries),
				querierpb.EncodeSeriesMetadata(histogramSeries),
			},
			expectedData: []querierpb.InstantVectorSeriesData{
				querierpb.EncodeInstantVectorSeriesData(floatData),
				querierpb.EncodeInstantVectorSeriesData(histogramData),
			},
		},
		"empty series interspersed with non-empty series": {
			seriesMetadata: []types.SeriesMetadata{emptySeries, floatSeries, emptySeries, histogramSeries, emptySeries},
			data:           []types.InstantVectorSeriesData{emptyData, floatData, emptyData, histogramData, emptyData},
			expectedMetadata: []querierpb.SeriesMetadata{
				querierpb.EncodeSeriesMetadata(floatSeries),
				querierpb.EncodeSeriesMetadata(histogramSeries),
			},
			expectedData: []querierpb.InstantVectorSeriesData{
				querierpb.EncodeInstantVectorSeriesData(floatData),
				querierpb.EncodeInstantVectorSeriesData(histogramData),
			},
		},
		"all series empty": {
			seriesMetadata:   []types.SeriesMetadata{emptySeries, emptySeries},
			data:             []types.InstantVectorSeriesData{emptyData, emptyData},
			expectedMetadata: []querierpb.SeriesMetadata{},
			expectedData:     []querierpb.InstantVectorSeriesData{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			o := &CacheOperator{
				seriesMetadata: testCase.seriesMetadata,
				data:           testCase.data,
			}

			seriesMetadata, data := o.encodeSeriesForCacheEntry()
			require.Equal(t, testCase.expectedMetadata, seriesMetadata, "expected empty series to be excluded from the cached series metadata")
			require.Equal(t, testCase.expectedData, data, "expected empty series to be excluded from the cached data")
			require.Len(t, data, len(seriesMetadata), "expected metadata and data to remain aligned")
		})
	}
}

func sortAnnotations(cacheEntry *CacheEntry) {
	for _, e := range cacheEntry.Extents {
		slices.Sort(e.Annotations.Infos)
		slices.Sort(e.Annotations.Warnings)
	}
}

type testMaterializer struct {
	t                        *testing.T
	ctx                      context.Context
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	materializedTimeRanges   []types.QueryTimeRange
	materializedOperators    []*operators.TestOperator
}

func (m *testMaterializer) ConvertNodeToInstantVectorOperator(node planning.Node, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	m.materializedTimeRanges = append(m.materializedTimeRanges, timeRange)

	data := testDataForTimeRange(timeRange)
	stats := statsForTimeRange(timeRange)
	decodedStats, err := stats.Decode(m.ctx, m.memoryConsumptionTracker)
	require.NoError(m.t, err)

	o := &operators.TestOperator{
		Series:                   data.seriesLabels(),
		Data:                     data.seriesData(m.t, m.memoryConsumptionTracker),
		EvaluationStats:          decodedStats,
		Annotations:              annotationsForTimeRange(timeRange),
		MemoryConsumptionTracker: m.memoryConsumptionTracker,
	}

	m.materializedOperators = append(m.materializedOperators, o)

	return o, nil
}

func testDataForTimeRange(timeRange types.QueryTimeRange) testSeriesSet {
	const seriesCount = 2
	series := make(testSeriesSet, 0, seriesCount)

	for seriesIdx := range seriesCount {
		var floats []promql.FPoint
		var histograms []promql.HPoint

		for idx := range timeRange.StepCount {
			stepT := timeRange.IndexTime(int64(idx))

			if stepT%(4*time.Minute).Milliseconds() == 0 {
				histograms = append(histograms, promql.HPoint{T: stepT, H: &histogram.FloatHistogram{Count: float64(stepT * 10)}})
			} else {
				floats = append(floats, promql.FPoint{T: stepT, F: float64(stepT * 10)})
			}
		}

		s := testSeries{
			labels:     labels.FromStrings(model.MetricNameLabel, "test_metric", "idx", strconv.Itoa(seriesIdx)),
			floats:     floats,
			histograms: histograms,
		}

		series = append(series, s)
	}

	return series
}

func annotationsForTimeRange(timeRange types.QueryTimeRange) annotations.Annotations {
	var annos annotations.Annotations

	for idx := range timeRange.StepCount {
		stepT := timeRange.IndexTime(int64(idx))

		if stepT%(2*time.Minute).Milliseconds() == 0 {
			annos.Add(fmt.Errorf("%w: something happened at T=%d", annotations.PromQLInfo, stepT))
		} else {
			annos.Add(fmt.Errorf("%w: something happened at T=%d", annotations.PromQLWarning, stepT))
		}
	}

	return annos
}

func statsForTimeRange(timeRange types.QueryTimeRange) types.EncodedOperatorEvaluationStats {
	samplesProcessed := make([]int64, 0, timeRange.StepCount)
	samplesReadIfSubsequentStep := make([]int64, 0, timeRange.StepCount)
	samplesReadIfFirstStep := make([]int64, 0, timeRange.StepCount)

	for idx := range timeRange.StepCount {
		stepT := timeRange.IndexTime(int64(idx))

		samplesProcessed = append(samplesProcessed, 100000000+stepT)
		samplesReadIfSubsequentStep = append(samplesReadIfSubsequentStep, 200000000+stepT)
		samplesReadIfFirstStep = append(samplesReadIfFirstStep, 300000000+stepT)
	}

	return types.EncodedOperatorEvaluationStats{
		AllSeries: types.EncodedSubsetStats{
			SamplesProcessedPerStep:     samplesProcessed,
			SamplesReadIfSubsequentStep: samplesReadIfSubsequentStep,
			SamplesReadIfFirstStep:      samplesReadIfFirstStep,
		},
		TimeRange: timeRange.Encode(),
	}
}

func extentFor(timeRange types.QueryTimeRange, oldestEvaluationTime time.Time, newestEvaluationTraceID string) CachedExtent {
	allSeries := testDataForTimeRange(timeRange)

	metadata := make([]querierpb.SeriesMetadata, 0, len(allSeries))
	data := make([]querierpb.InstantVectorSeriesData, 0, len(allSeries))
	for _, s := range allSeries {
		metadata = append(metadata, querierpb.SeriesMetadata{
			Labels: mimirpb.FromLabelsToLabelAdapters(s.labels),
		})

		data = append(data, querierpb.InstantVectorSeriesData{
			Floats:     mimirpb.FromFPointsToSamples(s.floats),
			Histograms: mimirpb.FromHPointsToHistograms(s.histograms),
		})
	}

	return CachedExtent{
		StartT: timeRange.StartT,
		EndT:   timeRange.EndT,

		SeriesMetadata: metadata,
		Data:           data,

		Annotations: querierpb.EncodeAnnotations(annotationsForTimeRange(timeRange), ""),
		Stats:       statsForTimeRange(timeRange),

		OldestEvaluationTime:    timestamp.FromTime(oldestEvaluationTime),
		NewestEvaluationTraceID: newestEvaluationTraceID,
	}
}

func withAnnotationsForTimeRange(extent CachedExtent, timeRange types.QueryTimeRange) CachedExtent {
	extent.Annotations = querierpb.EncodeAnnotations(annotationsForTimeRange(timeRange), "")
	return extent
}

func mangleSeriesMetadata(series []types.SeriesMetadata) {
	for idx, s := range series {
		s.Labels = labels.FromStrings(model.MetricNameLabel, "this_series_has_been_mangled", "idx", strconv.Itoa(idx))
	}
}

func mangleSeriesData(data types.InstantVectorSeriesData) {
	for _, p := range data.Floats {
		p.T = rand.Int64()
		p.F = rand.NormFloat64()
	}

	for _, p := range data.Histograms {
		p.T = rand.Int64()
		p.H.Count = rand.NormFloat64()
		p.H.Sum = rand.NormFloat64()
	}
}

type testCache struct {
	entries map[string]testCacheEntry

	setCount int
}

func newTestCache() *testCache {
	return &testCache{
		entries: make(map[string]testCacheEntry),
	}
}

type testCacheEntry struct {
	value []byte
	ttl   time.Duration
}

func (t *testCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte {
	results := make(map[string][]byte, len(keys))

	for _, key := range keys {
		entry, ok := t.entries[key]

		if ok {
			results[key] = entry.value
		}
	}

	return results
}

func (t *testCache) SetAsync(key string, value []byte, ttl time.Duration) {
	t.setCount++
	t.entries[key] = testCacheEntry{value: value, ttl: ttl}
}

type mockLimitsProvider struct {
	ttl          time.Duration
	oooTTL       time.Duration
	oooWindow    time.Duration
	maxFreshness time.Duration
}

func (m *mockLimitsProvider) GetMinResultsCacheTTL(ctx context.Context) (time.Duration, error) {
	return m.ttl, nil
}

func (m *mockLimitsProvider) GetMinOutOfOrderResultsCacheTTL(ctx context.Context) (time.Duration, error) {
	return m.oooTTL, nil
}

func (m *mockLimitsProvider) GetMaxOutOfOrderTimeWindow(ctx context.Context) (time.Duration, error) {
	return m.oooWindow, nil
}

func (m *mockLimitsProvider) GetMaxCacheFreshness(ctx context.Context) (time.Duration, error) {
	return m.maxFreshness, nil
}

func TestCacheOperator_CacheKey(t *testing.T) {
	generateCacheKey := func(tenantID string, desiredTimeRange types.QueryTimeRange, node planning.Node, params *planning.QueryParameters) []byte {
		o := newCacheOperator(nil, nil, node, desiredTimeRange, nil, posrange.PositionRange{}, params, nil, log.NewNopLogger(), cacheEntryInterval)
		ctx := user.InjectOrgID(context.Background(), tenantID)

		key, err := o.computeCacheKey(ctx)
		require.NoError(t, err)
		return key
	}

	originalTenantID := "user-1234"
	timeZero := timestamp.Time(0)
	originalStep := time.Minute
	originalStartT := timeZero.Add(48 * time.Hour).Add(time.Minute)
	originalEndT := originalStartT.Add(72*time.Hour + 10*time.Minute)
	originalTimeRange := types.NewRangeQueryTimeRange(originalStartT, originalEndT, originalStep)
	originalNode := createTestNode()

	originalParams := &planning.QueryParameters{
		OriginalExpression:       "some_expression",
		TimeRange:                types.NewRangeQueryTimeRange(timeZero.Add(time.Minute), timeZero.Add(8*24*time.Hour), time.Minute),
		EnableDelayedNameRemoval: false,
		LookbackDelta:            5 * time.Minute,
	}

	originalCacheKey := generateCacheKey(originalTenantID, originalTimeRange, originalNode, originalParams)

	require.Equal(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, originalNode, originalParams), "should generate the same cache key when called multiple times for exactly the same query")
	require.NotEqual(t, originalCacheKey, generateCacheKey("another-user", originalTimeRange, originalNode, originalParams), "should generate a different cache key when called multiple times with different tenant IDs")

	// Time range handling
	require.Equal(t, originalCacheKey, generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT.Add(time.Hour), originalEndT.Add(time.Hour), originalStep), originalNode, originalParams), "should generate the same cache key for the same query evaluated at a different time in the same cache interval")
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT.Add(cacheEntryInterval), originalEndT, originalStep), originalNode, originalParams), "should generate a different cache key for the same query evaluated starting in a different cache interval")
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT, originalEndT, 2*originalStep), originalNode, originalParams), "should generate a different cache key if the step is different")

	paramsWithDifferentTimeRange := *originalParams
	paramsWithDifferentTimeRange.TimeRange = types.NewRangeQueryTimeRange(timeZero.Add(200*time.Minute), timeZero.Add(8*24*time.Hour), time.Minute)
	require.Equal(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, originalNode, &paramsWithDifferentTimeRange), "should generate the same cache key even if the overall request is being evaluated for a different time range")

	// Same step, but start time isn't aligned to step grid
	offsetCacheKey := generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT.Add(10*time.Second), originalEndT, originalStep), originalNode, originalParams)
	require.NotEqual(t, originalCacheKey, offsetCacheKey, "should generate a different cache key if the start time isn't aligned to the original request's step grid")
	require.Equal(t, offsetCacheKey, generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT.Add(10*time.Second).Add(10*originalStep), originalEndT, originalStep), originalNode, originalParams), "should generate the same cache key when evaluated with the same offset from the step grid and in the same cache interval")
	require.NotEqual(t, offsetCacheKey, generateCacheKey(originalTenantID, types.NewRangeQueryTimeRange(originalStartT.Add(10*time.Second).Add(cacheEntryInterval), originalEndT, originalStep), originalNode, originalParams), "should not generate the same cache key when evaluated with the same offset from the step grid but in a different cache interval")

	// Lookback window, delayed name removal, expression
	paramsWithDifferentLookbackWindow := *originalParams
	paramsWithDifferentLookbackWindow.LookbackDelta = originalParams.LookbackDelta + time.Minute
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, originalNode, &paramsWithDifferentLookbackWindow), "should generate a different cache key if the lookback delta is different")

	paramsWithDifferentDelayedNameRemoval := *originalParams
	paramsWithDifferentDelayedNameRemoval.EnableDelayedNameRemoval = !originalParams.EnableDelayedNameRemoval
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, originalNode, &paramsWithDifferentDelayedNameRemoval), "should generate a different cache key if delayed name removal is not the same")

	paramsWithDifferentExpression := *originalParams
	paramsWithDifferentExpression.OriginalExpression = "different_expression"
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, originalNode, &paramsWithDifferentExpression), "should generate a different cache key if original expression is not the same")

	// Node handling
	differentNode := &core.NumberLiteral{NumberLiteralDetails: &core.NumberLiteralDetails{Value: 456}}
	require.NotEqual(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, differentNode, originalParams), "should generate a different cache key if node is not the same")
	equivalentNode := createTestNode()
	require.Equal(t, originalCacheKey, generateCacheKey(originalTenantID, originalTimeRange, equivalentNode, originalParams), "should generate the same cache key if an equivalent node is used")
}

func createTestNode() planning.Node {
	return &core.NumberLiteral{
		NumberLiteralDetails: &core.NumberLiteralDetails{
			Value: 123,
		},
	}
}
