// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"time"

	promts "github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// queryTimeRangeForSplit returns the query time range and range parameter override used to materialize one or more
// adjacent split ranges. A single range is evaluated as an instant query, multiple ranges are evaluated as a range
// query with one step per split range.
func queryTimeRangeForSplit(start, end, step int64) (types.QueryTimeRange, planning.RangeParams) {
	subRange := time.Duration(step) * time.Millisecond
	overrideRangeParams := planning.RangeParams{
		IsSet: true,
		Range: subRange,
		// The offset and timestamp are cleared
		Offset:       0,
		HasTimestamp: false,
	}

	var splitTimeRange types.QueryTimeRange

	if start+step == end {
		// Only a single range, create an instant query.
		splitTimeRange = types.NewInstantQueryTimeRange(promts.Time(end))
	} else {
		// Multiple ranges, create a range query with steps at the end timestamp of each range.
		splitTimeRange = types.NewRangeQueryTimeRange(promts.Time(start).Add(subRange), promts.Time(end), subRange)
	}

	return splitTimeRange, overrideRangeParams
}

// calculateInnerTimeRange applies the split node's own range parameters to produce the (startTs, endTs] range to
// divide. For a matrix selector, this is its storage time range. For a subquery, it is the subquery's split time range;
// offsets and @ modifiers in nested expressions are not reflected here.
func calculateInnerTimeRange(evalTime int64, timeParams planning.RangeParams) (startTs, endTs int64) {
	endTs = evalTime
	if timeParams.HasTimestamp {
		endTs = timeParams.Timestamp.UnixMilli()
	}

	endTs = endTs - timeParams.Offset.Milliseconds()
	startTs = endTs - timeParams.Range.Milliseconds()

	return startTs, endTs
}

// cacheabilityChecker reports whether a candidate range is safe to cache based on the time range it queries.
type cacheabilityChecker func(Range) (bool, error)

// newOOOCacheabilityChecker returns a checker for ranges whose End is the maximum timestamp queried from storage.
func newOOOCacheabilityChecker(oooThreshold int64) cacheabilityChecker {
	return func(r Range) (bool, error) {
		// The threshold and the end of the left-open, right-closed range are both inclusive.
		return oooThreshold <= 0 || r.End < oooThreshold, nil
	}
}

// computeSplitRanges divides (startTs, endTs] into split ranges using PromQL semantics.
//
// Split ranges use left-open, right-closed intervals: (Start, End], aligning with PromQL range vector boundary
// semantics.
// For matrix selectors, converting these to storage queries produces closed intervals [Start+1, End], since the
// storage API expects closed intervals [mint, maxt]. Subquery splits do not necessarily map to the same storage time
// range because their nested expressions can contain their own time modifiers.
//
// We shift aligned boundaries by -1ms. For matrix selectors, this aligns them with TSDB block boundaries (which use
// [MinTime, MaxTime) semantics), so a split (Start, End] corresponds exactly to block [Start+1ms, End+1ms). For
// subqueries, this aligns the subquery split boundaries rather than the storage ranges of all nested expressions. For
// example, in the matrix-selector case:
//   - Split (7:59:59.999, 9:59:59.999] gets samples where 8:00:00.000 <= t <= 9:59:59.999
//   - This would map exactly to a two hour block:
//   - Block [8:00:00.000, 10:00:00.000) contains samples where 8:00:00.000 <= t < 10:00:00.000
//
// Ranges that would be within the OOO window are not cached to avoid stale data being returned.
// The main results cache does cache results within the OOO window with a short TTL. If we also cached OOO results in
// the intermediate cache, we could end up serving stale results for longer as a cached result returned from the
// intermediate cache can end up in a result that's then cached in the result cache.
func computeSplitRanges(startTs, endTs int64, splitInterval time.Duration, isCacheable cacheabilityChecker) ([]Range, error) {
	splitIntervalMs := splitInterval.Milliseconds()
	alignedStart := computeBlockAlignedStart(startTs, splitInterval)

	var ranges []Range

	if alignedStart >= endTs {
		return []Range{{Start: startTs, End: endTs, Cacheable: false}}, nil
	}

	// Check if we have an uncacheable "head" range
	if startTs < alignedStart {
		head := Range{
			Start:     startTs,
			End:       alignedStart,
			Cacheable: false,
		}
		cacheable, err := isCacheable(head)
		if err != nil {
			return nil, err
		}
		if !cacheable {
			return []Range{{Start: startTs, End: endTs, Cacheable: false}}, nil
		}
		ranges = append(ranges, head)
	}

	var splitStart int64
	for splitStart = alignedStart; splitStart+splitIntervalMs <= endTs; splitStart += splitIntervalMs {
		splitEnd := splitStart + splitIntervalMs
		splitRange := Range{Start: splitStart, End: splitEnd, Cacheable: true}

		cacheable, err := isCacheable(splitRange)
		if err != nil {
			return nil, err
		}
		if !cacheable {
			ranges = append(ranges, Range{Start: splitStart, End: endTs, Cacheable: false})
			return ranges, nil
		}

		ranges = append(ranges, splitRange)
	}

	// Add tail range if needed
	if splitStart < endTs {
		ranges = append(ranges, Range{
			Start:     splitStart,
			End:       endTs,
			Cacheable: false,
		})
	}

	return ranges, nil
}

func computeBlockAlignedStart(startTs int64, splitInterval time.Duration) int64 {
	splitIntervalMs := splitInterval.Milliseconds()
	// -1 to adjust for block boundaries. Query splitting time ranges are left open, the same as for PromQL. However,
	// block boundaries are left closed, in the sense that a 2h block will store samples from e.g. 8h to 10h-1ms.
	alignedStart := (startTs/splitIntervalMs)*splitIntervalMs - 1
	if alignedStart < startTs {
		alignedStart += splitIntervalMs
	}
	return alignedStart
}
