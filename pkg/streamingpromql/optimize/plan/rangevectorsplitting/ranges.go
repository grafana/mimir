// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

// calculateInnerTimeRange converts the evaluation time and range parameters of the inner selector into the
// data-time (startTs, endTs] range that the split ranges are computed over. Any offset/@ modifier is applied
// here so that splits are expressed in data-time rather than query-time.
func calculateInnerTimeRange(evalTime int64, timeParams planning.RangeParams) (startTs, endTs int64) {
	endTs = evalTime
	if timeParams.HasTimestamp {
		endTs = timeParams.Timestamp.UnixMilli()
	}

	endTs = endTs - timeParams.Offset.Milliseconds()
	startTs = endTs - timeParams.Range.Milliseconds()

	return startTs, endTs
}

// computeSplitRanges divides (startTs, endTs] into split ranges using PromQL semantics.
//
// Split ranges use left-open, right-closed intervals: (Start, End], aligning with PromQL range vector boundary
// semantics.
// When converted to storage queries, these become closed intervals [Start+1, End] on both sides,
// since the storage API expects closed intervals [mint, maxt].
//
// To align with TSDB block boundaries (which use [MinTime, MaxTime) semantics), we shift
// aligned boundaries by -1ms. This ensures that a split (Start, End] corresponds exactly
// to block [Start+1ms, End+1ms). For example:
//   - Split (7:59:59.999, 9:59:59.999] gets samples where 8:00:00.000 <= t <= 9:59:59.999
//   - This would map exactly to a two hour block:
//   - Block [8:00:00.000, 10:00:00.000) contains samples where 8:00:00.000 <= t < 10:00:00.000
//
// Ranges that would be within the OOO window are not cached to avoid stale data being returned.
// The main results cache does cache results within the OOO window with a short TTL. If we also cached OOO results in
// the intermediate cache, we could end up serving stale results for longer as a cached result returned from the
// intermediate cache can end up in a result that's then cached in the result cache.
func computeSplitRanges(startTs, endTs int64, splitInterval time.Duration, oooThreshold int64) []Range {
	splitIntervalMs := splitInterval.Milliseconds()
	alignedStart := computeBlockAlignedStart(startTs, splitInterval)

	var ranges []Range

	if alignedStart >= endTs {
		return []Range{{Start: startTs, End: endTs, Cacheable: false}}
	}

	// Check if we have an uncacheable "head" range
	if startTs < alignedStart {
		// Check if head range would be in ooo window (which would mean nothing can be cached)
		if oooThreshold > 0 && alignedStart >= oooThreshold {
			return []Range{{Start: startTs, End: endTs, Cacheable: false}}
		}
		ranges = append(ranges, Range{
			Start:     startTs,
			End:       alignedStart,
			Cacheable: false,
		})
	}

	var splitStart int64
	for splitStart = alignedStart; splitStart+splitIntervalMs <= endTs; splitStart += splitIntervalMs {
		splitEnd := splitStart + splitIntervalMs

		// Check if range would be in ooo window
		// oooThreshold is inclusive - that is, a sample written at the oooThreshold can be OOO, which is why we use >=
		// for comparing the inclusive splitEnd with oooThreshold
		if oooThreshold > 0 && splitEnd >= oooThreshold {
			ranges = append(ranges, Range{
				Start:     splitStart,
				End:       endTs,
				Cacheable: false,
			})
			return ranges
		}

		ranges = append(ranges, Range{
			Start:     splitStart,
			End:       splitEnd,
			Cacheable: true,
		})
	}

	// Add tail range if needed
	if splitStart < endTs {
		ranges = append(ranges, Range{
			Start:     splitStart,
			End:       endTs,
			Cacheable: false,
		})
	}

	return ranges
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
