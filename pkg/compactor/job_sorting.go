// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"cmp"
	"slices"
	"strings"
)

const (
	CompactionOrderOldestFirst = "smallest-range-oldest-blocks-first"
	CompactionOrderNewestFirst = "newest-blocks-first"
)

var CompactionOrders = []string{CompactionOrderOldestFirst, CompactionOrderNewestFirst}

type JobsOrderFunc func(jobs []*Job) []*Job

// GetJobsOrderFunction returns a jobs ordering function, or nil, if name doesn't refer to any function.
func GetJobsOrderFunction(name string) JobsOrderFunc {
	switch name {
	case CompactionOrderNewestFirst:
		return sortJobsByNewestBlocksFirst
	case CompactionOrderOldestFirst:
		return sortJobsBySmallestRangeOldestBlocksFirst
	default:
		return nil
	}
}

// sortJobsBySmallestRangeOldestBlocksFirst returns input jobs sorted by smallest range, oldest min time first.
// The rationale of this sorting is that we may want to favor smaller ranges first (ie. to deduplicate samples
// sooner than later) and older ones are more likely to be "complete" (no missing block still to be uploaded).
// Split jobs are moved to the beginning of the output, because merge jobs are only generated if there are no split jobs in the
// same time range, so finishing split jobs first unblocks more jobs and gives opportunity to more compactors
// to work on them.
func sortJobsBySmallestRangeOldestBlocksFirst(jobs []*Job) []*Job {
	slices.SortStableFunc(jobs, func(a, b *Job) int {
		// Move split jobs to the front.
		if a.UseSplitting() && !b.UseSplitting() {
			return -1
		}

		if !a.UseSplitting() && b.UseSplitting() {
			return 1
		}

		checkLength := !a.UseSplitting() || !b.UseSplitting()
		// Don't check length for splitting jobs. We want to the oldest split blocks to be first, no matter the length.

		if checkLength {
			aLength := a.MaxTime() - a.MinTime()
			bLength := b.MaxTime() - b.MinTime()

			if aLength != bLength {
				return cmp.Compare(aLength, bLength)
			}
		}

		if a.MinTime() != b.MinTime() {
			return cmp.Compare(a.MinTime(), b.MinTime())
		}

		// Guarantee stable sort for tests.
		return strings.Compare(a.Key(), b.Key())
	})

	return jobs
}

// sortJobsByNewestBlocksFirst returns input jobs sorted by most recent time ranges first
// (regardless of their compaction level). The rationale of this sorting is that in case the
// compactor is lagging behind, we compact up to the largest range (eg. 24h) the most recent
// blocks first and the move to older ones. Most recent blocks are the one more likely to be queried.
func sortJobsByNewestBlocksFirst(jobs []*Job) []*Job {
	slices.SortStableFunc(jobs, func(a, b *Job) int {
		aMaxTime := a.MaxTime()
		bMaxTime := b.MaxTime()
		if aMaxTime != bMaxTime {
			return cmp.Compare(bMaxTime, aMaxTime)
		}

		aLength := aMaxTime - a.MinTime()
		bLength := bMaxTime - b.MinTime()
		if aLength != bLength {
			return cmp.Compare(aLength, bLength)
		}

		// Guarantee stable sort for tests.
		return strings.Compare(a.Key(), b.Key())
	})

	return jobs
}
