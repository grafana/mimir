// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"sort"
)

const (
	CompactionOrderOldestFirst                = "smallest-range-oldest-blocks-first"
	CompactionOrderNewestFirst                = "newest-blocks-first"
	CompactionOrderSplitFirstOldestBlocksNext = "split-first-smallest-range-oldest-blocks-next"
)

var CompactionOrders = []string{CompactionOrderOldestFirst, CompactionOrderNewestFirst, CompactionOrderSplitFirstOldestBlocksNext}

type JobsOrderFunc func(jobs []*Job) []*Job

// GetJobsOrderFunction returns jobs ordering function, or nil, if name doesn't refer to any function.
func GetJobsOrderFunction(name string) JobsOrderFunc {
	switch name {
	case CompactionOrderNewestFirst:
		return sortJobsByNewestBlocksFirst
	case CompactionOrderOldestFirst:
		return sortJobsBySmallestRangeOldestBlocksFirst
	case CompactionOrderSplitFirstOldestBlocksNext:
		return sortJobsByOldestSplitJobsFirstSmallestRangeOldestBlocksNext
	default:
		return nil
	}
}

// sortJobsBySmallestRangeOldestBlocksFirst returns input jobs sorted by smallest range, oldest min time first.
// The rationale of this sorting is that we may want to favor smaller ranges first (ie. to deduplicate samples
// sooner than later) and older ones are more likely to be "complete" (no missing block still to be uploaded).
func sortJobsBySmallestRangeOldestBlocksFirst(jobs []*Job) []*Job {
	sort.SliceStable(jobs, func(i, j int) bool {
		return compareJobsByLengthAndMinTime(true, jobs[i], jobs[j])
	})

	return jobs
}

// compareJobsByLengthAndMinTime returns true if first job has shorter duration (only if checkLength is true),
// or covers earlier minTime.
func compareJobsByLengthAndMinTime(checkLength bool, a, b *Job) bool {
	if checkLength {
		iLength := a.MaxTime() - a.MinTime()
		jLength := b.MaxTime() - b.MinTime()

		if iLength != jLength {
			return iLength < jLength
		}
	}

	if a.MinTime() != b.MinTime() {
		return a.MinTime() < b.MinTime()
	}

	// Guarantee stable sort for tests.
	return a.Key() < b.Key()
}

// sortJobsByOldestSplitJobsFirstSmallestRangeOldestBlocksNext moves split jobs to the beginning,
// from oldest splits to newer splits. Sorting of non-split jobs is the same as sortJobsBySmallestRangeOldestBlocksFirst.
// Idea is to prioritize split jobs, because merge jobs are only generated if there are no split jobs in the
// same time range, so finishing split jobs first unblocks more jobs and gives opportunity to more compactors
// to work on them.
func sortJobsByOldestSplitJobsFirstSmallestRangeOldestBlocksNext(jobs []*Job) []*Job {
	sort.SliceStable(jobs, func(i, j int) bool {
		// Move split jobs to the front.
		if jobs[i].UseSplitting() && !jobs[j].UseSplitting() {
			return true
		}

		if !jobs[i].UseSplitting() && jobs[j].UseSplitting() {
			return false
		}

		// Don't check length for splitting jobs. We want to the oldest split blocks to be first, no matter the length.
		if jobs[i].UseSplitting() && jobs[j].UseSplitting() {
			return compareJobsByLengthAndMinTime(false, jobs[i], jobs[j])
		}

		return compareJobsByLengthAndMinTime(true, jobs[i], jobs[j])
	})
	return jobs
}

// sortJobsByNewestBlocksFirst returns input jobs sorted by most recent time ranges first
// (regardless of their compaction level). The rationale of this sorting is that in case the
// compactor is lagging behind, we compact up to the largest range (eg. 24h) the most recent
// blocks first and the move to older ones. Most recent blocks are the one more likely to be queried.
func sortJobsByNewestBlocksFirst(jobs []*Job) []*Job {
	sort.SliceStable(jobs, func(i, j int) bool {
		iMaxTime := jobs[i].MaxTime()
		jMaxTime := jobs[j].MaxTime()
		if iMaxTime != jMaxTime {
			return iMaxTime > jMaxTime
		}

		iLength := iMaxTime - jobs[i].MinTime()
		jLength := jMaxTime - jobs[j].MinTime()
		if iLength != jLength {
			return iLength < jLength
		}

		// Guarantee stable sort for tests.
		return jobs[i].Key() < jobs[j].Key()
	})

	return jobs
}
