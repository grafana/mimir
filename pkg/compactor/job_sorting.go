// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"sort"
)

type jobsOrderFunc func(jobs []*Job) []*Job

// SortJobsBySmallestRangeOldestBlocksFirst returns input jobs sorted by smallest range, oldest min time first.
// The rationale of this sorting is that we may want to favor smaller ranges first (ie. to deduplicate samples
// sooner than later) and older ones are more likely to be "complete" (no missing block still to be uploaded).
func SortJobsBySmallestRangeOldestBlocksFirst(jobs []*Job) []*Job {
	sort.SliceStable(jobs, func(i, j int) bool {
		iLength := jobs[i].MaxTime() - jobs[i].MinTime()
		jLength := jobs[j].MaxTime() - jobs[j].MinTime()

		if iLength != jLength {
			return iLength < jLength
		}
		if jobs[i].MinTime() != jobs[j].MinTime() {
			return jobs[i].MinTime() < jobs[j].MinTime()
		}

		// Guarantee stable sort for tests.
		return jobs[i].Key() < jobs[j].Key()
	})

	return jobs
}

func SortJobsByOldestSplitJobsFirstSmallestRangeOldestBlocksNext(jobs []*Job) []*Job {
	sort.SliceStable(jobs, func(i, j int) bool {
		// Move split jobs to the front.
		if jobs[i].UseSplitting() && !jobs[j].UseSplitting() {
			return true
		}

		if !jobs[i].UseSplitting() && jobs[j].UseSplitting() {
			return false
		}

		// Don't check length for splitting jobs. We want to split oldest blocks first, no matter the length.
		checkLength := true
		if jobs[i].UseSplitting() && jobs[j].UseSplitting() {
			checkLength = false
		}

		if checkLength {
			iLength := jobs[i].MaxTime() - jobs[i].MinTime()
			jLength := jobs[j].MaxTime() - jobs[j].MinTime()

			if iLength != jLength {
				return iLength < jLength
			}
		}

		if jobs[i].MinTime() != jobs[j].MinTime() {
			return jobs[i].MinTime() < jobs[j].MinTime()
		}

		// Guarantee stable sort for tests.
		return jobs[i].Key() < jobs[j].Key()
	})
	return jobs
}

// SortJobsByNewestBlocksFirst returns input jobs sorted by most recent time ranges first
// (regardless of their compaction level). The rationale of this sorting is that in case the
// compactor is lagging behind, we compact up to the largest range (eg. 24h) the most recent
// blocks first and the move to older ones. Most recent blocks are the one more likely to be queried.
func SortJobsByNewestBlocksFirst(jobs []*Job) []*Job {
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
