// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

// orderObservationsForImport returns observations ordered so that, within each
// cluster, jobs appear in ascending start-offset order, and a job spanning multiple
// clusters appears only after every job that precedes it in any of its clusters;
// equal start offsets impose no ordering. Specs are sparse (a job lists only the
// clusters that had new data) and consecutive jobs can cover disjoint cluster sets,
// so there's no single offset to sort by.
//
// A cycle should be impossible, but if it is detected errObservationOffsetCycle is returned.
func orderObservationsForImport(observations []*observation, numClusters int) ([]*observation, error) {
	if numClusters <= 1 {
		slices.SortFunc(observations, func(a, b *observation) int {
			return cmp.Compare(a.spec.Ranges()[0].StartOffset, b.spec.Ranges()[0].StartOffset)
		})
		return observations, nil
	}

	observationsByCluster := groupByClusterID(observations, numClusters)
	// nextUnemittedByCluster[clusterID] indexes the next unemitted entry in observationsByCluster[clusterID].
	nextUnemittedByCluster := make([]int, numClusters)

	ordered := make([]*observation, 0, len(observations))
	for len(ordered) < len(observations) {
		emittedThisPass := false
		for clusterID := 0; clusterID < numClusters; clusterID++ {
			entries := observationsByCluster[clusterID]
			next := nextUnemittedByCluster[clusterID]

			if next >= len(entries) {
				continue
			}

			// Entries tied with the front are equally valid candidates; the front
			// itself can be blocked by another cluster while a tied sibling is not.
			frontOffset := entries[next].startOffset
			for i := next; i < len(entries) && entries[i].startOffset == frontOffset; i++ {
				candidate := entries[i].obs
				if !canEmit(candidate, observationsByCluster, nextUnemittedByCluster) {
					continue
				}

				ordered = append(ordered, candidate)
				emitCandidate(candidate, observationsByCluster, nextUnemittedByCluster)
				emittedThisPass = true
				break
			}
		}

		if !emittedThisPass {
			return nil, errObservationOffsetCycle
		}
	}
	return ordered, nil
}

var errObservationOffsetCycle = errors.New("observation offsets form a cycle across clusters")

type clusterObservation struct {
	obs *observation
	// startOffset is the cluster's start offset for the observation.
	startOffset int64
}

// groupByClusterID groups observations into one list per cluster (indexed
// by ID), each sorted ascending by that cluster's start offset.
func groupByClusterID(observations []*observation, numClusters int) [][]clusterObservation {
	observationsByCluster := make([][]clusterObservation, numClusters)
	for _, obs := range observations {
		for clusterID, clusterRange := range obs.spec.Ranges() {
			observationsByCluster[clusterID] = append(observationsByCluster[clusterID],
				clusterObservation{obs: obs, startOffset: clusterRange.StartOffset})
		}
	}
	for clusterID := range observationsByCluster {
		slices.SortFunc(observationsByCluster[clusterID], func(a, b clusterObservation) int {
			return cmp.Compare(a.startOffset, b.startOffset)
		})
	}
	return observationsByCluster
}

// canEmit reports whether no cluster the candidate belongs to has an unemitted entry
// with a smaller start offset, i.e. all of the candidate's predecessors are emitted.
func canEmit(candidate *observation, observationsByCluster [][]clusterObservation, nextUnemittedByCluster []int) bool {
	for clusterID, clusterRange := range candidate.spec.Ranges() {
		entries := observationsByCluster[clusterID]
		next := nextUnemittedByCluster[clusterID]
		if next >= len(entries) || entries[next].startOffset < clusterRange.StartOffset {
			return false
		}
	}
	return true
}

// emitCandidate advances the cursor of every cluster the candidate belongs to,
// first swapping the candidate into the front slot (it ties with the front but may
// not be it) so cursors keep pointing at unemitted entries.
func emitCandidate(candidate *observation, observationsByCluster [][]clusterObservation, nextUnemittedByCluster []int) {
	for clusterID := range candidate.spec.Ranges() {
		entries := observationsByCluster[clusterID]
		next := nextUnemittedByCluster[clusterID]
		// The same *observation pointer is shared across all cluster lists.
		for i := next; ; i++ {
			if entries[i].obs == candidate {
				if entries[next].startOffset != entries[i].startOffset {
					// This should not happen as canEmit() guarantees entries[next] and the
					// candidate share the same start offset, but a bug here could be subtle.
					panic(fmt.Sprintf("observation ordering invariant violated: expected job %q and job %q to share a start offset, got %d and %d",
						entries[next].obs.key.id, candidate.key.id, entries[next].startOffset, entries[i].startOffset))
				}
				entries[next], entries[i] = entries[i], entries[next]
				break
			}
		}
		nextUnemittedByCluster[clusterID]++
	}
}
