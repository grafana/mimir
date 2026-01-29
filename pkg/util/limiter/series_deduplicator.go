// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"github.com/prometheus/prometheus/model/labels"
)

// SeriesDeduplicator is used to deduplicate series and track the unique series memory consumption.
type SeriesDeduplicator struct {
	uniqueSeries   map[uint64]labels.Labels
	conflictSeries map[uint64][]labels.Labels

	// hashFunc as a function so that we can override in test.
	hashFunc func(labels.Labels) uint64
}

func NewSeriesDeduplicator() *SeriesDeduplicator {
	return &SeriesDeduplicator{
		uniqueSeries: make(map[uint64]labels.Labels),
		hashFunc:     func(l labels.Labels) uint64 { return l.Hash() },
	}
}

// Deduplicate checks if the given series has been seen before in this deduplicator's scope.
// If it's a duplicate, it returns the previously seen labels .
// If it's new, it returns the input labels.
//
// This method handles hash collisions by checking label equality even when hashes match.
func (sd *SeriesDeduplicator) Deduplicate(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	fingerprint := sd.hashFunc(newLabels)

	// Check if we've seen this hash before
	if existingLabels, foundDuplicate := sd.uniqueSeries[fingerprint]; !foundDuplicate {
		// newLabels is seen for the first time hence we track the series limit and its labels memory consumption.
		sd.uniqueSeries[fingerprint] = newLabels
		return sd.trackNewLabels(newLabels, tracker)
	} else if labels.Equal(existingLabels, newLabels) {
		// newLabels is seen before, deduplicate it by returning existingLabels.
		// No need to increase memory consumption.
		return existingLabels, nil
	}

	// newLabels' hash conflicted with existingLabels.
	if sd.conflictSeries == nil {
		// Track only second labels' hash conflict onward in this map. The first conflict always in uniqueSeries map.
		sd.conflictSeries = make(map[uint64][]labels.Labels)
	}
	hashConflictLabels := sd.conflictSeries[fingerprint]
	for _, existingConflictedLabels := range hashConflictLabels {
		// newLabels is seen before in conflictSeries map, hence just return the existingConflictedLabels.
		// No need to increase memory consumption.
		if labels.Equal(existingConflictedLabels, newLabels) {
			return existingConflictedLabels, nil
		}
	}
	// Despite there was a hash conflict, newLabels is actually seen for the first time hence we track the series limit and its labels memory consumption.
	sd.conflictSeries[fingerprint] = append(hashConflictLabels, newLabels)
	return sd.trackNewLabels(newLabels, tracker)
}

func (sd *SeriesDeduplicator) trackNewLabels(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	err := tracker.IncreaseMemoryConsumptionForLabels(newLabels)
	if err != nil {
		return labels.EmptyLabels(), err
	}
	return newLabels, nil
}

func (sd *SeriesDeduplicator) seriesCount() int {
	count := len(sd.uniqueSeries)
	for _, conflicts := range sd.conflictSeries {
		count += len(conflicts)
	}
	return count
}
