// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

// SeriesDeduplicator is used to deduplicate series within a single Select() call.
// This is scoped per selector/operand, not per query. For queries like "foo + foo",
// each operand gets its own deduplicator instance to ensure accurate memory tracking.
//
// This deduplicator handles:
// - Deduplication of series with the same labels within a Select() call
// - Hash collision handling when different series have the same hash
// - Tracking which series have already been seen to avoid double-counting memory
//
// SeriesDeduplicator is thread-safe and can be used concurrently from multiple goroutines.
// This is important when querying multiple sources (ingesters or store-gateways) in parallel,
// to ensure that series replicated across multiple sources are only counted once for memory tracking.
type SeriesDeduplicator struct {
	mtx            sync.Mutex
	uniqueSeries   map[uint64]labels.Labels
	conflictSeries map[uint64][]labels.Labels

	// hashFunc computes the hash of a labels.Labels. Defaults to labels.Labels.Hash() in production.
	// Can be overridden in tests to force hash collisions.
	hashFunc func(labels.Labels) uint64
}

// NewSeriesDeduplicator creates a new SeriesDeduplicator.
// This should be created fresh for each Select() call to ensure proper scoping.
func NewSeriesDeduplicator() *SeriesDeduplicator {
	return &SeriesDeduplicator{
		uniqueSeries: make(map[uint64]labels.Labels),
		hashFunc:     func(l labels.Labels) uint64 { return l.Hash() },
	}
}

// Deduplicate checks if the given series has been seen before in this deduplicator's scope.
// If it's a duplicate, it returns the previously seen labels and isDuplicate=true.
// If it's new, it returns the input labels and isDuplicate=false.
//
// This method handles hash collisions by checking label equality even when hashes match.
// This method is thread-safe.
func (sd *SeriesDeduplicator) Deduplicate(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	sd.mtx.Lock()
	defer sd.mtx.Unlock()

	fingerprint := sd.hashFunc(newLabels)

	// Check if we've seen this hash before
	if existingLabels, foundDuplicate := sd.uniqueSeries[fingerprint]; !foundDuplicate {
		// newLabels is seen for the first time hence we track the series limit and its labels memory consumption.
		sd.uniqueSeries[fingerprint] = newLabels
		return sd.trackNewLabels(newLabels, tracker)
	} else if labels.Equal(existingLabels, newLabels) {
		// newLabels is seen before, deduplicate it by returning existingLabels.
		return existingLabels, nil
	}

	// newLabels' hash conflicted with existingLabels.
	if sd.conflictSeries == nil {
		// Note that we only track second labels' hash conflict onward in this map. The first conflict always in uniqueSeries map.
		sd.conflictSeries = make(map[uint64][]labels.Labels)
	}
	hashConflictLabels := sd.conflictSeries[fingerprint]
	for _, existingConflictedLabels := range hashConflictLabels {
		// newLabels is seen before in conflictSeries map, hence just return the existingConflictedLabels.
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

// SeriesCount returns the total number of unique series seen by this deduplicator.
// This method is thread-safe.
func (sd *SeriesDeduplicator) SeriesCount() int {
	sd.mtx.Lock()
	defer sd.mtx.Unlock()

	count := len(sd.uniqueSeries)
	for _, conflicts := range sd.conflictSeries {
		count += len(conflicts)
	}
	return count
}
