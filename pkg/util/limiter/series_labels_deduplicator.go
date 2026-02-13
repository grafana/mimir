// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"errors"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

type seriesDeduplicatorCtxKey struct{}

var (
	sdCtxKey = seriesDeduplicatorCtxKey{}
)

// SeriesLabelsDeduplicator is used to deduplicate series and track the unique series memory consumption.
type SeriesLabelsDeduplicator interface {
	Deduplicate(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error)
}
type seriesDeduplicator struct {
	uniqueSeriesMx sync.Mutex
	uniqueSeries   map[uint64]labels.Labels
	conflictSeries map[uint64][]labels.Labels

	// hashFunc as a function so that we can override in test.
	hashFunc func(labels.Labels) uint64
}

func NewSeriesLabelsDeduplicator() SeriesLabelsDeduplicator {
	return &seriesDeduplicator{
		uniqueSeriesMx: sync.Mutex{},
		uniqueSeries:   make(map[uint64]labels.Labels),
		hashFunc:       func(l labels.Labels) uint64 { return l.Hash() },
	}
}

// ContextWithNewSeriesLabelsDeduplicator adds a new SeriesLabelsDeduplicator to the context.
func ContextWithNewSeriesLabelsDeduplicator(ctx context.Context) context.Context {
	return context.WithValue(ctx, sdCtxKey, NewSeriesLabelsDeduplicator())
}

func SeriesLabelsDeduplicatorFromContext(ctx context.Context) (SeriesLabelsDeduplicator, error) {
	sd, ok := ctx.Value(sdCtxKey).(SeriesLabelsDeduplicator)
	if !ok {
		return nil, errors.New("no series deduplicator in context")
	}
	return sd, nil
}

// Deduplicate checks if the given series has been seen before in this deduplicator's scope.
// If it's a duplicate, it returns the previously seen labels.
// If it's new, it returns the passed newLabels.
//
// This method handles hash collisions by checking label equality even when hashes match.
func (sd *seriesDeduplicator) Deduplicate(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	fingerprint := sd.hashFunc(newLabels)

	sd.uniqueSeriesMx.Lock()
	defer sd.uniqueSeriesMx.Unlock()

	// Check if we've seen this hash before.
	if existingLabels, found := sd.uniqueSeries[fingerprint]; !found {
		// See newLabels for the first time. Track the series limit and its labels memory consumption.
		sd.uniqueSeries[fingerprint] = newLabels
		return sd.trackNewLabels(newLabels, tracker)
	} else if labels.Equal(existingLabels, newLabels) {
		// Already see newLabels before. Deduplicate it by returning existingLabels. No need to increase memory consumption.
		return existingLabels, nil
	}

	// newLabels' hash conflicted with existingLabels.
	if sd.conflictSeries == nil {
		// Track only second labels' hash conflict onward in this map. The first conflict always in uniqueSeries map.
		sd.conflictSeries = make(map[uint64][]labels.Labels)
	}
	hashConflictLabels := sd.conflictSeries[fingerprint]
	for _, existingConflictedLabels := range hashConflictLabels {
		// See newLabels before in conflictSeries map, hence just return the existingConflictedLabels.
		// No need to increase memory consumption.
		if labels.Equal(existingConflictedLabels, newLabels) {
			return existingConflictedLabels, nil
		}
	}
	// Despite there was a hash conflict, we see newLabels for the first time. Track the series limit and its labels memory consumption.
	sd.conflictSeries[fingerprint] = append(hashConflictLabels, newLabels)
	return sd.trackNewLabels(newLabels, tracker)
}

func (sd *seriesDeduplicator) trackNewLabels(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	err := tracker.IncreaseMemoryConsumptionForLabels(newLabels)
	if err != nil {
		return labels.EmptyLabels(), err
	}
	return newLabels, nil
}
