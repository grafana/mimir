// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// bucketIndexLoadedSeries holds the result of a series load operation.
// This data structure is NOT concurrency safe.
type bucketIndexLoadedSeries struct {
	// Keeps the series that have been loaded from the index.
	series map[storage.SeriesRef][]byte
}

func newBucketIndexLoadedSeries() *bucketIndexLoadedSeries {
	return &bucketIndexLoadedSeries{
		series: map[storage.SeriesRef][]byte{},
	}
}

// loadSeriesForTime populates the given symbolized labels for the series identified by the reference if at least one chunk is within
// time selection.
// loadSeriesForTime also populates chunk metas slices if skipChunks if set to false. Chunks are also limited by the given time selection.
// loadSeriesForTime returns false, when there are no series data for given time range.
//
// Error is returned on decoding error or if the reference does not resolve to a known series.
func (l *bucketIndexLoadedSeries) loadSeriesForTime(ref storage.SeriesRef, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, mint, maxt int64, stats *queryStats) (ok bool, err error) {
	b, ok := l.series[ref]
	if !ok {
		return false, errors.Errorf("series %d not found", ref)
	}

	stats.seriesTouched++
	stats.seriesTouchedSizeSum += len(b)
	return decodeSeriesForTime(b, lset, chks, skipChunks, mint, maxt)
}
