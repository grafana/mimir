// SPDX-License-Identifier: AGPL-3.0-only

package otlparena

import (
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Arena provides pre-allocated slices for OTLP ingestion to reduce allocations.
// Instead of each request allocating its own slices from multiple sync.Pools
// (which can cause mutex contention under high load), an Arena is obtained
// once per request and provides all needed slice storage.
type Arena struct {
	timeseries []mimirpb.PreallocTimeseries
	metadata   []*mimirpb.MetricMetadata
}

const (
	defaultTimeseriesCap = 1024
	defaultMetadataCap   = 64
)

var arenaPool = sync.Pool{
	New: func() any {
		return &Arena{
			timeseries: make([]mimirpb.PreallocTimeseries, 0, defaultTimeseriesCap),
			metadata:   make([]*mimirpb.MetricMetadata, 0, defaultMetadataCap),
		}
	},
}

// Get obtains an Arena from the pool.
func Get() *Arena {
	return arenaPool.Get().(*Arena)
}

// Release returns the Arena to the pool after resetting it.
func (a *Arena) Release() {
	a.Reset()
	arenaPool.Put(a)
}

// Reset clears the arena for reuse, returning borrowed TimeSeries to their pool.
func (a *Arena) Reset() {
	// Return each TimeSeries to the pool
	for i := range a.timeseries {
		mimirpb.ReusePreallocTimeseries(&a.timeseries[i])
	}
	a.timeseries = a.timeseries[:0]
	a.metadata = a.metadata[:0]
}

// GetTimeseriesSlice returns a pointer to the timeseries slice for appending.
func (a *Arena) GetTimeseriesSlice() *[]mimirpb.PreallocTimeseries {
	return &a.timeseries
}

// GetMetadataSlice returns a pointer to the metadata slice for appending.
func (a *Arena) GetMetadataSlice() *[]*mimirpb.MetricMetadata {
	return &a.metadata
}
