// SPDX-License-Identifier: AGPL-3.0-only

// Package tenantshard holds the map implementations that store the series of one tenant shard,
// together with the interface the usage-tracker store uses to talk to them.
//
// There are two implementations, selected at runtime by the usage-tracker configuration:
// v1 keeps tombstones for the series that idle-series cleanup removes, while v2 keeps one
// spillmark per group instead, so cleanup never writes to the keys array.
package tenantshard

import (
	"fmt"
	"iter"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	v1 "github.com/grafana/mimir/pkg/usagetracker/tenantshard/v1"
	v2 "github.com/grafana/mimir/pkg/usagetracker/tenantshard/v2"
)

const (
	// NumShards is the number of shards used by the tracker store per tenant.
	// Implementations size themselves relative to it, so their copies must hold the same value.
	NumShards = 16

	// DefaultImplVersion is the implementation used unless it is configured otherwise.
	DefaultImplVersion = 1
)

// Map stores the last time each series of one tenant shard was seen.
// Implementations are not safe for concurrent use: callers hold the Map's lock around every call.
type Map interface {
	sync.Locker

	// Put inserts key and value into the map.
	// series is incremented if it's not nil and it's below limit, unless track is false.
	// If track is false, then the value is only updated if it's greater than the current value.
	Put(key uint64, value clock.Minutes, series, limit *atomic.Uint64, track bool) (created, rejected bool)

	// Load inserts key and value without checking whether key already exists.
	// No limits are checked, and the series count should be incremented by the caller.
	Load(key uint64, value clock.Minutes)

	// Count returns the number of series in the map.
	Count() int

	// Cleanup removes the series that were last seen at or before watermark, and returns how many
	// were removed. limit is the tenant-wide series limit, and may be nil.
	Cleanup(watermark clock.Minutes, limit *atomic.Uint64) int

	// EnsureCapacity makes sure that the map can store n elements without growing.
	EnsureCapacity(n uint32)

	// Items returns the number of series in the map and an iterator over them.
	Items() (length int, iterator iter.Seq2[uint64, clock.Minutes])

	// Stats returns a snapshot of the map's internal counters.
	Stats() Stats
}

// Stats is a point-in-time snapshot of a Map's internal counters, used for debugging.
// Counters that don't apply to the implementation in use are zero.
type Stats struct {
	// Resident is the number of elements held in the map, including the dead ones in v1.
	Resident uint32 `json:"resident"`
	// Dead is the number of dead elements (tombstones). Only v1 creates those.
	Dead uint32 `json:"dead"`
	// Spilled is the number of groups that spilled to the next one(s). Only v2 counts those.
	Spilled uint32 `json:"spilled"`
	// Limit is the resident count that triggers a rehash when reached.
	Limit uint32 `json:"limit"`
	// Length is the number of groups, i.e. the (identical) length of the index, keys and data arrays.
	Length int `json:"length"`
	// Rehashes is the number of rehashes since the Map was created.
	Rehashes uint32 `json:"rehashes"`
}

// Factory creates a Map with capacity for size elements.
type Factory func(size uint32) Map

// NewFactory returns a Factory that builds maps of the given implementation version.
func NewFactory(version int) (Factory, error) {
	switch version {
	case 1:
		return func(size uint32) Map { return v1Map{v1.New(size)} }, nil
	case 2:
		return func(size uint32) Map { return v2Map{v2.New(size)} }, nil
	default:
		return nil, fmt.Errorf("unsupported tenant shard map implementation version %d, supported versions are 1 and 2", version)
	}
}

// v1Map adapts v1.Map to the Map interface, and v2Map does the same for v2.Map.
// Only Stats needs adapting: the implementations don't import this package, so they can't return
// its Stats type. Every other method is promoted from the embedded map.
type v1Map struct{ *v1.Map }

func (m v1Map) Stats() Stats {
	s := m.Map.Stats()
	return Stats{
		Resident: s.Resident,
		Dead:     s.Dead,
		Limit:    s.Limit,
		Length:   s.Length,
		Rehashes: s.Rehashes,
	}
}

type v2Map struct{ *v2.Map }

func (m v2Map) Stats() Stats {
	s := m.Map.Stats()
	return Stats{
		Resident: s.Resident,
		Spilled:  s.Spilled,
		Limit:    s.Limit,
		Length:   s.Length,
		Rehashes: s.Rehashes,
	}
}
