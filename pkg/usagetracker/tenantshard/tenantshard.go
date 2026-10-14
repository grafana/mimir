// SPDX-License-Identifier: AGPL-3.0-only

// Package tenantshard holds the map implementations that store the series of one tenant shard,
// together with the interface the usage-tracker store uses to talk to them.
//
// The implementation is selected at runtime by the usage-tracker configuration. The only one left
// is v2, which keeps one spillmark per group for the series that idle-series cleanup removes, so
// cleanup never writes to the keys array.
package tenantshard

import (
	"fmt"
	"iter"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	v2 "github.com/grafana/mimir/pkg/usagetracker/tenantshard/v2"
)

const (
	// DefaultNumShards is the default number of shards used by the tracker store per tenant.
	// It is a balanced default: more shards lower lock contention and cleanup-induced tail
	// latency for very large tenants, but add fixed per-tenant overhead that is wasteful for
	// small tenants. It is configurable via -usage-tracker.num-shards; see that flag and the
	// usagetracker.Config.NumShards field for the full tradeoff.
	DefaultNumShards = 16

	// MaxNumShards is the maximum number of shards allowed per tenant.
	// The shard index is stored as a single byte (uint8) in snapshots and used to index
	// per-tenant shard slices, so it must fit in [0, 256).
	//
	// The count must also be a power of 2: the tracker store picks a series' shard by masking
	// its hash, which only matches hash % count when the count is a power of 2.
	MaxNumShards = 256

	// DefaultImplVersion is the implementation used unless it is configured otherwise.
	DefaultImplVersion = 2
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
	// Resident is the number of elements held in the map.
	Resident uint32 `json:"resident"`
	// Spilled is the number of groups that spilled to the next one(s).
	Spilled uint32 `json:"spilled"`
	// Limit is the resident count that triggers a rehash when reached.
	Limit uint32 `json:"limit"`
	// Length is the number of groups, i.e. the (identical) length of the index, keys and data arrays.
	Length int `json:"length"`
	// Rehashes is the number of rehashes since the Map was created.
	Rehashes uint32 `json:"rehashes"`
}

// Factory creates the per-tenant shard maps of one implementation.
// It also carries the number of shards each tenant is split into: the maps derive their
// per-shard target size from the tenant-wide series limit, so they need to know how many
// ways that limit is split, and the tracker store reads it back to size its shard slices.
// The zero value is not usable: build one with NewFactory.
type Factory struct {
	numShards int
	newMap    func(size, numShards uint32) Map
}

// New creates a Map with capacity for size elements.
func (f Factory) New(size uint32) Map {
	return f.newMap(size, uint32(f.numShards))
}

// NumShards is the number of shards each tenant's series are split into.
func (f Factory) NumShards() int {
	return f.numShards
}

// NewFactory returns a Factory that builds maps of the given implementation version,
// for tenants split into numShards shards.
func NewFactory(version int, numShards int) (Factory, error) {
	if !isPowerOfTwo(numShards) || numShards > MaxNumShards {
		return Factory{}, fmt.Errorf("invalid number of tenant shards %d, must be a power of 2 between 1 and %d", numShards, MaxNumShards)
	}
	f := Factory{numShards: numShards}
	switch version {
	case 2:
		f.newMap = func(size, numShards uint32) Map { return v2Map{v2.New(size, numShards)} }
	default:
		return Factory{}, fmt.Errorf("unsupported tenant shard map implementation version %d, the only supported version is 2", version)
	}
	return f, nil
}

// isPowerOfTwo is a local copy of the usagetracker helper: this package can't import the one
// that validates the configuration, because that package imports this one.
func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}

// v2Map adapts v2.Map to the Map interface.
// Only Stats needs adapting: the implementation doesn't import this package, so it can't return
// its Stats type. Every other method is promoted from the embedded map.
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
