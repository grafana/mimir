// SPDX-License-Identifier: AGPL-3.0-only

// Package store keeps the ingester's series: labels in compact shared buffers grouped by metric
// name with postings for the other labels, open chunks on the heap, completed chunks in
// memory-mapped chunk files, and series that left the emulated Go head in immutable cold blocks.
// Series are sharded by label hash so records apply to several shards in parallel, the way Go
// ingests with concurrent appenders; a series always lives in one shard, which keeps its samples
// in order.
package store

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// metadataKey identifies a metric family's metadata entry: type, help and unit.
type metadataKey struct {
	metricType int32
	help, unit string
}

type metadataEntry struct {
	metadata mimirpb.MetricMetadata
	seenMs   int64
}

type tenant struct {
	series *seriesByName
	// Snapshot metadata and the head's max time live only in shard 0.
	metadata map[string]map[metadataKey]metadataEntry

	maxTime int64
	// The oldest accepted sample, like the Go head's min time before its first compaction.
	minTime int64
	// The min time of the emulated Go head, which head compaction moves in block-range steps.
	headMin int64

	// Where the last compaction truncated the head, Prometheus's minValidTime.
	truncatedTo int64

	trackerGeneration uint64
	// Where the engine reports the series that are active, guarded by the shard's lock.
	cost CostAttribution
	// An engine's series of this shard by reference.
	byRef refIndex
	// Repeated batches can bypass the per-series location table; guarded by this shard.
	ingestLayouts *ingestLayoutCache
	// An engine's out-of-order head bounds, and the start of its oldest emulated block, in the
	// home shard.
	minOOOTime, maxOOOTime int64
	// The blocks an engine's compactions would have written, oldest first; replaced, never
	// changed, so lookups can keep reading a copy of the slice.
	blocks []emulatedBlock
	// The engine's out-of-order compactions retention still keeps the chunks of, oldest first.
	oooCompactions []outOfOrderCompaction
	// An engine's series of this shard that appenders created, by how many of them haven't committed or rolled back:
	// without samples until then, the head's garbage collection would take them, and their appender's samples with
	// them, like Prometheus's memSeries.pendingCommit keeps them.
	created map[uint64]int32
}

func newTenant() *tenant {
	return &tenant{
		series:      newSeriesByName(),
		metadata:    map[string]map[metadataKey]metadataEntry{},
		maxTime:     math.MinInt64,
		minTime:     math.MaxInt64,
		headMin:     math.MinInt64,
		truncatedTo: math.MinInt64,
		minOOOTime:  math.MaxInt64,
		maxOOOTime:  math.MinInt64,
		// Series start at generation 0, so they match the trackers on the first report.
		trackerGeneration: 1,
	}
}

type shardState struct {
	timedRWMutex
	tenant atomic.Pointer[tenant]
	disk   *chunks.DiskMapper
	cold   *coldState
}

// Creation is serialized by initialization or the shard write lock.
func (s *shardState) createTenant() *tenant {
	t := s.tenant.Load()
	if t == nil {
		t = newTenant()
		s.tenant.Store(t)
	}
	return t
}

// Store holds one tenant's sharded series.
type Store struct {
	tenantID string
	// Keeps the chunk files queries read from mapped until they are done, when guardChunkFiles set it up.
	reads readGuard
	// Bumped when a series is evicted from the head, which invalidates what the label lookups
	// cached about the series in it.
	evictEpoch atomic.Uint64
	shards     []*shardState

	exemplarsLock sync.Mutex
	exemplars     *exemplars.TenantExemplars[labels.Labels]

	// The files compactions dropped while the last head snapshot may still reference them, which the next snapshot
	// removes; only kept once DeferRemovals is called.
	removalsLock    sync.Mutex
	pendingRemovals []string

	// What retention last kept: cold blocks keep references to chunks it removed.
	retained atomic.Pointer[retained]
}

// newStore creates the shards and their chunk files.
func newStore(chunkDir, tenantID string, shardCount int) (*Store, error) {
	if chunkDir != "" {
		if err := os.RemoveAll(chunkDir); err != nil {
			return nil, fmt.Errorf("remove chunk directory %s: %w", chunkDir, err)
		}
	}
	shardCount = max(shardCount, 1)
	shards := make([]*shardState, shardCount)
	for index := range shards {
		disk, err := chunks.OpenDiskMapper(shardDir(chunkDir, index))
		if err != nil {
			return nil, err
		}
		cold, err := newColdState(coldDir(chunkDir, index))
		if err != nil {
			return nil, err
		}
		shards[index] = &shardState{disk: disk, cold: cold}
	}
	return fromShards(tenantID, shards), nil
}

func fromShards(tenantID string, shards []*shardState) *Store {
	if len(shards) > 0 {
		shards[0].createTenant()
	}

	s := &Store{
		tenantID: tenantID,
		shards:   shards,
	}
	s.retained.Store(retainedEverything)
	return s
}

func shardDir(directory string, shard int) string {
	if directory == "" {
		return ""
	}
	return filepath.Join(directory, fmt.Sprintf("shard-%03d", shard))
}

func coldDir(directory string, shard int) string {
	if directory == "" {
		return ""
	}
	return filepath.Join(directory, "cold", fmt.Sprintf("shard-%03d", shard))
}

// Close unmaps the chunk files and cold blocks.
func (s *Store) Close() error {
	var errs []error
	for _, shard := range s.shards {
		shard.Lock()
		errs = append(errs, shard.disk.Close(), shard.cold.close())
		shard.Unlock()
	}
	return errors.Join(errs...)
}

func shardFor(hash uint64, shards int) int {
	return int((hash >> 32) % uint64(shards))
}

// Like head truncation, retention drops whole chunks, so a chunk that straddles the cutoff stays.
// pruneRetained is pruneSeries for an engine's series in store shard shard: what kept keeps of its chunks stay, and
// its out-of-order head, which retention leaves alone, like Prometheus's.
func pruneRetained(series *Series, hash uint64, kept *retained, shard int) bool {
	series.chunks.retain(func(chunk *ChunkMeta) bool { return kept.keepsSeries(*chunk, shard, hash) })
	cutoff := kept.cutoffOf(hash)
	if series.floatHead != nil && series.floatHead.lastTimestamp() < cutoff {
		series.floatHead = nil
	}
	if series.histogram() != nil && series.histogram().Last().Timestamp < cutoff {
		series.setHistogram(nil)
	}
	return series.hasSamples()
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}

func satAdd(a, b int64) int64 {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		if b > 0 {
			return math.MaxInt64
		}
		return math.MinInt64
	}
	return sum
}

func satSub(a, b int64) int64 {
	difference := a - b
	if (b > 0 && difference > a) || (b < 0 && difference < a) {
		if b > 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	return difference
}

// rangeStart saturates, since lookups ask for ranges up to the ends of int64.
func rangeStart(timestamp int64) int64 {
	rem := timestamp % chunkRangeMs
	if rem < 0 {
		rem += chunkRangeMs
	}
	return satSub(timestamp, rem)
}

func rangeEnd(timestamp int64) int64 {
	return satAdd(rangeStart(timestamp), chunkRangeMs)
}

// Port of Prometheus computeChunkEndTime: spread the remaining range evenly over chunks of the
// observed sample rate so chunk boundaries align with block ranges.
func computeChunkEndTime(start, current, maxTime int64, ratioToFull float64) int64 {
	n := float64(maxTime-start) / (float64(current-start+1) * ratioToFull)
	if n <= 1 {
		return maxTime
	}
	return int64(float64(start) + float64(maxTime-start)/math.Floor(n))
}

// ShardByAllLabels is Mimir's `ShardByAllLabels`: 32-bit FNV-1 over the tenant ID and every label
// name and value.
func ShardByAllLabels(tenantID string, stored labels.Labels) uint32 {
	hash := uint32(2_166_136_261)
	add := func(s string) {
		for index := 0; index < len(s); index++ {
			hash *= 16_777_619
			hash ^= uint32(s[index])
		}
	}
	add(tenantID)
	it := stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		add(name)
		add(value)
	}
	return hash
}

// DeferRemovals keeps the chunk files and cold blocks that compactions drop on disk until the next head snapshot is
// written: the last one may reference them, and restoring it needs them.
func (s *Store) DeferRemovals() {
	for _, shard := range s.shards {
		shard.disk.DeferRemovals(s.queueRemoval)
		shard.cold.remove = s.queueRemoval
	}
}

func (s *Store) queueRemoval(path string) {
	s.removalsLock.Lock()
	s.pendingRemovals = append(s.pendingRemovals, path)
	s.removalsLock.Unlock()
}

func (s *Store) pendingRemovalCount() int {
	s.removalsLock.Lock()
	defer s.removalsLock.Unlock()
	return len(s.pendingRemovals)
}

// removePending removes the first count files queued, which a snapshot written since no longer references.
func (s *Store) removePending(count int) {
	s.removalsLock.Lock()
	paths := slices.Clone(s.pendingRemovals[:count])
	s.pendingRemovals = slices.Delete(s.pendingRemovals, 0, count)
	s.removalsLock.Unlock()
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "phase=deferred_remove_error path=%s error=%v\n", path, err)
		}
	}
}

func toExemplar(e *mimirpb.Exemplar) exemplars.Exemplar {
	out := exemplars.Exemplar{Value: e.Value, TimestampMs: e.TimestampMs}
	if len(e.Labels) > 0 {
		out.Labels = make([]exemplars.Label, len(e.Labels))
		for index, label := range e.Labels {
			out.Labels[index] = exemplars.Label{Name: label.Name, Value: label.Value}
		}
	}
	return out
}

func fromExemplar(e *exemplars.Exemplar) mimirpb.Exemplar {
	out := mimirpb.Exemplar{Value: e.Value, TimestampMs: e.TimestampMs}
	if len(e.Labels) > 0 {
		out.Labels = make([]mimirpb.LabelAdapter, len(e.Labels))
		for index, label := range e.Labels {
			out.Labels[index] = mimirpb.LabelAdapter{Name: label.Name, Value: label.Value}
		}
	}
	return out
}
