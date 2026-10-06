// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/oklog/ulid/v2"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	tsdbchunks "github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// headULID is the ULID Prometheus gives its head block, which callers use as its cache key.
var headULID = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")

// Series references keep their store shard in their low bits, so a reference finds its series
// without a global map.
const (
	refShardBits = 8
	maxShards    = 1 << refShardBits
)

// EngineOptions are the per-tenant TSDB options of Mimir's ingester that the engine honours.
type EngineOptions struct {
	// Store shards, each with its own lock and chunk files; the ingester appends to a tenant from
	// several goroutines at once.
	Shards int
	// How long cold data is kept, like the TSDB's block retention.
	RetentionMs int64
	// Like `tsdb.Options`.
	OutOfOrderTimeWindowMs int64
	MaxExemplars           int64
	// Prometheus's `TimelyCompaction`: the head compacts its oldest block range once it ends
	// behind the appendable window, instead of once the head spans 1.5 block ranges.
	TimelyCompaction        bool
	SeriesLifecycleCallback tsdb.SeriesLifecycleCallback
	// Mimir's owned series token of a series, `tsdb.Options.SecondaryHashFunction`.
	SecondaryHashFunction func(promlabels.Labels) uint32
}

// Engine is one tenant's series behind the per-tenant TSDB interface Mimir's ingester uses:
// Prometheus appenders, head statistics, index readers, queriers and compactions, over the store's
// name groups, hashed postings, compact labels, chunk files and cold blocks. It has no WAL: Close
// writes a head snapshot that the next Open restores, and a start without one begins empty.
type Engine struct {
	store    *Store
	tenantID string
	dir      string
	opts     EngineOptions
	callback tsdb.SeriesLifecycleCallback

	nextRef      atomic.Uint64
	oooWindow    atomic.Int64
	maxExemplars atomic.Int64
	// Whether an out-of-order window was ever set: out-of-order head compactions only run then.
	oooWasEnabled atomic.Bool
	// By store shard, the chunk reference below which out-of-order chunks were compacted out of
	// the out-of-order head; guarded by compactionLock and the shard's lock.
	oooWatermarks []uint64
	// By store shard, the tenant once it exists there: tenants are never removed, and appends
	// would otherwise look it up by name for every sample.
	tenants []atomic.Pointer[tenant]

	// Compactions and snapshots run one at a time.
	compactionLock sync.Mutex
	// Whether Open restored the head as it was at the last Close, or started without any data.
	restored bool
	closed   atomic.Bool
}

// A nop callback, for engines opened without one.
type noopCallback struct{}

func (noopCallback) PreCreation(promlabels.Labels) error                         { return nil }
func (noopCallback) PostCreation(promlabels.Labels)                              {}
func (noopCallback) PostDeletion(map[tsdbchunks.HeadSeriesRef]promlabels.Labels) {}

// storeDirName is where an engine keeps its chunk files, cold blocks and snapshot in its
// directory.
const storeDirName = "seriesstore"

// OpenEngine opens tenantID's engine in dir, restoring the head snapshot of its last Close. Without
// one, whatever the directory held is dropped: chunk files are only readable through a snapshot.
// Restored reports which happened, so the caller can replay what the head lost.
func OpenEngine(dir, tenantID string, opts EngineOptions) (*Engine, error) {
	if opts.Shards <= 0 {
		opts.Shards = 4
	}
	if opts.Shards > maxShards {
		return nil, fmt.Errorf("at most %d store shards", maxShards)
	}
	e := &Engine{tenantID: tenantID, dir: dir, opts: opts, callback: opts.SeriesLifecycleCallback}
	if e.callback == nil {
		e.callback = noopCallback{}
	}
	e.oooWindow.Store(opts.OutOfOrderTimeWindowMs)
	e.oooWasEnabled.Store(opts.OutOfOrderTimeWindowMs > 0)
	e.maxExemplars.Store(opts.MaxExemplars)
	retention := Retention{Ms: opts.RetentionMs, Set: opts.RetentionMs > 0}
	storeDir := ""
	if dir != "" {
		storeDir = filepath.Join(dir, storeDirName)
	}
	// Active series are the ingester's; the store's own window only matters to its unused reports.
	const activeWindowMs = 20 * 60 * 1000
	var state []byte
	if dir != "" {
		var err error
		if state, err = takeState(dir); err != nil {
			return nil, err
		}
	}
	if storeDir != "" {
		restored, err := Restore(activeWindowMs, retention, storeDir, 1)
		if err != nil {
			return nil, err
		}
		if restored != nil && len(restored.Store.shards) <= maxShards {
			e.store = restored.Store
			e.store.memoryIsHead = true
			e.oooWatermarks = make([]uint64, len(e.store.shards))
			e.restored = true
			e.createHome()
			err := e.adoptRestored()
			if err == nil && state != nil {
				err = e.restoreState(state)
			}
			if err != nil {
				_ = e.store.Close()
				return nil, err
			}
			return e, nil
		}
		if restored != nil {
			_ = restored.Store.Close()
		}
		// Nothing to restore: the directory is new, or its data can't be read back.
		entries, err := os.ReadDir(storeDir)
		e.restored = errors.Is(err, os.ErrNotExist) || (err == nil && len(entries) == 0)
	} else {
		e.restored = true
	}
	s, err := NewWithShards(activeWindowMs, retention, storeDir, opts.Shards, 1)
	if err != nil {
		return nil, err
	}
	e.store = s
	s.memoryIsHead = true
	e.oooWatermarks = make([]uint64, len(s.shards))
	e.createHome()
	return e, nil
}

// tenantLocked is the tenant in the store shard index, with the shard locked.
func (e *Engine) tenantLocked(index int) (*tenant, bool) {
	if t := e.tenants[index].Load(); t != nil {
		return t, true
	}
	t, ok := e.store.shards[index].tenants[e.tenantID]
	if ok {
		e.tenants[index].Store(t)
	}
	return t, ok
}

// createHome creates the tenant in its home shard, which keeps the head's times and blocks, so
// reading them never inserts it.
func (e *Engine) createHome() {
	e.tenants = make([]atomic.Pointer[tenant], len(e.store.shards))
	home := e.store.shards[0]
	home.Lock()
	defer home.Unlock()
	tenantOf(home.tenants, e.tenantID)
}

// adoptRestored gives the restored series references and their owned series tokens, and reports
// them created, like a head replaying its WAL.
func (e *Engine) adoptRestored() error {
	var builder promlabels.ScratchBuilder
	for index, shard := range e.store.shards {
		shard.Lock()
		t, ok := shard.tenants[e.tenantID]
		if !ok {
			shard.Unlock()
			continue
		}
		t.byRef.reset(t.series.len)
		var created []promlabels.Labels
		for groupID := range t.series.groups {
			entries := t.series.groups[groupID].entries
			for position := range entries {
				entry := &entries[position]
				ref := e.newRef(index)
				entry.series.ref = ref
				t.byRef.set(ref, refLocation{uint32(groupID), int32(position)})
				lset := toPromLabels(entry.labels, &builder)
				if e.opts.SecondaryHashFunction != nil {
					entry.series.ownedHash = e.opts.SecondaryHashFunction(lset)
				}
				entry.series.shardHash = promlabels.StableHash(lset)
				entry.series.inHead = true
				created = append(created, lset)
			}
		}
		shard.Unlock()
		for _, lset := range created {
			e.callback.PostCreation(lset)
		}
	}
	// The out-of-order head holds what the restored series have out of order.
	minOOOTime, maxOOOTime := int64(math.MaxInt64), int64(math.MinInt64)
	for _, shard := range e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				it := series.chunks.iter()
				for chunk, more := it.next(); more; chunk, more = it.next() {
					if chunk.OutOfOrder {
						minOOOTime, maxOOOTime = min(minOOOTime, chunk.MinTime), max(maxOOOTime, chunk.MaxTime)
					}
				}
				if n := len(series.outOfOrder); n > 0 {
					minOOOTime, maxOOOTime = min(minOOOTime, series.outOfOrder[0].T), max(maxOOOTime, series.outOfOrder[n-1].T)
				}
			})
		}
		shard.RUnlock()
	}
	if minOOOTime <= maxOOOTime {
		e.observeOOO(minOOOTime, maxOOOTime)
	}
	return nil
}

func (e *Engine) newRef(shard int) uint64 {
	return e.nextRef.Add(1)<<refShardBits | uint64(shard)
}

func refShard(ref uint64) int {
	return int(ref & (maxShards - 1))
}

// Restored reports whether Open found the head as the last Close left it, or no data at all. A
// false restored head lost what it had: its samples must be ingested again.
func (e *Engine) Restored() bool {
	return e.restored
}

// Dir is the engine's directory.
func (e *Engine) Dir() string {
	return e.dir
}

// Close writes the head snapshot the next Open restores, and releases the engine's files.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.compactionLock.Lock()
	defer e.compactionLock.Unlock()
	var err error
	if e.dir != "" {
		err = e.store.WriteSnapshot(nil)
		if err == nil {
			err = e.writeState()
		}
	}
	return errors.Join(err, e.store.Close())
}

// SetOutOfOrderTimeWindow and SetMaxExemplars apply a tenant's changed limits, like the TSDB's
// ApplyConfig.
func (e *Engine) SetOutOfOrderTimeWindow(windowMs int64) {
	e.oooWindow.Store(windowMs)
	if windowMs > 0 {
		e.oooWasEnabled.Store(true)
	}
}

func (e *Engine) SetMaxExemplars(maxExemplars int64) {
	e.maxExemplars.Store(maxExemplars)
	e.store.exemplarsLock.Lock()
	defer e.store.exemplarsLock.Unlock()
	if storage, ok := e.store.exemplars[e.tenantID]; ok && storage.Capacity() != int(maxExemplars) {
		storage.Resize(int(max(maxExemplars, 0)))
	}
}

// headTimes are the head's time bounds, kept in the tenant's home shard with the store's head
// emulation fields: minTime is Prometheus's head min time (math.MaxInt64 until the first sample),
// headMin and truncatedTo its min valid time after truncations.
func (e *Engine) home() (*shardState, *tenant) {
	home := e.store.shards[0]
	return home, home.tenants[e.tenantID]
}

func (e *Engine) headTimes() (minTime, maxTime, minValid int64) {
	home := e.store.shards[0]
	home.RLock()
	defer home.RUnlock()
	t, ok := home.tenants[e.tenantID]
	if !ok {
		return math.MaxInt64, math.MinInt64, math.MinInt64
	}
	return t.minTime, t.maxTime, t.truncatedTo
}

// MinTime, MaxTime, MinOOOTime and MaxOOOTime are the head's, like Prometheus's.
func (e *Engine) MinTime() int64 {
	minTime, _, _ := e.headTimes()
	return minTime
}

func (e *Engine) MaxTime() int64 {
	_, maxTime, _ := e.headTimes()
	return maxTime
}

func (e *Engine) MinOOOTime() int64 {
	home, t := e.home()
	home.RLock()
	defer home.RUnlock()
	return t.minOOOTime
}

func (e *Engine) MaxOOOTime() int64 {
	home, t := e.home()
	home.RLock()
	defer home.RUnlock()
	return t.maxOOOTime
}

func (e *Engine) observeOOO(minTime, maxTime int64) {
	home, t := e.home()
	home.Lock()
	defer home.Unlock()
	t.minOOOTime = min(t.minOOOTime, minTime)
	t.maxOOOTime = max(t.maxOOOTime, maxTime)
}

// appendableMinValidTime is Prometheus's: half a block range behind the head's max time, and
// never before where the head was last truncated.
func appendableMinValidTime(maxTime, minValid int64) int64 {
	return max(satSub(maxTime, chunkRangeMs/2), minValid)
}

// AppendableMinValidTime is the oldest in-order sample appenders accept, false until the head has
// its first sample.
func (e *Engine) AppendableMinValidTime() (int64, bool) {
	minTime, maxTime, minValid := e.headTimes()
	if minTime == math.MaxInt64 {
		return 0, false
	}
	return appendableMinValidTime(maxTime, minValid), true
}

// NumSeries is the head's series count.
func (e *Engine) NumSeries() uint64 {
	return e.store.NumSeries(e.tenantID)
}

// Meta is the head's block meta, like Prometheus's head block.
func (e *Engine) Meta() tsdb.BlockMeta {
	minTime, maxTime, _ := e.headTimes()
	return tsdb.BlockMeta{MinTime: minTime, MaxTime: maxTime, ULID: headULID, Stats: tsdb.BlockStats{NumSeries: e.NumSeries()}}
}

// initTime sets the head's time bounds from its first sample, like Prometheus's initAppender.
func (e *Engine) initTime(timestamp int64) {
	home, t := e.home()
	home.Lock()
	defer home.Unlock()
	if t.minTime == math.MaxInt64 {
		t.minTime = timestamp
		t.maxTime = timestamp
	}
}

// commitTimes extends the head's time bounds with committed in-order samples.
func (e *Engine) commitTimes(minTime, maxTime int64) {
	home, t := e.home()
	home.Lock()
	defer home.Unlock()
	if minTime < t.minTime {
		t.minTime = minTime
	}
	if maxTime > t.maxTime {
		t.maxTime = maxTime
	}
}

// compactable is Prometheus's head compaction check.
func (e *Engine) compactable() bool {
	minTime, maxTime, minValid := e.headTimes()
	if minTime == math.MaxInt64 {
		return false
	}
	if e.opts.TimelyCompaction {
		return rangeForTimestamp(minTime) < appendableMinValidTime(maxTime, minValid)
	}
	return maxTime-minTime > chunkRangeMs/2*3
}

// rangeForTimestamp is Prometheus's: the end of the block range of t.
func rangeForTimestamp(t int64) int64 {
	return (t/chunkRangeMs)*chunkRangeMs + chunkRangeMs
}

// Compact compacts the head's oldest block ranges while the head is compactable, then its
// out-of-order head, like Prometheus's DB.Compact, and drops data past the retention. Compacted
// data stays queryable in cold blocks.
func (e *Engine) Compact(context.Context) error {
	e.compactionLock.Lock()
	defer e.compactionLock.Unlock()
	compacted := false
	for e.compactable() {
		minTime := e.MinTime()
		e.truncateMemory(minTime, rangeForTimestamp(minTime))
		compacted = true
	}
	if compacted {
		e.compactOOOHead()
	}
	if e.opts.RetentionMs > 0 {
		home, t := e.home()
		home.RLock()
		cutoff := expiredBefore(t.blocks, satSub(nowMs(), e.opts.RetentionMs))
		home.RUnlock()
		if cutoff > math.MinInt64 {
			return e.prune(cutoff)
		}
	}
	return nil
}

// CompactHead compacts the head up to maxt, like Prometheus's CompactHead of a RangeHead ending at
// maxt, which truncates the head to maxt+1.
func (e *Engine) CompactHead(mint, maxt int64) error {
	e.compactionLock.Lock()
	defer e.compactionLock.Unlock()
	e.truncateMemory(mint, satAdd(maxt, 1))
	return nil
}

// CompactOOOHead compacts the out-of-order head, like Prometheus's.
func (e *Engine) CompactOOOHead(context.Context) error {
	e.compactionLock.Lock()
	defer e.compactionLock.Unlock()
	e.compactOOOHead()
	return nil
}

// truncateMemory is Prometheus's head compaction: the block over [blockMin, mint) is written, the
// head's min time and min valid time move to mint, then its garbage collection runs.
func (e *Engine) truncateMemory(blockMin, mint int64) {
	if minTime := e.MinTime(); minTime != math.MaxInt64 && minTime < mint {
		e.writeBlock(blockMin, mint, false, func(_ int, series *Series) bool {
			return inOrderIn(series, blockMin, mint-1, minTime)
		})
	}
	home, t := e.home()
	home.Lock()
	if t.minTime >= mint && t.minTime != math.MaxInt64 {
		home.Unlock()
		return
	}
	initialized := t.minTime != math.MaxInt64
	t.minTime = mint
	t.headMin = mint
	t.truncatedTo = mint
	if t.maxTime < mint {
		t.maxTime = mint
	}
	home.Unlock()
	if initialized {
		e.headGC()
	}
}

// compactOOOHead writes the open out-of-order chunks out and marks every out-of-order chunk
// compacted, like Prometheus's out-of-order head compaction; the samples stay queryable with their
// series. The head's garbage collection runs when any chunk was compacted, like truncateOOO.
func (e *Engine) compactOOOHead() {
	if !e.oooWasEnabled.Load() {
		return
	}
	watermarks := slices.Clone(e.oooWatermarks)
	compactedFrom, compactedTo := int64(math.MaxInt64), int64(math.MinInt64)
	for index, shard := range e.store.shards {
		shard.Lock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				if err := flushOutOfOrder(series, shard.disk); err != nil {
					fmt.Fprintf(os.Stderr, "phase=ooo_flush_error tenant=%s error=%v\n", e.tenantID, err)
				}
				// Out-of-order blocks are aligned on block ranges.
				if err := splitChunks(series, shard.disk, func(meta ChunkMeta) []int64 {
					if meta.OutOfOrder && uint64(meta.Ref) >= e.oooWatermarks[index] {
						return alignedCuts(meta)
					}
					return nil
				}); err != nil {
					fmt.Fprintf(os.Stderr, "phase=ooo_split_error tenant=%s error=%v\n", e.tenantID, err)
				}
				it := series.chunks.iter()
				for chunk, more := it.next(); more; chunk, more = it.next() {
					if chunk.OutOfOrder && uint64(chunk.Ref) >= e.oooWatermarks[index] {
						compactedFrom = min(compactedFrom, chunk.MinTime)
						compactedTo = max(compactedTo, chunk.MaxTime)
						watermarks[index] = max(watermarks[index], uint64(chunk.Ref)+1)
					}
				}
			})
		}
		shard.Unlock()
	}
	if compactedFrom > compactedTo {
		return
	}
	// Like Prometheus's, one block per block range of the out-of-order head's data.
	for blockMin := rangeStart(compactedFrom); blockMin <= compactedTo; blockMin += chunkRangeMs {
		blockMax := blockMin + chunkRangeMs
		e.writeBlock(blockMin, blockMax, true, func(shard int, series *Series) bool {
			return e.oooIn(shard, series, blockMin, blockMax-1)
		})
	}
	for index, shard := range e.store.shards {
		shard.Lock()
		e.oooWatermarks[index] = watermarks[index]
		shard.Unlock()
	}
	e.headGC()
}

// oooState reports whether a series has out-of-order data the out-of-order head holds, not yet
// compacted, and its oldest.
func (e *Engine) oooState(shard int, series *Series) (bool, int64) {
	has, oldest := false, int64(math.MaxInt64)
	watermark := e.oooWatermarks[shard]
	it := series.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if chunk.OutOfOrder && uint64(chunk.Ref) >= watermark {
			has, oldest = true, min(oldest, chunk.MinTime)
		}
	}
	if len(series.outOfOrder) > 0 {
		has, oldest = true, min(oldest, series.outOfOrder[0].T)
	}
	return has, oldest
}

// inOrderFrom reports whether a series has in-order data in a chunk ending at or after mint, and
// the start of the oldest such chunk: the series' min time once older chunks are truncated.
func inOrderFrom(series *Series, mint int64) (bool, int64) {
	has, oldest := false, int64(math.MaxInt64)
	it := series.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if !chunk.OutOfOrder && chunk.MaxTime >= mint {
			has, oldest = true, min(oldest, chunk.MinTime)
		}
	}
	if fh := series.floatHead; fh != nil && fh.lastTimestamp() >= mint {
		has, oldest = true, min(oldest, fh.minTime)
	}
	if hh := series.histogramHead; hh != nil && hh.Last().Timestamp >= mint {
		has, oldest = true, min(oldest, hh.FirstTimestamp())
	}
	return has, oldest
}

// headGC is Prometheus's head garbage collection after a truncation: series without in-order
// chunks since the head's min time nor out-of-order data in the out-of-order head leave for cold
// blocks and are reported deleted. The head's min time then moves to its oldest remaining data,
// and its out-of-order min time to what remains of the out-of-order head.
func (e *Engine) headGC() {
	mint, headMaxt, _ := e.headTimes()
	home, t := e.home()
	home.RLock()
	blocks := t.blocks
	home.RUnlock()
	actualMint, minOOOTime := int64(math.MaxInt64), int64(math.MaxInt64)
	deleted := map[tsdbchunks.HeadSeriesRef]promlabels.Labels{}
	var builder promlabels.ScratchBuilder
	// Against the order queries read the shards in: a query meets the collection at one shard,
	// rather than reaching each next shard just after it was locked and waiting on all of them.
	for index := len(e.store.shards) - 1; index >= 0; index-- {
		shard := e.store.shards[index]
		shard.Lock()
		if headGCShardHook != nil {
			headGCShardHook(index)
		}
		t, ok := shard.tenants[e.tenantID]
		if !ok {
			shard.Unlock()
			continue
		}
		leaving := map[uint64]labels.Labels{}
		t.series.forEach(func(entry *seriesEntry) {
			series := &entry.series
			// Chunks the collection drops are only in blocks, as their pieces.
			if err := splitChunks(series, shard.disk, func(meta ChunkMeta) []int64 {
				if !meta.OutOfOrder && meta.MaxTime < mint {
					return inOrderCuts(blocks, meta)
				}
				return nil
			}); err != nil {
				fmt.Fprintf(os.Stderr, "phase=chunk_split_error tenant=%s error=%v\n", e.tenantID, err)
			}
			inOrder, seriesMint := inOrderFrom(series, mint)
			ooo, oooMint := e.oooState(index, series)
			series.inHead = !series.headEvicted && (inOrder || ooo)
			if !series.inHead {
				leaving[series.ref] = entry.labels
				return
			}
			// A series with only out-of-order data has no in-order min time.
			if !inOrder {
				seriesMint = math.MinInt64
			}
			actualMint = min(actualMint, seriesMint)
			if ooo {
				minOOOTime = min(minOOOTime, oooMint)
			}
		})
		e.freezeLeaving(shard, t, leaving, deleted, &builder)
		shard.Unlock()
	}
	if actualMint == math.MaxInt64 {
		actualMint = mint
	}
	home, t = e.home()
	home.Lock()
	if actualMint > t.minTime {
		appendable := appendableMinValidTime(t.maxTime, t.truncatedTo)
		if actualMint < appendable {
			t.minTime, t.headMin, t.truncatedTo = actualMint, actualMint, actualMint
		} else {
			t.minTime, t.headMin, t.truncatedTo = appendable, appendable, appendable
		}
	}
	home.Unlock()
	// Samples may have gone in out of order down to the window's start.
	if window := e.oooWindow.Load(); headMaxt-window < minOOOTime {
		minOOOTime = headMaxt - window
	}
	home.Lock()
	t.minOOOTime = minOOOTime
	home.Unlock()
	if len(deleted) > 0 {
		e.callback.PostDeletion(deleted)
	}
}

// freezeUnlockedHook runs while a freeze writes its block, for tests.
var freezeUnlockedHook func()

// headGCShardHook runs as the head's garbage collection locks each shard, for tests.
var headGCShardHook func(shard int)

// freezeLeaving moves the tenant's series leaving to a cold block, with the shard locked, and
// records the ones that left in deleted. The shard is unlocked while the block is written, so
// queries and appends don't wait on it; the compaction lock keeps other freezes out.
func (e *Engine) freezeLeaving(shard *shardState, t *tenant, leaving map[uint64]labels.Labels, deleted map[tsdbchunks.HeadSeriesRef]promlabels.Labels, builder *promlabels.ScratchBuilder) {
	if len(leaving) == 0 {
		return
	}
	pending := prepareFreeze(shard)
	if pending == nil {
		return
	}
	shard.Unlock()
	if freezeUnlockedHook != nil {
		freezeUnlockedHook()
	}
	block := pending.build(shard.cold.directory)
	shard.Lock()
	installFreeze(shard, pending, block)
	for ref, stored := range leaving {
		if _, still := e.lookupLocked(t, ref); still {
			continue
		}
		t.byRef.delete(ref)
		deleted[tsdbchunks.HeadSeriesRef(ref)] = toPromLabels(stored, builder)
	}
	reindex(t)
}

// CompactSelectedSeries moves the series refs out of the head without moving its min time, like
// Mimir's CompactSelectedSeries: their data stays queryable in cold blocks. Series with
// out-of-order data in the out-of-order head stay: its compaction comes first.
func (e *Engine) CompactSelectedSeries(refs []storage.SeriesRef) error {
	if len(refs) == 0 {
		return nil
	}
	e.compactionLock.Lock()
	defer e.compactionLock.Unlock()
	minTime, maxt, _ := e.headTimes()
	if minTime > maxt {
		return nil
	}
	selected := make(map[uint64]struct{}, len(refs))
	for _, ref := range refs {
		selected[uint64(ref)] = struct{}{}
	}
	// Like Prometheus's, one block per block range of the head, of the series that can leave.
	for index, shard := range e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			for ref := range selected {
				if refShard(ref) != index {
					continue
				}
				entry, ok := e.lookupLocked(t, ref)
				if !ok || !e.canLeave(index, &entry.series, maxt) {
					delete(selected, ref)
				}
			}
		}
		shard.RUnlock()
	}
	if len(selected) == 0 {
		return nil
	}
	for blockMin := rangeStart(minTime); blockMin <= maxt; blockMin += chunkRangeMs {
		blockMax := blockMin + chunkRangeMs
		e.writeBlock(blockMin, blockMax, false, func(_ int, series *Series) bool {
			_, ok := selected[series.ref]
			return ok && inOrderIn(series, blockMin, blockMax-1, minTime)
		})
	}
	deleted := map[tsdbchunks.HeadSeriesRef]promlabels.Labels{}
	var builder promlabels.ScratchBuilder
	for index, shard := range e.store.shards {
		shard.Lock()
		t, ok := shard.tenants[e.tenantID]
		if !ok {
			shard.Unlock()
			continue
		}
		leaving := map[uint64]labels.Labels{}
		for ref := range selected {
			if refShard(ref) != index {
				continue
			}
			entry, ok := e.lookupLocked(t, ref)
			if !ok {
				continue
			}
			series := &entry.series
			if !e.canLeave(index, series, maxt) {
				continue
			}
			if err := splitChunks(series, shard.disk, func(meta ChunkMeta) []int64 {
				if !meta.OutOfOrder {
					return alignedCuts(meta)
				}
				return nil
			}); err != nil {
				return err
			}
			series.headEvicted = true
			e.store.evictEpoch.Add(1)
			series.inHead = false
			leaving[ref] = entry.labels
		}
		if len(leaving) > 0 {
			// Only the selected series leave.
			t.series.forEach(func(entry *seriesEntry) {
				if _, ok := leaving[entry.series.ref]; !ok {
					entry.series.inHead = true
				}
			})
		}
		e.freezeLeaving(shard, t, leaving, deleted, &builder)
		shard.Unlock()
	}
	if len(deleted) > 0 {
		e.callback.PostDeletion(deleted)
	}
	return nil
}

// canLeave is whether a selected series leaves the head, like Prometheus's: not with samples
// newer than the head's max time when the compaction started, nor with data in the out-of-order
// head, which would have no block.
func (e *Engine) canLeave(shard int, series *Series, maxt int64) bool {
	if newest, ok := series.maxTime(); ok && newest > maxt {
		return false
	}
	ooo, _ := e.oooState(shard, series)
	return !ooo
}

// prune drops data older than cutoff, like the TSDB's block retention.
func (e *Engine) prune(cutoff int64) error {
	for {
		current := e.store.prunedBefore.Load()
		if cutoff <= current || e.store.prunedBefore.CompareAndSwap(current, cutoff) {
			break
		}
	}
	deleted := map[tsdbchunks.HeadSeriesRef]promlabels.Labels{}
	var builder promlabels.ScratchBuilder
	var errs []error
	for _, shard := range e.store.shards {
		shard.Lock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.retain(func(entry *seriesEntry) bool {
				keep := pruneSeries(&entry.series, cutoff)
				if !keep && entry.series.ref != 0 {
					t.byRef.delete(entry.series.ref)
					deleted[tsdbchunks.HeadSeriesRef(entry.series.ref)] = toPromLabels(entry.labels, &builder)
				}
				return keep
			})
			reindex(t)
		}
		if shard == e.store.shards[0] {
			if t, ok := shard.tenants[e.tenantID]; ok {
				t.blocks = pruneBlocks(t.blocks, cutoff)
			}
		}
		shard.cold.pruneBefore(cutoff)
		errs = append(errs, shard.disk.TruncateBefore(cutoff))
		shard.Unlock()
	}
	if len(deleted) > 0 {
		e.callback.PostDeletion(deleted)
	}
	return errors.Join(errs...)
}

// refLocation is where a series ref is: its name group and its position there, which every
// removal from the group records again.
type refLocation struct {
	group uint32
	index int32
}

// lookupLocked finds the series ref, with its shard locked.
// reindex records every series' position after removals moved them, with the shard locked.
func reindex(t *tenant) {
	for groupID := range t.series.groups {
		entries := t.series.groups[groupID].entries
		for position := range entries {
			if ref := entries[position].series.ref; ref != 0 {
				t.byRef.set(ref, refLocation{uint32(groupID), int32(position)})
			}
		}
	}
}

func (e *Engine) lookupLocked(t *tenant, ref uint64) (*seriesEntry, bool) {
	location, ok := t.byRef.get(ref)
	if !ok {
		return nil, false
	}
	return e.locate(t, ref, location)
}

// locate finds the series ref at location, with its shard locked. References are never reused,
// so the entry at the recorded position is the series if it has its reference.
func (e *Engine) locate(t *tenant, ref uint64, location refLocation) (*seriesEntry, bool) {
	entries := t.series.groups[location.group].entries
	if index := int(location.index); index < len(entries) && entries[index].series.ref == ref {
		return &entries[index], true
	}
	// Every removal records the positions again, so this only keeps a missed one correct.
	for index := range entries {
		if entries[index].series.ref == ref {
			return &entries[index], true
		}
	}
	return nil, false
}

// toPromLabels converts stored labels to Prometheus labels, which own their data.
func toPromLabels(stored labels.Labels, builder *promlabels.ScratchBuilder) promlabels.Labels {
	builder.Reset()
	stored.Range(func(name, value string) { builder.Add(name, value) })
	return builder.Labels()
}

// tenantExemplars returns the tenant's exemplar storage, with the exemplars lock held.
func (e *Engine) tenantExemplars() *exemplars.TenantExemplars[labels.Labels] {
	capacity := int(max(e.maxExemplars.Load(), 0))
	storage, ok := e.store.exemplars[e.tenantID]
	if !ok {
		storage = exemplars.New[labels.Labels](capacity)
		e.store.exemplars[e.tenantID] = storage
	}
	if storage.Capacity() != capacity {
		storage.Resize(capacity)
	}
	return storage
}

// FsyncWLSegments is the durability point before a Kafka offset commit: the engine has no log, so
// what a crash loses comes back from Kafka instead.
func (e *Engine) FsyncWLSegments() error {
	return nil
}
