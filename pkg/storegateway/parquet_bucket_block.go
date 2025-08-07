// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/grafana/dskit/multierror"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type ParquetShardReaderCloser interface {
	storage.ParquetShard
	io.Closer
}

// ParquetShardReader implements the storage.ParquetShard and io.Closer interfaces
// in order to control custom lifecycle logic for compatibility with lazy & pooled readers.
// The block's pending readers counter is incremented when the reader is created and decremented on Close().
type parquetBucketShardReader struct {
	block *parquetBucketBlock
}

func (r *parquetBucketShardReader) LabelsFile() storage.ParquetFileView {
	return r.block.shardReaderCloser.LabelsFile()
}

func (r *parquetBucketShardReader) ChunksFile() storage.ParquetFileView {
	return r.block.shardReaderCloser.ChunksFile()
}

func (r *parquetBucketShardReader) TSDBSchema() (*schema.TSDBSchema, error) {
	return r.block.shardReaderCloser.TSDBSchema()
}

func (r *parquetBucketShardReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// parquetBucketBlock wraps access to the block's storage.ParquetShard interface
// with metadata, metrics and caching [etc once we fill in capabilities].
type parquetBucketBlock struct {
	meta              *block.Meta
	blockLabels       labels.Labels
	shardReaderCloser ParquetShardReaderCloser
	localDir          string

	pendingReaders sync.WaitGroup
	closedMtx      sync.RWMutex
	closed         bool

	// Indicates whether the block was queried.
	queried atomic.Bool
}

func newParquetBucketBlock(
	meta *block.Meta,
	shardReaderCloser ParquetShardReaderCloser,
	localDir string,
) *parquetBucketBlock {
	return &parquetBucketBlock{
		meta: meta,
		// Inject the block ID as a label to allow to match blocks by ID.
		blockLabels:       labels.FromStrings(block.BlockIDLabel, meta.ULID.String()),
		shardReaderCloser: shardReaderCloser,
		localDir:          localDir,
	}
}

func (b *parquetBucketBlock) ShardReader() ParquetShardReaderCloser {
	b.pendingReaders.Add(1)
	return &parquetBucketShardReader{block: b}
}

// matchLabels verifies whether the block matches the given matchers.
func (b *parquetBucketBlock) matchLabels(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(b.blockLabels.Get(m.Name)) {
			return false
		}
	}
	return true
}

// overlapsClosedInterval returns true if the block overlaps [mint, maxt).
func (b *parquetBucketBlock) overlapsClosedInterval(mint, maxt int64) bool {
	// The block itself is a half-open interval
	// [b.meta.MinTime, b.meta.MaxTime).
	return b.meta.MinTime <= maxt && mint < b.meta.MaxTime
}

func (b *parquetBucketBlock) MarkQueried() {
	b.queried.Store(true)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *parquetBucketBlock) Close() error {
	b.closedMtx.Lock()
	b.closed = true
	b.closedMtx.Unlock()

	b.pendingReaders.Wait()

	return b.shardReaderCloser.Close()
}

// parquetBlockSet holds all blocks.
type parquetBlockSet struct {
	mtx          sync.RWMutex          // nolint:unused
	blockSet     sync.Map              // nolint:unused                // Maps block's ulid.ULID to the *parquetBucketBlock.
	sortedBlocks []*parquetBucketBlock // nolint:unused // Blocks sorted by mint, then maxt.
}

// add adds a block to the set.
func (s *parquetBlockSet) add(b *parquetBucketBlock) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// The LoadOrStore verifies the block with the same id never ended up in the set more than once.
	_, ok := s.blockSet.LoadOrStore(b.meta.ULID, b)
	if ok {
		// This should not ever happen.
		return fmt.Errorf("block %s already exists in the set", b.meta.ULID)
	}

	s.sortedBlocks = append(s.sortedBlocks, b)

	// Always sort blocks by min time, then max time.
	sort.Slice(s.sortedBlocks, func(j, k int) bool {
		if s.sortedBlocks[j].meta.MinTime == s.sortedBlocks[k].meta.MinTime {
			return s.sortedBlocks[j].meta.MaxTime < s.sortedBlocks[k].meta.MaxTime
		}
		return s.sortedBlocks[j].meta.MinTime < s.sortedBlocks[k].meta.MinTime
	})
	return nil
}

// remove removes the block identified by id from the set. It returns the removed block if it was present in the set.
func (s *parquetBlockSet) remove(id ulid.ULID) *parquetBucketBlock {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	val, ok := s.blockSet.LoadAndDelete(id)
	if !ok {
		return nil
	}

	for i, b := range s.sortedBlocks {
		if b.meta.ULID != id {
			continue
		}
		s.sortedBlocks = append(s.sortedBlocks[:i], s.sortedBlocks[i+1:]...)
		break
	}

	return val.(*parquetBucketBlock)
}

func (s *parquetBlockSet) contains(id ulid.ULID) bool {
	_, ok := s.blockSet.Load(id)
	return ok
}

func (s *parquetBlockSet) len() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.sortedBlocks)
}

// filter iterates over a time-ordered list of non-closed blocks that cover date between mint and maxt. It supports overlapping
// blocks. It only guaranties that a block is held open during the execution of fn.
func (s *parquetBlockSet) filter(mint, maxt int64, blockMatchers []*labels.Matcher, fn func(b *parquetBucketBlock)) {
	if mint > maxt {
		return
	}

	s.mtx.RLock()

	// Fill the given interval with the blocks within the request mint and maxt.
	bs := make([]*parquetBucketBlock, 0, len(s.sortedBlocks))
	for _, b := range s.sortedBlocks {
		// NOTE: s.sortedBlocks are expected to be sorted in minTime order, their intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		if b.overlapsClosedInterval(mint, maxt) {
			// Include the block in the list of matching ones only if there are no block-level matchers
			// or they actually match.
			if len(blockMatchers) == 0 || b.matchLabels(blockMatchers) {
				bs = append(bs, b)
			}
		}
	}

	s.mtx.RUnlock()

	step := func(b *parquetBucketBlock) {
		b.closedMtx.RLock()
		defer b.closedMtx.RUnlock()
		if !b.closed {
			fn(b)
		}
	}

	for _, b := range bs {
		step(b)
	}
}

// forEach iterates over all non-closed blocks in the set. It only guaranties that a block is held open during the execution of fn.
func (s *parquetBlockSet) forEach(fn func(b *parquetBucketBlock)) {
	s.blockSet.Range(func(_, val any) bool {
		b := val.(*parquetBucketBlock)

		b.closedMtx.RLock()
		defer b.closedMtx.RUnlock()

		if !b.closed {
			fn(b)
		}
		return true
	})
}

// closeAll closes all blocks in the set and returns all encountered errors after trying all blocks.
func (s *parquetBlockSet) closeAll() error {
	errs := multierror.New()
	s.blockSet.Range(func(_, val any) bool {
		errs.Add(val.(*parquetBucketBlock).Close())
		return true
	})
	return errs.Err()
}

// openBlocksULIDs returns the ULIDs of all blocks in the set which are not closed.
func (s *parquetBlockSet) openBlocksULIDs() []ulid.ULID {
	ulids := make([]ulid.ULID, 0, s.len())
	s.forEach(func(b *parquetBucketBlock) {
		ulids = append(ulids, b.meta.ULID)
	})
	return ulids
}

// allBlockULIDs returns the ULIDs of all blocks in the set regardless whether they are closed or not.
func (s *parquetBlockSet) allBlockULIDs() []ulid.ULID {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	ulids := make([]ulid.ULID, 0, len(s.sortedBlocks))
	for _, b := range s.sortedBlocks {
		ulids = append(ulids, b.meta.ULID)
	}
	return ulids

}

// timerange returns the minimum and maximum timestamp available in the set.
func (s *parquetBlockSet) timerange() (mint, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if len(s.sortedBlocks) == 0 {
		return math.MaxInt64, math.MinInt64
	}

	mint = math.MaxInt64
	maxt = math.MinInt64

	// NOTE: s.sortedBlocks are expected to be sorted in minTime order.
	for _, b := range s.sortedBlocks {
		if b.meta.MinTime < mint {
			mint = b.meta.MinTime
		}
		if b.meta.MaxTime > maxt {
			maxt = b.meta.MaxTime
		}
	}

	return mint, maxt
}
