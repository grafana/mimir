// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"sync"

	"github.com/grafana/dskit/multierror"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/storage"
	"go.uber.org/atomic"
)

// parquetBlockSet holds all blocks.
type parquetBlockSet struct {
	mtx          sync.RWMutex          // nolint:unused
	blockSet     sync.Map              // nolint:unused                // Maps block's ulid.ULID to the *bucketBlock.
	sortedBlocks []*parquetBucketBlock // nolint:unused // Blocks sorted by mint, then maxt.
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

// parquetBucketBlock wraps access to the block's storage.ParquetShard interface
// with metadata, metrics and caching [etc once we fill in capabilities].
type parquetBucketBlock struct {
	BlockID ulid.ULID

	storage.ParquetShard

	pendingReaders sync.WaitGroup
	closedMtx      sync.RWMutex
	closed         bool

	// Indicates whether the block was queried.
	queried atomic.Bool
}

func newParquetBlockWithMeta(
	blockID ulid.ULID,
	shard storage.ParquetShard,
) *parquetBucketBlock {
	return &parquetBucketBlock{
		ParquetShard: shard,
		BlockID:      blockID,
	}
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

	return nil // TODO manage reader opening and closing through pools like indexheader does.
}
