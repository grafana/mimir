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
	mtx          sync.RWMutex            // nolint:unused
	blockSet     sync.Map                // nolint:unused                // Maps block's ulid.ULID to the *bucketBlock.
	sortedBlocks []*parquetBlockWithMeta // nolint:unused // Blocks sorted by mint, then maxt.
}

// closeAll closes all blocks in the set and returns all encountered errors after trying all blocks.
func (s *parquetBlockSet) closeAll() error {
	errs := multierror.New()
	s.blockSet.Range(func(_, val any) bool {
		errs.Add(val.(*parquetBlockWithMeta).Close())
		return true
	})
	return errs.Err()
}

// parquetBlockWithMeta wraps storage.ParquetShardOpener with metadata adds metadata about the block.
type parquetBlockWithMeta struct {
	storage.ParquetShardOpener
	BlockID ulid.ULID

	pendingReaders sync.WaitGroup
	closedMtx      sync.RWMutex
	closed         bool

	// Indicates whether the block was queried.
	queried atomic.Bool
}

func (b *parquetBlockWithMeta) MarkQueried() {
	b.queried.Store(true)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *parquetBlockWithMeta) Close() error {
	b.closedMtx.Lock()
	b.closed = true
	b.closedMtx.Unlock()

	b.pendingReaders.Wait()

	return nil // TODO manage reader opening and closing through pools like indexheader does.
}
