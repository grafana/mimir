// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_finder_bucket_index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

var (
	errBucketIndexBlocksFinderNotRunning = errors.New("bucket index blocks finder is not running")
	errInvalidBlocksRange                = errors.New("invalid blocks time range")
)

type BucketIndexBlocksFinderConfig struct {
	IndexLoader              bucketindex.LoaderConfig
	MaxStalePeriod           time.Duration
	IgnoreDeletionMarksDelay time.Duration
}

// BucketIndexBlocksFinder implements BlocksFinder interface and find blocks in the bucket
// looking up the bucket index.
type BucketIndexBlocksFinder struct {
	services.Service

	cfg    BucketIndexBlocksFinderConfig
	loader *bucketindex.Loader
	logger log.Logger
}

func NewBucketIndexBlocksFinder(cfg BucketIndexBlocksFinderConfig, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) *BucketIndexBlocksFinder {
	loader := bucketindex.NewLoader(cfg.IndexLoader, bkt, cfgProvider, logger, reg)

	return &BucketIndexBlocksFinder{
		cfg:     cfg,
		loader:  loader,
		Service: loader,
		logger:  logger,
	}
}

// GetBlocks implements BlocksFinder.
func (f *BucketIndexBlocksFinder) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, error) {
	if f.State() != services.Running {
		return nil, errBucketIndexBlocksFinderNotRunning
	}
	if maxT < minT {
		return nil, errInvalidBlocksRange
	}

	// Get the bucket index for this user.
	idx, err := f.loader.GetIndex(ctx, userID)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit edge case, happening when a new tenant has not shipped blocks to the storage yet
		// so the bucket index hasn't been created yet.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Ensure the bucket index is not too old.
	if time.Since(idx.GetUpdatedAt()) > f.cfg.MaxStalePeriod {
		return nil, newBucketIndexTooOldError(idx.GetUpdatedAt(), f.cfg.MaxStalePeriod)
	}

	matchingBlocks := map[ulid.ULID]*bucketindex.Block{}

	// Filter blocks containing samples within the range.
	for _, block := range idx.Blocks {
		if !block.Within(minT, maxT) {
			continue
		}

		matchingBlocks[block.ID] = block
	}

	for _, mark := range idx.BlockDeletionMarks {
		// Exclude blocks marked for deletion. This is the same logic as Thanos IgnoreDeletionMarkFilter.
		if time.Since(time.Unix(mark.DeletionTime, 0)).Seconds() > f.cfg.IgnoreDeletionMarksDelay.Seconds() {
			delete(matchingBlocks, mark.ID)
			continue
		}
	}

	// Convert matching blocks into a list.
	blocks := make(bucketindex.Blocks, 0, len(matchingBlocks))
	for _, b := range matchingBlocks {
		blocks = append(blocks, b)
	}

	return blocks, nil
}

func newBucketIndexTooOldError(updatedAt time.Time, maxStalePeriod time.Duration) error {
	return errors.New(globalerror.BucketIndexTooOld.Message(fmt.Sprintf("the bucket index is too old. It was last updated at %s, which exceeds the maximum allowed staleness period of %v", updatedAt.UTC().Format(time.RFC3339Nano), maxStalePeriod)))
}
