// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

// multiBucketBlocksFinder finds the blocks of a tenant in more than one bucket. It has one BlocksFinder
// for each bucket, in bucket read order.
type multiBucketBlocksFinder struct {
	services.Service

	finders            []BlocksFinder
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func newMultiBucketBlocksFinder(finders []BlocksFinder) (*multiBucketBlocksFinder, error) {
	subservices := make([]services.Service, 0, len(finders))
	for _, finder := range finders {
		subservices = append(subservices, finder)
	}

	manager, err := services.NewManager(subservices...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the blocks finders service manager")
	}

	f := &multiBucketBlocksFinder{
		finders:            finders,
		subservices:        manager,
		subservicesWatcher: services.NewFailureWatcher(),
	}
	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)
	return f, nil
}

func (f *multiBucketBlocksFinder) starting(ctx context.Context) error {
	f.subservicesWatcher.WatchManager(f.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, f.subservices); err != nil {
		return errors.Wrap(err, "unable to start the blocks finders")
	}
	return nil
}

func (f *multiBucketBlocksFinder) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-f.subservicesWatcher.Chan():
		return errors.Wrap(err, "blocks finder failed")
	}
}

func (f *multiBucketBlocksFinder) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), f.subservices)
}

// GetBlocks implements BlocksFinder. Each finder applies its own bucket index staleness check and
// deletion marks. GetBlocks returns the union of the blocks that the finders find. If more than one
// finder finds the same block, the block of the first finder is returned. The returned metadata is the
// metadata of the bucket index that has the oldest update time. GetBlocks returns an error if one of the
// finders returns an error.
func (f *multiBucketBlocksFinder) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, *bucketindex.Metadata, error) {
	blocksPerFinder := make([]bucketindex.Blocks, len(f.finders))
	metadataPerFinder := make([]*bucketindex.Metadata, len(f.finders))

	err := concurrency.ForEachJob(ctx, len(f.finders), len(f.finders), func(ctx context.Context, idx int) error {
		blocks, metadata, err := f.finders[idx].GetBlocks(ctx, userID, minT, maxT)
		if err != nil {
			return err
		}
		blocksPerFinder[idx] = blocks
		metadataPerFinder[idx] = metadata
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		mergedBlocks   bucketindex.Blocks
		oldestMetadata *bucketindex.Metadata
		foundBlockIDs  = map[ulid.ULID]struct{}{}
	)
	for idx := range f.finders {
		if metadata := metadataPerFinder[idx]; metadata != nil && (oldestMetadata == nil || metadata.UpdatedAt < oldestMetadata.UpdatedAt) {
			oldestMetadata = metadata
		}

		for _, block := range blocksPerFinder[idx] {
			if _, found := foundBlockIDs[block.ID]; found {
				continue
			}
			foundBlockIDs[block.ID] = struct{}{}
			mergedBlocks = append(mergedBlocks, block)
		}
	}

	return mergedBlocks, oldestMetadata, nil
}
