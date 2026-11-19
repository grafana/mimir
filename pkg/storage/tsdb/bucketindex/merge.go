// SPDX-License-Identifier: AGPL-3.0-only

package bucketindex

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// ReadMergedIndex reads the bucket index of each bucket of a *bucket.BlockLocationBucket and merges them with
// MergeIndexes. The returned locations map each block to the index of the bucket that holds it. It returns
// ErrIndexNotFound if no bucket has a bucket index. If bkt isn't a *bucket.BlockLocationBucket, it is the same
// as ReadIndex and the returned locations are nil.
func ReadMergedIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, logger log.Logger) (*Index, map[ulid.ULID]int, error) {
	lb, ok := bkt.(*bucket.BlockLocationBucket)
	if !ok {
		idx, err := ReadIndex(ctx, bkt, userID, cfgProvider, logger)
		return idx, nil, err
	}

	indexes := make([]*Index, len(lb.Buckets()))
	found := false
	for i, b := range lb.Buckets() {
		idx, err := ReadIndex(ctx, b, userID, cfgProvider, logger)
		if errors.Is(err, ErrIndexNotFound) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("bucket %s: %w", lb.BucketNames()[i], err)
		}
		indexes[i] = idx
		found = true
	}
	if !found {
		return nil, nil, ErrIndexNotFound
	}

	idx, locations := MergeIndexes(indexes)
	return idx, locations, nil
}

// MergeIndexes merges the bucket indexes of several buckets, in bucket order. Nil indexes are skipped.
// A block that is in several buckets is taken from the first one, which owns it. Only the deletion marks
// of the owner bucket are kept, so a copy of a block in another bucket doesn't hide the block. The merged
// index has the oldest version and update time of the merged indexes. The returned locations map each block
// to the index of its owner bucket.
func MergeIndexes(indexes []*Index) (*Index, map[ulid.ULID]int) {
	merged := &Index{}
	locations := map[ulid.ULID]int{}
	first := true
	for i, idx := range indexes {
		if idx == nil {
			continue
		}
		if first || idx.Version < merged.Version {
			merged.Version = idx.Version
		}
		if first || idx.UpdatedAt < merged.UpdatedAt {
			merged.UpdatedAt = idx.UpdatedAt
		}
		first = false

		for _, b := range idx.Blocks {
			if _, ok := locations[b.ID]; !ok {
				locations[b.ID] = i
				merged.Blocks = append(merged.Blocks, b)
			}
		}
	}

	seenMarks := map[ulid.ULID]bool{}
	for i, idx := range indexes {
		if idx == nil {
			continue
		}
		for _, m := range idx.BlockDeletionMarks {
			if owner, ok := locations[m.ID]; (ok && owner != i) || seenMarks[m.ID] {
				continue
			}
			seenMarks[m.ID] = true
			merged.BlockDeletionMarks = append(merged.BlockDeletionMarks, m)
		}
	}

	return merged, locations
}
