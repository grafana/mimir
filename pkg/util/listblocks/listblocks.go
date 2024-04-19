// SPDX-License-Identifier: AGPL-3.0-only

package listblocks

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// LoadMetaFilesAndMarkers reads the bucket and loads the meta files for the provided user.
// No-compact marker files are also read and returned all the time.
// If showDeleted is true, then deletion marker files are also read and returned.
// If ulidMinTime is non-zero, then only blocks with ULID time higher than that are read,
// this is useful to filter the results for users with high amount of blocks without reading the metas
// (but it can be inexact since ULID time can differ from block min/max times range).
func LoadMetaFilesAndMarkers(ctx context.Context, bkt objstore.BucketReader, user string, showDeleted bool, ulidMinTime time.Time) (metas map[ulid.ULID]*block.Meta, deletionDetails map[ulid.ULID]block.DeletionMark, noCompactDetails map[ulid.ULID]block.NoCompactMark, _ error) {
	deletedBlocks := map[ulid.ULID]bool{}
	noCompactMarkerFiles := []string(nil)
	deletionMarkerFiles := []string(nil)

	// Find blocks marked for deletion and no-compact.
	err := bkt.Iter(ctx, path.Join(user, block.MarkersPathname), func(s string) error {
		if id, ok := block.IsDeletionMarkFilename(path.Base(s)); ok {
			deletedBlocks[id] = true
			deletionMarkerFiles = append(deletionMarkerFiles, s)
		}
		if _, ok := block.IsNoCompactMarkFilename(path.Base(s)); ok {
			noCompactMarkerFiles = append(noCompactMarkerFiles, s)
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	metaPaths := []string(nil)
	err = bkt.Iter(ctx, user, func(s string) error {
		if id, ok := block.IsBlockDir(s); ok {
			if !showDeleted && deletedBlocks[id] {
				return nil
			}

			// Block's ULID is typically higher than min/max time of the block,
			// unless somebody was ingesting data with timestamps in the future.
			if !ulidMinTime.IsZero() && ulid.Time(id.Time()).Before(ulidMinTime) {
				return nil
			}

			metaPaths = append(metaPaths, path.Join(s, "meta.json"))
		}
		return nil
	})

	if err != nil {
		return nil, nil, nil, err
	}

	if showDeleted {
		deletionDetails, err = fetchMarkerDetails[block.DeletionMark](ctx, bkt, deletionMarkerFiles)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	noCompactDetails, err = fetchMarkerDetails[block.NoCompactMark](ctx, bkt, noCompactMarkerFiles)
	if err != nil {
		return nil, nil, nil, err
	}
	metas, err = fetchMetas(ctx, bkt, metaPaths)
	return metas, deletionDetails, noCompactDetails, err
}

const concurrencyLimit = 32

func fetchMarkerDetails[MARKER_TYPE block.Marker](ctx context.Context, bkt objstore.BucketReader, markers []string) (map[ulid.ULID]MARKER_TYPE, error) {
	mu := sync.Mutex{}
	details := map[ulid.ULID]MARKER_TYPE{}

	return details, concurrency.ForEachJob(ctx, len(markers), concurrencyLimit, func(ctx context.Context, idx int) error {
		r, err := bkt.Get(ctx, markers[idx])
		if err != nil {
			if bkt.IsObjNotFoundErr(err) {
				return nil
			}

			return err
		}
		defer r.Close()

		dec := json.NewDecoder(r)

		var m MARKER_TYPE
		if err := dec.Decode(&m); err != nil {
			return err
		}

		mu.Lock()
		details[m.BlockULID()] = m
		mu.Unlock()
		return nil
	})
}

func fetchMetas(ctx context.Context, bkt objstore.BucketReader, metaFiles []string) (map[ulid.ULID]*block.Meta, error) {
	mu := sync.Mutex{}
	metas := map[ulid.ULID]*block.Meta{}

	return metas, concurrency.ForEachJob(ctx, len(metaFiles), concurrencyLimit, func(ctx context.Context, idx int) error {
		r, err := bkt.Get(ctx, metaFiles[idx])
		if err != nil {
			if bkt.IsObjNotFoundErr(err) {
				return nil
			}

			return err
		}
		defer r.Close()

		m, err := block.ReadMeta(r)
		if err != nil {
			return err
		}

		mu.Lock()
		metas[m.ULID] = m
		mu.Unlock()

		return nil
	})
}

func SortBlocks(metas map[ulid.ULID]*block.Meta) []*block.Meta {
	var blocks []*block.Meta

	for _, b := range metas {
		blocks = append(blocks, b)
	}

	sort.Slice(blocks, func(i, j int) bool {
		// By min-time
		if blocks[i].MinTime != blocks[j].MinTime {
			return blocks[i].MinTime < blocks[j].MinTime
		}

		// Duration
		duri := blocks[i].MaxTime - blocks[i].MinTime
		durj := blocks[j].MaxTime - blocks[j].MinTime
		if duri != durj {
			return duri < durj
		}

		// Compactor shard
		shardi := blocks[i].Thanos.Labels[tsdb.CompactorShardIDExternalLabel]
		shardj := blocks[j].Thanos.Labels[tsdb.CompactorShardIDExternalLabel]

		if shardi != "" && shardj != "" && shardi != shardj {
			shardiIndex, shardiCount, erri := sharding.ParseShardIDLabelValue(shardi)
			shardjIndex, shardjCount, errj := sharding.ParseShardIDLabelValue(shardj)
			if erri != nil || errj != nil {
				// Ff failed parsing any of labels, fallback to lexicographical sort.
				return shardi < shardj
			} else if shardiCount != shardjCount {
				// If parsed but shard count differs, first sort by shard count.
				return shardiCount < shardjCount
			}

			// Otherwise, sort by shard count, this should be the happy path when there are sharded blocks.
			return shardiIndex < shardjIndex
		}

		// ULID time.
		return blocks[i].ULID.Time() < blocks[j].ULID.Time()
	})
	return blocks
}

func GetFormattedBlockSize(b *block.Meta) string {
	if len(b.Thanos.Files) == 0 {
		return ""
	}

	size := GetBlockSizeBytes(b)

	return humanize.IBytes(size)
}

func GetBlockSizeBytes(b *block.Meta) uint64 {
	size := uint64(0)
	for _, f := range b.Thanos.Files {
		size += uint64(f.SizeBytes)
	}
	return size
}
