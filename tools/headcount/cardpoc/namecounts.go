// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// NameCounts is every metric name's series count over one block time
// range, read from index-header postings spans only.
type NameCounts struct {
	MinT, MaxT int64
	Blocks     []ulid.ULID
	Counts     map[string]int
	// IndexHeaderBytes is the on-disk size of the index-header and
	// sparse-index-header files of the blocks read: an upper bound on the
	// bytes touched, none of them from object storage.
	IndexHeaderBytes int64
	Elapsed          time.Duration
}

// Total returns the sum of every name's count.
func (nc NameCounts) Total() int {
	n := 0
	for _, c := range nc.Counts {
		n += c
	}
	return n
}

// BlockRange is one [MinT, MaxT) time range shared by one or more blocks.
type BlockRange struct{ MinT, MaxT int64 }

// BlockRanges returns the distinct time ranges of the blocks under
// bucketDir/anonymous, earliest first.
func BlockRanges(bucketDir string) ([]BlockRange, error) {
	metas, err := readMetas(bucketDir)
	if err != nil {
		return nil, err
	}
	seen := map[BlockRange]bool{}
	var out []BlockRange
	for _, m := range metas {
		r := BlockRange{m.MinTime, m.MaxTime}
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MinT < out[j].MinT })
	return out, nil
}

// NameCountsForRange sums each metric name's postings-span count over the
// blocks under bucketDir whose time range is exactly r. The sum is exact
// only when those blocks are series-disjoint, so it requires them to be
// split-compactor shards of the same shard count with distinct shard IDs,
// or a single block, and returns an error otherwise.
func NameCountsForRange(bucketDir string, r BlockRange) (NameCounts, error) {
	metas, err := readMetas(bucketDir)
	if err != nil {
		return NameCounts{}, err
	}
	var selected []*block.Meta
	for _, m := range metas {
		if m.MinTime == r.MinT && m.MaxTime == r.MaxT {
			selected = append(selected, m)
		}
	}
	if len(selected) == 0 {
		return NameCounts{}, fmt.Errorf("no block covers exactly [%d, %d)", r.MinT, r.MaxT)
	}
	if err := checkDisjointShards(selected); err != nil {
		return NameCounts{}, err
	}

	anonymousDir := filepath.Join(bucketDir, "anonymous")
	ubkt, err := filesystem.NewBucket(anonymousDir)
	if err != nil {
		return NameCounts{}, err
	}
	defer ubkt.Close()
	bkt := objstore.WithNoopInstr(ubkt)

	nc := NameCounts{MinT: r.MinT, MaxT: r.MaxT, Counts: map[string]int{}}
	start := time.Now()
	for _, m := range selected {
		if err := addBlockNameCounts(bkt, anonymousDir, m.ULID, nc.Counts); err != nil {
			return NameCounts{}, fmt.Errorf("block %s: %w", m.ULID, err)
		}
		nc.Blocks = append(nc.Blocks, m.ULID)
	}
	nc.Elapsed = time.Since(start)

	for _, id := range nc.Blocks {
		for _, f := range []string{"index-header", "sparse-index-header"} {
			if st, err := os.Stat(filepath.Join(anonymousDir, id.String(), f)); err == nil {
				nc.IndexHeaderBytes += st.Size()
			}
		}
	}
	return nc, nil
}

func addBlockNameCounts(bkt objstore.InstrumentedBucketReader, anonymousDir string, id ulid.ULID, counts map[string]int) error {
	ctx := context.Background()
	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)
	hr, err := indexheader.NewStreamBinaryReader(ctx, id, bkt, anonymousDir, indexheader.Config{}, 32, gokitlog.NewNopLogger(), metrics)
	if err != nil {
		return fmt.Errorf("opening index-header: %w", err)
	}
	defer hr.Close()

	offsets, err := hr.LabelValuesOffsets(ctx, "__name__", "", nil)
	if err != nil {
		return err
	}
	for _, off := range offsets {
		counts[off.LabelValue] += int(spanCount(off.Off.Start, off.Off.End))
	}
	return nil
}

// checkDisjointShards returns nil if metas is a single block, or a set of
// split-compactor shards with one shard count and no repeated shard ID.
func checkDisjointShards(metas []*block.Meta) error {
	if len(metas) == 1 {
		return nil
	}
	seen := map[string]bool{}
	var count string
	for _, m := range metas {
		id := m.Thanos.Labels[block.CompactorShardIDExternalLabel]
		if id == "" {
			return fmt.Errorf("block %s has no shard ID, so %d blocks for one range may share series", m.ULID, len(metas))
		}
		index, of, err := parseShardID(id)
		if err != nil {
			return fmt.Errorf("block %s: %w", m.ULID, err)
		}
		if count == "" {
			count = of
		} else if of != count {
			return fmt.Errorf("blocks for one range use shard counts %s and %s", count, of)
		}
		if seen[index] {
			return fmt.Errorf("shard %s appears twice for one range", id)
		}
		seen[index] = true
	}
	return nil
}

// parseShardID splits a "<index>_of_<count>" shard ID.
func parseShardID(id string) (index, count string, err error) {
	var i, n int
	if _, err := fmt.Sscanf(id, "%d_of_%d", &i, &n); err != nil {
		return "", "", fmt.Errorf("shard ID %q: %w", id, err)
	}
	return fmt.Sprint(i), fmt.Sprint(n), nil
}

func readMetas(bucketDir string) ([]*block.Meta, error) {
	anonymousDir := filepath.Join(bucketDir, "anonymous")
	entries, err := os.ReadDir(anonymousDir)
	if err != nil {
		return nil, err
	}
	var metas []*block.Meta
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := block.ReadMetaFromDir(filepath.Join(anonymousDir, e.Name()))
		if err != nil {
			continue // not a block directory.
		}
		metas = append(metas, m)
	}
	return metas, nil
}
