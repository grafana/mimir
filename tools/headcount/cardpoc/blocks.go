// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// Block is one block's series, grouped by __name__, plus enough metadata
// to tell which blocks a merge should skip or dedup against each other.
type Block struct {
	ID               ulid.ULID
	Level            int
	Sources          []ulid.ULID // meta.json's Compaction.Sources
	MinTime, MaxTime int64

	// SeriesByName maps each __name__ value present in the block to the
	// stable hash (labels.StableHash) of every one of its series. Grouping
	// by name up front is what E6 and E9 need; summing every group's
	// length gives the whole-block series count E2-E4 need.
	SeriesByName map[string][]uint64
}

// TotalSeries returns the block's own series count: the sum of every
// name's series, with no cross-block deduplication.
func (b Block) TotalSeries() int {
	n := 0
	for _, hashes := range b.SeriesByName {
		n += len(hashes)
	}
	return n
}

// LoadBlocks reads every block under bucketDir/anonymous.
func LoadBlocks(bucketDir string) ([]Block, error) {
	anonymousDir := filepath.Join(bucketDir, "anonymous")
	entries, err := os.ReadDir(anonymousDir)
	if err != nil {
		return nil, err
	}

	var blocks []Block
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(anonymousDir, e.Name())
		meta, err := block.ReadMetaFromDir(dir)
		if err != nil {
			continue // not a block directory (or mid-write/mid-deletion).
		}

		b, err := loadBlock(dir, meta)
		if err != nil {
			return nil, fmt.Errorf("block %s: %w", e.Name(), err)
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func loadBlock(dir string, meta *block.Meta) (Block, error) {
	r, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return Block{}, err
	}
	defer r.Close()

	ctx := context.Background()
	p, err := r.Postings(ctx, "", "")
	if err != nil {
		return Block{}, err
	}

	byName := map[string][]uint64{}
	var b labels.ScratchBuilder
	for p.Next() {
		if err := r.Series(p.At(), &b, nil); err != nil {
			return Block{}, err
		}
		lset := b.Labels()
		name := lset.Get("__name__")
		byName[name] = append(byName[name], labels.StableHash(lset))
	}
	if err := p.Err(); err != nil {
		return Block{}, err
	}

	return Block{
		ID:           meta.ULID,
		Level:        meta.Compaction.Level,
		Sources:      meta.Compaction.Sources,
		MinTime:      meta.MinTime,
		MaxTime:      meta.MaxTime,
		SeriesByName: byName,
	}, nil
}
