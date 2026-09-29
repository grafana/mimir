// SPDX-License-Identifier: AGPL-3.0-only

// Package truth computes a distinct-series count directly from on-disk
// block content, independent of both cardgen's in-memory bookkeeping and
// model.Model.Truth's scan of the population. Agreement between all three
// is what makes the generated fixtures trustworthy: a bug in cardgen or in
// the model would otherwise go unnoticed by checking either against
// itself.
package truth

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// CountDistinctSeries opens every block under bucketDir/anonymous and
// returns the number of distinct series across all of them, deduplicated
// by a hash of their label set. A series present in more than one block
// (for example, live across two of cardgen's hourly level-1 blocks, or
// counted once per compacted shard) is counted once.
func CountDistinctSeries(bucketDir string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(bucketDir, "anonymous"))
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	seen := map[uint64]struct{}{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(bucketDir, "anonymous", e.Name())
		if _, err := block.ReadMetaFromDir(dir); err != nil {
			continue // not a block directory (or mid-write/mid-deletion).
		}
		if err := addBlockSeries(ctx, dir, seen); err != nil {
			return 0, fmt.Errorf("reading series from block %s: %w", e.Name(), err)
		}
	}
	return len(seen), nil
}

func addBlockSeries(ctx context.Context, blockDir string, seen map[uint64]struct{}) error {
	r, err := index.NewFileReader(filepath.Join(blockDir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return err
	}
	defer r.Close()

	p, err := r.Postings(ctx, "", "")
	if err != nil {
		return err
	}

	var b labels.ScratchBuilder
	for p.Next() {
		if err := r.Series(p.At(), &b, nil); err != nil {
			return err
		}
		seen[labels.StableHash(b.Labels())] = struct{}{}
	}
	return p.Err()
}
