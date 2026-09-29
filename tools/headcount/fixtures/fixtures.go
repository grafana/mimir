// SPDX-License-Identifier: AGPL-3.0-only

// Package fixtures builds the three bucket snapshots the rest of the POC
// runs experiments against, by driving a real Mimir compactor process
// against generated level-1 blocks:
//
//   - l1: cardgen's own output, before any compaction.
//   - handover: compacted with a long deletion delay, so the level-1
//     sources and the compactor's output coexist on disk, as they briefly
//     do in a real bucket between a compaction and the next cleanup.
//   - compacted: compacted with no deletion delay, so a cleanup pass has
//     already removed the sources.
package fixtures

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

// Config controls where fixtures are built and which compactor binary
// drives them.
type Config struct {
	// Dir holds the three snapshots (Dir/l1, Dir/handover, Dir/compacted)
	// and is otherwise free for scratch work directories.
	Dir string
	// MimirBinary is a path to a built `mimir` binary. Build lets a caller
	// supply one already built (for example, once per test run) instead of
	// paying a build cost on every call.
	MimirBinary string
}

// Result is the outcome of building fixtures for one profile.
type Result struct {
	L1, Handover, Compacted string // paths to each snapshot's bucket root
}

// Build generates pop's level-1 blocks and produces all three snapshots
// under cfg.Dir, using partitions for cardgen's partition assignment.
func Build(pop *model.Model, partitions int, seed int64, cfg Config) (Result, error) {
	if cfg.MimirBinary == "" {
		return Result{}, fmt.Errorf("cfg.MimirBinary is required")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return Result{}, err
	}

	l1 := filepath.Join(cfg.Dir, "l1")
	if err := os.MkdirAll(filepath.Join(l1, "anonymous"), 0o755); err != nil {
		return Result{}, err
	}
	if _, err := cardgen.Generate(pop, filepath.Join(l1, "anonymous"), cardgen.Config{Partitions: partitions, Seed: seed}); err != nil {
		return Result{}, fmt.Errorf("generating level-1 blocks: %w", err)
	}

	handover, err := compactSnapshot(cfg, l1, "handover", compactorOptions{
		Partitions:    partitions,
		DeletionDelay: "1h", // long enough that the cleaner never removes sources during the run.
		WantSources:   true,
	})
	if err != nil {
		return Result{}, fmt.Errorf("building handover snapshot: %w", err)
	}

	compacted, err := compactSnapshot(cfg, l1, "compacted", compactorOptions{
		Partitions:    partitions,
		DeletionDelay: "0s", // sources are removed as soon as the cleaner next runs.
		WantSources:   false,
	})
	if err != nil {
		return Result{}, fmt.Errorf("building compacted snapshot: %w", err)
	}

	return Result{L1: l1, Handover: handover, Compacted: compacted}, nil
}
