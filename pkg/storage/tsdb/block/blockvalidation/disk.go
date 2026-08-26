// SPDX-License-Identifier: AGPL-3.0-only

package blockvalidation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// CheckBlockOnDiskOptions configures CheckBlockOnDisk.
type CheckBlockOnDiskOptions struct {
	// CheckChunks, when true, walks every chunk in the block and verifies
	// its CRC32. When false, only the index structure and chunk segment
	// headers are validated.
	CheckChunks bool
	// MaxBlockSizeBytes, when > 0, caps the total file size declared in
	// meta.Thanos.Files. Useful for re-checking the limit just before the
	// expensive structural walk.
	MaxBlockSizeBytes int64
}

// CheckBlockOnDisk verifies that:
//
//  1. The sum of file sizes does not exceed opts.MaxBlockSizeBytes (when set).
//  2. Every file listed in meta.Thanos.Files exists at blockDir as a regular
//     file with the declared SizeBytes (meta.json itself is exempt from the
//     size match because it may be persisted under a different name on disk).
//  3. block.VerifyBlock succeeds against meta's declared time range.
//
// CheckBlockOnDisk does not download the block; callers must materialise
// the block on disk first. CheckChunks is forwarded to block.VerifyBlock.
func CheckBlockOnDisk(ctx context.Context, logger log.Logger, blockDir string, meta *block.Meta, opts CheckBlockOnDiskOptions) error {
	if meta == nil {
		return fmt.Errorf("missing block metadata")
	}

	if err := checkMaxBlockSize(meta.Thanos.Files, opts.MaxBlockSizeBytes); err != nil {
		return err
	}

	for _, f := range meta.Thanos.Files {
		fi, err := os.Stat(filepath.Join(blockDir, filepath.FromSlash(f.RelPath)))
		if err != nil {
			return fmt.Errorf("failed to stat %s: %w", f.RelPath, err)
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("not a file: %s", f.RelPath)
		}
		if f.RelPath != block.MetaFilename && fi.Size() != f.SizeBytes {
			return fmt.Errorf("file size mismatch for %s", f.RelPath)
		}
	}

	if err := block.VerifyBlock(ctx, logger, blockDir, meta.MinTime, meta.MaxTime, opts.CheckChunks); err != nil {
		return fmt.Errorf("error validating block: %w", err)
	}

	return nil
}
