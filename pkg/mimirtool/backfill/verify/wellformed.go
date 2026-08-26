// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/block/blockvalidation"
)

// WellFormedVerifier checks that a block on disk is structurally valid by
// delegating to blockvalidation.CheckBlockOnDisk, which first verifies that
// every file declared in meta.Thanos.Files exists at the expected size and
// then runs block.VerifyBlock. In Deep mode (checkChunks=true) the
// structural walk performs a full CRC32 check over every chunk; in Medium
// mode (checkChunks=false) it validates only the index structure and chunk
// segment headers.
//
// This verifier is a thin wrapper around shared validation routines: the
// same rules run server-side in the compactor block-upload handler. Do not
// reimplement the walk here.
type WellFormedVerifier struct {
	logger      log.Logger
	checkChunks bool
}

// NewWellFormedVerifier constructs a WellFormedVerifier configured for the
// given Mode.
func NewWellFormedVerifier(logger log.Logger, mode Mode) *WellFormedVerifier {
	return &WellFormedVerifier{
		logger:      logger,
		checkChunks: mode == Deep,
	}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *WellFormedVerifier) Name() string { return "well-formed" }

// Verify returns nil if the block at blockDir passes the well-formed check.
// On any failure (missing files, file size mismatch, mangled index,
// chunk-checksum mismatch in deep mode, ...) it returns a wrapped error
// suitable for inclusion in the verification Report.
//
// Caveat: block.VerifyBlock (invoked via blockvalidation.CheckBlockOnDisk)
// does not check ctx.Err() inside its postings walk (see
// pkg/storage/tsdb/block/index.go:177-299). Cancellation via ctx can only
// abort between blocks, not mid-walk.
func (v *WellFormedVerifier) Verify(ctx context.Context, blockDir string, meta block.Meta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := blockvalidation.CheckBlockOnDisk(ctx, v.logger, blockDir, &meta, blockvalidation.CheckBlockOnDiskOptions{
		CheckChunks: v.checkChunks,
	})
	if err != nil {
		return fmt.Errorf("well-formed check failed: %w", err)
	}
	return nil
}
