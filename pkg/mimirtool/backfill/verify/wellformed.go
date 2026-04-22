// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// WellFormedVerifier checks that a block's index and chunks are structurally
// valid by delegating to pkg/storage/tsdb/block.VerifyBlock. In Deep mode
// (checkChunks=true) it performs a full CRC32 walk over every chunk; in
// Medium mode (checkChunks=false) it validates only the index structure and
// chunk-segment headers.
//
// This verifier is a thin wrapper: all actual byte-twiddling happens inside
// block.VerifyBlock / GatherBlockHealthStats. Do not reimplement the walk.
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
// On any failure (missing files, mangled index, chunk-checksum mismatch in
// deep mode, ...) it returns a wrapped error suitable for inclusion in the
// verification Report.
//
// Caveat: block.VerifyBlock does not check ctx.Err() inside its postings
// walk (see pkg/storage/tsdb/block/index.go:177-299). Cancellation via ctx
// can only abort between blocks, not mid-walk.
func (v *WellFormedVerifier) Verify(ctx context.Context, blockDir string, meta block.Meta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := block.VerifyBlock(ctx, v.logger, blockDir, meta.MinTime, meta.MaxTime, v.checkChunks); err != nil {
		return fmt.Errorf("well-formed check failed: %w", err)
	}
	return nil
}
