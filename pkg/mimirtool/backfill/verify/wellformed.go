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
// delegating to blockvalidation.CheckBlockOnDisk. In Deep mode the structural
// walk performs a full CRC32 check over every chunk; in Medium mode it
// validates only the index structure and chunk segment headers.
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
// On any failure it returns a wrapped error.
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
