// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/block/blockvalidation"
)

// MetaCheckVerifier runs the standard blockvalidation.CheckMeta header check.
//
// MetaCheckVerifier does not enforce a max block size: backfill is client-side
// and does not have access to the per-tenant
// CompactorBlockUploadMaxBlockSizeBytes limit. The compactor still rejects
// oversized blocks server-side.
type MetaCheckVerifier struct {
	logger log.Logger
}

// NewMetaCheckVerifier constructs a MetaCheckVerifier. Verification depth has
// no effect on this verifier: meta-only checks never read block contents.
func NewMetaCheckVerifier(logger log.Logger) *MetaCheckVerifier {
	return &MetaCheckVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *MetaCheckVerifier) Name() string { return "meta-check" }

// Verify returns nil if meta passes the read-only checks. The blockDir
// argument is unused; meta-only checks do not touch the filesystem.
func (v *MetaCheckVerifier) Verify(_ context.Context, _ string, meta block.Meta) error {
	return blockvalidation.CheckMeta(&meta, blockvalidation.CheckMetaOptions{})
}
