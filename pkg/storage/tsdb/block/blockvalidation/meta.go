// SPDX-License-Identifier: AGPL-3.0-only

// Package blockvalidation provides reusable validation routines for TSDB
// blocks destined for ingestion into a Mimir tenant. The package is shared
// between the compactor's block-upload handlers and mimirtool's backfill
// pre-upload verifier so that both paths apply the same acceptance rules.
package blockvalidation

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// MaxBlockSizeBytesFormat is the error message format used when a block
// exceeds the configured maximum size. Exported so callers can match against
// it in tests.
const MaxBlockSizeBytesFormat = "block exceeds the maximum block size limit of %d bytes"

// MaxBlockSizeExceededError reports that the sum of declared file sizes in a
// block exceeded the configured maximum. Callers can errors.As against it to
// recover the limit and the observed total for logging or metrics.
//
// SizeBytes may be negative when the sum overflowed int64; this matches the
// raw value the compactor previously logged before the move.
type MaxBlockSizeExceededError struct {
	LimitBytes int64
	SizeBytes  int64
}

func (e *MaxBlockSizeExceededError) Error() string {
	return fmt.Sprintf(MaxBlockSizeBytesFormat, e.LimitBytes)
}

// uploadSource is the value written to meta.Thanos.Source by
// SanitizeForUpload to mark a block as having entered the cluster via the
// block-upload API.
const uploadSource = "upload"

// allowedRelPath matches the relative paths that may appear in
// meta.Thanos.Files alongside the meta.json file itself. Any other path is
// rejected by CheckMeta.
var allowedRelPath = regexp.MustCompile(`^(index|chunks/\d{6})$`)

// CheckMetaOptions configures CheckMeta.
type CheckMetaOptions struct {
	// MaxBlockSizeBytes is an upper bound on the sum of file sizes declared
	// in meta.Thanos.Files. A value of 0 disables the check.
	MaxBlockSizeBytes int64
}

// CheckMeta runs read-only validation against meta. It does not mutate meta.
// The returned error is suitable for surfacing to the user; callers that
// need an HTTP status code can wrap or inspect the message directly.
//
// CheckMeta accepts the deprecated external labels recognised by
// SanitizeForUpload (DeprecatedTenantIDExternalLabel,
// DeprecatedIngesterIDExternalLabel, DeprecatedShardIDExternalLabel) and an
// empty CompactorShardIDExternalLabel. SanitizeForUpload removes those
// labels before persisting the block, so treating them as no-ops here keeps
// the upload and backfill-verify paths in agreement about what is
// acceptable.
func CheckMeta(meta *block.Meta, opts CheckMetaOptions) error {
	if meta == nil {
		return errors.New("missing block metadata")
	}

	if meta.Thanos.Downsample.Resolution > 0 {
		return errors.New("block contains downsampled data")
	}

	for l, v := range meta.Thanos.Labels {
		switch l {
		case block.CompactorShardIDExternalLabel:
			if v == "" {
				// Empty shard labels are stripped by SanitizeForUpload; accept here.
				continue
			}
			if _, _, err := sharding.ParseShardIDLabelValue(v); err != nil {
				return fmt.Errorf("invalid %s external label: %q", block.CompactorShardIDExternalLabel, v)
			}
		case block.DeprecatedTenantIDExternalLabel, block.DeprecatedIngesterIDExternalLabel, block.DeprecatedShardIDExternalLabel:
			// Deprecated labels are removed by SanitizeForUpload; accept here.
			continue
		default:
			return fmt.Errorf("unsupported external label: %s", l)
		}
	}

	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}
		if !allowedRelPath.MatchString(f.RelPath) {
			return fmt.Errorf("file with invalid path: %s", f.RelPath)
		}
		if f.SizeBytes <= 0 {
			return fmt.Errorf("file with invalid size: %s", f.RelPath)
		}
	}

	if err := CheckMaxBlockSize(meta.Thanos.Files, opts.MaxBlockSizeBytes); err != nil {
		return err
	}

	if meta.Version != block.TSDBVersion1 {
		return fmt.Errorf("version must be %d", block.TSDBVersion1)
	}

	if meta.MinTime < 0 || meta.MaxTime < 0 || meta.MaxTime < meta.MinTime {
		return fmt.Errorf("invalid minTime/maxTime: minTime=%d, maxTime=%d", meta.MinTime, meta.MaxTime)
	}

	now := time.Now()
	if meta.MinTime > now.UnixMilli() || meta.MaxTime > now.UnixMilli() {
		return fmt.Errorf("block time(s) greater than the present: minTime=%d (%s), maxTime=%d (%s)",
			meta.MinTime, formatTime(time.UnixMilli(meta.MinTime)),
			meta.MaxTime, formatTime(time.UnixMilli(meta.MaxTime)))
	}

	return nil
}

// SanitizeForUpload mutates meta to bring it in line with the conventions
// the compactor enforces when a block enters the cluster via the
// block-upload API:
//
//   - meta.ULID is overwritten with blockID (the upload's URL parameter)
//   - empty CompactorShardIDExternalLabel and deprecated external labels are removed
//   - meta.Compaction.Parents is cleared and Sources is reset to {blockID}
//   - meta.Thanos.Source is set to "upload"
//
// SanitizeForUpload performs no validation; callers should run CheckMeta
// afterwards (or before, to surface issues unaffected by the mutations).
// The function is safe to call when meta is nil; it returns immediately in
// that case.
func SanitizeForUpload(logger log.Logger, meta *block.Meta, blockID ulid.ULID) {
	if meta == nil {
		return
	}

	meta.ULID = blockID

	for l, v := range meta.Thanos.Labels {
		switch l {
		case block.CompactorShardIDExternalLabel:
			if v == "" {
				level.Debug(logger).Log("msg", "removing empty external label", "label", l)
				delete(meta.Thanos.Labels, l)
			}
		case block.DeprecatedTenantIDExternalLabel, block.DeprecatedIngesterIDExternalLabel, block.DeprecatedShardIDExternalLabel:
			level.Debug(logger).Log("msg", "removing unused external label", "label", l, "value", v)
			delete(meta.Thanos.Labels, l)
		}
	}

	meta.Compaction.Parents = nil
	meta.Compaction.Sources = []ulid.ULID{blockID}
	meta.Thanos.Source = uploadSource
}

// CheckMaxBlockSize returns a non-nil error if the sum of file sizes exceeds
// maxBlockSizeBytes. maxBlockSizeBytes <= 0 disables the check.
func CheckMaxBlockSize(files []block.File, maxBlockSizeBytes int64) error {
	if maxBlockSizeBytes <= 0 {
		return nil
	}

	blockSizeBytes := int64(0)
	for _, f := range files {
		if f.SizeBytes < 0 {
			return errors.New("invalid negative file size in block metadata")
		}
		blockSizeBytes += f.SizeBytes
		if blockSizeBytes < 0 {
			// overflow
			break
		}
	}

	if blockSizeBytes > maxBlockSizeBytes || blockSizeBytes < 0 {
		return &MaxBlockSizeExceededError{LimitBytes: maxBlockSizeBytes, SizeBytes: blockSizeBytes}
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
