// SPDX-License-Identifier: AGPL-3.0-only

// Package verify provides a pluggable pre-upload verification framework for mimirtool backfill. Per-block and batch-level checks are composed via functional options.
package verify

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// BlockVerifier runs a single check against one block.
type BlockVerifier interface {
	// Name return the name of this check, for reporting and identification.
	Name() string
	// Verify returns nil if the block passes. A non-nil error is recorded as a
	// per-block, per-check failure in the Report.
	Verify(ctx context.Context, blockDir string, meta block.Meta) error
}

// BatchVerifier runs a single check across all blocks in one invocation,
// for checks that require analysis of multiple blocks.
type BatchVerifier interface {
	Name() string
	// Verify runs the verification for the batch. It should return nil on
	// success, and error if the verification fails. Verifiers should record
	// individual failures to the provided Report, and the error text will be
	// recorded as a failure summary in the Report.
	Verify(ctx context.Context, blocks []BlockRef, report *Report) error
}

// BlockRef records per-block directory and meta information for processing by a BatchVerifier.
type BlockRef struct {
	Dir  string
	Meta block.Meta
}

// Verifier is the top-level object for executing verification.
type Verifier struct {
	logger log.Logger
	opts   options
}

// NewVerifier assembles a Verifier from the given Options.
func NewVerifier(logger log.Logger, opts ...Option) *Verifier {
	o := options{
		failFast:    true,
		concurrency: 0,
	}
	for _, apply := range opts {
		apply(&o)
	}
	return &Verifier{logger: logger, opts: o}
}

// Run executes all registered per-block verifiers in parallel, then all batch
// verifiers sequentially. It returns a Report aggregating per-block, per-check
// failures. Inspect Report.HasFailures() or Report.Err() to determine what
// errors were detected during verification.
//
// If fail-fast mode is true, Run returns after the first error is detected,
// whereas all checks are run when that mode is disabled.
func (v *Verifier) Run(ctx context.Context, blockDirs []string) *Report {
	report := newReport(len(blockDirs))

	// Parse meta once per block up-front; refs feeds both per-block and batch stages.
	refs := make([]BlockRef, 0, len(blockDirs))
	var refsMu sync.Mutex

	limit := v.opts.concurrency
	if limit <= 0 {
		if n := runtime.GOMAXPROCS(0); n < 4 {
			limit = n
		} else {
			limit = 4
		}
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(limit)

	for _, dir := range blockDirs {
		eg.Go(func() error {
			if err := egCtx.Err(); err != nil {
				return nil
			}

			// TODO: The uploader fixes thanos.files during upload, so we could
			// fail verification for blocks that would be automatically fixed for
			// upload.
			meta, err := block.ReadMetaFromDir(dir)
			if err != nil {
				report.Add("", "meta", dir, "", fmt.Errorf("failed to read meta.json: %w", err))
				if v.opts.failFast {
					return errFailFast
				}
				return nil
			}

			refsMu.Lock()
			// TODO: could save memory by zeroing out meta we don't need,
			// like slices of files and compaction details.
			refs = append(refs, BlockRef{Dir: dir, Meta: *meta})
			refsMu.Unlock()

			// TODO: We should probably add a verifier for duplicate block ULID
			blockULID := meta.ULID.String()
			blockLogger := log.With(v.logger, "block", blockULID)
			level.Info(blockLogger).Log("msg", "verifying block")

			for _, check := range v.opts.blockChecks {
				if err := egCtx.Err(); err != nil {
					return nil
				}
				if verr := check.Verify(egCtx, dir, *meta); verr != nil {
					if err := egCtx.Err(); err != nil {
						return nil
					}
					report.Add(blockULID, check.Name(), dir, getShardID(meta), verr)
					if v.opts.failFast {
						return errFailFast
					}
					continue
				}
			}

			return nil
		})
	}

	_ = eg.Wait() // errFailFast is a signal, not a reportable error

	if err := ctx.Err(); err != nil {
		report.Add("", "context", "", "", fmt.Errorf("verification aborted before all blocks were checked: %w", err))
	}

	// Only run batch checks if the context is still good, and if the report has
	// failures, only run if we've been asked to do a full report.
	if ctx.Err() == nil && (!report.HasFailures() || !v.opts.failFast) {
		for _, bcheck := range v.opts.batchChecks {
			level.Info(v.logger).Log("check", bcheck.Name(), "msg", "running batch check")
			err := bcheck.Verify(ctx, refs, report)
			if err != nil {
				// Batch verifiers are expected to write their own reports, but failure
				// to do so would cause verification to incorrectly pass. Instead, add
				// a report item for the error summary.
				report.Add("", bcheck.Name(), "", "", err)
				if v.opts.failFast {
					break
				}
			}
		}
	} else if len(v.opts.batchChecks) > 0 {
		reason := "per-block fail-fast"
		if ctx.Err() != nil {
			reason = "context canceled"
		}
		level.Info(v.logger).Log(
			"msg", "skipping batch checks",
			"reason", reason,
			"batch_check_count", len(v.opts.batchChecks),
		)
	}

	total, failedBlocks, totalFailures := report.Summary()
	outcome := "all_checked"
	if v.opts.failFast && report.HasFailures() {
		outcome = "aborted"
	}
	level.Info(v.logger).Log(
		"msg", "verification complete",
		"total_blocks", total,
		"failed_blocks", failedBlocks,
		"failures", totalFailures,
		"outcome", outcome,
	)

	return report
}

// errFailFast is an internal sentinel used to short-circuit the errgroup in
// fail-fast mode.
var errFailFast = errors.New("verify: fail-fast triggered")

// getShardID is a convenience function for getting the shard ID of a block
// from the meta. Returns empty if none found.
func getShardID(m *block.Meta) string {
	return parseBlockShard(m).id
}

// blockShard is the parsed compactor shard of a block. A count of 0 means the
// block is unsharded, which includes blocks whose shard label is missing,
// empty or malformed.
type blockShard struct {
	// id is the label value as written in the block, or empty if unsharded.
	id string
	// index is the zero-based shard index.
	index uint64
	// count is the shard count, or 0 if unsharded.
	count uint64
}

// parseBlockShard parses the compactor shard external label of a block. Any
// parse failure yields the zero value, which is an unsharded block.
func parseBlockShard(m *block.Meta) blockShard {
	v := m.Thanos.Labels[block.CompactorShardIDExternalLabel]
	index, count, err := sharding.ParseShardIDLabelValue(v)
	if err != nil {
		return blockShard{}
	}
	// TODO we can (optionally?) check here that the shard count is a power of 2,
	// and that the count is greater than the index.
	return blockShard{id: v, index: index, count: count}
}

// shardedRef pairs a block with its parsed shard so batch checks parse each
// label once and can sort without touching the caller's slice.
type shardedRef struct {
	ref   *BlockRef
	shard blockShard
}

// newShardedRefs returns one shardedRef per block, in input order, each
// pointing at the corresponding element of blocks.
func newShardedRefs(blocks []BlockRef) []shardedRef {
	refs := make([]shardedRef, len(blocks))
	for i := range blocks {
		refs[i] = shardedRef{ref: &blocks[i], shard: parseBlockShard(&blocks[i].Meta)}
	}
	return refs
}

// compareBlockShards orders shards with unsharded first, then by shard count
// and then by shard index, both numerically. Two blocks are in the same shard
// when it returns 0.
func compareBlockShards(a, b blockShard) int {
	return cmp.Or(
		cmp.Compare(a.count, b.count),
		cmp.Compare(a.index, b.index),
	)
}

// shardLabel describes the shard of a block for failure messages.
func shardLabel(s blockShard) string {
	if s.count == 0 {
		return "unsharded block"
	}
	return "block in shard " + s.id
}

// shardMixKind classifies how the shards of a set of blocks combine.
type shardMixKind int

const (
	// shardMixNone means the blocks are all unsharded, or all sharded with one
	// shard count, so they may share a UTC day or an overlap region.
	shardMixNone shardMixKind = iota
	// shardMixUnsharded means unsharded and sharded blocks are mixed.
	shardMixUnsharded
	// shardMixCounts means sharded blocks with different shard counts are mixed.
	shardMixCounts
)

// shardMix is the result of classifyShardMix.
type shardMix struct {
	kind     shardMixKind
	counts   int
	minCount uint64
	maxCount uint64
}

// classifyShardMix reports whether refs mix unsharded and sharded blocks or
// shard counts. The refs must be sorted with compareBlockShards as the primary
// key.
func classifyShardMix(refs []shardedRef) shardMix {
	if len(refs) == 0 {
		return shardMix{}
	}
	first, last := refs[0].shard, refs[len(refs)-1].shard
	if first.count == last.count {
		return shardMix{}
	}
	if first.count == 0 {
		return shardMix{kind: shardMixUnsharded}
	}
	counts := 1
	for i := 1; i < len(refs); i++ {
		if refs[i].shard.count != refs[i-1].shard.count {
			counts++
		}
	}
	return shardMix{kind: shardMixCounts, counts: counts, minCount: first.count, maxCount: last.count}
}

// mixed reports whether the blocks cannot share a UTC day or an overlap region.
func (m shardMix) mixed() bool { return m.kind != shardMixNone }

// String returns a fixed-size description of the mix. It never lists blocks or
// individual shard counts, so per-block messages stay bounded.
func (m shardMix) String() string {
	switch m.kind {
	case shardMixUnsharded:
		return "mixes unsharded and sharded blocks"
	case shardMixCounts:
		return fmt.Sprintf("mixes %d different shard counts between %d and %d", m.counts, m.minCount, m.maxCount)
	default:
		return "does not mix shard counts"
	}
}
