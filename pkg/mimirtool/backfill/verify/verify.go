// SPDX-License-Identifier: AGPL-3.0-only

// Package verify provides a pluggable pre-upload verification framework for mimirtool backfill. Per-block and batch-level checks are composed via functional options; no global registry.
package verify

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// Mode controls the depth of per-block verification.
type Mode int

const (
	// Deep runs the full index walk and chunk-checksum verification.
	Deep Mode = iota
	// Medium runs only header-level checks (no chunk-checksum walk).
	Medium
)

// BlockVerifier runs a single check against one block.
// Implementations MUST NOT retain strings derived from meta past the call's
// return; callers of block data from shared pools must strings.Clone first.
type BlockVerifier interface {
	// Name returns the stable check name used in log lines and report entries.
	Name() string
	// Verify returns nil if the block passes. A non-nil error is recorded as a
	// per-block, per-check failure in the Report.
	Verify(ctx context.Context, blockDir string, meta block.Meta) error
}

// BatchVerifier runs a single check across all blocks in one invocation.
// Used for future duplicate-day and overlap detection (ships with zero
// implementations in v1; the interface seam is proved by a test double).
type BatchVerifier interface {
	Name() string
	Verify(ctx context.Context, blocks []BlockRef) error
}

// BlockRef is the input to BatchVerifier.Verify. Dir is the on-disk block
// directory; Meta is the already-parsed meta.json to avoid re-reading.
type BlockRef struct {
	Dir  string
	Meta block.Meta
}

// Verifier orchestrates per-block and batch-level checks.
type Verifier struct {
	logger log.Logger
	opts   options
}

// NewVerifier assembles a Verifier from functional options. Defaults: Deep
// mode, fail-fast, concurrency=0 (auto = min(GOMAXPROCS, 4) at Run time).
func NewVerifier(logger log.Logger, opts ...Option) *Verifier {
	o := options{
		mode:        Deep,
		failFast:    true,
		concurrency: 0,
	}
	for _, apply := range opts {
		apply(&o)
	}
	return &Verifier{logger: logger, opts: o}
}

// Mode returns the verifier's configured depth mode. Child verifiers that
// care about Deep vs Medium may consult this when constructed.
func (v *Verifier) Mode() Mode { return v.opts.mode }

// Run executes all registered per-block verifiers in parallel (bounded by
// concurrency), then all batch verifiers sequentially on the survivors. It
// returns a Report aggregating per-block, per-check failures. Run never
// returns a Go error for a verification failure; inspect Report.HasFailures()
// or Report.Err() instead. Run DOES record I/O failures reading meta.json as
// a special "meta" failure in the Report.
//
// Fail-fast semantics (opts.failFast == true): the first worker that records
// a failure calls cancel() on the errgroup context; peer workers observe
// ctx.Done() between blocks and return without scheduling new work.
// Already-running block.VerifyBlock calls are NOT aborted mid-walk because
// the Prometheus TSDB postings walk does not check ctx between iterations
// (see pkg/storage/tsdb/block/index.go). Stray in-flight failures may still
// be recorded.
//
// In fail-fast mode, if any per-block check fails, batch-level checks are
// NOT executed. An info log line "skipping batch checks due to per-block
// fail-fast" is emitted to make this explicit to users who registered a
// BatchVerifier.
//
// Full-report (opts.failFast == false): all workers run to completion and
// all batch checks run.
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
		dir := dir
		eg.Go(func() error {
			if err := egCtx.Err(); err != nil {
				// Context cancelled (fail-fast peer). Do not schedule new work.
				return nil
			}

			meta, err := block.ReadMetaFromDir(dir)
			if err != nil {
				report.Add("", "meta", dir, fmt.Errorf("failed to read meta.json: %w", err))
				if v.opts.failFast {
					return errFailFast
				}
				return nil
			}

			refsMu.Lock()
			refs = append(refs, BlockRef{Dir: dir, Meta: *meta})
			refsMu.Unlock()

			blockULID := meta.ULID.String()
			blockLogger := log.With(v.logger, "block", blockULID)
			level.Info(blockLogger).Log("msg", "verifying block")

			for _, check := range v.opts.blockChecks {
				if err := egCtx.Err(); err != nil {
					return nil
				}
				if verr := check.Verify(egCtx, dir, *meta); verr != nil {
					level.Error(blockLogger).Log("check", check.Name(), "msg", verr.Error())
					report.Add(blockULID, check.Name(), dir, verr)
					if v.opts.failFast {
						return errFailFast
					}
				}
			}

			level.Info(blockLogger).Log("msg", "verified")
			return nil
		})
	}

	_ = eg.Wait() // errFailFast is a signal, not a reportable error

	// Batch stage: run after per-block settles. Skipped if fail-fast already tripped.
	// Emit an explicit log line on skip so users who registered a BatchVerifier
	// aren't left wondering why it didn't run.
	batchShouldRun := !report.HasFailures() || !v.opts.failFast
	if batchShouldRun {
		for _, bcheck := range v.opts.batchChecks {
			if err := bcheck.Verify(ctx, refs); err != nil {
				level.Error(v.logger).Log("check", bcheck.Name(), "msg", err.Error())
				report.Add("", bcheck.Name(), "", err)
			}
		}
	} else if len(v.opts.batchChecks) > 0 {
		level.Info(v.logger).Log(
			"msg", "skipping batch checks due to per-block fail-fast",
			"batch_check_count", len(v.opts.batchChecks),
		)
	}

	// Summary line.
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
// fail-fast mode. It is never returned to the caller of Run.
var errFailFast = errors.New("verify: fail-fast triggered")
