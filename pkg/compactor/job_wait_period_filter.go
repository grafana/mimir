// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"container/list"
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// jobWaitPeriodFilter filters out 1st level compaction jobs containing blocks uploaded within the wait period.
type jobWaitPeriodFilter struct {
	waitPeriod        time.Duration
	oooWaitPeriod     time.Duration
	skipFutureMaxTime bool
	concurrency       int
	bkt               objstore.Bucket
	logger            log.Logger
}

func newJobWaitPeriodFilter(waitPeriod time.Duration, oooWaitPeriod time.Duration, skipFutureMaxTime bool, concurrency int, bkt objstore.Bucket, logger log.Logger) *jobWaitPeriodFilter {
	return &jobWaitPeriodFilter{
		waitPeriod:        waitPeriod,
		oooWaitPeriod:     oooWaitPeriod,
		skipFutureMaxTime: skipFutureMaxTime,
		concurrency:       concurrency,
		bkt:               bkt,
		logger:            logger,
	}
}

type jobWaitPeriodResult struct {
	block *block.Meta // populated by a block within the wait period or that failed to be checked, nil otherwise
	err   error       // populated if checking the block failed
}

// Filter filters out jobs for which the configured wait period hasn't been honored yet or jobs that couldn't be checked due to an error.
// Context cancellation is either returned as an error or treated as a failed check, depending on when it's observed.
func (f *jobWaitPeriodFilter) Filter(ctx context.Context, jobs []*Job) ([]*Job, error) {
	results, err := jobsWaitPeriodElapsed(ctx, jobs, f.waitPeriod, f.oooWaitPeriod, f.skipFutureMaxTime, f.bkt, f.concurrency, f.logger)
	if err != nil {
		return nil, err
	}

	kept := jobs[:0]
	for i, job := range jobs {
		if res := results[i]; res.err != nil && f.bkt.IsObjNotFoundErr(res.err) {
			level.Info(f.logger).Log("msg", "skipping compaction job because a source block's meta.json was not found", "groupKey", job.Key(), "block", res.block.String())
		} else if res.err != nil {
			level.Warn(f.logger).Log("msg", "skipping compaction job because the check if it contains recently uploaded blocks has failed", "groupKey", job.Key(), "block", res.block.String(), "err", res.err)
		} else if res.block != nil {
			level.Info(f.logger).Log("msg", "skipping compaction job because blocks in this job were uploaded too recently (within wait period)", "groupKey", job.Key(), "waitPeriodNotElapsedFor", res.block.String())
		} else {
			kept = append(kept, job)
		}
	}

	return kept, nil
}

// jobsWaitPeriodElapsed returns a result for each input job.
// Blocks are checked with up to maxConcurrency concurrent requests.
// A job's remaining blocks are no longer checked once one of them excludes the job.
func jobsWaitPeriodElapsed(ctx context.Context, jobs []*Job, waitPeriod time.Duration, oooWaitPeriod time.Duration, skipFutureMaxTime bool, userBucket objstore.Bucket, maxConcurrency int, logger log.Logger) ([]jobWaitPeriodResult, error) {
	results := make([]jobWaitPeriodResult, len(jobs))

	if waitPeriod <= 0 && oooWaitPeriod <= 0 {
		return results, nil
	}

	blocks := interleaveJobBlocks(jobs)
	done := make([]atomic.Bool, len(jobs))
	now := time.Now()

	err := concurrency.ForEachJob(ctx, len(blocks), maxConcurrency, func(ctx context.Context, idx int) error {
		b := blocks[idx]
		if done[b.jobIdx].Load() {
			return nil
		}

		elapsed, err := blockWaitPeriodElapsed(ctx, b.meta, waitPeriod, oooWaitPeriod, now, skipFutureMaxTime, userBucket, logger)
		if (err != nil || !elapsed) && done[b.jobIdx].CompareAndSwap(false, true) {
			results[b.jobIdx] = jobWaitPeriodResult{block: b.meta, err: err}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// blockWaitPeriodElapsed returns whether the first level compaction wait period has elapsed
// for the input block as of now.
func blockWaitPeriodElapsed(ctx context.Context, meta *block.Meta, waitPeriod time.Duration, oooWaitPeriod time.Duration, now time.Time, skipFutureMaxTime bool, userBucket objstore.Bucket, logger log.Logger) (bool, error) {
	blockWaitPeriod := waitPeriod
	if meta.OutOfOrder || meta.Compaction.FromOutOfOrder() {
		blockWaitPeriod = oooWaitPeriod
	}

	if blockWaitPeriod <= 0 {
		return true, nil
	}

	blockThreshold := now.Add(-blockWaitPeriod)

	attrs, err := block.GetMetaAttributes(ctx, meta, userBucket)
	if err != nil {
		return false, err
	}

	if attrs.LastModified.After(blockThreshold) {
		return false, nil
	}

	if skipFutureMaxTime && timestamp.Time(meta.MaxTime).After(blockThreshold) {
		level.Info(logger).Log("msg", "skipping compaction job because block max time is within the wait period, despite Last Modified being old enough", "block", meta.ULID.String(), "blockMaxTime", meta.MaxTime, "threshold", blockThreshold, "lastModified", attrs.LastModified)
		return false, nil
	}

	return true, nil
}

// interleavedBlock is a source block of a job, identified by the job's position within the input jobs.
type interleavedBlock struct {
	jobIdx int
	meta   *block.Meta
}

// interleaveJobBlocks flattens a list of jobs into a list of blocks that know which job they are from.
// The goal of interleaving blocks is to potentially minimize lookups later since a single block from a job could mean no more blocks
// from that job need to be looked up.
func interleaveJobBlocks(jobs []*Job) []interleavedBlock {
	jobMetas := make([][]*block.Meta, len(jobs))
	activeJobIndexes := list.New()
	total := 0
	for i, job := range jobs {
		if job.MinCompactionLevel() > 1 {
			continue
		}

		jobMetas[i] = job.Metas()
		if len(jobMetas[i]) > 0 {
			activeJobIndexes.PushBack(i)
			total += len(jobMetas[i])
		}
	}

	blocks := make([]interleavedBlock, 0, total)
	for blockInJob := 0; activeJobIndexes.Len() > 0; blockInJob++ {
		for e := activeJobIndexes.Front(); e != nil; {
			jobIdx := e.Value.(int)
			blocks = append(blocks, interleavedBlock{jobIdx: jobIdx, meta: jobMetas[jobIdx][blockInJob]})

			next := e.Next()
			if blockInJob+1 == len(jobMetas[jobIdx]) {
				// The job has no blocks left
				activeJobIndexes.Remove(e)
			}
			e = next
		}
	}
	return blocks
}
