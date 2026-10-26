// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// jobWaitPeriodFilter filters out 1st level compaction jobs containing blocks uploaded within the wait period.
type jobWaitPeriodFilter struct {
	waitPeriod        time.Duration
	oooWaitPeriod     time.Duration
	skipFutureMaxTime bool
	bkt               objstore.Bucket
	logger            log.Logger
}

func newJobWaitPeriodFilter(waitPeriod time.Duration, oooWaitPeriod time.Duration, skipFutureMaxTime bool, bkt objstore.Bucket, logger log.Logger) *jobWaitPeriodFilter {
	return &jobWaitPeriodFilter{
		waitPeriod:        waitPeriod,
		oooWaitPeriod:     oooWaitPeriod,
		skipFutureMaxTime: skipFutureMaxTime,
		bkt:               bkt,
		logger:            logger,
	}
}

// Filter filters out jobs for which the configured wait period hasn't been honored yet.
func (f *jobWaitPeriodFilter) Filter(ctx context.Context, jobs []*Job) []*Job {
	for i := 0; i < len(jobs); {
		if elapsed, notElapsedBlock, err := jobWaitPeriodElapsed(ctx, jobs[i], f.waitPeriod, f.oooWaitPeriod, f.skipFutureMaxTime, f.bkt, f.logger); err != nil {
			level.Warn(f.logger).Log("msg", "not enforcing compaction wait period because the check if compaction job contains recently uploaded blocks has failed", "groupKey", jobs[i].Key(), "err", err)

			// Keep the job.
			i++
		} else if !elapsed {
			level.Info(f.logger).Log("msg", "skipping compaction job because blocks in this job were uploaded too recently (within wait period)", "groupKey", jobs[i].Key(), "waitPeriodNotElapsedFor", notElapsedBlock.String())
			jobs = append(jobs[:i], jobs[i+1:]...)
		} else {
			i++
		}
	}

	return jobs
}

// jobWaitPeriodElapsed returns whether the 1st level compaction wait period has
// elapsed for the input job. If the wait period has not elapsed, then this function
// also returns the Meta of the first source block encountered for which the wait
// period has not elapsed yet.
func jobWaitPeriodElapsed(ctx context.Context, job *Job, waitPeriod time.Duration, oooWaitPeriod time.Duration, skipFutureMaxTime bool, userBucket objstore.Bucket, logger log.Logger) (bool, *block.Meta, error) {
	if waitPeriod <= 0 && oooWaitPeriod <= 0 {
		return true, nil, nil
	}

	if job.MinCompactionLevel() > 1 {
		return true, nil, nil
	}

	now := time.Now()
	threshold := now.Add(-waitPeriod)
	oooThreshold := now.Add(-oooWaitPeriod)

	for _, meta := range job.Metas() {
		isOOO := meta.OutOfOrder || meta.Compaction.FromOutOfOrder()

		blockWaitPeriod := waitPeriod
		blockThreshold := threshold
		if isOOO {
			blockWaitPeriod = oooWaitPeriod
			blockThreshold = oooThreshold
		}

		if blockWaitPeriod <= 0 {
			continue
		}

		attrs, err := block.GetMetaAttributes(ctx, meta, userBucket)
		if err != nil {
			return false, meta, err
		}

		if attrs.LastModified.After(blockThreshold) {
			return false, meta, nil
		}

		if skipFutureMaxTime && timestamp.Time(meta.MaxTime).After(blockThreshold) {
			level.Info(logger).Log("msg", "skipping compaction job because block max time is within the wait period, despite Last Modified being old enough", "block", meta.ULID.String(), "blockMaxTime", meta.MaxTime, "threshold", blockThreshold, "lastModified", attrs.LastModified)
			return false, meta, nil
		}
	}

	return true, nil, nil
}
