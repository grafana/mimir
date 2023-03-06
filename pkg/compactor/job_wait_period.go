// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// jobWaitPeriodElapsed returns whether the 1st level compaction wait period has
// elapsed for the input job.
func jobWaitPeriodElapsed(ctx context.Context, job *Job, waitPeriod time.Duration, userBucket objstore.Bucket) (bool, error) {
	if waitPeriod <= 0 {
		return true, nil
	}

	if job.MinCompactionLevel() > 1 {
		return true, nil
	}

	if ok, err := jobContainsBlocksUploadedAfter(ctx, job, userBucket, time.Now().Add(-waitPeriod)); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}

	return true, nil
}

// jobContainsBlocksUploadedAfter returns whether the input job contains blocks which
// have been uploaded after the input threshold timestamp.
func jobContainsBlocksUploadedAfter(ctx context.Context, job *Job, userBucket objstore.Bucket, threshold time.Time) (bool, error) {
	for _, meta := range job.Metas() {
		metaPath := path.Join(meta.ULID.String(), block.MetaFilename)

		attrs, err := userBucket.Attributes(ctx, metaPath)
		if err != nil {
			return false, errors.Wrapf(err, "unable to get object attributes for %s", metaPath)
		}

		if attrs.LastModified.After(threshold) {
			return true, nil
		}
	}

	return false, nil
}
