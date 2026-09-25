// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/block/blockvalidation"
)

var errBackfillJobHasNoID = errors.New("backfill job has no backfill ID")

// executeBackfillCleanupJob deletes the data and markers of a tenant's backfill. Only markers of this backfill are deleted,
// since a new backfill can start while an interrupted cleanup is finishing.
func (e *schedulerExecutor) executeBackfillCleanupJob(ctx context.Context, c *MultitenantCompactor, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	if spec.BackfillCleanup == nil || spec.BackfillCleanup.BackfillId == "" {
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, errBackfillJobHasNoID
	}
	backfillID := spec.BackfillCleanup.BackfillId
	tenant := spec.Tenant
	logger := log.With(e.logger, "user", tenant, "backfill_id", backfillID)

	deleted, err := bucket.DeletePrefix(ctx, c.bucketClient, backfill.DataPrefix(backfillID, tenant), logger)
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to delete backfill data: %w", err)
	}

	// The cleanup marker is deleted last so that cleanup is planned again if this job is interrupted
	for _, phase := range []string{backfill.PhaseValidate, backfill.PhaseCompact, backfill.PhaseCopy, backfill.PhaseBackfill, backfill.PhaseCleanup} {
		if err := deleteBackfillMarker(ctx, c.bucketClient, phase, tenant, backfillID); err != nil {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
		}
	}

	level.Info(logger).Log("msg", "backfill cleanup completed", "deleted_objects", deleted)
	return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
}

// executeBackfillValidateJob validates an uploaded block of a backfill and marks it complete. A block that fails
// validation fails the whole backfill.
func (e *schedulerExecutor) executeBackfillValidateJob(ctx context.Context, c *MultitenantCompactor, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	job := spec.BackfillBlock
	if job == nil || job.BackfillId == "" {
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, errBackfillJobHasNoID
	}
	var blockID ulid.ULID
	if err := blockID.UnmarshalBinary(job.BlockId); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, fmt.Errorf("failed to parse block ID: %w", err)
	}
	tenant := spec.Tenant
	logger := log.With(e.logger, "user", tenant, "backfill_id", job.BackfillId, "block", blockID.String())
	dataBkt := e.jobBucket(c, tenant, job.BackfillId)

	validated, err := dataBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to check block meta: %w", err)
	}
	if validated {
		return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
	}

	fail := func(reason error) (compactorschedulerpb.UpdateType, error) {
		level.Warn(logger).Log("msg", "backfill block failed validation, cleaning up the backfill", "err", reason)
		if err := backfill.WriteMarker(ctx, c.bucketClient, backfill.PhaseCleanup, tenant, backfill.Marker{BackfillID: job.BackfillId}); err != nil {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to write cleanup marker: %w", err)
		}
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, reason
	}

	meta, err := c.loadUploadingMeta(ctx, dataBkt, blockID)
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to read uploading meta: %w", err)
	}
	if meta == nil {
		return fail(errors.New("block has no uploading meta file"))
	}

	maxBlockSizeBytes := c.cfgProvider.CompactorBlockUploadMaxBlockSizeBytes(tenant)
	if err := blockvalidation.CheckMaxBlockSize(meta.Thanos.Files, maxBlockSizeBytes); err != nil {
		return fail(err)
	}
	blockDir, err := c.prepareBlockForValidation(ctx, dataBkt, blockID)
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
	}
	defer c.removeTemporaryBlockDirectory(blockDir)
	err = blockvalidation.CheckBlockOnDisk(ctx, logger, blockDir, meta, blockvalidation.CheckBlockOnDiskOptions{
		CheckChunks:       c.cfgProvider.CompactorBlockUploadVerifyChunks(tenant),
		MaxBlockSizeBytes: maxBlockSizeBytes,
	})
	if err != nil {
		if ctx.Err() != nil {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
		}
		return fail(err)
	}

	// TODO: this counts backfilled blocks in the block upload metrics, which may not be wanted
	if err := c.markBlockComplete(ctx, logger, tenant, dataBkt, blockID, meta); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
	}
	level.Info(logger).Log("msg", "backfill block validated")
	return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
}

// executeBackfillPhasePlanningJob plans the work of the current phase of a tenant's backfill. A phase that has no work
// left advances to the next phase, which is then planned in the same job.
func (e *schedulerExecutor) executeBackfillPhasePlanningJob(ctx context.Context, c *MultitenantCompactor, compactDir string, spec *compactorschedulerpb.JobSpec) (*compactorschedulerpb.PlannedJobsRequest, error) {
	tenant := spec.Tenant
	bkt := c.bucketClient

	backfillMarker, hasBackfill, err := backfill.ReadMarker(ctx, bkt, backfill.PhaseBackfill, tenant)
	if err != nil {
		return nil, err
	}
	cleanupMarker, hasCleanup, err := backfill.ReadMarker(ctx, bkt, backfill.PhaseCleanup, tenant)
	if err != nil {
		return nil, err
	}
	if hasCleanup {
		if !hasBackfill || cleanupMarker.BackfillID == backfillMarker.BackfillID {
			return backfillCleanupPlan(cleanupMarker.BackfillID), nil
		}
		// A cleanup that was interrupted after deleting its backfill marker, before a new backfill started
		if err := bkt.Delete(ctx, backfill.PhaseMarkerPath(backfill.PhaseCleanup, tenant)); err != nil && !bkt.IsObjNotFoundErr(err) {
			return nil, fmt.Errorf("failed to delete stale cleanup marker: %w", err)
		}
	}
	if !hasBackfill {
		return &compactorschedulerpb.PlannedJobsRequest{}, nil
	}

	backfillID := backfillMarker.BackfillID
	phase, err := currentBackfillPhase(ctx, bkt, tenant)
	if err != nil {
		return nil, err
	}
	advance := func(next string) error {
		phase = next
		return backfill.WriteMarker(ctx, bkt, next, tenant, backfill.Marker{BackfillID: backfillID})
	}

	hasOutstanding := spec.BackfillPhasePlanning.GetHasOutstandingJobs()
	dataBkt := e.jobBucket(c, tenant, backfillID)
	for {
		switch phase {
		case backfill.PhaseBackfill:
			// Uploads are ongoing
			return &compactorschedulerpb.PlannedJobsRequest{Unchanged: true}, nil
		case backfill.PhaseValidate:
			if hasOutstanding {
				return &compactorschedulerpb.PlannedJobsRequest{Unchanged: true}, nil
			}
			jobs, incomplete, err := planBackfillValidation(ctx, dataBkt, backfillID)
			if err != nil {
				return nil, err
			}
			if incomplete {
				if err := advance(backfill.PhaseCleanup); err != nil {
					return nil, err
				}
				return backfillCleanupPlan(backfillID), nil
			}
			if len(jobs) > 0 {
				return &compactorschedulerpb.PlannedJobsRequest{Jobs: jobs}, nil
			}
			if err := advance(backfill.PhaseCompact); err != nil {
				return nil, err
			}
		case backfill.PhaseCompact:
			jobs, err := e.executePlanningJob(ctx, c, compactDir, dataBkt, tenant, backfillID)
			if err != nil {
				return nil, err
			}
			if len(jobs) > 0 || hasOutstanding {
				return &compactorschedulerpb.PlannedJobsRequest{Jobs: jobs}, nil
			}
			if err := advance(backfill.PhaseCopy); err != nil {
				return nil, err
			}
		case backfill.PhaseCopy:
			return nil, errors.New("backfill copy phase is not supported yet")
		default:
			return nil, fmt.Errorf("unknown backfill phase %q", phase)
		}
	}
}

// currentBackfillPhase returns the latest phase of a tenant's backfill that has a marker
func currentBackfillPhase(ctx context.Context, bkt objstore.BucketReader, tenant string) (string, error) {
	for _, phase := range []string{backfill.PhaseCopy, backfill.PhaseCompact, backfill.PhaseValidate} {
		exists, err := bkt.Exists(ctx, backfill.PhaseMarkerPath(phase, tenant))
		if err != nil {
			return "", fmt.Errorf("failed to check %s marker: %w", phase, err)
		}
		if exists {
			return phase, nil
		}
	}
	return backfill.PhaseBackfill, nil
}

// planBackfillValidation returns a validate job for each uploaded block that has not been validated yet. It reports
// whether any block is incomplete, meaning it has neither an uploading nor a final meta file.
func planBackfillValidation(ctx context.Context, dataBkt objstore.BucketReader, backfillID string) (jobs []*compactorschedulerpb.PlannedJob, incomplete bool, err error) {
	err = dataBkt.Iter(ctx, "", func(name string) error {
		blockID, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		validated, err := dataBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
		if err != nil {
			return err
		}
		if validated {
			return nil
		}
		uploaded, err := dataBkt.Exists(ctx, path.Join(blockID.String(), uploadingMetaFilename))
		if err != nil {
			return err
		}
		if !uploaded {
			incomplete = true
			return nil
		}
		jobs = append(jobs, &compactorschedulerpb.PlannedJob{
			Id: blockID.String(),
			Job: &compactorschedulerpb.PlannedJob_BackfillValidate{
				BackfillValidate: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID, BlockId: blockID.Bytes()},
			},
		})
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to list backfill blocks: %w", err)
	}
	return jobs, incomplete, nil
}

func backfillCleanupPlan(backfillID string) *compactorschedulerpb.PlannedJobsRequest {
	return &compactorschedulerpb.PlannedJobsRequest{
		Jobs: []*compactorschedulerpb.PlannedJob{{
			// The scheduler ignores this ID since a tenant has at most one cleanup job
			Id:  "cleanup",
			Job: &compactorschedulerpb.PlannedJob_BackfillCleanup{BackfillCleanup: &compactorschedulerpb.BackfillCleanupJob{BackfillId: backfillID}},
		}},
	}
}

// deleteBackfillMarker deletes a tenant's marker for a phase if it belongs to the given backfill
func deleteBackfillMarker(ctx context.Context, bkt objstore.Bucket, phase, tenant, backfillID string) error {
	m, ok, err := backfill.ReadMarker(ctx, bkt, phase, tenant)
	if err != nil {
		return err
	}
	if !ok || m.BackfillID != backfillID {
		return nil
	}
	if err := bkt.Delete(ctx, backfill.PhaseMarkerPath(phase, tenant)); err != nil && !bkt.IsObjNotFoundErr(err) {
		return fmt.Errorf("failed to delete %s marker: %w", phase, err)
	}
	return nil
}
