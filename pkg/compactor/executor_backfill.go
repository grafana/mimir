// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/block/blockvalidation"
)

var (
	errBackfillBlockJobMissing = errors.New("backfill block job is missing")
	errBackfillBlockHasNoMeta  = errors.New("backfill block has no meta.json")
)

// Suffixes of the markers of backfill blocks
const (
	validatedMarkSuffix = "-validated" // the block passed validation
	copiedMarkSuffix    = "-copied"    // the block was copied to blocks storage
)

// backfillBucket returns the bucket holding backfill data and phase markers
func backfillBucket(c *MultitenantCompactor) objstore.Bucket {
	return bucket.NewPrefixedBucketClient(c.bucketClient, backfill.BucketPrefix)
}

// backfillDataBucket returns the bucket holding the blocks of a backfill, given the bucket from backfillBucket
func backfillDataBucket(bkt objstore.Bucket, tenant string, backfillID uuid.UUID) objstore.Bucket {
	return bucket.NewPrefixedBucketClient(bkt, backfill.DataPrefix(tenant, backfillID))
}

// backfillBlockMarkPath returns the path of a block's marker, relative to the backfill's data
func backfillBlockMarkPath(blockID ulid.ULID, suffix string) string {
	return path.Join(block.MarkersPathname, blockID.String()+suffix)
}

// plannedWork is the result of a plan job
type plannedWork struct {
	jobs     []*compactorschedulerpb.PlannedCompactionJob
	backfill *compactorschedulerpb.PlannedBackfillJobs
	// accepted runs after the scheduler accepts the plan, if set
	accepted func(context.Context) error
}

func unchangedBackfillPlan() plannedWork {
	return plannedWork{backfill: &compactorschedulerpb.PlannedBackfillJobs{Unchanged: true}}
}

func clearBackfillPlan() plannedWork {
	return plannedWork{backfill: &compactorschedulerpb.PlannedBackfillJobs{Clear: true}}
}

func backfillBlockJobsPlan(jobType compactorschedulerpb.JobType, backfillID uuid.UUID, blockIDs [][]byte) plannedWork {
	return plannedWork{backfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: jobType, BackfillId: backfillID[:], BlockIds: blockIDs}}
}

// executeBackfillPhasePlanningJob plans the work of a tenant's backfill and deletes the data of backfills that have no
// markers left. A cleanup is planned before any other backfill, and other backfills wait their turn.
func (e *schedulerExecutor) executeBackfillPhasePlanningJob(ctx context.Context, c *MultitenantCompactor, compactDir string, spec *compactorschedulerpb.JobSpec) (plannedWork, error) {
	tenant := spec.Tenant
	hasOutstanding := spec.BackfillPhasePlanning.GetHasOutstandingJobs()
	logger := log.With(e.logger, "user", tenant)
	bkt := backfillBucket(c)

	// Backfills are listed before their markers are read. A backfill's marker is written before its data, so a listed
	// backfill without markers is never one that is starting.
	backfillIDs, err := backfill.ListBackfills(ctx, bkt, tenant)
	if err != nil {
		return plannedWork{}, err
	}
	slices.SortFunc(backfillIDs, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })

	var cleanupID, activeID uuid.UUID
	var cleanupMarker backfill.Marker
	var hasCleanup, hasActive bool
	for _, id := range backfillIDs {
		m, ok, err := backfill.ReadMarker(ctx, bkt, tenant, id, backfill.PhaseCleanup)
		if err != nil {
			return plannedWork{}, err
		}
		if ok {
			if !hasCleanup {
				cleanupID, cleanupMarker, hasCleanup = id, m, true
			}
			continue
		}
		ok, err = bkt.Exists(ctx, backfill.PhaseMarkerPath(tenant, id, backfill.PhaseBackfill))
		if err != nil {
			return plannedWork{}, fmt.Errorf("failed to check backfill marker: %w", err)
		}
		if ok {
			if !hasActive {
				activeID, hasActive = id, true
			}
			continue
		}
		// Written after its backfill was cleaned up, such as by a late upload or a worker that missed the clear
		level.Info(logger).Log("msg", "deleting orphaned backfill data", "backfill_id", id)
		if _, err := bucket.DeletePrefix(ctx, bkt, backfill.Prefix(tenant, id), logger); err != nil {
			return plannedWork{}, fmt.Errorf("failed to delete orphaned backfill data: %w", err)
		}
	}

	switch {
	case hasCleanup:
		return e.planBackfillCleanup(ctx, bkt, log.With(logger, "backfill_id", cleanupID), tenant, cleanupID, cleanupMarker, hasOutstanding)
	case hasActive:
		return e.planBackfillPhase(ctx, c, compactDir, bkt, log.With(logger, "backfill_id", activeID), tenant, activeID, hasOutstanding)
	default:
		return clearBackfillPlan(), nil
	}
}

// planBackfillPhase plans the work of the current phase of a backfill. A phase with no work left advances to the next
// phase, which is planned in the same job.
func (e *schedulerExecutor) planBackfillPhase(ctx context.Context, c *MultitenantCompactor, compactDir string, bkt objstore.Bucket, logger log.Logger, tenant string, backfillID uuid.UUID, hasOutstanding bool) (plannedWork, error) {
	phase, err := currentBackfillPhase(ctx, bkt, tenant, backfillID)
	if err != nil {
		return plannedWork{}, err
	}
	advance := func(next string) error {
		level.Info(logger).Log("msg", "advancing backfill phase", "from", phase, "to", next)
		phase = next
		return backfill.WriteMarker(ctx, bkt, tenant, backfillID, next, backfill.Marker{})
	}

	dataBkt := backfillDataBucket(bkt, tenant, backfillID)
	for {
		switch phase {
		case backfill.PhaseBackfill:
			// TODO: clean up backfills whose uploads never finish
			return unchangedBackfillPlan(), nil
		case backfill.PhaseValidate:
			if hasOutstanding {
				return unchangedBackfillPlan(), nil
			}
			blockIDs, err := listUnvalidatedBackfillBlocks(ctx, dataBkt)
			if err != nil {
				return plannedWork{}, err
			}
			if len(blockIDs) > 0 {
				return backfillBlockJobsPlan(compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, backfillID, blockIDs), nil
			}
			if err := advance(backfill.PhaseCompact); err != nil {
				return plannedWork{}, err
			}
		case backfill.PhaseCompact:
			jobs, err := e.executePlanningJob(ctx, c, compactDir, tenant, dataBkt, backfillID[:])
			if err != nil {
				return plannedWork{}, err
			}
			if len(jobs) > 0 || hasOutstanding {
				return plannedWork{jobs: jobs}, nil
			}
			if err := advance(backfill.PhaseCopy); err != nil {
				return plannedWork{}, err
			}
		case backfill.PhaseCopy:
			if hasOutstanding {
				return unchangedBackfillPlan(), nil
			}
			blockIDs, err := listUncopiedBackfillBlocks(ctx, dataBkt)
			if err != nil {
				return plannedWork{}, err
			}
			if len(blockIDs) > 0 {
				return backfillBlockJobsPlan(compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, backfillID, blockIDs), nil
			}
			if err := advance(backfill.PhaseCleanup); err != nil {
				return plannedWork{}, err
			}
			return e.planBackfillCleanup(ctx, bkt, logger, tenant, backfillID, backfill.Marker{}, false)
		default:
			return plannedWork{}, fmt.Errorf("unknown backfill phase %q", phase)
		}
	}
}

// planBackfillCleanup clears the work of a backfill, then deletes its data and markers once workers have had time to
// stop the cleared jobs.
func (e *schedulerExecutor) planBackfillCleanup(ctx context.Context, bkt objstore.Bucket, logger log.Logger, tenant string, backfillID uuid.UUID, marker backfill.Marker, hasOutstanding bool) (plannedWork, error) {
	if marker.ClearedAt.IsZero() {
		plan := clearBackfillPlan()
		// The delay starts once the scheduler has dropped the work, so the time is recorded after the clear is accepted
		plan.accepted = func(ctx context.Context) error {
			marker.ClearedAt = time.Now()
			return backfill.WriteMarker(ctx, bkt, tenant, backfillID, backfill.PhaseCleanup, marker)
		}
		return plan, nil
	}
	if hasOutstanding || time.Since(marker.ClearedAt) < e.cfg.BackfillCleanupDelay {
		return clearBackfillPlan(), nil
	}

	deleted, err := bucket.DeletePrefix(ctx, bkt, backfill.DataPrefix(tenant, backfillID), logger)
	if err != nil {
		return plannedWork{}, fmt.Errorf("failed to delete backfill data: %w", err)
	}
	// The cleanup marker is deleted last so an interrupted cleanup is planned again rather than its leftovers being
	// treated as orphaned
	for _, phase := range []string{backfill.PhaseValidate, backfill.PhaseCompact, backfill.PhaseCopy, backfill.PhaseBackfill, backfill.PhaseCleanup} {
		if err := bkt.Delete(ctx, backfill.PhaseMarkerPath(tenant, backfillID, phase)); err != nil && !bkt.IsObjNotFoundErr(err) {
			return plannedWork{}, fmt.Errorf("failed to delete %s marker: %w", phase, err)
		}
	}

	level.Info(logger).Log("msg", "backfill cleanup completed", "deleted_objects", deleted)
	return clearBackfillPlan(), nil
}

// currentBackfillPhase returns the latest phase before cleanup that has a marker
func currentBackfillPhase(ctx context.Context, bkt objstore.BucketReader, tenant string, backfillID uuid.UUID) (string, error) {
	for _, phase := range []string{backfill.PhaseCopy, backfill.PhaseCompact, backfill.PhaseValidate} {
		exists, err := bkt.Exists(ctx, backfill.PhaseMarkerPath(tenant, backfillID, phase))
		if err != nil {
			return "", fmt.Errorf("failed to check %s marker: %w", phase, err)
		}
		if exists {
			return phase, nil
		}
	}
	return backfill.PhaseBackfill, nil
}

// backfillBlockMarks are the blocks of a backfill that have each kind of marker
type backfillBlockMarks struct {
	deleted   map[ulid.ULID]struct{}
	validated map[ulid.ULID]struct{}
	copied    map[ulid.ULID]struct{}
}

func listBackfillBlockMarks(ctx context.Context, dataBkt objstore.BucketReader) (backfillBlockMarks, error) {
	marks := backfillBlockMarks{deleted: map[ulid.ULID]struct{}{}, validated: map[ulid.ULID]struct{}{}, copied: map[ulid.ULID]struct{}{}}
	err := dataBkt.Iter(ctx, block.MarkersPathname+objstore.DirDelim, func(name string) error {
		base := path.Base(name)
		if blockID, ok := block.IsDeletionMarkFilename(base); ok {
			marks.deleted[blockID] = struct{}{}
			return nil
		}
		var marked map[ulid.ULID]struct{}
		id, ok := strings.CutSuffix(base, validatedMarkSuffix)
		if ok {
			marked = marks.validated
		} else if id, ok = strings.CutSuffix(base, copiedMarkSuffix); ok {
			marked = marks.copied
		} else {
			return nil
		}
		if blockID, err := ulid.Parse(id); err == nil {
			marked[blockID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return backfillBlockMarks{}, fmt.Errorf("failed to list backfill block markers: %w", err)
	}
	return marks, nil
}

// listUnvalidatedBackfillBlocks returns the uploaded blocks of a backfill that are not marked validated
func listUnvalidatedBackfillBlocks(ctx context.Context, dataBkt objstore.BucketReader) ([][]byte, error) {
	marks, err := listBackfillBlockMarks(ctx, dataBkt)
	if err != nil {
		return nil, err
	}

	var blockIDs [][]byte
	err = dataBkt.Iter(ctx, "", func(name string) error {
		if blockID, ok := block.IsBlockDir(name); ok {
			if _, validated := marks.validated[blockID]; !validated {
				blockIDs = append(blockIDs, blockID.Bytes())
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list backfill blocks: %w", err)
	}
	return blockIDs, nil
}

// listUncopiedBackfillBlocks returns the blocks of a backfill that have a meta.json and are neither marked for deletion
// nor copied. A block without a meta.json is the partial output of an interrupted compaction.
func listUncopiedBackfillBlocks(ctx context.Context, dataBkt objstore.BucketReader) ([][]byte, error) {
	marks, err := listBackfillBlockMarks(ctx, dataBkt)
	if err != nil {
		return nil, err
	}

	var blockIDs [][]byte
	err = dataBkt.Iter(ctx, "", func(name string) error {
		blockID, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		if _, ok := marks.deleted[blockID]; ok {
			return nil
		}
		if _, ok := marks.copied[blockID]; ok {
			return nil
		}
		complete, err := dataBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
		if err != nil || !complete {
			return err
		}
		blockIDs = append(blockIDs, blockID.Bytes())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list backfill blocks: %w", err)
	}
	return blockIDs, nil
}

// parseBackfillBlockJob returns the backfill and the block of a backfill block job
func parseBackfillBlockJob(job *compactorschedulerpb.BackfillBlockJob) (uuid.UUID, ulid.ULID, error) {
	if job == nil {
		return uuid.UUID{}, ulid.ULID{}, errBackfillBlockJobMissing
	}
	backfillID, err := uuid.FromBytes(job.BackfillId)
	if err != nil {
		return uuid.UUID{}, ulid.ULID{}, fmt.Errorf("failed to parse backfill ID: %w", err)
	}
	var blockID ulid.ULID
	if err := blockID.UnmarshalBinary(job.BlockId); err != nil {
		return uuid.UUID{}, ulid.ULID{}, fmt.Errorf("failed to parse block ID: %w", err)
	}
	return backfillID, blockID, nil
}

// executeBackfillValidateJob validates an uploaded block of a backfill and marks it validated. A block that fails
// validation aborts the backfill.
func (e *schedulerExecutor) executeBackfillValidateJob(ctx context.Context, c *MultitenantCompactor, compactDir string, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	backfillID, blockID, err := parseBackfillBlockJob(spec.BackfillBlock)
	if err != nil {
		level.Error(e.logger).Log("msg", "invalid backfill validate job, abandoning job", "user", spec.Tenant, "err", err)
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, err
	}
	tenant := spec.Tenant
	logger := log.With(e.logger, "user", tenant, "backfill_id", backfillID, "block", blockID)
	bkt := backfillBucket(c)
	dataBkt := backfillDataBucket(bkt, tenant, backfillID)
	startTime := time.Now()

	invalid := func(reason error) (compactorschedulerpb.UpdateType, error) {
		level.Warn(logger).Log("msg", "backfill block failed validation, aborting the backfill", "err", reason)
		// An existing cleanup marker is kept, since it may have recorded its clear
		exists, err := bkt.Exists(ctx, backfill.PhaseMarkerPath(tenant, backfillID, backfill.PhaseCleanup))
		if err == nil && !exists {
			err = backfill.WriteMarker(ctx, bkt, tenant, backfillID, backfill.PhaseCleanup, backfill.Marker{})
		}
		if err != nil {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to abort backfill: %w", err)
		}
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, reason
	}

	rc, err := dataBkt.Get(ctx, path.Join(blockID.String(), block.MetaFilename))
	if err != nil {
		if dataBkt.IsObjNotFoundErr(err) {
			return invalid(errBackfillBlockHasNoMeta)
		}
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to read block meta: %w", err)
	}
	meta, err := block.ReadMeta(rc)
	if err != nil {
		return invalid(fmt.Errorf("failed to read block meta: %w", err))
	}

	maxBlockSizeBytes := c.cfgProvider.CompactorBlockUploadMaxBlockSizeBytes(tenant)
	// Checked before downloading, so an oversized block doesn't fill the disk
	if err := blockvalidation.CheckMaxBlockSize(meta.Thanos.Files, maxBlockSizeBytes); err != nil {
		return invalid(err)
	}

	if err := os.MkdirAll(compactDir, 0o750); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to create compaction directory: %w", err)
	}
	blockDir, err := os.MkdirTemp(compactDir, "backfill-validate-")
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to create block directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(blockDir); err != nil {
			level.Warn(logger).Log("msg", "failed to remove block directory", "path", blockDir, "err", err)
		}
	}()
	if err := objstore.DownloadDir(ctx, logger, dataBkt, blockID.String(), blockID.String(), blockDir); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to download block: %w", err)
	}

	err = blockvalidation.CheckBlockOnDisk(ctx, logger, blockDir, meta, blockvalidation.CheckBlockOnDiskOptions{
		CheckChunks:       c.cfgProvider.CompactorBlockUploadVerifyChunks(tenant),
		MaxBlockSizeBytes: maxBlockSizeBytes,
	})
	if err != nil {
		// A canceled job or a full disk says nothing about the block
		if ctx.Err() != nil || errors.Is(err, syscall.ENOSPC) {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
		}
		return invalid(err)
	}

	if err := dataBkt.Upload(ctx, backfillBlockMarkPath(blockID, validatedMarkSuffix), bytes.NewReader(nil)); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to write validated marker: %w", err)
	}
	c.jobDuration.WithLabelValues(jobTypeBackfillValidate, "").Observe(time.Since(startTime).Seconds())
	level.Info(logger).Log("msg", "backfill block validated")
	return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
}

// executeBackfillCopyJob copies a block of a backfill to the tenant's blocks storage and marks it copied. meta.json is
// copied last, so the block isn't visible to the tenant until all of its files are copied.
func (e *schedulerExecutor) executeBackfillCopyJob(ctx context.Context, c *MultitenantCompactor, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	backfillID, blockID, err := parseBackfillBlockJob(spec.BackfillBlock)
	if err != nil {
		level.Error(e.logger).Log("msg", "invalid backfill copy job, abandoning job", "user", spec.Tenant, "err", err)
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, err
	}
	tenant := spec.Tenant
	logger := log.With(e.logger, "user", tenant, "backfill_id", backfillID, "block", blockID)
	srcBkt := backfillDataBucket(backfillBucket(c), tenant, backfillID)
	dstBkt := bucket.NewUserBucketClient(tenant, c.bucketClient, c.cfgProvider)
	startTime := time.Now()

	metaPath := path.Join(blockID.String(), block.MetaFilename)
	rc, err := srcBkt.Get(ctx, metaPath)
	if err != nil {
		if srcBkt.IsObjNotFoundErr(err) {
			level.Warn(logger).Log("msg", "backfill block has no meta.json, abandoning job")
			return compactorschedulerpb.UPDATE_TYPE_ABANDON, errBackfillBlockHasNoMeta
		}
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to read block meta: %w", err)
	}
	// The meta is copied as is, so the raw content is kept
	metaContent, err := io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to read block meta: %w", err)
	}
	meta, err := block.ReadMeta(io.NopCloser(bytes.NewReader(metaContent)))
	if err != nil {
		level.Warn(logger).Log("msg", "backfill block has a corrupt meta.json, abandoning job", "err", err)
		return compactorschedulerpb.UPDATE_TYPE_ABANDON, fmt.Errorf("failed to decode block meta: %w", err)
	}

	// TODO: use a server side copy, which objstore doesn't support
	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}
		if err := copyBackfillBlockFile(ctx, srcBkt, dstBkt, path.Join(blockID.String(), f.RelPath), f.SizeBytes); err != nil {
			return compactorschedulerpb.UPDATE_TYPE_REASSIGN, err
		}
	}
	if err := dstBkt.Upload(ctx, metaPath, bytes.NewReader(metaContent)); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to upload block meta: %w", err)
	}
	if err := srcBkt.Upload(ctx, backfillBlockMarkPath(blockID, copiedMarkSuffix), bytes.NewReader(nil)); err != nil {
		return compactorschedulerpb.UPDATE_TYPE_REASSIGN, fmt.Errorf("failed to write copied marker: %w", err)
	}
	c.jobDuration.WithLabelValues(jobTypeBackfillCopy, "").Observe(time.Since(startTime).Seconds())
	level.Info(logger).Log("msg", "backfill block copied", "files", len(meta.Thanos.Files))
	return compactorschedulerpb.UPDATE_TYPE_COMPLETE, nil
}

// copyBackfillBlockFile streams a file between buckets. The size comes from the block's meta, so the upload doesn't
// have to buffer the file to find it.
func copyBackfillBlockFile(ctx context.Context, src objstore.BucketReader, dst objstore.Bucket, name string, size int64) error {
	r, err := src.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", name, err)
	}
	defer r.Close()
	sized := objstore.ObjectSizerReadCloser{ReadCloser: r, Size: func() (int64, error) { return size, nil }}
	if err := dst.Upload(ctx, name, sized); err != nil {
		return fmt.Errorf("failed to upload %s: %w", name, err)
	}
	return nil
}
