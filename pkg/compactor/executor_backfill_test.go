// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSchedulerExecutor_ExecuteBackfillPhasePlanningJob(t *testing.T) {
	const tenant = "test-tenant"
	backfillID := uuid.New()
	otherBackfillID := uuid.New()

	// Blocks in separate 24h ranges, the largest compaction range, aren't compacted, while blocks sharing a 2h range are
	uploadedBlockMeta := func(minTime, maxTime time.Time) *block.Meta {
		m := createBlockMeta(t, minTime, maxTime)
		m.Compaction = tsdb.BlockMetaCompaction{Level: 1, Sources: []ulid.ULID{m.ULID}}
		return m
	}
	rangeStart := time.Now().Add(-72 * time.Hour).Truncate(24 * time.Hour)
	block1 := uploadedBlockMeta(rangeStart, rangeStart.Add(30*time.Minute))
	block2 := uploadedBlockMeta(rangeStart.Add(time.Hour), rangeStart.Add(90*time.Minute))
	block3 := uploadedBlockMeta(rangeStart.Add(25*time.Hour), rangeStart.Add(26*time.Hour))

	marker := backfill.Marker{}
	clearedMarker := func(ago time.Duration) backfill.Marker {
		return backfill.Marker{ClearedAt: time.Now().Add(-ago)}
	}
	blockIDs := func(metas ...*block.Meta) [][]byte {
		ids := make([][]byte, 0, len(metas))
		for _, m := range metas {
			ids = append(ids, m.ULID.Bytes())
		}
		return ids
	}
	clearPlan := &compactorschedulerpb.PlannedBackfillJobs{Clear: true}
	unchangedPlan := &compactorschedulerpb.PlannedBackfillJobs{Unchanged: true}

	cases := map[string]struct {
		markers        map[string]backfill.Marker
		blocks         []*block.Meta // uploaded with a meta.json
		validated      []*block.Meta
		deleted        []*block.Meta
		copied         []*block.Meta
		otherMarkers   map[string]backfill.Marker // of another backfill of the tenant
		otherBlocks    []*block.Meta
		hasOutstanding bool

		expectBackfill         *compactorschedulerpb.PlannedBackfillJobs
		expectCompactionJobs   bool
		expectAccepted         bool
		expectMarkers          []string
		expectClearedAt        bool
		expectDataDeleted      bool
		expectOtherMarkers     []string
		expectOtherDataDeleted bool
	}{
		"no backfill clears": {
			expectBackfill: clearPlan,
		},
		"ongoing uploads are unchanged": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker},
			blocks:         []*block.Meta{block1},
			expectBackfill: unchangedPlan,
			expectMarkers:  []string{backfill.PhaseBackfill},
		},
		"validate plans unvalidated blocks": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker},
			blocks:         []*block.Meta{block1, block3},
			validated:      []*block.Meta{block3},
			expectBackfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, BackfillId: backfillID[:], BlockIds: blockIDs(block1)},
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseValidate},
		},
		"validate is unchanged while jobs are outstanding": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker},
			blocks:         []*block.Meta{block1},
			hasOutstanding: true,
			expectBackfill: unchangedPlan,
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseValidate},
		},
		"validated blocks with nothing to compact advance to copy": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker},
			blocks:         []*block.Meta{block1, block3},
			validated:      []*block.Meta{block1, block3},
			expectBackfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, BackfillId: backfillID[:], BlockIds: blockIDs(block1, block3)},
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCompact, backfill.PhaseCopy},
		},
		"validated blocks to compact advance to compact": {
			markers:              map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker},
			blocks:               []*block.Meta{block1, block2},
			validated:            []*block.Meta{block1, block2},
			expectCompactionJobs: true,
			expectMarkers:        []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCompact},
		},
		"compact with no jobs stays while jobs are outstanding": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker, backfill.PhaseCompact: marker},
			blocks:         []*block.Meta{block3},
			hasOutstanding: true,
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCompact},
		},
		"copy skips deleted and copied blocks": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseCopy: marker},
			blocks:         []*block.Meta{block1, block2, block3},
			deleted:        []*block.Meta{block1},
			copied:         []*block.Meta{block2},
			expectBackfill: &compactorschedulerpb.PlannedBackfillJobs{JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, BackfillId: backfillID[:], BlockIds: blockIDs(block3)},
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseCopy},
		},
		"copy is unchanged while jobs are outstanding": {
			markers:        map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseCopy: marker},
			blocks:         []*block.Meta{block1},
			hasOutstanding: true,
			expectBackfill: unchangedPlan,
			expectMarkers:  []string{backfill.PhaseBackfill, backfill.PhaseCopy},
		},
		"copied blocks start cleanup": {
			markers:         map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseCopy: marker},
			blocks:          []*block.Meta{block1},
			copied:          []*block.Meta{block1},
			expectBackfill:  clearPlan,
			expectAccepted:  true,
			expectMarkers:   []string{backfill.PhaseBackfill, backfill.PhaseCopy, backfill.PhaseCleanup},
			expectClearedAt: true,
		},
		"cleanup records when its clear is accepted": {
			markers:         map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker, backfill.PhaseCleanup: marker},
			blocks:          []*block.Meta{block1},
			expectBackfill:  clearPlan,
			expectAccepted:  true,
			expectMarkers:   []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCleanup},
			expectClearedAt: true,
		},
		"cleanup keeps data within the delay": {
			markers:         map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseCleanup: clearedMarker(time.Minute)},
			blocks:          []*block.Meta{block1},
			expectBackfill:  clearPlan,
			expectMarkers:   []string{backfill.PhaseBackfill, backfill.PhaseCleanup},
			expectClearedAt: true,
		},
		"cleanup keeps data while jobs are outstanding": {
			markers:         map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseCleanup: clearedMarker(time.Hour)},
			blocks:          []*block.Meta{block1},
			hasOutstanding:  true,
			expectBackfill:  clearPlan,
			expectMarkers:   []string{backfill.PhaseBackfill, backfill.PhaseCleanup},
			expectClearedAt: true,
		},
		"cleanup deletes data and markers after the delay": {
			markers:           map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker, backfill.PhaseCompact: marker, backfill.PhaseCopy: marker, backfill.PhaseCleanup: clearedMarker(time.Hour)},
			blocks:            []*block.Meta{block1},
			copied:            []*block.Meta{block1},
			expectBackfill:    clearPlan,
			expectDataDeleted: true,
		},
		"cleanup finishes after its backfill marker was deleted": {
			markers:           map[string]backfill.Marker{backfill.PhaseCleanup: clearedMarker(time.Hour)},
			blocks:            []*block.Meta{block1},
			expectBackfill:    clearPlan,
			expectDataDeleted: true,
		},
		"orphaned backfill data is deleted": {
			markers:                map[string]backfill.Marker{backfill.PhaseBackfill: marker},
			blocks:                 []*block.Meta{block1},
			otherBlocks:            []*block.Meta{block3},
			expectBackfill:         unchangedPlan,
			expectMarkers:          []string{backfill.PhaseBackfill},
			expectOtherDataDeleted: true,
		},
		"cleanup goes before another backfill": {
			markers:            map[string]backfill.Marker{backfill.PhaseBackfill: marker, backfill.PhaseValidate: marker},
			blocks:             []*block.Meta{block1},
			otherMarkers:       map[string]backfill.Marker{backfill.PhaseCleanup: marker},
			otherBlocks:        []*block.Meta{block3},
			expectBackfill:     clearPlan,
			expectAccepted:     true,
			expectMarkers:      []string{backfill.PhaseBackfill, backfill.PhaseValidate},
			expectOtherMarkers: []string{backfill.PhaseCleanup},
		},
		"no backfill markers clears": {
			otherBlocks:            []*block.Meta{block3},
			expectBackfill:         clearPlan,
			expectOtherDataDeleted: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			bkt := objstore.NewInMemBucket()
			backfillBkt := bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix)
			dataBkt := backfillDataBucket(backfillBkt, tenant, backfillID)
			otherDataBkt := backfillDataBucket(backfillBkt, tenant, otherBackfillID)

			for phase, m := range tc.markers {
				require.NoError(t, backfill.WriteMarker(ctx, backfillBkt, tenant, backfillID, phase, m))
			}
			for phase, m := range tc.otherMarkers {
				require.NoError(t, backfill.WriteMarker(ctx, backfillBkt, tenant, otherBackfillID, phase, m))
			}
			for _, m := range tc.otherBlocks {
				uploadBlockMeta(t, otherDataBkt, m)
			}
			for _, m := range tc.blocks {
				uploadBlockMeta(t, dataBkt, m)
			}
			for _, m := range tc.validated {
				require.NoError(t, dataBkt.Upload(ctx, path.Join(block.MarkersPathname, m.ULID.String()+validatedMarkSuffix), bytes.NewReader(nil)))
			}
			for _, m := range tc.deleted {
				uploadDeletionMark(t, dataBkt, m.ULID)
			}
			for _, m := range tc.copied {
				require.NoError(t, dataBkt.Upload(ctx, path.Join(block.MarkersPathname, m.ULID.String()+copiedMarkSuffix), bytes.NewReader(nil)))
			}
			dataObjects := countObjects(t, dataBkt)

			schedulerExec, c := prepareBackfillExecutorTest(t, bkt)
			spec := &compactorschedulerpb.JobSpec{
				Tenant:                tenant,
				JobType:               compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING,
				BackfillPhasePlanning: &compactorschedulerpb.BackfillPhasePlanningJob{HasOutstandingJobs: tc.hasOutstanding},
			}
			plan, err := schedulerExec.executeBackfillPhasePlanningJob(ctx, c, t.TempDir(), spec)
			require.NoError(t, err)

			require.Equal(t, tc.expectBackfill, plan.backfill)
			require.Equal(t, tc.expectCompactionJobs, len(plan.jobs) > 0)
			for _, job := range plan.jobs {
				require.Equal(t, backfillID[:], job.Job.BackfillId)
			}
			require.Equal(t, tc.expectAccepted, plan.accepted != nil)
			if plan.accepted != nil {
				require.NoError(t, plan.accepted(ctx))
			}

			readMarkers := func(id uuid.UUID) (phases []string, clearedAt bool) {
				for _, phase := range []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCompact, backfill.PhaseCopy, backfill.PhaseCleanup} {
					m, ok, err := backfill.ReadMarker(ctx, backfillBkt, tenant, id, phase)
					require.NoError(t, err)
					if ok {
						phases = append(phases, phase)
						clearedAt = clearedAt || !m.ClearedAt.IsZero()
					}
				}
				return phases, clearedAt
			}
			markers, clearedAt := readMarkers(backfillID)
			require.Equal(t, tc.expectMarkers, markers)
			require.Equal(t, tc.expectClearedAt, clearedAt)
			otherMarkers, _ := readMarkers(otherBackfillID)
			require.Equal(t, tc.expectOtherMarkers, otherMarkers)

			if tc.expectDataDeleted {
				require.Zero(t, countObjects(t, dataBkt))
			} else {
				require.Equal(t, dataObjects, countObjects(t, dataBkt))
			}
			if tc.expectOtherDataDeleted {
				require.Zero(t, countObjects(t, otherDataBkt))
			} else {
				require.Equal(t, len(tc.otherBlocks), countObjects(t, otherDataBkt))
			}
		})
	}
}

func countObjects(t *testing.T, bkt objstore.Bucket) int {
	t.Helper()
	count := 0
	require.NoError(t, bkt.Iter(context.Background(), "", func(string) error {
		count++
		return nil
	}, objstore.WithRecursiveIter()))
	return count
}

func TestSchedulerExecutor_ExecuteBackfillValidateJob(t *testing.T) {
	const tenant = "test-tenant"
	backfillID := uuid.New()

	cases := map[string]struct {
		setupBlock     func(t *testing.T, bkt objstore.Bucket) ulid.ULID
		cleanupMarker  *backfill.Marker
		expectStatus   compactorschedulerpb.UpdateType
		expectCleanup  *backfill.Marker
		expectValidate bool
	}{
		"valid block is marked validated": {
			setupBlock: func(t *testing.T, bkt objstore.Bucket) ulid.ULID {
				return createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
			},
			expectStatus:   compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectValidate: true,
		},
		"corrupt block aborts the backfill": {
			setupBlock: func(t *testing.T, bkt objstore.Bucket) ulid.ULID {
				blockID := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
				dataBkt := backfillDataBucket(bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix), tenant, backfillID)
				require.NoError(t, dataBkt.Upload(t.Context(), path.Join(blockID.String(), block.IndexFilename), bytes.NewReader([]byte("corrupt"))))
				return blockID
			},
			expectStatus:  compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectCleanup: &backfill.Marker{},
		},
		"block without meta.json aborts the backfill": {
			setupBlock: func(t *testing.T, bkt objstore.Bucket) ulid.ULID {
				blockID := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
				dataBkt := backfillDataBucket(bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix), tenant, backfillID)
				require.NoError(t, dataBkt.Delete(t.Context(), path.Join(blockID.String(), block.MetaFilename)))
				return blockID
			},
			expectStatus:  compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectCleanup: &backfill.Marker{},
		},
		"aborting keeps an existing cleanup marker": {
			setupBlock: func(t *testing.T, bkt objstore.Bucket) ulid.ULID {
				blockID := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
				dataBkt := backfillDataBucket(bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix), tenant, backfillID)
				require.NoError(t, dataBkt.Delete(t.Context(), path.Join(blockID.String(), block.MetaFilename)))
				return blockID
			},
			cleanupMarker: &backfill.Marker{ClearedAt: time.Date(2026, 11, 25, 14, 30, 0, 0, time.UTC)},
			expectStatus:  compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectCleanup: &backfill.Marker{ClearedAt: time.Date(2026, 11, 25, 14, 30, 0, 0, time.UTC)},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			bkt := objstore.NewInMemBucket()
			backfillBkt := bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix)
			dataBkt := backfillDataBucket(backfillBkt, tenant, backfillID)
			blockID := tc.setupBlock(t, bkt)
			if tc.cleanupMarker != nil {
				require.NoError(t, backfill.WriteMarker(ctx, backfillBkt, tenant, backfillID, backfill.PhaseCleanup, *tc.cleanupMarker))
			}

			schedulerExec, c := prepareBackfillExecutorTest(t, bkt)
			spec := &compactorschedulerpb.JobSpec{
				Tenant:        tenant,
				JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE,
				BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID[:], BlockId: blockID.Bytes()},
			}
			status, err := schedulerExec.executeBackfillValidateJob(ctx, c, t.TempDir(), spec)
			if tc.expectStatus == compactorschedulerpb.UPDATE_TYPE_COMPLETE {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tc.expectStatus, status)

			validated, err := dataBkt.Exists(ctx, backfillBlockMarkPath(blockID, validatedMarkSuffix))
			require.NoError(t, err)
			require.Equal(t, tc.expectValidate, validated)

			cleanup, ok, err := backfill.ReadMarker(ctx, backfillBkt, tenant, backfillID, backfill.PhaseCleanup)
			require.NoError(t, err)
			if tc.expectCleanup == nil {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.True(t, tc.expectCleanup.ClearedAt.Equal(cleanup.ClearedAt))
			}
		})
	}
}

func TestSchedulerExecutor_ExecuteBackfillCopyJob(t *testing.T) {
	const tenant = "test-tenant"
	backfillID := uuid.New()

	cases := map[string]struct {
		removeMeta   bool
		expectStatus compactorschedulerpb.UpdateType
	}{
		"block is copied to blocks storage": {
			expectStatus: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		},
		"block without meta.json is abandoned": {
			removeMeta:   true,
			expectStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			bkt := objstore.NewInMemBucket()
			dataBkt := backfillDataBucket(bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix), tenant, backfillID)
			userBkt := bucket.NewUserBucketClient(tenant, bkt, nil)

			blockID := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
			// Blocks that reach copy list their files, which snapshotted test blocks don't
			meta, err := block.DownloadMeta(ctx, log.NewNopLogger(), dataBkt, blockID)
			require.NoError(t, err)
			require.NoError(t, dataBkt.Iter(ctx, blockID.String(), func(name string) error {
				attrs, err := dataBkt.Attributes(ctx, name)
				if err != nil {
					return err
				}
				if relPath := strings.TrimPrefix(name, blockID.String()+"/"); relPath != block.MetaFilename {
					meta.Thanos.Files = append(meta.Thanos.Files, block.File{RelPath: relPath, SizeBytes: attrs.Size})
				}
				return nil
			}, objstore.WithRecursiveIter()))
			uploadBlockMeta(t, dataBkt, &meta)
			if tc.removeMeta {
				require.NoError(t, dataBkt.Delete(ctx, path.Join(blockID.String(), block.MetaFilename)))
			}

			schedulerExec, c := prepareBackfillExecutorTest(t, bkt)
			spec := &compactorschedulerpb.JobSpec{
				Tenant:        tenant,
				JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_COPY,
				BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID[:], BlockId: blockID.Bytes()},
			}
			status, err := schedulerExec.executeBackfillCopyJob(ctx, c, spec)
			require.Equal(t, tc.expectStatus, status)

			copied, markErr := dataBkt.Exists(ctx, backfillBlockMarkPath(blockID, copiedMarkSuffix))
			require.NoError(t, markErr)
			if tc.expectStatus != compactorschedulerpb.UPDATE_TYPE_COMPLETE {
				require.ErrorIs(t, err, errBackfillBlockHasNoMeta)
				require.False(t, copied)
				require.Zero(t, countObjects(t, userBkt))
				return
			}
			require.NoError(t, err)
			require.True(t, copied)

			// Every file of the block is copied as is
			require.Equal(t, len(meta.Thanos.Files)+1, countObjects(t, userBkt))
			for _, name := range []string{block.MetaFilename, block.IndexFilename, "chunks/000001"} {
				require.Equal(t, readObject(t, dataBkt, path.Join(blockID.String(), name)), readObject(t, userBkt, path.Join(blockID.String(), name)))
			}
		})
	}
}

func TestSchedulerExecutor_ExecuteCompactionJob_Backfill(t *testing.T) {
	const tenant = "test-tenant"
	backfillID := uuid.New()

	bkt := objstore.NewInMemBucket()
	dataBkt := backfillDataBucket(bucket.NewPrefixedBucketClient(bkt, backfill.BucketPrefix), tenant, backfillID)
	block1 := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 10, 20)
	block2 := createBackfillTSDBBlock(t, bkt, tenant, backfillID, 20, 30)
	dataObjects := countObjects(t, dataBkt)

	schedulerExec, c := prepareBackfillExecutorTest(t, bkt)
	spec := &compactorschedulerpb.JobSpec{
		Tenant:  tenant,
		JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION,
		Job:     &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{block1.Bytes(), block2.Bytes()}, BackfillId: backfillID[:]},
	}
	status, err := schedulerExec.executeCompactionJob(t.Context(), c, t.TempDir(), &compactorschedulerpb.JobKey{Id: "test-job-id"}, spec)
	require.NoError(t, err)
	require.Equal(t, compactorschedulerpb.UPDATE_TYPE_COMPLETE, status)

	// The compacted block and the deletion marks of its sources stay within the backfill
	require.Greater(t, countObjects(t, dataBkt), dataObjects)
	for _, blockID := range []ulid.ULID{block1, block2} {
		marked, err := dataBkt.Exists(t.Context(), block.DeletionMarkFilepath(blockID))
		require.NoError(t, err)
		require.True(t, marked)
	}
	require.Zero(t, countObjects(t, bucket.NewUserBucketClient(tenant, bkt, nil)))
}

func prepareBackfillExecutorTest(t *testing.T, bkt objstore.Bucket) (*schedulerExecutor, *MultitenantCompactor) {
	t.Helper()
	cfg := makeTestCompactorConfig(t)
	mockCfg := newMockConfigProvider()
	c := prepareCompactorForExecutorTest(t, cfg, bkt, mockCfg)
	// Starting the compactor wraps its bucket this way, which writes the markers that backfill planning reads
	c.bucketClient = block.BucketWithGlobalMarkers(bkt)
	compactor, planner, err := splitAndMergeCompactorFactory(t.Context(), cfg, mockCfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	c.blocksCompactorProvider = compactor
	c.blocksPlanner = planner
	return newTestSchedulerExecutor(t, cfg, nil), c
}

func createBackfillTSDBBlock(t *testing.T, bkt objstore.Bucket, tenant string, backfillID uuid.UUID, minT, maxT int64) ulid.ULID {
	t.Helper()
	return createTSDBBlock(t, bkt, path.Join(backfill.BucketPrefix, backfill.DataPrefix(tenant, backfillID)), minT, maxT, 2, nil)
}

func readObject(t *testing.T, bkt objstore.Bucket, name string) []byte {
	t.Helper()
	r, err := bkt.Get(t.Context(), name)
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return content
}
