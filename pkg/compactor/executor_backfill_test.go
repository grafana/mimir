// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"io"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSchedulerExecutor_ExecuteBackfillCleanupJob(t *testing.T) {
	const (
		tenant      = "tenant-1"
		backfillID  = "01K5ZQ3Y8V0M6T2B4C9D7E1F3G"
		newerID     = "01K61B2N4P6R8S0T2V4W6X8Y0Z"
		blockPrefix = "01K5ZQ4A2B3C4D5E6F7G8H9J0K/"
	)

	tests := map[string]struct {
		markers   map[string]string // phase to backfill ID
		remaining []string
	}{
		"deletes the data and markers of the backfill": {
			markers: map[string]string{
				backfill.PhaseBackfill: backfillID,
				backfill.PhaseValidate: backfillID,
				backfill.PhaseCompact:  backfillID,
				backfill.PhaseCopy:     backfillID,
				backfill.PhaseCleanup:  backfillID,
			},
		},
		"keeps the markers of a newer backfill": {
			markers: map[string]string{
				backfill.PhaseBackfill: newerID,
				backfill.PhaseValidate: newerID,
				backfill.PhaseCleanup:  backfillID,
			},
			remaining: []string{
				backfill.PhaseMarkerPath(backfill.PhaseBackfill, tenant),
				backfill.PhaseMarkerPath(backfill.PhaseValidate, tenant),
			},
		},
		"finishes a cleanup that was interrupted": {
			markers: map[string]string{
				backfill.PhaseCleanup: backfillID,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			for phase, id := range tc.markers {
				require.NoError(t, backfill.WriteMarker(t.Context(), bkt, phase, tenant, backfill.Marker{BackfillID: id}))
			}
			upload := func(name string) {
				require.NoError(t, bkt.Upload(t.Context(), name, bytes.NewReader([]byte("data"))))
			}
			upload(backfill.DataPrefix(backfillID, tenant) + "/" + blockPrefix + "index")
			upload(backfill.DataPrefix(backfillID, tenant) + "/" + blockPrefix + "chunks/000001")
			// The data of other backfills and tenants is never touched
			otherData := []string{
				backfill.DataPrefix(newerID, tenant) + "/" + blockPrefix + "index",
				backfill.DataPrefix(backfillID, "tenant-2") + "/" + blockPrefix + "index",
			}
			for _, name := range otherData {
				upload(name)
			}

			cfg := makeTestCompactorConfig(t)
			exec := newTestSchedulerExecutor(t, cfg, nil)
			c := &MultitenantCompactor{backfillBucketClient: bkt}
			spec := &compactorschedulerpb.JobSpec{
				Tenant:          tenant,
				JobType:         compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP,
				BackfillCleanup: &compactorschedulerpb.BackfillCleanupJob{BackfillId: backfillID},
			}

			status, err := exec.executeBackfillCleanupJob(t.Context(), c, spec)
			require.NoError(t, err)
			require.Equal(t, compactorschedulerpb.UPDATE_TYPE_COMPLETE, status)

			var objects []string
			require.NoError(t, bkt.Iter(t.Context(), "", func(name string) error {
				objects = append(objects, name)
				return nil
			}, objstore.WithRecursiveIter()))
			require.ElementsMatch(t, append(tc.remaining, otherData...), objects)
		})
	}
}

func TestSchedulerExecutor_ExecutePlanningJob_Backfill(t *testing.T) {
	const (
		tenant     = "tenant-1"
		backfillID = "01K5ZQ3Y8V0M6T2B4C9D7E1F3G"
		// 2025-09-25T16:00:00Z, the start of a 2h range
		rangeStart = int64(1758816000000)
	)

	tests := map[string]struct {
		backfillID string
		expectJobs int
	}{
		"cell mode waits for recently uploaded blocks": {
			backfillID: "",
			expectJobs: 0,
		},
		"backfill mode does not wait for recently uploaded blocks": {
			backfillID: backfillID,
			expectJobs: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)
			cfg.CompactionWaitPeriod = time.Hour
			cfg.SchedulerClientConfig.BackfillModeEnabled = tc.backfillID != ""
			cfg.SchedulerClientConfig.LastContactTimeout = 5 * time.Minute

			bkt := objstore.NewInMemBucket()
			mockCfg := newMockConfigProvider()
			exec := newTestSchedulerExecutor(t, cfg, nil)
			c := prepareCompactorForExecutorTest(t, cfg, bkt, mockCfg)
			compactor, planner, err := splitAndMergeCompactorFactory(t.Context(), cfg, mockCfg, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)
			c.blocksCompactorProvider = compactor
			c.blocksPlanner = planner

			// Two level 1 blocks that fill the same 2h range, uploaded just now
			blockPrefix := tenant
			if tc.backfillID != "" {
				blockPrefix = backfill.DataPrefix(tc.backfillID, tenant)
			}
			createTSDBBlock(t, bkt, blockPrefix, rangeStart, rangeStart+time.Hour.Milliseconds(), 2, nil)
			createTSDBBlock(t, bkt, blockPrefix, rangeStart+time.Hour.Milliseconds(), rangeStart+2*time.Hour.Milliseconds(), 2, nil)

			jobs, err := exec.executePlanningJob(t.Context(), c, t.TempDir(), exec.jobBucket(c, tenant, tc.backfillID), tenant, tc.backfillID)
			require.NoError(t, err)
			require.Len(t, jobs, tc.expectJobs)
			for _, j := range jobs {
				require.Equal(t, tc.backfillID, j.GetCompaction().BackfillId)
			}
		})
	}
}

func TestSchedulerExecutor_ExecuteBackfillValidateJob(t *testing.T) {
	const (
		tenant     = "tenant-1"
		backfillID = "01K5ZQ3Y8V0M6T2B4C9D7E1F3G"
		// 2025-09-25T16:00:00Z
		minT = int64(1758816000000)
	)

	tests := map[string]struct {
		prepare         func(t *testing.T, bkt objstore.Bucket, blockDir string) // blockDir holds meta.json when called
		expectStatus    compactorschedulerpb.UpdateType
		expectErr       bool
		expectValidated bool // meta.json exists and uploading-meta.json does not
		expectCleanup   bool
		expectUnchanged bool // meta.json and uploading-meta.json are as prepared
	}{
		"valid uploaded block": {
			prepare:         func(t *testing.T, bkt objstore.Bucket, blockDir string) { moveToUploadingMeta(t, bkt, blockDir) },
			expectStatus:    compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectValidated: true,
		},
		"already validated block": {
			prepare:         func(*testing.T, objstore.Bucket, string) {},
			expectStatus:    compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectValidated: true,
		},
		"corrupt block": {
			prepare: func(t *testing.T, bkt objstore.Bucket, blockDir string) {
				moveToUploadingMeta(t, bkt, blockDir)
				require.NoError(t, bkt.Delete(t.Context(), path.Join(blockDir, block.IndexFilename)))
			},
			expectStatus:    compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectErr:       true,
			expectCleanup:   true,
			expectUnchanged: true,
		},
		"block without meta files": {
			prepare: func(t *testing.T, bkt objstore.Bucket, blockDir string) {
				require.NoError(t, bkt.Delete(t.Context(), path.Join(blockDir, block.MetaFilename)))
			},
			expectStatus:    compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectErr:       true,
			expectCleanup:   true,
			expectUnchanged: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.BackfillModeEnabled = true
			cfg.SchedulerClientConfig.LastContactTimeout = 5 * time.Minute

			bkt := objstore.NewInMemBucket()
			exec := newTestSchedulerExecutor(t, cfg, nil)
			c := prepareCompactorForExecutorTest(t, cfg, bkt, newMockConfigProvider())

			dataPrefix := backfill.DataPrefix(backfillID, tenant)
			blockID := createTSDBBlock(t, bkt, dataPrefix, minT, minT+time.Hour.Milliseconds(), 2, nil)
			blockDir := path.Join(dataPrefix, blockID.String())
			tc.prepare(t, bkt, blockDir)
			exists := func(name string) bool {
				ok, err := bkt.Exists(t.Context(), path.Join(blockDir, name))
				require.NoError(t, err)
				return ok
			}
			hadMeta, hadUploadingMeta := exists(block.MetaFilename), exists(uploadingMetaFilename)

			spec := &compactorschedulerpb.JobSpec{
				Tenant:        tenant,
				JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE,
				BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID, BlockId: blockID.Bytes()},
			}
			status, err := exec.executeBackfillValidateJob(t.Context(), c, spec)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectStatus, status)

			if tc.expectValidated {
				require.True(t, exists(block.MetaFilename))
				require.False(t, exists(uploadingMetaFilename))
			}
			if tc.expectUnchanged {
				require.Equal(t, hadMeta, exists(block.MetaFilename))
				require.Equal(t, hadUploadingMeta, exists(uploadingMetaFilename))
			}
			m, ok, err := backfill.ReadMarker(t.Context(), bkt, backfill.PhaseCleanup, tenant)
			require.NoError(t, err)
			require.Equal(t, tc.expectCleanup, ok)
			if ok {
				require.Equal(t, backfillID, m.BackfillID)
			}
		})
	}
}

func TestSchedulerExecutor_ExecuteBackfillCopyJob(t *testing.T) {
	const (
		tenant     = "tenant-1"
		backfillID = "01K5ZQ3Y8V0M6T2B4C9D7E1F3G"
		// 2025-09-25T16:00:00Z
		minT = int64(1758816000000)
	)

	cfg := makeTestCompactorConfig(t)
	cfg.SchedulerClientConfig.BackfillModeEnabled = true
	cfg.SchedulerClientConfig.LastContactTimeout = 5 * time.Minute

	bkt := objstore.NewInMemBucket()
	exec := newTestSchedulerExecutor(t, cfg, nil)
	c := prepareCompactorForExecutorTest(t, cfg, bkt, newMockConfigProvider())
	c.backfillBucketClient = objstore.NewInMemBucket()

	// block.Upload fills in the file list, like the uploads and compactions of a backfill
	dir := t.TempDir()
	series := []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "node"),
		labels.FromStrings("__name__", "up", "job", "api"),
		labels.FromStrings("__name__", "up", "job", "db"),
	}
	blockID, err := block.CreateBlock(t.Context(), dir, series, 120, minT, minT+2*time.Hour.Milliseconds(), labels.EmptyLabels())
	require.NoError(t, err)
	srcBkt := exec.jobBucket(c, tenant, backfillID)
	meta, err := block.Upload(t.Context(), log.NewNopLogger(), srcBkt, filepath.Join(dir, blockID.String()), nil)
	require.NoError(t, err)

	spec := &compactorschedulerpb.JobSpec{
		Tenant:        tenant,
		JobType:       compactorschedulerpb.JOB_TYPE_BACKFILL_COPY,
		BackfillBlock: &compactorschedulerpb.BackfillBlockJob{BackfillId: backfillID, BlockId: blockID.Bytes()},
	}
	status, err := exec.executeBackfillCopyJob(t.Context(), c, spec)
	require.NoError(t, err)
	require.Equal(t, compactorschedulerpb.UPDATE_TYPE_COMPLETE, status)

	read := func(bkt objstore.BucketReader, name string) []byte {
		r, err := bkt.Get(t.Context(), name)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		content, err := io.ReadAll(r)
		require.NoError(t, err)
		return content
	}
	dstBkt := bucket.NewPrefixedBucketClient(bkt, tenant)
	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}
		name := path.Join(blockID.String(), f.RelPath)
		require.Equal(t, read(srcBkt, name), read(dstBkt, name), name)
	}
	copiedMeta, err := block.DownloadMeta(t.Context(), log.NewNopLogger(), dstBkt, blockID)
	require.NoError(t, err)
	require.Equal(t, *meta, copiedMeta)

	copied, err := srcBkt.Exists(t.Context(), copiedMarkFilepath(blockID))
	require.NoError(t, err)
	require.True(t, copied)
}

// moveToUploadingMeta makes a block look like an upload that has not been validated yet
func moveToUploadingMeta(t *testing.T, bkt objstore.Bucket, blockDir string) {
	t.Helper()
	metaPath := path.Join(blockDir, block.MetaFilename)
	r, err := bkt.Get(t.Context(), metaPath)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(t.Context(), path.Join(blockDir, uploadingMetaFilename), r))
	require.NoError(t, r.Close())
	require.NoError(t, bkt.Delete(t.Context(), metaPath))
}

func TestSchedulerExecutor_ExecuteBackfillPhasePlanningJob(t *testing.T) {
	const (
		tenant     = "tenant-1"
		backfillID = "01K5ZQ3Y8V0M6T2B4C9D7E1F3G"
		newerID    = "01K61B2N4P6R8S0T2V4W6X8Y0Z"
		// 2025-09-25T16:00:00Z, the start of a 2h range
		rangeStart = int64(1758816000000)
	)

	type blockState int
	const (
		validated  blockState = iota // has meta.json
		uploaded                     // has uploading-meta.json
		incomplete                   // has neither
		copied                       // has meta.json and a copied marker
		deleted                      // has meta.json and a deletion mark
	)

	tests := map[string]struct {
		markers         map[string]string // phase to backfill ID
		blocks          []blockState      // blocks of the backfill in the backfill marker, each covering the next hour
		hasOutstanding  bool
		expectUnchanged bool
		expectCleanupID string
		expectValidate  bool // a validate job for each uploaded block
		expectCompact   bool
		expectCopy      bool // a copy job for each validated block
		expectMarkers   map[string]string
	}{
		"no markers": {
			expectMarkers: map[string]string{},
		},
		"uploads ongoing": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID},
			expectUnchanged: true,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID},
		},
		"cleanup of the current backfill": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCleanup: backfillID},
			expectCleanupID: backfillID,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCleanup: backfillID},
		},
		"interrupted cleanup": {
			markers:         map[string]string{backfill.PhaseCleanup: backfillID},
			expectCleanupID: backfillID,
			expectMarkers:   map[string]string{backfill.PhaseCleanup: backfillID},
		},
		"stale cleanup of an older backfill": {
			markers:         map[string]string{backfill.PhaseBackfill: newerID, backfill.PhaseCleanup: backfillID},
			expectUnchanged: true,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: newerID},
		},
		"validation with outstanding jobs": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
			blocks:          []blockState{uploaded},
			hasOutstanding:  true,
			expectUnchanged: true,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
		},
		"blocks awaiting validation": {
			markers:        map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
			blocks:         []blockState{validated, uploaded, uploaded},
			expectValidate: true,
			expectMarkers:  map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
		},
		"incomplete upload": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
			blocks:          []blockState{uploaded, incomplete},
			expectCleanupID: backfillID,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCleanup: backfillID},
		},
		"validation finished": {
			markers:       map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID},
			blocks:        []blockState{validated, validated},
			expectCompact: true,
			expectMarkers: map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID},
		},
		"copy with outstanding jobs": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID},
			blocks:          []blockState{validated},
			hasOutstanding:  true,
			expectUnchanged: true,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID},
		},
		"blocks awaiting copy": {
			markers:       map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID},
			blocks:        []blockState{validated, copied, deleted, incomplete, validated},
			expectCopy:    true,
			expectMarkers: map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID},
		},
		"copy finished": {
			markers:         map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID},
			blocks:          []blockState{copied, deleted},
			expectCleanupID: backfillID,
			expectMarkers:   map[string]string{backfill.PhaseBackfill: backfillID, backfill.PhaseValidate: backfillID, backfill.PhaseCompact: backfillID, backfill.PhaseCopy: backfillID, backfill.PhaseCleanup: backfillID},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.BackfillModeEnabled = true
			cfg.SchedulerClientConfig.LastContactTimeout = 5 * time.Minute

			bkt := objstore.NewInMemBucket()
			mockCfg := newMockConfigProvider()
			exec := newTestSchedulerExecutor(t, cfg, nil)
			c := prepareCompactorForExecutorTest(t, cfg, bkt, mockCfg)
			compactor, planner, err := splitAndMergeCompactorFactory(t.Context(), cfg, mockCfg, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)
			c.blocksCompactorProvider = compactor
			c.blocksPlanner = planner

			for phase, id := range tc.markers {
				require.NoError(t, backfill.WriteMarker(t.Context(), bkt, phase, tenant, backfill.Marker{BackfillID: id}))
			}

			dataPrefix := backfill.DataPrefix(backfillID, tenant)
			var uploadedIDs, validatedIDs [][]byte
			for i, state := range tc.blocks {
				minT := rangeStart + int64(i)*time.Hour.Milliseconds()
				id := createTSDBBlock(t, bkt, dataPrefix, minT, minT+time.Hour.Milliseconds(), 2, nil)
				metaPath := path.Join(dataPrefix, id.String(), block.MetaFilename)
				switch state {
				case validated:
					validatedIDs = append(validatedIDs, id.Bytes())
				case copied:
					require.NoError(t, bkt.Upload(t.Context(), path.Join(dataPrefix, copiedMarkFilepath(id)), bytes.NewReader(nil)))
				case deleted:
					require.NoError(t, bkt.Upload(t.Context(), path.Join(dataPrefix, block.DeletionMarkFilepath(id)), bytes.NewReader([]byte("{}"))))
				case uploaded:
					r, err := bkt.Get(t.Context(), metaPath)
					require.NoError(t, err)
					require.NoError(t, bkt.Upload(t.Context(), path.Join(dataPrefix, id.String(), uploadingMetaFilename), r))
					require.NoError(t, r.Close())
					require.NoError(t, bkt.Delete(t.Context(), metaPath))
					uploadedIDs = append(uploadedIDs, id.Bytes())
				case incomplete:
					require.NoError(t, bkt.Delete(t.Context(), metaPath))
				}
			}

			spec := &compactorschedulerpb.JobSpec{
				Tenant:                tenant,
				JobType:               compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING,
				BackfillPhasePlanning: &compactorschedulerpb.BackfillPhasePlanningJob{HasOutstandingJobs: tc.hasOutstanding},
			}
			req, err := exec.executeBackfillPhasePlanningJob(t.Context(), c, t.TempDir(), spec)
			require.NoError(t, err)

			require.Equal(t, tc.expectUnchanged, req.Unchanged)
			var cleanupIDs, validateBlocks, copyBlocks [][]byte
			var compactionJobs int
			for _, j := range req.Jobs {
				switch job := j.Job.(type) {
				case *compactorschedulerpb.PlannedJob_BackfillCleanup:
					cleanupIDs = append(cleanupIDs, []byte(job.BackfillCleanup.BackfillId))
				case *compactorschedulerpb.PlannedJob_BackfillValidate:
					require.Equal(t, backfillID, job.BackfillValidate.BackfillId)
					validateBlocks = append(validateBlocks, job.BackfillValidate.BlockId)
				case *compactorschedulerpb.PlannedJob_BackfillCopy:
					require.Equal(t, backfillID, job.BackfillCopy.BackfillId)
					copyBlocks = append(copyBlocks, job.BackfillCopy.BlockId)
				case *compactorschedulerpb.PlannedJob_Compaction:
					require.Equal(t, backfillID, job.Compaction.BackfillId)
					compactionJobs++
				default:
					require.Failf(t, "unexpected planned job", "%T", j.Job)
				}
			}
			if tc.expectCleanupID != "" {
				require.Equal(t, [][]byte{[]byte(tc.expectCleanupID)}, cleanupIDs)
			} else {
				require.Empty(t, cleanupIDs)
			}
			if tc.expectValidate {
				require.ElementsMatch(t, uploadedIDs, validateBlocks)
			} else {
				require.Empty(t, validateBlocks)
			}
			if tc.expectCopy {
				require.ElementsMatch(t, validatedIDs, copyBlocks)
			} else {
				require.Empty(t, copyBlocks)
			}
			if tc.expectCompact {
				require.Positive(t, compactionJobs)
			} else {
				require.Zero(t, compactionJobs)
			}

			markers := map[string]string{}
			for _, phase := range []string{backfill.PhaseBackfill, backfill.PhaseValidate, backfill.PhaseCompact, backfill.PhaseCopy, backfill.PhaseCleanup} {
				m, ok, err := backfill.ReadMarker(t.Context(), bkt, phase, tenant)
				require.NoError(t, err)
				if ok {
					markers[phase] = m.BackfillID
				}
			}
			require.Equal(t, tc.expectMarkers, markers)
		})
	}
}

func TestSchedulerExecutor_ExecuteBackfillCleanupJob_MissingBackfillID(t *testing.T) {
	cfg := makeTestCompactorConfig(t)
	exec := newTestSchedulerExecutor(t, cfg, nil)
	c := &MultitenantCompactor{backfillBucketClient: objstore.NewInMemBucket()}
	spec := &compactorschedulerpb.JobSpec{
		Tenant:  "tenant-1",
		JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP,
	}

	status, err := exec.executeBackfillCleanupJob(t.Context(), c, spec)
	require.ErrorIs(t, err, errBackfillJobHasNoID)
	require.Equal(t, compactorschedulerpb.UPDATE_TYPE_ABANDON, status)
}
