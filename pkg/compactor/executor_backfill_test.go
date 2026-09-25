// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/backfill"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
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
			c := &MultitenantCompactor{bucketClient: bkt}
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
	)

	tests := map[string]struct {
		markers         map[string]string // phase to backfill ID
		blocks          []blockState      // blocks of the backfill in the backfill marker, each covering the next hour
		hasOutstanding  bool
		expectUnchanged bool
		expectCleanupID string
		expectValidate  bool // a validate job for each uploaded block
		expectCompact   bool
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
			var uploadedIDs [][]byte
			for i, state := range tc.blocks {
				minT := rangeStart + int64(i)*time.Hour.Milliseconds()
				id := createTSDBBlock(t, bkt, dataPrefix, minT, minT+time.Hour.Milliseconds(), 2, nil)
				metaPath := path.Join(dataPrefix, id.String(), block.MetaFilename)
				switch state {
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
			var cleanupIDs, validateBlocks [][]byte
			var compactionJobs int
			for _, j := range req.Jobs {
				switch job := j.Job.(type) {
				case *compactorschedulerpb.PlannedJob_BackfillCleanup:
					cleanupIDs = append(cleanupIDs, []byte(job.BackfillCleanup.BackfillId))
				case *compactorschedulerpb.PlannedJob_BackfillValidate:
					require.Equal(t, backfillID, job.BackfillValidate.BackfillId)
					validateBlocks = append(validateBlocks, job.BackfillValidate.BlockId)
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
	c := &MultitenantCompactor{bucketClient: objstore.NewInMemBucket()}
	spec := &compactorschedulerpb.JobSpec{
		Tenant:  "tenant-1",
		JobType: compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP,
	}

	status, err := exec.executeBackfillCleanupJob(t.Context(), c, spec)
	require.ErrorIs(t, err, errBackfillJobHasNoID)
	require.Equal(t, compactorschedulerpb.UPDATE_TYPE_ABANDON, status)
}
