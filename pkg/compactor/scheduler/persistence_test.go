// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

var testBlockIDs = [][]byte{ulid.MustNewDefault(time.Now()).Bytes()}

func setupBboltManager(t *testing.T) *BboltJobPersistenceManager {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	mgr, err := openBboltJobPersistenceManager(dbPath, log.NewNopLogger())
	require.NoError(t, err)
	return mgr
}

func newTestCompactionJob(id string) TrackedJob {
	job := NewTrackedCompactionJob(
		id,
		&CompactionJob{blocks: nil, isSplit: false},
		1,
		time.Now(),
		clock.New(),
	)
	return job
}

func newTestPlanJob() TrackedJob {
	job := NewTrackedPlanJob(time.Now(), clock.New())
	return job
}

func TestJobPersistenceManagerFactory(t *testing.T) {
	tempDir := t.TempDir()

	tests := map[string]struct {
		cfg         Config
		expectError bool
		errorMsg    string
	}{
		"bbolt persistence": {
			cfg: Config{
				persistenceType: "bbolt",
				bboltPath:       filepath.Join(tempDir, "test.db"),
			},
			expectError: false,
		},
		"nop persistence": {
			cfg: Config{
				persistenceType: "none",
			},
			expectError: false,
		},
		"unrecognized persistence type": {
			cfg: Config{
				persistenceType: "invalid",
			},
			expectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mgr, err := jobPersistenceManagerFactory(tc.cfg, log.NewNopLogger())

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, mgr)
			err = mgr.Close()
			require.NoError(t, err)
		})
	}
}

func TestBboltJobPersistenceManager_InitializeTenant(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	tenant := "tenant"
	persister, err := mgr.InitializeTenant(tenant)
	require.NoError(t, err)
	require.NotNil(t, persister)

	// Verify the bucket was created
	err = mgr.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(tenant))
		require.NotNil(t, bucket)
		return nil
	})
	require.NoError(t, err)
}

func TestBboltJobPersistenceManager_DeleteTenant(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	tenant := "tenant"
	_, err := mgr.InitializeTenant(tenant)
	require.NoError(t, err)

	err = mgr.DeleteTenant(tenant)
	require.NoError(t, err)

	// Verify the bucket was deleted
	err = mgr.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(tenant))
		require.Nil(t, bucket)
		return nil
	})
	require.NoError(t, err)
}

func TestBboltJobPersistenceManager_RecoverAll(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	allowlist := util.NewAllowList(nil, nil)
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jobTrackerFactory := func(tenant string, persister JobPersister) *JobTracker {
		return NewJobTracker(persister, tenant, infiniteLeases, metrics.trackerMetricsForTenant(tenant))
	}

	// Empty recovery should succeed
	ctm, err := mgr.RecoverAll(allowlist, jobTrackerFactory)
	require.NoError(t, err)
	require.Empty(t, ctm)

	tenantPersister, err := mgr.InitializeTenant("foo")
	require.NoError(t, err)
	err = tenantPersister.WriteJob(newTestCompactionJob("id"))
	require.NoError(t, err)
	err = tenantPersister.WriteJob(newTestPlanJob())
	require.NoError(t, err)

	ctm, err = mgr.RecoverAll(allowlist, jobTrackerFactory)
	require.NoError(t, err)
	require.Len(t, ctm, 1)
	require.Contains(t, ctm, "foo")

	jobTracker := ctm["foo"]
	require.Len(t, jobTracker.incompleteJobs, 2)
	require.Contains(t, jobTracker.incompleteJobs, "id")
	require.Contains(t, jobTracker.incompleteJobs, planJobId)
}

func TestBboltJobPersister_WriteReadDelete(t *testing.T) {
	// TODO: Clean up
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	persister, err := mgr.InitializeTenant("tenant")
	require.NoError(t, err)

	now := time.Now()
	writtenCompactionJob := &TrackedCompactionJob{
		baseTrackedJob: baseTrackedJob{
			id:           "job1",
			creationTime: now,
			statusTime:   now.Add(10 * time.Second),
			numLeases:    1,
			epoch:        234,
		},
		value: &CompactionJob{blocks: testBlockIDs, isSplit: true},
		order: 1,
	}

	err = persister.WriteJob(writtenCompactionJob)
	require.NoError(t, err)

	// Read it back
	var jobs []TrackedJob
	readJobs := func() error {
		return mgr.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("tenant"))
			if b == nil {
				return errors.New("bucket should not be missing")
			}
			jobs = jobsFromTenantBucket("tenant", b, mgr.logger)
			return nil
		})
	}

	err = readJobs()
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	readJob := jobs[0].(*TrackedCompactionJob)
	require.Equal(t, writtenCompactionJob.id, readJob.id)
	require.Equal(t, writtenCompactionJob.creationTime.Unix(), readJob.creationTime.Unix())
	require.Equal(t, writtenCompactionJob.status, readJob.status)
	require.Equal(t, writtenCompactionJob.statusTime.Unix(), readJob.statusTime.Unix())
	require.Equal(t, writtenCompactionJob.numLeases, readJob.numLeases)
	require.Equal(t, writtenCompactionJob.epoch, readJob.epoch)

	require.Equal(t, writtenCompactionJob.value.blocks, readJob.value.blocks)
	require.Equal(t, writtenCompactionJob.value.isSplit, readJob.value.isSplit)

	require.Equal(t, writtenCompactionJob.order, readJob.order)

	// Delete it, then expect it to be gone
	err = persister.DeleteJob(writtenCompactionJob)
	require.NoError(t, err)
	err = readJobs()
	require.NoError(t, err)
	require.Empty(t, jobs)

	writtenPlanJob := &TrackedPlanJob{
		baseTrackedJob: baseTrackedJob{
			id:           planJobId,
			creationTime: now,
			status:       compactorschedulerpb.STORED_JOB_STATUS_LEASED,
			statusTime:   now.Add(10 * time.Second),
			numLeases:    1,
			epoch:        234,
		},
	}

	err = persister.WriteJob(writtenPlanJob)
	require.NoError(t, err)

	err = readJobs()
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	readPlanJob := jobs[0].(*TrackedPlanJob)

	require.Equal(t, writtenPlanJob.id, readPlanJob.id)
	require.Equal(t, writtenPlanJob.creationTime.Unix(), readPlanJob.creationTime.Unix())
	require.Equal(t, writtenPlanJob.status, readPlanJob.status)
	require.Equal(t, writtenPlanJob.statusTime.Unix(), readPlanJob.statusTime.Unix())
	require.Equal(t, writtenPlanJob.numLeases, readPlanJob.numLeases)
	require.Equal(t, writtenPlanJob.epoch, readPlanJob.epoch)

	err = persister.DeleteJob(writtenPlanJob)
	require.NoError(t, err)
	err = readJobs()
	require.NoError(t, err)
	require.Empty(t, jobs)
}
