// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

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

func newTestCompactionJob(id string) *Job[*CompactionJob] {
	job := &Job[*CompactionJob]{
		id:           id,
		value:        &CompactionJob{blocks: nil, isSplit: false},
		creationTime: time.Now(),
		numLeases:    0,
		epoch:        123,
	}
	return job
}

func newTestPlanJob(id string) *Job[struct{}] {
	job := &Job[struct{}]{
		id:           id,
		value:        struct{}{},
		creationTime: time.Now(),
		numLeases:    1,
		epoch:        345,
	}
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

func TestBboltJobPersistenceManager_InitializePlanning(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	// Initialize planning
	persister, err := mgr.InitializePlanning()
	require.NoError(t, err)
	require.NotNil(t, persister)

	// Verify the bucket was created
	err = mgr.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("planning"))
		require.NotNil(t, bucket)
		return nil
	})
	require.NoError(t, err)
}

func TestBboltJobPersistenceManager_InitializeTenant(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	persister, err := mgr.InitializeTenant("tenant1")
	require.NoError(t, err)
	require.NotNil(t, persister)

	// Verify the bucket was created with a prefix
	err = mgr.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("utenant1"))
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
		bucket := tx.Bucket(mgr.tenantName(tenant))
		require.Nil(t, bucket)
		return nil
	})
	require.NoError(t, err)

	// Deleting a tenant that does not exist should not error
	err = mgr.DeleteTenant("missing")
	require.NoError(t, err)
}

func TestBboltJobPersistenceManager_RecoverAll(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	allowlist := util.NewAllowList(nil, nil)
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	planTracker := NewJobTracker(&NopJobPersister[struct{}]{}, InfiniteLeases, "plan", metrics)
	compactionTrackerFactory := func(persister JobPersister[*CompactionJob]) *JobTracker[*CompactionJob] {
		return NewJobTracker(persister, InfiniteLeases, "compaction", metrics)
	}

	// Empty recovery should succeed
	ctm, err := mgr.RecoverAll(allowlist, planTracker, compactionTrackerFactory)
	require.NoError(t, err)
	require.Empty(t, ctm)

	tenantPersister, err := mgr.InitializeTenant("foo")
	require.NoError(t, err)
	err = tenantPersister.WriteJob(newTestCompactionJob("id"))
	require.NoError(t, err)

	planPersister, err := mgr.InitializePlanning()
	require.NoError(t, err)
	err = planPersister.WriteJob(newTestPlanJob("bar"))
	require.NoError(t, err)

	// Before recovery the plan tracker should be empty
	require.Empty(t, planTracker.allJobs)
	ctm, err = mgr.RecoverAll(allowlist, planTracker, compactionTrackerFactory)
	require.NoError(t, err)

	require.Len(t, ctm, 1)
	require.Contains(t, ctm, "foo")
	require.Len(t, ctm["foo"].allJobs, 1)
	require.Contains(t, ctm["foo"].allJobs, "id")

	require.Len(t, planTracker.allJobs, 1)
	require.Contains(t, planTracker.allJobs, "bar")
}

func TestBboltJobPersister_WriteReadDelete(t *testing.T) {
	mgr := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	t.Run("compaction job", func(t *testing.T) {
		persister, err := mgr.InitializeTenant("tenant")
		require.NoError(t, err)

		now := time.Now()
		written := &Job[*CompactionJob]{
			id:                "job1",
			value:             &CompactionJob{blocks: testBlockIDs, isSplit: true},
			creationTime:      now,
			leaseCreationTime: now.Add(10 * time.Second),
			numLeases:         1,
			epoch:             234,
		}

		err = persister.WriteJob(written)
		require.NoError(t, err)

		// Read it back
		var jobs []*Job[*CompactionJob]
		p := persister.(*BboltJobPersister[*CompactionJob])
		readJobs := func() error {
			return p.db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket(p.name)
				if b == nil {
					return errors.New("bucket should not be missing")
				}
				jobs, err = p.recover(b)
				return err
			})
		}
		err = readJobs()

		require.NoError(t, err)
		require.Len(t, jobs, 1)
		readJob := jobs[0]

		require.Equal(t, written.id, readJob.id)
		require.Equal(t, written.value.blocks, readJob.value.blocks)
		require.Equal(t, written.value.isSplit, readJob.value.isSplit)
		require.Equal(t, written.creationTime.Unix(), readJob.creationTime.Unix())
		require.Equal(t, written.leaseCreationTime.Unix(), readJob.leaseCreationTime.Unix())
		require.Equal(t, written.numLeases, readJob.numLeases)
		require.Equal(t, written.epoch, readJob.epoch)

		err = persister.DeleteJob(written)
		require.NoError(t, err)

		err = readJobs()
		require.NoError(t, err)
		require.Empty(t, jobs)
	})

	t.Run("plan job", func(t *testing.T) {
		persister, err := mgr.InitializePlanning()
		require.NoError(t, err)

		now := time.Now()
		written := &Job[struct{}]{
			id:                "job1",
			value:             struct{}{},
			creationTime:      now,
			leaseCreationTime: now.Add(10 * time.Second),
			numLeases:         1,
			epoch:             234,
		}

		err = persister.WriteJob(written)
		require.NoError(t, err)
		p := persister.(*BboltJobPersister[struct{}])
		var jobs []*Job[struct{}]
		// Read it back
		readJobs := func() error {
			return mgr.db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("planning"))
				if b == nil {
					return errors.New("bucket should not be missing")
				}
				jobs, err = p.recover(b)
				return err
			})
		}
		err = readJobs()
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		readJob := jobs[0]

		// Compare
		assert.Equal(t, written.id, readJob.id)
		assert.Equal(t, written.creationTime.Unix(), readJob.creationTime.Unix())
		assert.Equal(t, written.leaseCreationTime.Unix(), readJob.leaseCreationTime.Unix())
		assert.Equal(t, written.numLeases, readJob.numLeases)
		assert.Equal(t, written.epoch, readJob.epoch)

		err = persister.DeleteJob(written)
		require.NoError(t, err)

		err = readJobs()
		require.NoError(t, err)
		require.Empty(t, jobs)
	})
}
