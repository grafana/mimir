// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/util"
)

var testBlockIDs = [][]byte{ulid.MustNewDefault(time.Now()).Bytes()}

func setupBboltManager(t *testing.T) (*BboltJobPersistenceManager, *bbolt.DB) {
	tempDir := t.TempDir()
	dbDir := filepath.Join(tempDir, "shards")
	mgr, err := openBboltJobPersistenceManager(dbDir, 1, log.NewNopLogger())
	require.NoError(t, err)
	return mgr, mgr.dbs[0]
}

func newTestCompactionJob(id string) TrackedJob {
	job := NewTrackedCompactionJob(
		id,
		&CompactionJob{blocks: nil, isSplit: false},
		1,
		time.Now(),
	)
	return job
}

func newTestPlanJob() TrackedJob {
	job := NewTrackedPlanJob(time.Now())
	return job
}

func TestBboltJobPersistenceManager_InitializeTenant(t *testing.T) {
	mgr, db := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	tenant := "tenant"
	persister, err := mgr.InitializeTenant(tenant)
	require.NoError(t, err)
	require.NotNil(t, persister)

	// Verify the bucket was created
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(tenant))
		require.NotNil(t, bucket)
		return nil
	})
	require.NoError(t, err)
}

func TestBboltJobPersistenceManager_RecoverAll(t *testing.T) {
	mgr, _ := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	allowedTenants := util.NewAllowList(nil, nil)
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jobTrackerFactory := func(tenant string, persister JobPersister) *JobTracker {
		return NewJobTracker(persister, tenant, clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant(tenant), log.NewNopLogger())
	}

	// Empty recovery should succeed
	ctm, err := mgr.RecoverAll(allowedTenants, jobTrackerFactory)
	require.NoError(t, err)
	require.Empty(t, ctm)

	tenantPersister, err := mgr.InitializeTenant("foo")
	require.NoError(t, err)
	err = tenantPersister.WriteJob(newTestCompactionJob("id"))
	require.NoError(t, err)
	err = tenantPersister.WriteJob(newTestPlanJob())
	require.NoError(t, err)

	ctm, err = mgr.RecoverAll(allowedTenants, jobTrackerFactory)
	require.NoError(t, err)
	require.Len(t, ctm, 1)
	require.Contains(t, ctm, "foo")

	jobTracker := ctm["foo"]
	require.Len(t, jobTracker.incompleteJobs, 2)
	require.Contains(t, jobTracker.incompleteJobs, "id")
	require.Contains(t, jobTracker.incompleteJobs, planJobId)
}

func TestBboltJobPersistenceManager_RecoverAll_Cleanup(t *testing.T) {
	mgr, db := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jobTrackerFactory := func(tenant string, persister JobPersister) *JobTracker {
		return NewJobTracker(persister, tenant, clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant(tenant), log.NewNopLogger())
	}

	// Create a bucket with an invalid tenant name
	invalidTenant := "@"
	require.Error(t, tenant.ValidTenantID(invalidTenant))
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte(invalidTenant))
		return err
	})
	require.NoError(t, err)

	// Create a bucket for a tenant that is not in the allow list
	disallowedTenant := "disallowed"
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte(disallowedTenant))
		return err
	})
	require.NoError(t, err)

	// Create a valid tenant with one good job and one corrupt job
	validTenant := "valid-tenant"
	persister, err := mgr.InitializeTenant(validTenant)
	require.NoError(t, err)

	compactionJob := newTestCompactionJob("id")
	corruptKey := []byte("corrupt")
	err = persister.WriteJob(compactionJob)
	require.NoError(t, err)
	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(validTenant))
		return b.Put(corruptKey, []byte("invalid protobuf"))
	})
	require.NoError(t, err)

	allowedTenants := util.NewAllowList(nil, []string{disallowedTenant})
	jobTrackers, err := mgr.RecoverAll(allowedTenants, jobTrackerFactory)
	require.NoError(t, err)

	// Only the valid tenant should be recovered
	require.Len(t, jobTrackers, 1)
	require.Contains(t, jobTrackers, validTenant)

	// The valid job should be the only one recovered
	require.Len(t, jobTrackers[validTenant].incompleteJobs, 1)
	require.Contains(t, jobTrackers[validTenant].incompleteJobs, compactionJob.ID())

	// Verify the database state after cleanup
	err = db.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket([]byte(invalidTenant)), "invalid tenant bucket should be deleted")
		require.Nil(t, tx.Bucket([]byte(disallowedTenant)), "disallowed tenant bucket should be deleted")

		b := tx.Bucket([]byte(validTenant))
		require.NotNil(t, b, "valid tenant bucket should remain")
		require.Nil(t, b.Get(corruptKey), "corrupt key should be deleted")
		require.NotNil(t, b.Get([]byte(compactionJob.ID())), "valid job key should remain")
		return nil
	})
	require.NoError(t, err)
}

func TestBboltJobPersister_Drop(t *testing.T) {
	mgr, db := setupBboltManager(t)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	tenant := "tenant"
	jp, err := mgr.InitializeTenant(tenant)
	require.NoError(t, err)

	err = jp.Drop()
	require.NoError(t, err)

	// Verify the bucket was deleted
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(tenant))
		require.Nil(t, bucket)
		return nil
	})
	require.NoError(t, err)
}

func TestShardForTenant(t *testing.T) {
	tenants := []string{"foo", "bar", "baz"}
	t.Run("single shard maps everything to 0", func(t *testing.T) {
		for _, tenant := range tenants {
			require.Equal(t, 0, shardForTenant(tenant, 1))
		}
	})
	t.Run("multiple shards map within range", func(t *testing.T) {
		shards := 16
		for _, tenant := range tenants {
			shard := shardForTenant(tenant, shards)
			require.GreaterOrEqual(t, shard, 0)
			require.Less(t, shard, shards)
		}
	})
}

func TestMetadataBucketNameIsInvalidTenantID(t *testing.T) {
	// We do not want to potentially clash with a tenant name
	require.Error(t, tenant.ValidTenantID(metadataBucketName))
}

func TestBboltJobPersistenceManager_CreationTimePersists(t *testing.T) {
	dir := t.TempDir()

	mgr, err := openBboltJobPersistenceManager(dir, 1, log.NewNopLogger())
	require.NoError(t, err)
	originalCreationTime := mgr.CreationTime()
	require.False(t, originalCreationTime.IsZero())
	require.NoError(t, mgr.Close())

	// Reopen with the same shard count
	mgr, err = openBboltJobPersistenceManager(dir, 1, log.NewNopLogger())
	require.NoError(t, err)
	require.True(t, mgr.CreationTime().Equal(originalCreationTime), "creation time should be preserved on reopen")
	require.NoError(t, mgr.Close())
}

func TestRunMigration_ScaleUp(t *testing.T) {
	dir := t.TempDir()

	// Open with 1 shard and write some data
	mgr, err := openBboltJobPersistenceManager(dir, 1, log.NewNopLogger())
	require.NoError(t, err)
	originalCreationTime := mgr.CreationTime()
	require.False(t, originalCreationTime.IsZero())

	tenants := []string{"foo", "bar", "baz"}
	for _, tenant := range tenants {
		p, err := mgr.InitializeTenant(tenant)
		require.NoError(t, err)
		require.NoError(t, p.WriteJob(newTestCompactionJob("job")))
	}
	require.NoError(t, mgr.Close())

	// Reopen with 2 shards
	mgr, err = openBboltJobPersistenceManager(dir, 2, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.True(t, mgr.CreationTime().Equal(originalCreationTime), "creation time should be preserved after scale-up migration")
	require.Len(t, mgr.dbs, 2)

	allowedTenants := util.NewAllowList(nil, nil)
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	trackers, err := mgr.RecoverAll(allowedTenants, func(tenant string, persister JobPersister) *JobTracker {
		return NewJobTracker(persister, tenant, clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant(tenant), log.NewNopLogger())
	})
	require.NoError(t, err)
	require.Len(t, trackers, len(tenants))

	for _, tenant := range tenants {
		jt, ok := trackers[tenant]
		require.True(t, ok, "missing tracker for %s", tenant)
		require.Contains(t, jt.incompleteJobs, "job")

		// Verify the tenant is in the correct shard
		expectedShard := shardForTenant(tenant, 2)
		err := mgr.dbs[expectedShard].View(func(tx *bbolt.Tx) error {
			require.NotNil(t, tx.Bucket([]byte(tenant)), "tenant %s should be in shard %d", tenant, expectedShard)
			return nil
		})
		require.NoError(t, err)

		// Verify the tenant is not in the other shard
		otherShard := 1 - expectedShard
		err = mgr.dbs[otherShard].View(func(tx *bbolt.Tx) error {
			require.Nil(t, tx.Bucket([]byte(tenant)), "tenant %s should not be in shard %d", tenant, otherShard)
			return nil
		})
		require.NoError(t, err)
	}
}

func TestRunMigration_ScaleDown(t *testing.T) {
	dir := t.TempDir()

	// Open with 2 shards, place a tenant on each shard directly.
	mgr, err := openBboltJobPersistenceManager(dir, 2, log.NewNopLogger())
	require.NoError(t, err)
	originalCreationTime := mgr.CreationTime()
	require.False(t, originalCreationTime.IsZero())

	p0, err := mgr.initializeTenantOnDB(mgr.dbs[0], "tenant1")
	require.NoError(t, err)
	require.NoError(t, p0.WriteJob(newTestCompactionJob("job1")))

	p1, err := mgr.initializeTenantOnDB(mgr.dbs[1], "tenant2")
	require.NoError(t, err)
	require.NoError(t, p1.WriteJob(newTestCompactionJob("job2")))

	require.NoError(t, mgr.Close())

	// Reopen with 1 shard.
	mgr, err = openBboltJobPersistenceManager(dir, 1, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.True(t, mgr.CreationTime().Equal(originalCreationTime), "creation time should be preserved after scale-down migration")

	require.Len(t, mgr.dbs, 1)

	// The extra shard file should have been deleted
	_, err = os.Stat(shardFilePath(dir, 1))
	require.True(t, os.IsNotExist(err), "extra shard file should be deleted")

	allowedTenants := util.NewAllowList(nil, nil)
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	trackers, err := mgr.RecoverAll(allowedTenants, func(tenant string, persister JobPersister) *JobTracker {
		return NewJobTracker(persister, tenant, clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant(tenant), log.NewNopLogger())
	})
	require.NoError(t, err)
	require.Len(t, trackers, 2)

	require.Contains(t, trackers["tenant1"].incompleteJobs, "job1")
	require.Contains(t, trackers["tenant2"].incompleteJobs, "job2")
}

func TestBboltJobPersister_WriteReadDelete(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		job                  TrackedJob
		verifySpecificFields func(t *testing.T, written, read TrackedJob)
	}{
		"compaction job": {
			job: &TrackedCompactionJob{
				baseTrackedJob: baseTrackedJob{
					id:           "id",
					creationTime: now,
					status:       compactorschedulerpb.STORED_JOB_STATUS_AVAILABLE,
					statusTime:   now.Add(10 * time.Second),
					numLeases:    1,
					epoch:        234,
				},
				value: &CompactionJob{blocks: testBlockIDs, isSplit: true},
				order: 1,
			},
			verifySpecificFields: func(t *testing.T, written, read TrackedJob) {
				writtenJob := written.(*TrackedCompactionJob)
				readJob, ok := read.(*TrackedCompactionJob)
				require.True(t, ok)
				require.Equal(t, writtenJob.value.blocks, readJob.value.blocks)
				require.Equal(t, writtenJob.value.isSplit, readJob.value.isSplit)
				require.Equal(t, writtenJob.order, readJob.order)
			},
		},
		"plan job": {
			job: &TrackedPlanJob{
				baseTrackedJob: baseTrackedJob{
					id:           planJobId,
					creationTime: now,
					status:       compactorschedulerpb.STORED_JOB_STATUS_COMPLETE,
					statusTime:   now.Add(10 * time.Second),
					numLeases:    1,
					epoch:        234,
				},
			},
			verifySpecificFields: func(t *testing.T, written, read TrackedJob) {
				// No fields to validate
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mgr, db := setupBboltManager(t)
			t.Cleanup(func() {
				require.NoError(t, mgr.Close())
			})

			persister, err := mgr.InitializeTenant("tenant")
			require.NoError(t, err)

			err = persister.WriteJob(tc.job)
			require.NoError(t, err)

			// Helper function since the test reads twice
			readJobs := func() (compactionJobs []*TrackedCompactionJob, planJob *TrackedPlanJob, cleanup *keyCleanup, err error) {
				err = db.View(func(tx *bbolt.Tx) error {
					b := tx.Bucket([]byte("tenant"))
					if b == nil {
						return errors.New("bucket should not be missing")
					}
					compactionJobs, planJob, cleanup = jobsFromTenantBucket(b)
					return nil
				})
				return
			}

			compactionJobs, planJob, cleanup, err := readJobs()
			require.NoError(t, err)
			require.Nil(t, cleanup)
			var readJob TrackedJob
			if len(compactionJobs) == 1 {
				readJob = compactionJobs[0]
			} else {
				readJob = planJob
			}
			require.Equal(t, tc.job.ID(), readJob.ID())
			require.Equal(t, tc.job.CreationTime().Unix(), readJob.CreationTime().Unix())
			require.Equal(t, tc.job.Status(), readJob.Status())
			require.Equal(t, tc.job.StatusTime().Unix(), readJob.StatusTime().Unix())
			require.Equal(t, tc.job.NumLeases(), readJob.NumLeases())
			require.Equal(t, tc.job.Epoch(), readJob.Epoch())

			tc.verifySpecificFields(t, tc.job, readJob)

			err = persister.DeleteJob(tc.job)
			require.NoError(t, err)
			compactionJobs, planJob, cleanup, err = readJobs()
			require.NoError(t, err)
			require.Nil(t, cleanup)
			require.Empty(t, compactionJobs)
			require.Nil(t, planJob)
		})
	}
}
