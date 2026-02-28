// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dskit_tenant "github.com/grafana/dskit/tenant"
	"go.etcd.io/bbolt"

	"github.com/grafana/mimir/pkg/util"
)

func jobPersistenceManagerFactory(cfg Config, logger log.Logger) (JobPersistenceManager, error) {
	switch cfg.persistenceType {
	case "bbolt":
		return openBboltJobPersistenceManager(cfg.bboltPath, logger)
	case "none":
		return &NopJobPersistenceManager{}, nil
	default:
		return nil, fmt.Errorf("unrecognized compactor scheduler persistence type: %s", cfg.persistenceType)
	}
}

type JobPersistenceManager interface {
	InitializeTenant(tenant string) (JobPersister, error)
	DeleteTenant(tenant string) error
	RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (map[string]*JobTracker, error)
	Close() error
}

type JobPersister interface {
	WriteJob(job TrackedJob) error
	DeleteJob(job TrackedJob) error
	WriteAndDeleteJobs(writes, deletes []TrackedJob) error
}

func openBboltJobPersistenceManager(path string, logger log.Logger) (*BboltJobPersistenceManager, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	return &BboltJobPersistenceManager{
		db:     db,
		logger: logger,
	}, nil
}

type BboltJobPersistenceManager struct {
	db     *bbolt.DB
	logger log.Logger
}

func (m *BboltJobPersistenceManager) InitializeTenant(tenant string) (JobPersister, error) {
	tenantName := []byte(tenant)
	// Create a tenant bucket for the first time
	err := m.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(tenantName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return newBboltJobPersister(m.db, tenantName, m.logger), nil
}

func (m *BboltJobPersistenceManager) DeleteTenant(tenant string) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket([]byte(tenant))
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *BboltJobPersistenceManager) RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (map[string]*JobTracker, error) {
	jobTrackers := make(map[string]*JobTracker)

	var (
		numCompactionJobsRecovered int
		numPlanJobsRecovered       int
		numBucketCleanups          int
		numKeyCleanups             int
	)

	level.Info(m.logger).Log("msg", "starting job recovery")

	// Iterate through each bucket and create the corresponding JobTracker
	// This is Update and not View due to the need to delete buckets that are no longer needed and values that failed to deserialize
	err := m.db.Update(func(tx *bbolt.Tx) error {

		// Deletions can not occur during iteration since it interacts with the bucket cursor
		// This maps tenants to required cleanup. If the cleanup is nil then the tenant bucket itself should be deleted
		cleanup := make(map[string]*keyCleanup)

		// No bucket modification can occur within the ForEach
		err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			tenant := string(name)
			if err := dskit_tenant.ValidTenantID(tenant); err != nil {
				level.Warn(m.logger).Log("msg", "will delete bbolt bucket with invalid tenant name", "user", tenant, "err", err)
				cleanup[tenant] = nil
				return nil
			}
			if !allowedTenants.IsAllowed(tenant) {
				level.Warn(m.logger).Log("msg", "will delete bbolt bucket of tenant no longer allowed", "user", tenant)
				cleanup[tenant] = nil
				return nil
			}

			compactionJobs, planJob, keyCleanup := jobsFromTenantBucket(b)
			numCompactionJobsRecovered += len(compactionJobs)
			if planJob != nil {
				numPlanJobsRecovered += 1
			}
			if keyCleanup != nil {
				cleanup[tenant] = keyCleanup
			}

			jp := newBboltJobPersister(m.db, []byte(tenant), m.logger)
			jt := jobTrackerFactory(tenant, jp)
			jt.recoverFrom(compactionJobs, planJob)
			jobTrackers[tenant] = jt
			return nil
		})
		if err != nil {
			return err
		}

		numBucketCleanups, numKeyCleanups = m.cleanup(tx, cleanup)

		return nil
	})
	if err != nil {
		level.Error(m.logger).Log("msg", "failed job recovery", "err", err)
		return nil, err
	}

	level.Info(m.logger).Log(
		"msg", "completed job recovery",
		"num_tenants_recovered", len(jobTrackers),
		"num_compaction_jobs_recovered", numCompactionJobsRecovered,
		"num_plan_jobs_recovered", numPlanJobsRecovered,
		"num_bucket_cleanups", numBucketCleanups,
		"num_key_cleanups", numKeyCleanups,
	)

	return jobTrackers, nil
}

// keyCleanup is only valid within the transaction in which it was created
type keyCleanup struct {
	bucket    *bbolt.Bucket
	keyErrors []keyError
}

// keyError is only valid within the transaction in which it was created
type keyError struct {
	key []byte
	err error
}

func (m *BboltJobPersistenceManager) cleanup(tx *bbolt.Tx, cleanup map[string]*keyCleanup) (numBucketCleanups, numKeyCleanups int) {
	for tenant, kc := range cleanup {
		name := []byte(tenant)
		if kc == nil {
			// Delete the bucket
			if err := tx.DeleteBucket(name); err != nil {
				level.Warn(m.logger).Log("msg", "failed to delete bbolt bucket", "err", err)
				continue
			}
			numBucketCleanups++
			continue
		}

		// Delete the keys that failed
		for _, ke := range kc.keyErrors {
			failureLogger := log.With(m.logger, "user", tenant, "key", string(ke.key), "err", ke.err)

			if err := kc.bucket.Delete(ke.key); err != nil {
				// Only returns an error if we're in a read-only transaction, which should never be the case
				// This isn't the actual write, but instead an addition to the write transaction which hasn't reached commit yet
				level.Warn(failureLogger).Log("msg", "failed to delete job that failed to deserialize", "delete_err", err)
				continue
			}
			numKeyCleanups++
			level.Warn(failureLogger).Log("msg", "deleted job that failed to deserialize")
		}
	}
	return
}

func jobsFromTenantBucket(bucket *bbolt.Bucket) ([]*TrackedCompactionJob, *TrackedPlanJob, *keyCleanup) {
	compactionJobs := make([]*TrackedCompactionJob, 0, 10)
	var planJob *TrackedPlanJob // may be nil
	var keyErrs []keyError

	c := bucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) == reservedJobIdLen {
			sk := string(k[0])
			switch sk {
			case planJobId:
				job, err := deserializePlanJob(v)
				if err != nil {
					keyErrs = append(keyErrs, keyError{k, err})
					continue
				}
				planJob = job
			default:
				keyErrs = append(keyErrs, keyError{k, errors.New("unknown key")})
			}
		} else {
			compactionJob, err := deserializeCompactionJob(k, v)
			if err != nil {
				keyErrs = append(keyErrs, keyError{k, err})
				continue
			}
			compactionJobs = append(compactionJobs, compactionJob)
		}
	}

	var kc *keyCleanup
	if len(keyErrs) > 0 {
		kc = &keyCleanup{bucket, keyErrs}
	}

	return compactionJobs, planJob, kc
}

func (m *BboltJobPersistenceManager) Close() error {
	return m.db.Close()
}

type BboltJobPersister struct {
	db     *bbolt.DB
	name   []byte
	logger log.Logger
}

func newBboltJobPersister(db *bbolt.DB, name []byte, logger log.Logger) *BboltJobPersister {
	return &BboltJobPersister{
		db:     db,
		name:   name,
		logger: logger,
	}
}

func (jp *BboltJobPersister) WriteJob(j TrackedJob) error {
	content, err := j.Serialize()
	if err != nil {
		return err
	}
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Put([]byte(j.ID()), content)
	})
}

func (jp *BboltJobPersister) DeleteJob(j TrackedJob) error {
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Delete([]byte(j.ID()))
	})
}

func (jp *BboltJobPersister) WriteAndDeleteJobs(writes, deletes []TrackedJob) error {
	jobBytes := make([][]byte, len(writes))
	var err error
	for i, j := range writes {
		jobBytes[i], err = j.Serialize()
		if err != nil {
			return fmt.Errorf("failed serializing a job: %w", err)
		}
	}
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		for i, jb := range jobBytes {
			if err := b.Put([]byte(writes[i].ID()), jb); err != nil {
				return err
			}
		}
		for _, j := range deletes {
			if err := b.Delete([]byte(j.ID())); err != nil {
				return err
			}
		}
		return nil
	})
}

type NopJobPersistenceManager struct{}

func (n *NopJobPersistenceManager) InitializeTenant(tenant string) (JobPersister, error) {
	return &NopJobPersister{}, nil
}

func (n *NopJobPersistenceManager) DeleteTenant(tenant string) error {
	return nil
}

func (n *NopJobPersistenceManager) RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(string, JobPersister) *JobTracker) (map[string]*JobTracker, error) {
	return make(map[string]*JobTracker), nil
}

func (n *NopJobPersistenceManager) Close() error {
	return nil
}

type NopJobPersister struct{}

func (n *NopJobPersister) WriteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister) DeleteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister) WriteAndDeleteJobs(writes []TrackedJob, deletes []TrackedJob) error {
	return nil
}
