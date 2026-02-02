// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
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
	RecoverAll(allowlist *util.AllowList, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (map[string]*JobTracker, error)
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

func (m *BboltJobPersistenceManager) RecoverAll(allowedTenants *util.AllowList, compactionTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (jobTrackers map[string]*JobTracker, err error) {
	jobTrackers = make(map[string]*JobTracker)
	numJobsRecovered := 0

	level.Info(m.logger).Log("msg", "starting job recovery")
	// Iterate through each bucket and create the corresponding JobTracker
	err = m.db.Update(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			tenant := string(name)
			// Must be a tenant bucket
			if err := dskit_tenant.ValidTenantID(tenant); err != nil {
				// Log a warning and delete
				level.Warn(m.logger).Log("msg", "deleting bbolt bucket with invalid name", "name", tenant, "err", err)
				return tx.DeleteBucket(name)
			}
			if !allowedTenants.IsAllowed(tenant) {
				// This tenant is no longer allowed, remove their bucket
				level.Warn(m.logger).Log("msg", "deleting bbolt bucket of tenant no longer allowed", "tenant", tenant)
				return tx.DeleteBucket(name)
			}

			jobs := jobsFromTenantBucket(tenant, b, m.logger)
			numJobsRecovered += len(jobs)

			jp := newBboltJobPersister(m.db, []byte(tenant), m.logger)
			jt := compactionTrackerFactory(tenant, jp)
			jt.recoverFrom(jobs)
			jobTrackers[tenant] = jt
			return nil
		})
	})
	if err != nil {
		level.Error(m.logger).Log("msg", "failed job recovery", "err", err)
		return nil, err
	}
	level.Info(m.logger).Log("msg", "completed job recovery", "num_tenants_recovered", len(jobTrackers), "num_jobs_recovered", numJobsRecovered)
	return jobTrackers, nil
}

func jobsFromTenantBucket(tenant string, bucket *bbolt.Bucket, logger log.Logger) []TrackedJob {
	jobs := make([]TrackedJob, 0, 10)
	c := bucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		job, err := deserializeJob(k, v)
		if err != nil {
			// If we can't deserialize a value assume it is corrupt. We don't want it sticking around forever, so delete it.
			level.Warn(logger).Log("msg", "failed to deserialize job, deleting it", "tenant", tenant, "key", string(k), "err", err)
			if err := bucket.Delete(k); err != nil {
				// Only returns an error if we're in a read-only transaction, which should never be the case.
				// This isn't the actual write, but instead an addition to the write transaction which hasn't reached commit yet.
				level.Warn(logger).Log("msg", "failed to delete job that failed to deserialize", "tenant", tenant, "key", string(k), "err", err)
			}
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs
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
	return &NopJobPersister[*CompactionJob]{}, nil
}

func (n *NopJobPersistenceManager) InitializePlanning() (JobPersister, error) {
	return &NopJobPersister[struct{}]{}, nil
}

func (n *NopJobPersistenceManager) DeleteTenant(tenant string) error {
	return nil
}

func (n *NopJobPersistenceManager) RecoverAll(allowList *util.AllowList, jobTrackerFactory func(string, JobPersister) *JobTracker) (jobTrackers map[string]*JobTracker, err error) {
	return make(map[string]*JobTracker), nil
}

func (n *NopJobPersistenceManager) Close() error {
	return nil
}

type NopJobPersister[V any] struct{}

func (n *NopJobPersister[V]) WriteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister[V]) DeleteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister[V]) WriteAndDeleteJobs(writes []TrackedJob, deletes []TrackedJob) error {
	return nil
}
