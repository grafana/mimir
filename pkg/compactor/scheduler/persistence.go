// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.etcd.io/bbolt"
	bbolt_errors "go.etcd.io/bbolt/errors"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
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
	InitializeTenant(tenant string) (JobPersister[*CompactionJob], error)
	InitializePlanning() (JobPersister[struct{}], error)
	DeleteTenant(tenant string) error
	RecoverAll(
		allowlist *util.AllowList,
		planTracker *JobTracker[struct{}],
		compactionTrackerFactory func(JobPersister[*CompactionJob]) *JobTracker[*CompactionJob],
	) (compactionTrackers map[string]*JobTracker[*CompactionJob], err error)
	Close() error
}

type JobPersister[V any] interface {
	WriteJob(job *Job[V]) error
	DeleteJob(job *Job[V]) error
	WriteAndDeleteJobs(writes, deletes []*Job[V]) error
}

func serializePlanJob(j *Job[struct{}]) ([]byte, error) {
	info := compactorschedulerpb.StoredJobInfo{
		CreationTime:      j.creationTime.Unix(),
		LeaseCreationTime: j.leaseCreationTime.Unix(),
		NumLeases:         int32(j.numLeases),
		Epoch:             j.epoch,
	}
	return info.Marshal()
}

func deserializePlanJob(content []byte) (*Job[struct{}], error) {
	var info compactorschedulerpb.StoredJobInfo
	if err := info.Unmarshal(content); err != nil {
		return nil, err
	}
	job := &Job[struct{}]{
		value:             struct{}{},
		creationTime:      time.Unix(info.CreationTime, 0),
		leaseCreationTime: time.Unix(info.LeaseCreationTime, 0),
		numLeases:         int(info.NumLeases),
		epoch:             info.Epoch,
	}
	// Renewal times are not serialized, a buffer time is used across restarts instead
	job.lastRenewalTime = job.leaseCreationTime
	return job, nil

}

func serializeCompactionJob(j *Job[*CompactionJob]) ([]byte, error) {
	stored := &compactorschedulerpb.StoredCompactionJob{
		Info: &compactorschedulerpb.StoredJobInfo{
			CreationTime:      j.creationTime.Unix(),
			LeaseCreationTime: j.leaseCreationTime.Unix(),
			NumLeases:         int32(j.numLeases),
			Epoch:             j.epoch,
		},
		Job: &compactorschedulerpb.CompactionJob{
			BlockIds: j.value.blocks,
			Split:    j.value.isSplit,
		},
	}
	return stored.Marshal()
}

func deserializeCompactionJob(b []byte) (*Job[*CompactionJob], error) {
	var stored compactorschedulerpb.StoredCompactionJob
	err := stored.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	if stored.Info == nil || stored.Job == nil {
		return nil, errors.New("invalid compaction job can not be deserialized")
	}
	job := &Job[*CompactionJob]{
		value: &CompactionJob{
			blocks:  stored.Job.BlockIds,
			isSplit: stored.Job.Split,
		},
		creationTime:      time.Unix(stored.Info.CreationTime, 0),
		leaseCreationTime: time.Unix(stored.Info.LeaseCreationTime, 0),
		numLeases:         int(stored.Info.NumLeases),
		epoch:             stored.Info.Epoch,
	}
	// Renewal times are not serialized, a buffer time is used across restarts instead
	job.lastRenewalTime = job.leaseCreationTime
	return job, nil
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

func (m *BboltJobPersistenceManager) tenantName(tenant string) []byte {
	return []byte("u" + tenant)
}

func (m *BboltJobPersistenceManager) createBucket(name []byte) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(name)
		return err
	})
}

func (m *BboltJobPersistenceManager) InitializeTenant(tenant string) (JobPersister[*CompactionJob], error) {
	// Create a tenant bucket for the first time
	tenantName := m.tenantName(tenant)
	err := m.createBucket(tenantName)
	if err != nil {
		return nil, err
	}
	return newBboltJobPersister(m.db, tenantName, serializeCompactionJob, deserializeCompactionJob, m.logger), nil
}

func (m *BboltJobPersistenceManager) InitializePlanning() (JobPersister[struct{}], error) {
	// Create the planning bucket for the first time
	planningName := []byte("planning")
	err := m.db.Update(func(tx *bbolt.Tx) error {
		// If not exists so this can be unconditionally called
		_, err := tx.CreateBucketIfNotExists(planningName)
		return err
	})
	if err != nil {
		return nil, err
	}
	return newBboltJobPersister(m.db, planningName, serializePlanJob, deserializePlanJob, m.logger), nil
}

func (m *BboltJobPersistenceManager) DeleteTenant(tenant string) error {
	tenantName := m.tenantName(tenant)
	return m.db.Update(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket(tenantName)
		if err != nil && !errors.Is(err, bbolt_errors.ErrBucketNotFound) {
			return err
		}
		return nil
	})
}

func (m *BboltJobPersistenceManager) RecoverAll(
	allowedTenants *util.AllowList,
	planTracker *JobTracker[struct{}],
	compactionTrackerFactory func(JobPersister[*CompactionJob]) *JobTracker[*CompactionJob],
) (compactionTrackers map[string]*JobTracker[*CompactionJob], err error) {

	compactionTrackers = make(map[string]*JobTracker[*CompactionJob])
	numJobsRecovered := 0

	level.Info(m.logger).Log("msg", "starting job recovery")
	// Iterate through each bucket and create the corresponding JobTracker
	err = m.db.Update(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			bucketName := string(name)
			if bucketName == "planning" {
				jp := newBboltJobPersister(m.db, []byte(bucketName), serializePlanJob, deserializePlanJob, m.logger)
				jobs, err := jp.recover(b)
				if err != nil {
					return err
				}
				numJobsRecovered += len(jobs)
				planTracker.recoverFrom(jobs)
				return nil
			}

			// Must be a tenant bucket
			if len(bucketName) <= 1 || !strings.HasPrefix(bucketName, "u") {
				// Log a warning and delete
				level.Warn(m.logger).Log("msg", "deleting bbolt bucket with invalid name", "name", bucketName)
				return tx.DeleteBucket(name)
			}
			tenant, _ := strings.CutPrefix(bucketName, "u")
			if !allowedTenants.IsAllowed(tenant) {
				// This tenant is no longer allowed, remove their bucket
				level.Warn(m.logger).Log("msg", "deleting bbolt bucket of tenant no longer allowed", "tenant", tenant)
				return tx.DeleteBucket(name)
			}

			jp := newBboltJobPersister(m.db, m.tenantName(tenant), serializeCompactionJob, deserializeCompactionJob, m.logger)
			jt := compactionTrackerFactory(jp)
			jobs, err := jp.recover(b)
			if err != nil {
				return err
			}
			numJobsRecovered += len(jobs)
			jt.recoverFrom(jobs)
			compactionTrackers[tenant] = jt
			return nil
		})
	})
	if err != nil {
		level.Error(m.logger).Log("msg", "failed job recovery", "err", err)
		return nil, err
	}
	level.Info(m.logger).Log("msg", "completed job recovery", "num_tenants_recovered", len(compactionTrackers), "num_jobs_recovered", numJobsRecovered)
	return compactionTrackers, nil
}

func (m *BboltJobPersistenceManager) Close() error {
	return m.db.Close()
}

type BboltJobPersister[V any] struct {
	db           *bbolt.DB
	name         []byte
	serializer   func(*Job[V]) ([]byte, error)
	deserializer func([]byte) (*Job[V], error)
	logger       log.Logger
}

func newBboltJobPersister[V any](db *bbolt.DB, name []byte, serializer func(*Job[V]) ([]byte, error), deserializer func([]byte) (*Job[V], error), logger log.Logger) *BboltJobPersister[V] {
	return &BboltJobPersister[V]{
		db:           db,
		name:         name,
		serializer:   serializer,
		deserializer: deserializer,
		logger:       logger,
	}
}

// recover is not exposed in the Job due to the argument required
func (jp *BboltJobPersister[V]) recover(bucket *bbolt.Bucket) ([]*Job[V], error) {
	// Recover jobs from database
	jobs := make([]*Job[V], 0, 10)
	c := bucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		job, err := jp.deserializer(v)
		if err != nil {
			// If we can't deserialize a value assume it is corrupt. We don't want it sticking aroud forever, so delete it.
			level.Warn(jp.logger).Log("msg", "failed to deserialize job, deleting it", "bucket", string(jp.name), "key", string(k), "err", err)
			if err := bucket.Delete(k); err != nil {
				// Only returns an error if we're in a read-only transaction, which should never be the case.
				// This isn't the actual write, but instead an addition to the write transaction which hasn't reached commit yet.
				level.Warn(jp.logger).Log("msg", "failed to delete job that failed to deserialize", "bucket", string(jp.name), "key", string(k), "err", err)
			}
			continue
		}

		job.id = string(k)
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (jp *BboltJobPersister[V]) WriteJob(j *Job[V]) error {
	content, err := jp.serializer(j)
	if err != nil {
		return err
	}
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Put([]byte(j.id), content)
	})
}

func (jp *BboltJobPersister[V]) DeleteJob(j *Job[V]) error {
	return jp.db.Update(func(t *bbolt.Tx) error {
		b := t.Bucket(jp.name)
		if b == nil {
			// This should never happen
			return fmt.Errorf("bucket does not exist for %s", jp.name)
		}
		return b.Delete([]byte(j.id))
	})
}

func (jp *BboltJobPersister[V]) WriteAndDeleteJobs(writes, deletes []*Job[V]) error {
	jobBytes := make([][]byte, len(writes))
	var err error
	for i, j := range writes {
		jobBytes[i], err = jp.serializer(j)
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
			if err := b.Put([]byte(writes[i].id), jb); err != nil {
				return err
			}
		}
		for _, j := range deletes {
			if err := b.Delete([]byte(j.id)); err != nil {
				return err
			}
		}
		return nil
	})
}

type NopJobPersistenceManager struct{}

func (n *NopJobPersistenceManager) InitializeTenant(tenant string) (JobPersister[*CompactionJob], error) {
	return &NopJobPersister[*CompactionJob]{}, nil
}

func (n *NopJobPersistenceManager) InitializePlanning() (JobPersister[struct{}], error) {
	return &NopJobPersister[struct{}]{}, nil
}

func (n *NopJobPersistenceManager) DeleteTenant(tenant string) error {
	return nil
}

func (n *NopJobPersistenceManager) RecoverAll(
	allowList *util.AllowList,
	planTracker *JobTracker[struct{}],
	compactionTrackerFactory func(JobPersister[*CompactionJob]) *JobTracker[*CompactionJob],
) (compactionTrackers map[string]*JobTracker[*CompactionJob], err error) {
	return make(map[string]*JobTracker[*CompactionJob]), nil
}

func (n *NopJobPersistenceManager) Close() error {
	return nil
}

type NopJobPersister[V any] struct{}

func (n *NopJobPersister[V]) WriteJob(job *Job[V]) error {
	return nil
}

func (n *NopJobPersister[V]) DeleteJob(job *Job[V]) error {
	return nil
}

func (n *NopJobPersister[V]) WriteAndDeleteJobs(writes []*Job[V], deletes []*Job[V]) error {
	return nil
}
