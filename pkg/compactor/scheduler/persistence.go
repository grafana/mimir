// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"fmt"
	"time"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/util"
)

func jobPersistenceManagerFactory(cfg Config, logger log.Logger) (JobPersistenceManager, error) {
	switch cfg.PersistenceType {
	case "bbolt":
		return openBboltJobPersistenceManager(cfg.Bbolt.Dir, cfg.Bbolt.ShardCount, logger)
	case "none":
		return &NopJobPersistenceManager{}, nil
	default:
		return nil, fmt.Errorf("unrecognized compactor scheduler persistence type: %s", cfg.PersistenceType)
	}
}

type JobPersistenceManager interface {
	CreationTime() time.Time
	InitializeTenant(tenant string) (JobPersister, error)
	RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(tenant string, persister JobPersister) *JobTracker) (map[string]*JobTracker, error)
	Close() error
}

type JobPersister interface {
	Drop() error
	WriteJob(job TrackedJob) error
	DeleteJob(job TrackedJob) error
	WriteAndDeleteJobs(writes, deletes []TrackedJob) error
}

type NopJobPersistenceManager struct{}

func (n *NopJobPersistenceManager) CreationTime() time.Time {
	return time.Now()
}

func (n *NopJobPersistenceManager) InitializeTenant(tenant string) (JobPersister, error) {
	return &NopJobPersister{}, nil
}

func (n *NopJobPersistenceManager) RecoverAll(allowedTenants *util.AllowList, jobTrackerFactory func(string, JobPersister) *JobTracker) (map[string]*JobTracker, error) {
	return make(map[string]*JobTracker), nil
}

func (n *NopJobPersistenceManager) Close() error {
	return nil
}

type NopJobPersister struct{}

func (n *NopJobPersister) Drop() error {
	return nil
}

func (n *NopJobPersister) WriteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister) DeleteJob(job TrackedJob) error {
	return nil
}

func (n *NopJobPersister) WriteAndDeleteJobs(writes []TrackedJob, deletes []TrackedJob) error {
	return nil
}
