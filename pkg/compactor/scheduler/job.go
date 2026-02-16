// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

const (
	reservedJobIdLen = 1
	planJobId        = "p"
	infiniteLeases   = 0
)

type TrackedJob interface {
	ID() string
	CreationTime() time.Time
	Status() compactorschedulerpb.StoredJobStatus
	StatusTime() time.Time // time of last renewal or time of completion
	IsLeased() bool
	IsComplete() bool
	MarkLeased(time.Time)
	MarkComplete(time.Time)
	RenewLease(time.Time)
	ClearLease()
	NumLeases() int
	Epoch() int64
	Serialize() ([]byte, error)
	ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse
	CopyBase() TrackedJob // copies the underlying base job to allow for mutation (mark leased/clear lease)
	Order() uint32        // the order of jobs where lower is higher priority
}

type baseTrackedJob struct {
	id           string
	creationTime time.Time
	status       compactorschedulerpb.StoredJobStatus
	statusTime   time.Time
	numLeases    int
	epoch        int64 // used to avoid conflict since a job can be reassigned/replaced
}

func newBaseTrackedJob(id string, creationTime time.Time) baseTrackedJob {
	return baseTrackedJob{
		id:           id,
		creationTime: creationTime,
		status:       compactorschedulerpb.STORED_JOB_STATUS_AVAILABLE,
		epoch:        rand.Int63(),
	}
}

func (j *baseTrackedJob) ID() string {
	return j.id
}

func (j *baseTrackedJob) CreationTime() time.Time {
	return j.creationTime
}

func (j *baseTrackedJob) Status() compactorschedulerpb.StoredJobStatus {
	return j.status
}

func (j *baseTrackedJob) StatusTime() time.Time {
	return j.statusTime
}

func (j *baseTrackedJob) IsLeased() bool {
	return j.status == compactorschedulerpb.STORED_JOB_STATUS_LEASED
}

func (j *baseTrackedJob) IsComplete() bool {
	return j.status == compactorschedulerpb.STORED_JOB_STATUS_COMPLETE
}

func (j *baseTrackedJob) MarkLeased(now time.Time) {
	j.status = compactorschedulerpb.STORED_JOB_STATUS_LEASED
	j.statusTime = now
	j.numLeases += 1
	j.epoch += 1
}

func (j *baseTrackedJob) MarkComplete(now time.Time) {
	j.status = compactorschedulerpb.STORED_JOB_STATUS_COMPLETE
	j.statusTime = now
}

func (j *baseTrackedJob) RenewLease(now time.Time) {
	j.statusTime = now
}

func (j *baseTrackedJob) ClearLease() {
	j.status = compactorschedulerpb.STORED_JOB_STATUS_AVAILABLE
	j.statusTime = time.Time{}
}

func (j *baseTrackedJob) NumLeases() int {
	return j.numLeases
}

func (j *baseTrackedJob) Epoch() int64 {
	return j.epoch
}

type TrackedCompactionJob struct {
	baseTrackedJob
	value *CompactionJob
	order uint32
}

func NewTrackedCompactionJob(id string, value *CompactionJob, order uint32, creationTime time.Time) *TrackedCompactionJob {
	return &TrackedCompactionJob{
		baseTrackedJob: newBaseTrackedJob(id, creationTime),
		value:          value,
		order:          order,
	}
}

func (j *TrackedCompactionJob) CopyBase() TrackedJob {
	return &TrackedCompactionJob{
		baseTrackedJob: j.baseTrackedJob,
		value:          j.value,
		order:          j.order,
	}
}

func (j *TrackedCompactionJob) Serialize() ([]byte, error) {
	stored := &compactorschedulerpb.StoredCompactionJob{
		Info: &compactorschedulerpb.StoredJobInfo{
			CreationTime: j.creationTime.Unix(),
			Status:       j.status,
			StatusTime:   j.statusTime.Unix(),
			NumLeases:    int32(j.numLeases),
			Epoch:        j.epoch,
		},
		Job: &compactorschedulerpb.CompactionJob{
			BlockIds: j.value.blocks,
			Split:    j.value.isSplit,
		},
		Order: j.order,
	}
	return stored.Marshal()
}

func (j *TrackedCompactionJob) ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse {
	return &compactorschedulerpb.LeaseJobResponse{
		Key: &compactorschedulerpb.JobKey{
			Id:    j.id,
			Epoch: j.epoch,
		},
		Spec: &compactorschedulerpb.JobSpec{
			JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			Tenant:  tenant,
			Job: &compactorschedulerpb.CompactionJob{
				BlockIds: j.value.blocks,
				Split:    j.value.isSplit,
			},
		},
	}
}

func (j *TrackedCompactionJob) Order() uint32 {
	return j.order
}

type TrackedPlanJob struct {
	baseTrackedJob
}

func NewTrackedPlanJob(creationTime time.Time) *TrackedPlanJob {
	return &TrackedPlanJob{
		baseTrackedJob: newBaseTrackedJob(planJobId, creationTime),
	}
}

func (j *TrackedPlanJob) NumLeases() int {
	// Trick to avoid imposing a maximum number of leases on plan jobs. Plan jobs will always represent the same task.
	return 0
}

func (j *TrackedPlanJob) CopyBase() TrackedJob {
	return &TrackedPlanJob{
		baseTrackedJob: j.baseTrackedJob,
	}
}

func (j *TrackedPlanJob) Serialize() ([]byte, error) {
	info := compactorschedulerpb.StoredJobInfo{
		CreationTime: j.creationTime.Unix(),
		Status:       compactorschedulerpb.StoredJobStatus(j.status),
		StatusTime:   j.StatusTime().Unix(),
		NumLeases:    int32(j.numLeases),
		Epoch:        j.epoch,
	}
	return info.Marshal()
}

func (j *TrackedPlanJob) ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse {
	return &compactorschedulerpb.LeaseJobResponse{
		Key: &compactorschedulerpb.JobKey{
			Id:    planJobId,
			Epoch: j.epoch,
		},
		Spec: &compactorschedulerpb.JobSpec{
			Tenant:  tenant,
			JobType: compactorschedulerpb.JOB_TYPE_PLANNING,
		},
	}
}

func (j *TrackedPlanJob) Order() uint32 {
	return math.MaxUint32
}

func deserializeJob(k []byte, v []byte) (TrackedJob, error) {
	if len(k) == reservedJobIdLen {
		sk := string(k[0])
		switch sk {
		case planJobId:
			return deserializePlanJob(v)
		default:
			return nil, fmt.Errorf("unknown key: %s", sk)
		}
	}
	return deserializeCompactionJob(k, v)
}

func deserializePlanJob(content []byte) (TrackedJob, error) {
	var info compactorschedulerpb.StoredJobInfo
	if err := info.Unmarshal(content); err != nil {
		return nil, err
	}
	return &TrackedPlanJob{
		baseTrackedJob: baseTrackedJob{
			id:           planJobId,
			creationTime: time.Unix(info.CreationTime, 0),
			status:       info.Status,
			statusTime:   time.Unix(info.StatusTime, 0),
			numLeases:    int(info.NumLeases),
			epoch:        info.Epoch,
		},
	}, nil
}

func deserializeCompactionJob(k []byte, v []byte) (*TrackedCompactionJob, error) {
	var stored compactorschedulerpb.StoredCompactionJob
	err := stored.Unmarshal(v)
	if err != nil {
		return nil, err
	}
	if stored.Info == nil || stored.Job == nil {
		return nil, errors.New("invalid compaction job can not be deserialized")
	}
	return &TrackedCompactionJob{
		baseTrackedJob: baseTrackedJob{
			id:           string(k),
			creationTime: time.Unix(stored.Info.CreationTime, 0),
			status:       stored.Info.Status,
			statusTime:   time.Unix(stored.Info.StatusTime, 0),
			numLeases:    int(stored.Info.NumLeases),
			epoch:        stored.Info.Epoch,
		},
		value: &CompactionJob{
			blocks:  stored.Job.BlockIds,
			isSplit: stored.Job.Split,
		},
		order: stored.Order,
	}, nil
}
