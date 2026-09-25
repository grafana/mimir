// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// Job keys, which are also job IDs:
//   - single character: a job type with at most one instance per tenant
//   - typedJobKeyPrefix + type byte + planner ID: a job type with many instances per tenant
//   - anything else: a compaction job
const (
	reservedJobIdLen     = 1
	planJobId            = "p"
	backfillCleanupJobId = "b"
	typedJobKeyPrefix    = "#"
	validateJobKeyType   = 'v'
	copyJobKeyType       = 'c'
	infiniteLeases       = 0
)

func isTypedJobKey(id string) bool {
	return strings.HasPrefix(id, typedJobKeyPrefix)
}

func typedJobKey(keyType byte, plannerID string) string {
	return typedJobKeyPrefix + string(keyType) + plannerID
}

// blockJobTypeForKey returns the backfill block job type of a typed job key, if it has one
func blockJobTypeForKey(id string) (compactorschedulerpb.JobType, bool) {
	// A typed key must have a type byte and a non-empty planner ID
	if !isTypedJobKey(id) || len(id) < len(typedJobKeyPrefix)+2 {
		return compactorschedulerpb.JOB_TYPE_UNKNOWN, false
	}
	switch id[len(typedJobKeyPrefix)] {
	case validateJobKeyType:
		return compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, true
	case copyJobKeyType:
		return compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, true
	}
	return compactorschedulerpb.JOB_TYPE_UNKNOWN, false
}

func blockJobKeyType(jobType compactorschedulerpb.JobType) byte {
	if jobType == compactorschedulerpb.JOB_TYPE_BACKFILL_COPY {
		return copyJobKeyType
	}
	return validateJobKeyType
}

type TrackedJob interface {
	ID() string
	Type() compactorschedulerpb.JobType
	CreationTime() time.Time
	Status() compactorschedulerpb.StoredJobStatus
	StatusTime() time.Time // time of last renewal or time of completion
	IsLeased() bool
	IsComplete() bool
	MarkLeased(time.Time)
	MarkComplete(time.Time)
	RenewLease(time.Time)
	DecrementLeaseCount()
	ClearLease()
	NumLeases() int
	Epoch() int64
	Serialize() ([]byte, error)
	ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse
	CopyBase() TrackedJob // copies the underlying base job to allow for mutation (mark leased/clear lease)
	Order() uint32        // the order of jobs where lower is higher priority
	Blocks() [][]byte     // the blocks this job works on, used to avoid repeating work
}

func isPlanJob(j TrackedJob) bool {
	return j.Type() == compactorschedulerpb.JOB_TYPE_PLANNING
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

func (j *baseTrackedJob) DecrementLeaseCount() {
	if j.numLeases > 0 { // defensive
		j.numLeases -= 1
	}
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

func (j *baseTrackedJob) storedInfo() *compactorschedulerpb.StoredJobInfo {
	return &compactorschedulerpb.StoredJobInfo{
		CreationTime: j.creationTime.Unix(),
		Status:       j.status,
		StatusTime:   j.statusTime.Unix(),
		NumLeases:    int32(j.numLeases),
		Epoch:        j.epoch,
	}
}

func baseTrackedJobFromInfo(id string, info *compactorschedulerpb.StoredJobInfo) baseTrackedJob {
	return baseTrackedJob{
		id:           id,
		creationTime: time.Unix(info.CreationTime, 0),
		status:       info.Status,
		statusTime:   time.Unix(info.StatusTime, 0),
		numLeases:    int(info.NumLeases),
		epoch:        info.Epoch,
	}
}

type CompactionJob struct {
	blocks  [][]byte
	isSplit bool
}

type TrackedCompactionJob struct {
	baseTrackedJob
	value           *CompactionJob
	order           uint32
	totalBlockBytes uint64
}

func NewTrackedCompactionJob(id string, value *CompactionJob, order uint32, totalBlockBytes uint64, creationTime time.Time) *TrackedCompactionJob {
	return &TrackedCompactionJob{
		baseTrackedJob:  newBaseTrackedJob(id, creationTime),
		value:           value,
		order:           order,
		totalBlockBytes: totalBlockBytes,
	}
}

func (j *TrackedCompactionJob) Type() compactorschedulerpb.JobType {
	return compactorschedulerpb.JOB_TYPE_COMPACTION
}

func (j *TrackedCompactionJob) CopyBase() TrackedJob {
	return &TrackedCompactionJob{
		baseTrackedJob:  j.baseTrackedJob,
		value:           j.value,
		order:           j.order,
		totalBlockBytes: j.totalBlockBytes,
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
			BlockIds:         j.value.blocks,
			Split:            j.value.isSplit,
			TotalBlocksBytes: j.totalBlockBytes,
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
				BlockIds:         j.value.blocks,
				Split:            j.value.isSplit,
				TotalBlocksBytes: j.totalBlockBytes,
			},
		},
	}
}

func (j *TrackedCompactionJob) Order() uint32 {
	return j.order
}

func (j *TrackedCompactionJob) Blocks() [][]byte {
	return j.value.blocks
}

type TrackedPlanJob struct {
	baseTrackedJob
}

func NewTrackedPlanJob(creationTime time.Time) *TrackedPlanJob {
	return &TrackedPlanJob{
		baseTrackedJob: newBaseTrackedJob(planJobId, creationTime),
	}
}

func (j *TrackedPlanJob) Type() compactorschedulerpb.JobType {
	return compactorschedulerpb.JOB_TYPE_PLANNING
}

func (j *TrackedPlanJob) CopyBase() TrackedJob {
	return &TrackedPlanJob{
		baseTrackedJob: j.baseTrackedJob,
	}
}

func (j *TrackedPlanJob) Serialize() ([]byte, error) {
	return j.storedInfo().Marshal()
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
	return 0
}

func (j *TrackedPlanJob) Blocks() [][]byte {
	return nil
}

func deserializePlanJob(content []byte) (*TrackedPlanJob, error) {
	var info compactorschedulerpb.StoredJobInfo
	if err := info.Unmarshal(content); err != nil {
		return nil, err
	}
	return &TrackedPlanJob{baseTrackedJob: baseTrackedJobFromInfo(planJobId, &info)}, nil
}

// TrackedBackfillCleanupJob deletes the data and markers of a backfill
type TrackedBackfillCleanupJob struct {
	baseTrackedJob
	backfillID string
}

func NewTrackedBackfillCleanupJob(backfillID string, creationTime time.Time) *TrackedBackfillCleanupJob {
	return &TrackedBackfillCleanupJob{
		baseTrackedJob: newBaseTrackedJob(backfillCleanupJobId, creationTime),
		backfillID:     backfillID,
	}
}

func (j *TrackedBackfillCleanupJob) Type() compactorschedulerpb.JobType {
	return compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP
}

func (j *TrackedBackfillCleanupJob) CopyBase() TrackedJob {
	return &TrackedBackfillCleanupJob{
		baseTrackedJob: j.baseTrackedJob,
		backfillID:     j.backfillID,
	}
}

func (j *TrackedBackfillCleanupJob) Serialize() ([]byte, error) {
	stored := &compactorschedulerpb.StoredBackfillCleanupJob{
		Info:       j.storedInfo(),
		BackfillId: j.backfillID,
	}
	return stored.Marshal()
}

func (j *TrackedBackfillCleanupJob) ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse {
	return &compactorschedulerpb.LeaseJobResponse{
		Key: &compactorschedulerpb.JobKey{
			Id:    backfillCleanupJobId,
			Epoch: j.epoch,
		},
		Spec: &compactorschedulerpb.JobSpec{
			Tenant:          tenant,
			JobType:         compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP,
			BackfillCleanup: &compactorschedulerpb.BackfillCleanupJob{BackfillId: j.backfillID},
		},
	}
}

func (j *TrackedBackfillCleanupJob) Order() uint32 {
	return 1
}

func (j *TrackedBackfillCleanupJob) Blocks() [][]byte {
	return nil
}

func deserializeBackfillCleanupJob(content []byte) (*TrackedBackfillCleanupJob, error) {
	var stored compactorschedulerpb.StoredBackfillCleanupJob
	if err := stored.Unmarshal(content); err != nil {
		return nil, err
	}
	if stored.Info == nil {
		return nil, errors.New("invalid backfill cleanup job can not be deserialized")
	}
	return &TrackedBackfillCleanupJob{
		baseTrackedJob: baseTrackedJobFromInfo(backfillCleanupJobId, stored.Info),
		backfillID:     stored.BackfillId,
	}, nil
}

// TrackedBackfillBlockJob validates or copies a single block of a backfill
type TrackedBackfillBlockJob struct {
	baseTrackedJob
	jobType    compactorschedulerpb.JobType // JOB_TYPE_BACKFILL_VALIDATE or JOB_TYPE_BACKFILL_COPY
	backfillID string
	block      []byte
	order      uint32
}

func NewTrackedBackfillBlockJob(jobType compactorschedulerpb.JobType, plannerID string, backfillID string, block []byte, order uint32, creationTime time.Time) *TrackedBackfillBlockJob {
	return &TrackedBackfillBlockJob{
		baseTrackedJob: newBaseTrackedJob(typedJobKey(blockJobKeyType(jobType), plannerID), creationTime),
		jobType:        jobType,
		backfillID:     backfillID,
		block:          block,
		order:          order,
	}
}

func (j *TrackedBackfillBlockJob) Type() compactorschedulerpb.JobType {
	return j.jobType
}

func (j *TrackedBackfillBlockJob) CopyBase() TrackedJob {
	return &TrackedBackfillBlockJob{
		baseTrackedJob: j.baseTrackedJob,
		jobType:        j.jobType,
		backfillID:     j.backfillID,
		block:          j.block,
		order:          j.order,
	}
}

func (j *TrackedBackfillBlockJob) Serialize() ([]byte, error) {
	stored := &compactorschedulerpb.StoredBackfillBlockJob{
		Info:  j.storedInfo(),
		Job:   j.blockJob(),
		Order: j.order,
	}
	return stored.Marshal()
}

func (j *TrackedBackfillBlockJob) ToLeaseResponse(tenant string) *compactorschedulerpb.LeaseJobResponse {
	return &compactorschedulerpb.LeaseJobResponse{
		Key: &compactorschedulerpb.JobKey{
			Id:    j.id,
			Epoch: j.epoch,
		},
		Spec: &compactorschedulerpb.JobSpec{
			Tenant:        tenant,
			JobType:       j.jobType,
			BackfillBlock: j.blockJob(),
		},
	}
}

func (j *TrackedBackfillBlockJob) Order() uint32 {
	return j.order
}

func (j *TrackedBackfillBlockJob) Blocks() [][]byte {
	return [][]byte{j.block}
}

func (j *TrackedBackfillBlockJob) blockJob() *compactorschedulerpb.BackfillBlockJob {
	return &compactorschedulerpb.BackfillBlockJob{
		BackfillId: j.backfillID,
		BlockId:    j.block,
	}
}

func deserializeBackfillBlockJob(k []byte, v []byte, jobType compactorschedulerpb.JobType) (*TrackedBackfillBlockJob, error) {
	var stored compactorschedulerpb.StoredBackfillBlockJob
	if err := stored.Unmarshal(v); err != nil {
		return nil, err
	}
	if stored.Info == nil || stored.Job == nil {
		return nil, errors.New("invalid backfill block job can not be deserialized")
	}
	return &TrackedBackfillBlockJob{
		baseTrackedJob: baseTrackedJobFromInfo(string(k), stored.Info),
		jobType:        jobType,
		backfillID:     stored.Job.BackfillId,
		block:          stored.Job.BlockId,
		order:          stored.Order,
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
		order:           stored.Order,
		totalBlockBytes: stored.Job.TotalBlocksBytes,
	}, nil
}
