// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"slices"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// lane is an in-memory identifier of pending work logically enqueued together
type lane uint8

const (
	lanePolicySimple   = "simple"
	lanePolicyBackfill = "backfill"
)

const (
	planLane lane = iota
	compactionLane
	backfillPhasePlanLane
	backfillCompactionLane
)

func (l lane) String() string {
	switch l {
	case planLane:
		return "plan"
	case compactionLane:
		return "compaction"
	case backfillPhasePlanLane:
		return "backfill_phase_plan"
	case backfillCompactionLane:
		return "backfill_compaction"
	}
	return ""
}

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	AllLanes() []lane                                                      // All possible lanes defined by this policy.
	CompactionLanes() []lane                                               // The lanes that carry compaction jobs.
	JobTypes() []compactorschedulerpb.JobType                              // The job types this policy assigns to lanes.
	LaneForJob(TrackedJob) lane                                            // The lane this job is assigned to. A job must always map to some lane.
	LaneForJobType(compactorschedulerpb.JobType) (lane, bool)              // The lane jobs of this type are assigned to, if the policy supports the type.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error) // The lanes this worker requested, or an error.
}

type LanePolicyConfig struct {
	Policy string `yaml:"policy" category:"experimental"`
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Policy, prefix+".policy", lanePolicySimple, fmt.Sprintf("The lane policy the compactor scheduler should use. Valid values: %s, %s. %s is required if and only if -compactor-scheduler.discovery-polls includes %s.", lanePolicySimple, lanePolicyBackfill, lanePolicyBackfill, discoveryPollBackfills))
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.Policy {
	case lanePolicySimple:
		return newSimpleLanePolicy(), nil
	case lanePolicyBackfill:
		return newBackfillLanePolicy(), nil
	default:
		return nil, fmt.Errorf("unrecognized lane policy: %s", cfg.Policy)
	}
}

// simpleLanePolicy assigns a lane per job type
type simpleLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
	jobTypes        []compactorschedulerpb.JobType
}

func newSimpleLanePolicy() lanePolicy {
	return &simpleLanePolicy{
		allLanes:        []lane{planLane, compactionLane},
		compactionLanes: []lane{compactionLane},
		jobTypes:        []compactorschedulerpb.JobType{compactorschedulerpb.JOB_TYPE_PLANNING, compactorschedulerpb.JOB_TYPE_COMPACTION},
	}
}

func (slp *simpleLanePolicy) LaneForJob(j TrackedJob) lane {
	if j.ID() == planJobId {
		return planLane
	}
	return compactionLane
}

func (slp *simpleLanePolicy) AllLanes() []lane {
	return slp.allLanes
}

func (slp *simpleLanePolicy) CompactionLanes() []lane {
	return slp.compactionLanes
}

func (slp *simpleLanePolicy) JobTypes() []compactorschedulerpb.JobType {
	return slp.jobTypes
}

func (slp *simpleLanePolicy) LaneForJobType(jobType compactorschedulerpb.JobType) (lane, bool) {
	switch jobType {
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		return planLane, true
	case compactorschedulerpb.JOB_TYPE_COMPACTION:
		return compactionLane, true
	}
	return 0, false
}

// requestedLanes maps a lease request to scheduler lanes
func (slp *simpleLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	return lanesForRequest(req, slp.allLanes, slp.LaneForJobType)
}

// backfillLanePolicy is simpleLanePolicy with lanes for backfill phase plan jobs and backfill compaction jobs
type backfillLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
	jobTypes        []compactorschedulerpb.JobType
}

func newBackfillLanePolicy() lanePolicy {
	return &backfillLanePolicy{
		allLanes:        []lane{planLane, compactionLane, backfillPhasePlanLane, backfillCompactionLane},
		compactionLanes: []lane{compactionLane, backfillCompactionLane},
		jobTypes: []compactorschedulerpb.JobType{
			compactorschedulerpb.JOB_TYPE_PLANNING,
			compactorschedulerpb.JOB_TYPE_COMPACTION,
			compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING,
			compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION,
		},
	}
}

func (blp *backfillLanePolicy) LaneForJob(j TrackedJob) lane {
	switch j.Type() {
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		return planLane
	case compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING:
		return backfillPhasePlanLane
	case compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION:
		return backfillCompactionLane
	}
	return compactionLane
}

func (blp *backfillLanePolicy) AllLanes() []lane {
	return blp.allLanes
}

func (blp *backfillLanePolicy) CompactionLanes() []lane {
	return blp.compactionLanes
}

func (blp *backfillLanePolicy) JobTypes() []compactorschedulerpb.JobType {
	return blp.jobTypes
}

func (blp *backfillLanePolicy) LaneForJobType(jobType compactorschedulerpb.JobType) (lane, bool) {
	switch jobType {
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		return planLane, true
	case compactorschedulerpb.JOB_TYPE_COMPACTION:
		return compactionLane, true
	case compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING:
		return backfillPhasePlanLane, true
	case compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION:
		return backfillCompactionLane, true
	}
	return 0, false
}

func (blp *backfillLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	return lanesForRequest(req, blp.allLanes, blp.LaneForJobType)
}

// lanesForRequest validates the lanes of a lease request, using laneForJobType to map each requested job type to a lane
func lanesForRequest(req *compactorschedulerpb.LeaseJobRequest, allLanes []lane, laneForJobType func(compactorschedulerpb.JobType) (lane, bool)) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return allLanes, nil
	}
	if numLanes > len(allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(allLanes), numLanes)
	}

	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		l, ok := laneForJobType(ln.JobType)
		if !ok {
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		if slices.Contains(lanes, l) {
			return nil, fmt.Errorf("duplicate lane in request: %q", ln.JobType.String())
		}
		lanes = append(lanes, l)
	}
	return lanes, nil
}
