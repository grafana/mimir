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

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	AllLanes() []lane                                                      // All possible lanes defined by this policy.
	CompactionLanes() []lane                                               // The lanes that carry compaction jobs.
	JobTypes() []compactorschedulerpb.JobType                              // The job types this policy assigns to lanes.
	LaneForJob(TrackedJob) lane                                            // The lane this job is assigned to. The job's type must be supported by the policy.
	LanesForJobType(compactorschedulerpb.JobType) ([]lane, bool)           // The lanes jobs of this type are assigned to, if the policy supports the type.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error) // The lanes this worker requested, or an error.
	PlannedLanes(planner compactorschedulerpb.JobType) []lane              // The lanes holding only jobs produced by this plan job type. Empty if it plans nothing under this policy.
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

// laneAssignment assigns all jobs of a type to a set of lanes.
type laneAssignment struct {
	jobType   compactorschedulerpb.JobType
	lanes     []lane
	plannedBy compactorschedulerpb.JobType // the plan job type producing jobs of this type, if any
}

// staticLanePolicy assigns each supported job type a fixed set of lanes. Assignment order is the default lease order.
type staticLanePolicy struct {
	assignments     []laneAssignment
	allLanes        []lane
	compactionLanes []lane
	jobTypes        []compactorschedulerpb.JobType
	plannedLanes    map[compactorschedulerpb.JobType][]lane // the lanes produced by each plan job type
}

func newStaticLanePolicy(compactionLanes []lane, assignments ...laneAssignment) *staticLanePolicy {
	p := &staticLanePolicy{assignments: assignments, compactionLanes: compactionLanes, plannedLanes: make(map[compactorschedulerpb.JobType][]lane)}
	// Planning replaces the contents of its planned lanes, so all jobs in a lane must share a planner
	lanePlanners := make(map[lane]compactorschedulerpb.JobType)
	for _, a := range assignments {
		for _, l := range a.lanes {
			planner, seen := lanePlanners[l]
			if !seen {
				lanePlanners[l] = a.plannedBy
				p.allLanes = append(p.allLanes, l)
				// Jobs the scheduler generates itself have no planner
				if a.plannedBy != compactorschedulerpb.JOB_TYPE_UNKNOWN {
					p.plannedLanes[a.plannedBy] = append(p.plannedLanes[a.plannedBy], l)
				}
			} else if planner != a.plannedBy {
				panic(fmt.Sprintf("lane %s holds jobs from more than one planner", l))
			}
		}
		p.jobTypes = append(p.jobTypes, a.jobType)
	}
	return p
}

func newSimpleLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_PLANNING, lanes: []lane{planLane}},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_COMPACTION, lanes: []lane{compactionLane}, plannedBy: compactorschedulerpb.JOB_TYPE_PLANNING},
	)
}

func newBackfillLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane, backfillCompactionLane},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_PLANNING, lanes: []lane{planLane}},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_COMPACTION, lanes: []lane{compactionLane}, plannedBy: compactorschedulerpb.JOB_TYPE_PLANNING},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING, lanes: []lane{backfillPhasePlanLane}},
		laneAssignment{jobType: compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION, lanes: []lane{backfillCompactionLane}, plannedBy: compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING},
	)
}

func (p *staticLanePolicy) LaneForJob(j TrackedJob) lane {
	lanes, ok := p.LanesForJobType(j.Type())
	if !ok {
		panic(fmt.Sprintf("lane policy does not support job type %s", j.Type()))
	}
	// Currently jobs assume they are assigned to the first lane (since there is only one lane currently, but this may change later)
	return lanes[0]
}

func (p *staticLanePolicy) LanesForJobType(jobType compactorschedulerpb.JobType) ([]lane, bool) {
	for _, a := range p.assignments {
		if a.jobType == jobType {
			return a.lanes, true
		}
	}
	return nil, false
}

func (p *staticLanePolicy) PlannedLanes(planner compactorschedulerpb.JobType) []lane {
	return p.plannedLanes[planner]
}

func (p *staticLanePolicy) AllLanes() []lane {
	return p.allLanes
}

func (p *staticLanePolicy) CompactionLanes() []lane {
	return p.compactionLanes
}

func (p *staticLanePolicy) JobTypes() []compactorschedulerpb.JobType {
	return p.jobTypes
}

func (p *staticLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied which means all lanes
		return p.allLanes, nil
	}
	if numLanes > len(p.jobTypes) {
		return nil, fmt.Errorf("at most %d job types supported, provided %d", len(p.jobTypes), numLanes)
	}

	jobTypes := make([]compactorschedulerpb.JobType, 0, numLanes)
	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		if slices.Contains(jobTypes, ln.JobType) {
			return nil, fmt.Errorf("duplicate job type in lane request: %q", ln.JobType.String())
		}
		jobTypes = append(jobTypes, ln.JobType)
		jobTypeLanes, ok := p.LanesForJobType(ln.JobType)
		if !ok {
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		// Job types may share lanes, so each lane is only requested once
		for _, l := range jobTypeLanes {
			if !slices.Contains(lanes, l) {
				lanes = append(lanes, l)
			}
		}
	}
	return lanes, nil
}
