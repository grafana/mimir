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
	jobType compactorschedulerpb.JobType
	lanes   []lane
}

// staticLanePolicy assigns each supported job type its own lanes. Assignment order is the default lease order.
type staticLanePolicy struct {
	assignments     []laneAssignment
	allLanes        []lane
	compactionLanes []lane
	jobTypes        []compactorschedulerpb.JobType
}

func newStaticLanePolicy(compactionLanes []lane, assignments ...laneAssignment) *staticLanePolicy {
	p := &staticLanePolicy{assignments: assignments, compactionLanes: compactionLanes}
	for _, a := range assignments {
		p.allLanes = append(p.allLanes, a.lanes...)
		p.jobTypes = append(p.jobTypes, a.jobType)
	}
	return p
}

func newSimpleLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_PLANNING, []lane{planLane}},
		laneAssignment{compactorschedulerpb.JOB_TYPE_COMPACTION, []lane{compactionLane}},
	)
}

func newBackfillLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane, backfillCompactionLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_PLANNING, []lane{planLane}},
		laneAssignment{compactorschedulerpb.JOB_TYPE_COMPACTION, []lane{compactionLane}},
		laneAssignment{compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING, []lane{backfillPhasePlanLane}},
		laneAssignment{compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION, []lane{backfillCompactionLane}},
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
	if numLanes > len(p.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(p.allLanes), numLanes)
	}

	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		jobTypeLanes, ok := p.LanesForJobType(ln.JobType)
		if !ok {
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		for _, l := range jobTypeLanes {
			if slices.Contains(lanes, l) {
				return nil, fmt.Errorf("duplicate lane in request: %q", ln.JobType.String())
			}
			lanes = append(lanes, l)
		}
	}
	return lanes, nil
}
