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

// laneAssignment assigns all jobs of a type to a lane
type laneAssignment struct {
	jobType compactorschedulerpb.JobType
	lane    lane
}

// staticLanePolicy assigns each supported job type its own lane. Assignment order is the default lease order.
type staticLanePolicy struct {
	assignments     []laneAssignment
	allLanes        []lane
	compactionLanes []lane
	jobTypes        []compactorschedulerpb.JobType
}

func newStaticLanePolicy(compactionLanes []lane, assignments ...laneAssignment) *staticLanePolicy {
	p := &staticLanePolicy{assignments: assignments, compactionLanes: compactionLanes}
	for _, a := range assignments {
		p.allLanes = append(p.allLanes, a.lane)
		p.jobTypes = append(p.jobTypes, a.jobType)
	}
	return p
}

func newSimpleLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_PLANNING, planLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_COMPACTION, compactionLane},
	)
}

func newBackfillLanePolicy() lanePolicy {
	return newStaticLanePolicy(
		[]lane{compactionLane, backfillCompactionLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_PLANNING, planLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_COMPACTION, compactionLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING, backfillPhasePlanLane},
		laneAssignment{compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION, backfillCompactionLane},
	)
}

func (p *staticLanePolicy) LaneForJob(j TrackedJob) lane {
	if l, ok := p.LaneForJobType(j.Type()); ok {
		return l
	}
	return compactionLane
}

func (p *staticLanePolicy) LaneForJobType(jobType compactorschedulerpb.JobType) (lane, bool) {
	for _, a := range p.assignments {
		if a.jobType == jobType {
			return a.lane, true
		}
	}
	return 0, false
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
		l, ok := p.LaneForJobType(ln.JobType)
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
