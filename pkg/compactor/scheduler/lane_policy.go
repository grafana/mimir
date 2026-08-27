// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"slices"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// lane is an in-memory identifier of pending work logically enqueued together. Its value is
// exported as a metric label, so renaming one changes existing metrics.
type lane string

const (
	lanePolicySimple = "simple"

	planLane       lane = "plan"
	compactionLane lane = "compaction"
)

type laneTransition struct {
	lane lane
	kind rotationTransition
}

// Defines how to map jobs and requests into lanes
type lanePolicy interface {
	AllLanes() []lane                                                      // All possible lanes defined by this policy.
	CompactionLanes() []lane                                               // The lanes that carry compaction jobs.
	LaneForJob(TrackedJob) lane                                            // The lane this job is assigned to. A job must always map to some lane.
	LanesForRequest(*compactorschedulerpb.LeaseJobRequest) ([]lane, error) // The lanes this worker requested, or an error.
}

type LanePolicyConfig struct {
	Policy string `yaml:"policy" category:"experimental"`
}

func (cfg *LanePolicyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Policy, prefix+".policy", "simple", "The lane policy the compactor scheduler should use. Valid values: "+lanePolicySimple)
}

func newLanePolicy(cfg LanePolicyConfig) (lanePolicy, error) {
	switch cfg.Policy {
	case "simple":
		return newSimpleLanePolicy(), nil
	default:
		return nil, fmt.Errorf("unrecognized lane policy: %s", cfg.Policy)
	}
}

// simpleLanePolicy assigns a lane per job type
type simpleLanePolicy struct {
	allLanes        []lane
	compactionLanes []lane
}

func newSimpleLanePolicy() lanePolicy {
	return &simpleLanePolicy{
		allLanes:        []lane{planLane, compactionLane},
		compactionLanes: []lane{compactionLane},
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

// requestedLanes maps a lease request to scheduler lanes
func (slp *simpleLanePolicy) LanesForRequest(req *compactorschedulerpb.LeaseJobRequest) ([]lane, error) {
	numLanes := len(req.LaneRequests)
	if numLanes == 0 {
		// No lanes supplied, provide a default
		return slp.AllLanes(), nil
	}
	if numLanes > len(slp.allLanes) {
		return nil, fmt.Errorf("at most %d lanes supported, provided %d", len(slp.allLanes), numLanes)
	}

	lanes := make([]lane, 0, numLanes)
	for _, ln := range req.LaneRequests {
		var l lane
		switch ln.JobType {
		case compactorschedulerpb.JOB_TYPE_PLANNING:
			l = planLane
		case compactorschedulerpb.JOB_TYPE_COMPACTION:
			l = compactionLane
		default:
			return nil, fmt.Errorf("unknown job type in lane request: %q", ln.JobType.String())
		}
		if slices.Contains(lanes, l) {
			return nil, fmt.Errorf("duplicate lane in request: %q", ln.JobType.String())
		}
		lanes = append(lanes, l)
	}
	return lanes, nil
}
